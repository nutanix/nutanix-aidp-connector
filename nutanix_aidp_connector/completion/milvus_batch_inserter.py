#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Milvus Batch Inserter - accumulates enriched parquet data from multiple files
and performs a single combined bulk insert to amortize the per-insert overhead.

The Milvus bulk insert wait (~15s) dominates the completion handler.  By batching
N files into one combined parquet we pay that cost once instead of N times.
"""

import gc
import logging
import os
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import boto3
    from botocore.exceptions import ClientError
    from pymilvus import BulkInsertState, connections, utility
    from pymilvus.orm.types import CONSISTENCY_BOUNDED
    from pymilvus.exceptions import MilvusException
    BULK_INSERT_AVAILABLE = True
except ImportError:
    BULK_INSERT_AVAILABLE = False
    MilvusException = None  # type: ignore

try:
    from utils.telemetry import get_metrics
    TELEMETRY_AVAILABLE = True
except ImportError:
    TELEMETRY_AVAILABLE = False

CONSISTENCY = CONSISTENCY_BOUNDED
MILVUS_JOB_LIMIT_ERROR_CODE = 2100
BULK_INSERT_JOB_LIMIT_RETRIES = 5
BULK_INSERT_JOB_LIMIT_BACKOFF_SEC = 5


@dataclass
class MilvusInsertItem:
    """A single file's enriched parquet data ready for Milvus insertion.

    Holds the merged DataFrame in memory so the caller can clean up
    temp files immediately after filtered routing without blocking the batcher.

    ``on_flush_complete`` callbacks run only after the combined bulk insert
    succeeds.  The completion handler uses these to defer S3 cleanup and
    baseline notification until the data is safely in Milvus.
    """
    dataframe: Any  # pandas DataFrame
    total_rows: int
    batch_id: str
    job_id: str
    file_id: str
    on_flush_complete: Optional[List[Any]] = None  # list of zero-arg callables
    enqueued_at: float = field(default_factory=time.time)


class MilvusBatchInserter:
    """
    Accumulates per-file enriched parquets and flushes them as a single combined
    parquet to Milvus via ``do_bulk_insert``.

    Flush triggers (whichever comes first):
      - ``max_files`` items accumulated
      - ``max_wait_seconds`` elapsed since the first item in the current batch

    A dedicated worker thread monitors the time-based trigger.  Items are also
    checked on every ``add()`` call for count-based flushing.
    """

    def __init__(
        self,
        config_manager,
        max_files: int = 10,
        max_wait_seconds: float = 15.0,
    ):
        self.config_manager = config_manager
        self.max_files = max(1, max_files)
        self.max_wait_seconds = max(1.0, max_wait_seconds)

        self._batch: List[MilvusInsertItem] = []
        self._lock = threading.Lock()
        self._flush_event = threading.Event()
        self._running = False
        self._worker: Optional[threading.Thread] = None
        self._batch_start_time: Optional[float] = None
        self._s3_client = None
        self._s3_client_lock = threading.Lock()

        logger.info(
            "MilvusBatchInserter initialized: max_files=%d, max_wait_seconds=%.1f",
            self.max_files, self.max_wait_seconds,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        if self._running:
            return
        self._running = True
        self._worker = threading.Thread(
            target=self._timer_loop, daemon=True, name="milvus-batch-timer"
        )
        self._worker.start()
        logger.info("MilvusBatchInserter started")

    def stop(self):
        logger.info("MilvusBatchInserter stopping...")
        self._running = False
        self._flush_event.set()
        if self._worker and self._worker.is_alive():
            self._worker.join(timeout=120)
        # Final flush for any remaining items
        items = []
        with self._lock:
            if self._batch:
                items = list(self._batch)
                self._batch.clear()
                self._batch_start_time = None
        if items:
            logger.info("Flushing %d remaining item(s) on shutdown", len(items))
            self._do_flush(items)
        logger.info("MilvusBatchInserter stopped")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, item: MilvusInsertItem):
        """
        Enqueue an enriched parquet item.  If the batch reaches ``max_files``
        the batch is flushed synchronously on the caller's thread so that
        back-pressure is applied to completion workers.
        """
        flush_items = None
        with self._lock:
            self._batch.append(item)
            if self._batch_start_time is None:
                self._batch_start_time = time.time()

            if len(self._batch) >= self.max_files:
                flush_items = list(self._batch)
                self._batch.clear()
                self._batch_start_time = None

        if flush_items:
            self._do_flush(flush_items)
        else:
            self._flush_event.set()

    # ------------------------------------------------------------------
    # Timer thread
    # ------------------------------------------------------------------

    def _timer_loop(self):
        while self._running:
            self._flush_event.wait(timeout=1.0)
            self._flush_event.clear()

            flush_items = None
            with self._lock:
                if self._batch and self._batch_start_time is not None:
                    elapsed = time.time() - self._batch_start_time
                    if elapsed >= self.max_wait_seconds:
                        flush_items = list(self._batch)
                        self._batch.clear()
                        self._batch_start_time = None

            if flush_items:
                self._do_flush(flush_items)

    # ------------------------------------------------------------------
    # Flush implementation
    # ------------------------------------------------------------------

    def _get_s3_client(self):
        """Lazily create and cache a boto3 S3 client from storage config."""
        with self._s3_client_lock:
            if self._s3_client is not None:
                return self._s3_client

            storage_config = self.config_manager.get_storage_config()
            objects_endpoint = storage_config.objects_endpoint
            if "://" in objects_endpoint:
                objects_endpoint = objects_endpoint.split("://")[1]

            if ":" in objects_endpoint:
                host, port = objects_endpoint.split(":", 1)
                protocol = "https" if int(port) == 443 else "http"
            else:
                objects_endpoint = f"{objects_endpoint}:80"
                protocol = "http"

            endpoint_url = f"{protocol}://{objects_endpoint}"
            access_key = storage_config.objects_access_key or os.getenv("OBJECTS_ACCESS_KEY", "")
            secret_key = storage_config.objects_secret_key or os.getenv("OBJECTS_SECRET_KEY", "")

            self._s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                verify=storage_config.objects_verify_ssl,
                config=boto3.session.Config(signature_version="s3v4"),
            )
            return self._s3_client

    def _do_flush(self, items: List[MilvusInsertItem]):
        """Combine DataFrames, upload, bulk-insert, wait, cleanup."""
        if not items:
            return

        t_start = time.perf_counter()
        total_rows = sum(it.total_rows for it in items)
        file_ids = [it.file_id for it in items]
        logger.info(
            "Flushing Milvus batch: %d file(s), %d total row(s), file_ids=%s",
            len(items), total_rows, file_ids[:5],
        )

        storage_config = self.config_manager.get_storage_config()
        if not storage_config or not storage_config.milvus_enabled:
            logger.debug("Milvus disabled, skipping batch flush")
            return

        milvus_bucket = storage_config.milvus_bucket
        collection_name = storage_config.milvus_collection
        s3_client = self._get_s3_client()

        stage_timings: Dict[str, float] = {}

        # 1. Combine all DataFrames into one parquet file
        t_combine = time.perf_counter()
        combined_path = None
        temp_dir = None
        try:
            dfs = [it.dataframe for it in items if it.dataframe is not None and len(it.dataframe) > 0]
            if not dfs:
                logger.error("No valid DataFrames in batch, skipping Milvus insert")
                return

            combined_df = pd.concat(dfs, ignore_index=True)
            del dfs
            actual_rows = len(combined_df)
            temp_dir = tempfile.mkdtemp(prefix="milvus_batch_")
            combined_filename = f"batch_{uuid.uuid4().hex[:12]}.parquet"
            combined_path = os.path.join(temp_dir, combined_filename)
            combined_df.to_parquet(combined_path, index=False)
            del combined_df
            # Release item DataFrames now that they are combined
            for it in items:
                it.dataframe = None
            gc.collect()
            stage_timings["combine_sec"] = round(time.perf_counter() - t_combine, 3)
            logger.info("Combined %d file(s) into %s (%d rows)", len(items), combined_filename, actual_rows)
        except Exception as e:
            logger.error("Error combining DataFrames for batch flush: %s", e)
            if temp_dir and os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            return

        # 2. Upload combined parquet to Milvus S3 bucket
        combined_key = f"milvus_batch/{combined_filename}"
        t_upload = time.perf_counter()
        try:
            s3_client.upload_file(combined_path, milvus_bucket, combined_key)
            s3_client.head_object(Bucket=milvus_bucket, Key=combined_key)
            stage_timings["upload_milvus_sec"] = round(time.perf_counter() - t_upload, 3)
        except Exception as e:
            logger.error("Failed to upload combined parquet to %s/%s: %s", milvus_bucket, combined_key, e)
            if temp_dir and os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            return

        # 3. Connect to Milvus and call do_bulk_insert
        milvus_uri = f"http://{storage_config.milvus_host}:{storage_config.milvus_port}"
        try:
            connections.connect(uri=milvus_uri, alias="default")
        except Exception:
            try:
                connections.disconnect("default")
            except Exception:
                pass
            connections.connect(uri=milvus_uri, alias="default")

        task_id = None
        t_bulk_call = time.perf_counter()
        for attempt in range(BULK_INSERT_JOB_LIMIT_RETRIES):
            try:
                logger.info(
                    "Triggering batched bulk insert (%d files, %d rows)%s",
                    len(items), actual_rows,
                    f" (retry {attempt+1}/{BULK_INSERT_JOB_LIMIT_RETRIES})" if attempt > 0 else "",
                )
                task_id = utility.do_bulk_insert(
                    collection_name=collection_name,
                    files=[combined_key],
                    consistency_level=CONSISTENCY,
                )
                logger.info("Created bulk insert task %s for batch (%d files)", task_id, len(items))
                stage_timings["bulk_insert_call_sec"] = round(time.perf_counter() - t_bulk_call, 3)
                if TELEMETRY_AVAILABLE:
                    metrics = get_metrics()
                    if metrics and metrics.enabled:
                        metrics.record_milvus_insert(collection=collection_name)
                break
            except Exception as e:
                is_job_limit = (
                    (MilvusException is not None and isinstance(e, MilvusException) and getattr(e, "code", None) == MILVUS_JOB_LIMIT_ERROR_CODE)
                    or "number of jobs" in str(e).lower()
                    or "jobs has reached the limit" in str(e).lower()
                )
                if is_job_limit and attempt < BULK_INSERT_JOB_LIMIT_RETRIES - 1:
                    backoff = BULK_INSERT_JOB_LIMIT_BACKOFF_SEC * (2 ** attempt)
                    logger.warning("Milvus bulk insert limit hit (attempt %d/%d), retrying in %ds: %s",
                                   attempt+1, BULK_INSERT_JOB_LIMIT_RETRIES, backoff, e)
                    time.sleep(backoff)
                else:
                    logger.error("Error creating batched bulk insert task: %s", e)
                    if TELEMETRY_AVAILABLE:
                        metrics = get_metrics()
                        if metrics and metrics.enabled:
                            metrics.record_milvus_insert_failure(collection=collection_name, error_type=type(e).__name__)
                    self._cleanup_milvus_file(s3_client, milvus_bucket, combined_key)
                    if temp_dir and os.path.exists(temp_dir):
                        import shutil
                        shutil.rmtree(temp_dir, ignore_errors=True)
                    # Still fire cleanup callbacks so NeMo-Retriever parquets don't leak in S3
                    for item in items:
                        if item.on_flush_complete:
                            for cb in item.on_flush_complete:
                                try:
                                    cb()
                                except Exception as cb_err:
                                    logger.warning("Post-flush callback failed for file_id=%s: %s", item.file_id, cb_err)
                    return

        # 4. Wait for bulk insert to complete
        t_wait = time.perf_counter()
        if task_id is not None:
            imported_rows = 0
            while True:
                time.sleep(1)
                try:
                    task = utility.get_bulk_insert_state(task_id=task_id)
                    if task.state_name == "Completed":
                        imported_rows = task.row_count
                        logger.info("Batched bulk insert task %s completed: %d rows imported", task_id, imported_rows)
                        break
                    elif task.state == BulkInsertState.ImportFailed:
                        logger.error("Batched bulk insert task %s failed: %s", task_id, task.failed_reason)
                        break
                except Exception as e:
                    logger.warning("Error checking bulk insert task %s: %s", task_id, e)
        stage_timings["wait_bulk_insert_sec"] = round(time.perf_counter() - t_wait, 3)

        total_sec = round(time.perf_counter() - t_start, 3)
        stage_timings_str = " ".join(f"{k}={v}" for k, v in sorted(stage_timings.items()))
        logger.info(
            "PIPELINE_LATENCY stage=milvus_batch_insert files=%d rows=%d total_sec=%s %s",
            len(items), actual_rows, total_sec, stage_timings_str,
        )
        if TELEMETRY_AVAILABLE:
            metrics = get_metrics()
            if metrics and metrics.enabled:
                for stage_key, duration in stage_timings.items():
                    metrics.record_pipeline_stage_duration(f"milvus_batch_{stage_key.replace('_sec','')}", duration)
                metrics.record_pipeline_stage_duration("milvus_batch_total", total_sec)

        # 5. Fire per-item post-flush callbacks (S3 cleanup, baseline notification)
        for item in items:
            if item.on_flush_complete:
                for cb in item.on_flush_complete:
                    try:
                        cb()
                    except Exception as cb_err:
                        logger.warning("Post-flush callback failed for file_id=%s: %s", item.file_id, cb_err)

        # 6. Cleanup
        self._cleanup_milvus_file(s3_client, milvus_bucket, combined_key)
        if temp_dir and os.path.exists(temp_dir):
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)

    # ------------------------------------------------------------------
    # Cleanup helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _cleanup_milvus_file(s3_client, bucket: str, key: str):
        try:
            s3_client.delete_object(Bucket=bucket, Key=key)
            logger.debug("Cleaned up Milvus batch file %s/%s", bucket, key)
        except Exception as e:
            logger.warning("Error cleaning up Milvus batch file %s/%s: %s", bucket, key, e)
