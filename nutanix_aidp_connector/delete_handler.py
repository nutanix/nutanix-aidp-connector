#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Delete handler for removing records from Milvus when S3 objects are deleted.
Uses batched deletes with a persistent connection to minimise Milvus overhead.
"""

import logging
import queue
import time
from typing import Optional, Dict, List
from threading import Thread, Event
from urllib.parse import unquote_plus

logger = logging.getLogger(__name__)

try:
    from pymilvus import connections, Collection
    DELETE_HANDLER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"pymilvus not available: {e}")
    DELETE_HANDLER_AVAILABLE = False

BATCH_MAX_FILES = 50
BATCH_MAX_WAIT_SECONDS = 5.0


class DeleteHandler:
    """Handle S3 object delete events by removing records from Milvus.

    Drains the delete queue in batches (up to BATCH_MAX_FILES or
    BATCH_MAX_WAIT_SECONDS), queries PKs for all files in one pass,
    issues a single combined ``collection.delete()`` and one ``flush()``
    per batch.
    """

    def __init__(self, config_manager=None):
        self.config_manager = config_manager
        self.running = False
        self.delete_queue = None
        self.delete_thread = None
        self.stop_event = Event()

        self._connection_alias = "delete_handler"
        self._connected = False
        self._collection_cache: Dict[str, 'Collection'] = {}

        logger.info("DeleteHandler initialized")

    def set_delete_queue(self, delete_queue):
        self.delete_queue = delete_queue

    # ------------------------------------------------------------------
    # Milvus connection helpers (connect once, reuse)
    # ------------------------------------------------------------------

    def _ensure_connection(self, milvus_host: str, milvus_port: int):
        if self._connected:
            return
        try:
            connections.disconnect(self._connection_alias)
        except Exception:
            pass
        connections.connect(alias=self._connection_alias, host=milvus_host, port=milvus_port)
        self._connected = True
        self._collection_cache.clear()

    def _get_collection(self, collection_name: str) -> 'Collection':
        if collection_name in self._collection_cache:
            return self._collection_cache[collection_name]
        col = Collection(collection_name, using=self._connection_alias)
        try:
            col.load()
        except Exception as e:
            err = str(e).lower()
            if "already loaded" not in err and "already in memory" not in err:
                logger.warning(f"Could not load collection '{collection_name}': {e}")
        self._collection_cache[collection_name] = col
        return col

    def _disconnect(self):
        if self._connected:
            try:
                connections.disconnect(self._connection_alias)
            except Exception:
                pass
            self._connected = False
            self._collection_cache.clear()

    # ------------------------------------------------------------------
    # PK query (unchanged logic, but operates on a cached collection)
    # ------------------------------------------------------------------

    def _query_pks_by_source(self, collection, source_name: str) -> List:
        # Milvus like: escape only % (wildcard) and " (string delimiter).
        # Do NOT escape _ — Milvus does not support \_ and will throw a parse error.
        # Unescaped _ acts as single-char wildcard but still matches literal _ in practice.
        escaped_like = source_name.replace('"', '\\"').replace('%', '\\%')
        escaped_eq = source_name.replace('"', '\\"')
        for filter_expr, desc in [
            (f'source["source_name"] like "{escaped_like}%"', "prefix"),
            (f'source["source_name"] == "{escaped_eq}"', "exact"),
        ]:
            try:
                # consistency_level='Strong' ensures we see data from recent bulk inserts
                # that completed on a different connection (e.g. MilvusBatchInserter)
                res = collection.query(
                    expr=filter_expr, output_fields=["pk"], limit=16384,
                    consistency_level="Strong"
                )
                if res and len(res) > 0:
                    return [r["pk"] for r in res]
            except Exception as e:
                logger.warning(f"Delete: query with {desc} failed: {e}")
        return []

    # ------------------------------------------------------------------
    # Batched delete core
    # ------------------------------------------------------------------

    def _process_batch(self, events: List[Dict]):
        """Process a batch of delete events with a single flush."""
        if not events:
            return

        if not DELETE_HANDLER_AVAILABLE:
            logger.warning("Delete handler not available - pymilvus not installed")
            return

        storage_config = self.config_manager.get_storage_config()
        if not storage_config or not storage_config.milvus_enabled:
            logger.debug("Milvus is disabled, skipping delete batch")
            return

        collection_name = storage_config.milvus_collection
        milvus_host = storage_config.milvus_host
        milvus_port = storage_config.milvus_port

        try:
            self._ensure_connection(milvus_host, milvus_port)
        except Exception as e:
            logger.error(f"Failed to connect to Milvus for delete batch: {e}")
            self._connected = False
            return

        # --- Primary collection ---
        all_pks: List = []
        file_pk_counts: Dict[str, int] = {}
        collection = self._get_collection(collection_name)

        for ev in events:
            s3_key_decoded = unquote_plus(ev['s3_key'])
            if ev.get('s3_version'):
                source_name = f"{ev['s3_bucket']}/{s3_key_decoded}/{ev['s3_version']}"
            else:
                source_name = f"{ev['s3_bucket']}/{s3_key_decoded}"
            ev['_source_name'] = source_name

            pks = self._query_pks_by_source(collection, source_name)
            file_pk_counts[source_name] = len(pks)
            all_pks.extend(pks)

        batch_delete_count = 0
        if all_pks:
            pk_expr = f"pk in {all_pks}"
            logger.info(f"Batch delete: removing {len(all_pks)} records across {len(events)} files from '{collection_name}'")
            try:
                collection.delete(expr=pk_expr)
                batch_delete_count = len(all_pks)
            except Exception as e:
                logger.error(f"Batch delete failed on '{collection_name}': {e}")
                self._disconnect()
                return

        # Single flush for the whole batch
        try:
            collection.flush()
        except Exception as e:
            logger.warning(f"Flush after batch delete failed: {e}")
            self._disconnect()

        # --- Filtered collections ---
        filtered_pks_by_col: Dict[str, List] = {}
        if storage_config.milvus_enable_filtered_collections:
            filtered_names = []
            for attr in ('milvus_filtered_text_collection_name',
                         'milvus_filtered_tables_collection_name',
                         'milvus_filtered_charts_collection_name'):
                name = getattr(storage_config, attr, None)
                if name and name.strip():
                    filtered_names.append(name.strip())

            for fc_name in filtered_names:
                try:
                    fc = self._get_collection(fc_name)
                    fc_pks: List = []
                    for ev in events:
                        pks = self._query_pks_by_source(fc, ev['_source_name'])
                        fc_pks.extend(pks)
                    if fc_pks:
                        fc.delete(expr=f"pk in {fc_pks}")
                        fc.flush()
                        filtered_pks_by_col[fc_name] = len(fc_pks)
                        logger.info(f"Batch delete: removed {len(fc_pks)} records from filtered '{fc_name}'")
                except Exception as e:
                    logger.warning(f"Error batch-deleting from filtered collection '{fc_name}': {e}")

        # --- Per-file logging ---
        for ev in events:
            src = ev['_source_name']
            cnt = file_pk_counts.get(src, 0)
            path = f"s3://{ev['s3_bucket']}/{ev['s3_key']}"
            if cnt > 0:
                logger.info(f"Successfully removed {cnt} records for {path} (source_name: {src}) from collection {collection_name}")
            else:
                logger.info(f"No records found to remove for {path} (source_name: {src})")

        logger.info(f"Batch delete complete: {len(events)} files, {batch_delete_count} primary records, "
                     f"{sum(filtered_pks_by_col.values())} filtered records")

    # ------------------------------------------------------------------
    # Worker loop — drains queue into batches
    # ------------------------------------------------------------------

    def delete_worker(self):
        logger.info("Delete worker started (batched mode)")

        while not self.stop_event.is_set():
            try:
                if self.delete_queue is None:
                    time.sleep(1)
                    continue

                batch: List[Dict] = []
                deadline = time.monotonic() + BATCH_MAX_WAIT_SECONDS

                # Drain up to BATCH_MAX_FILES or until deadline
                while len(batch) < BATCH_MAX_FILES:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        ev = self.delete_queue.get(timeout=min(remaining, 1.0))
                    except queue.Empty:
                        if self.stop_event.is_set():
                            break
                        continue
                    except Exception as e:
                        logger.warning(f"Unexpected error reading delete queue: {e}")
                        if self.stop_event.is_set():
                            break
                        continue
                    if ev is None:
                        continue
                    if not ev.get('s3_bucket') or not ev.get('s3_key'):
                        logger.warning(f"Invalid delete event (missing bucket/key): {ev}")
                        continue
                    batch.append(ev)

                if batch:
                    logger.info(f"Delete worker: processing batch of {len(batch)} files")
                    t0 = time.perf_counter()
                    self._process_batch(batch)
                    logger.info(f"Delete batch took {time.perf_counter() - t0:.2f}s for {len(batch)} files")

            except Exception as e:
                logger.error(f"Error in delete worker: {e}", exc_info=True)

        self._disconnect()
        logger.info("Delete worker stopped")

    # ------------------------------------------------------------------
    # Public handle_delete (kept for direct / non-queued callers)
    # ------------------------------------------------------------------

    def handle_delete(self, s3_bucket: str, s3_key: str, s3_version: str = None) -> Dict:
        ev = {'s3_bucket': s3_bucket, 's3_key': s3_key, 's3_version': s3_version}
        self._process_batch([ev])
        src = ev.get('_source_name', f"{s3_bucket}/{s3_key}")
        return {'success': True, 'source_name': src, 'collection_name': '', 'delete_count': 0}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        if self.running:
            logger.warning("Delete handler already running")
            return
        self.running = True
        self.stop_event.clear()
        self.delete_thread = Thread(target=self.delete_worker, daemon=True)
        self.delete_thread.start()
        logger.info("Delete handler started")

    def stop(self, timeout: int = 30):
        if not self.running:
            logger.warning("Delete handler not running")
            return
        logger.info("Stopping delete handler...")
        self.stop_event.set()
        if self.delete_thread:
            self.delete_thread.join(timeout=timeout)
        self._disconnect()
        self.running = False
        logger.info("Delete handler stopped")

