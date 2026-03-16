#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Completion handler for result storage and cleanup
"""

import os
import json
import logging
import re
import time
import tempfile
import gc
import shutil
import threading
from typing import Dict, Optional, List, Any
from pathlib import Path

logger = logging.getLogger(__name__)

# Optional pandas import for parquet filtering
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    logger.warning("pandas not available - filtered collection routing will be disabled")

logger = logging.getLogger(__name__)

# Import for bulk insert to Milvus
try:
    import boto3
    from botocore.exceptions import ClientError
    from pymilvus import BulkInsertState, connections, utility
    from pymilvus.orm.types import CONSISTENCY_BOUNDED
    from pymilvus.exceptions import MilvusException
    BULK_INSERT_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Bulk insert dependencies not available: {e}")
    BULK_INSERT_AVAILABLE = False
    MilvusException = None  # type: ignore

# Import telemetry for metrics
try:
    from utils.telemetry import get_metrics
    TELEMETRY_AVAILABLE = True
except ImportError:
    TELEMETRY_AVAILABLE = False

# Constants for bulk insert
CONSISTENCY = CONSISTENCY_BOUNDED
# Milvus error code: too many concurrent bulk-insert jobs (transient, retry with backoff)
MILVUS_JOB_LIMIT_ERROR_CODE = 2100
BULK_INSERT_JOB_LIMIT_RETRIES = 5
BULK_INSERT_JOB_LIMIT_BACKOFF_SEC = 5


class CompletionHandler:
    """Handle completed jobs: store results and cleanup files"""
    
    def __init__(self, completed_metadata_dir: str, dead_letter_dir: str,
                 kafka_consumer=None, config_manager=None,
                 on_baseline_file_complete=None):
        """
        Initialize completion handler
        
        Args:
            completed_metadata_dir: Directory to store completed metadata
            dead_letter_dir: Directory for dead letter queue
            kafka_consumer: Kafka consumer for offset commits
            config_manager: Config manager for accessing storage config
            on_baseline_file_complete: Callback for when baseline files complete
                                       Signature: (s3_bucket, s3_key, version_id) -> None
        """
        self.completed_metadata_dir = completed_metadata_dir
        self.dead_letter_dir = dead_letter_dir
        self.kafka_consumer = kafka_consumer
        self.config_manager = config_manager
        self.on_baseline_file_complete = on_baseline_file_complete
        self.milvus_batch_inserter = None
        
        # Create directories
        Path(completed_metadata_dir).mkdir(parents=True, exist_ok=True)
        Path(dead_letter_dir).mkdir(parents=True, exist_ok=True)
        
        # Processing state
        self.running = False
        
        # Track processed parquet files to avoid duplicate processing
        # Since each file has a unique search path (embeddings_{bucket}_{object_name}/),
        # different jobs won't find the same parquet files. However, this set protects against:
        # 1. Job retries: If a job fails and is retried with a new job_id, it would search
        #    the same path and find the same parquet files. This prevents reprocessing them.
        # 2. Edge cases: Defensive programming against any unexpected duplicate processing.
        # Note: This set may grow over time. Consider periodic cleanup if memory becomes an issue.
        self.processed_parquet_files = set()
        self.parquet_files_lock = threading.Lock()
        
        logger.info(f"CompletionHandler initialized: "
                   f"metadata_dir={completed_metadata_dir}, "
                   f"dead_letter_dir={dead_letter_dir}")
    
    def set_baseline_callback(self, callback):
        """
        Set the callback for baseline file completion.
        Called when baseline files are successfully processed.
        
        Args:
            callback: Function with signature (s3_bucket, s3_key, version_id) -> None
        """
        self.on_baseline_file_complete = callback
        logger.info("Baseline completion callback registered")
    
    def handle_completion(self, completion_data: Dict):
        """
        Handle a completed job
        
        Args:
            completion_data: Completion data dictionary
        """
        job_id = completion_data.get('job_id')
        batch_id = completion_data.get('batch_id')
        file_id = completion_data.get('file_id')
        file_detail = completion_data.get('file_detail', {})
        trace_id = completion_data.get('trace_id')  # Trace ID for debugging/correlation with NeMo-Retriever server logs
        result = completion_data.get('result')
        
        # FIRST: Check if result contains error indicators - if so, skip bulk insert immediately
        # This handles cases where NeMo-Retriever returned an error but status tracker incorrectly marked it as completed
        # We check this BEFORE counting rows to catch errors early
        has_processing_error = False
        error_message = None
        if result is not None:
            # Convert result to string for error checking (handles both string and other types)
            result_str = str(result) if not isinstance(result, str) else result
            result_lower = result_str.lower()
            
            # Check for error indicators
            error_indicators = [
                "error while processing",
                "failed to process",
                "embedding error",
                "hibernated endpoint",
                "milvusexception",
                "failed to store embeddings",
                "illegal vector data",
                ": failed",
                "failed\n"
            ]
            
            for indicator in error_indicators:
                if indicator in result_lower:
                    has_processing_error = True
                    error_message = f"Result contains error indicator '{indicator}': {result_str[:200]}"
                    logger.error(f"Job {job_id} failed with processing error detected in result: {error_message}")
                    break
        
        # Count rows in result data structure (if available)
        # Note: When using StoreEmbedTask, the result may not contain the actual data rows
        # because data is written to parquet files instead. The result might just be a completion status.
        # However, when using data_only=True in fetch_job_result, result IS the data list (like Ingestor.ingest())
        result_row_count = None
        result_data_structure = None
        if result is not None:
            try:
                # Log the structure of the result for debugging
                logger.debug(f"Job {job_id} result type: {type(result)}")
                
                # Check if result is a status message string (not actual data)
                # Status messages like "Job completed and cleaned up from client state" should not be counted as rows
                if isinstance(result, str):
                    # If result is a string, it's likely a status message, not data rows
                    logger.debug(f"Job {job_id} result is a string (likely status message): {result[:100]}")
                    result_data_structure = f"string status message (not data rows)"
                    result_row_count = 0  # Treat status messages as 0 rows
                elif isinstance(result, (list, tuple)):
                    result_row_count = len(result)
                    result_data_structure = f"list/tuple with {result_row_count} items"
                elif isinstance(result, dict):
                    # Check if result contains a DataFrame or list of records
                    if 'data' in result:
                        data = result['data']
                        if isinstance(data, (list, tuple)):
                            result_row_count = len(data)
                            result_data_structure = f"dict with 'data' key containing list/tuple of {result_row_count} items"
                            # Log sample of data structure
                            if len(data) > 0:
                                logger.debug(f"Job {job_id} result['data'][0] type: {type(data[0])}, sample keys: {list(data[0].keys()) if isinstance(data[0], dict) else 'N/A'}")
                        elif hasattr(data, '__len__'):
                            result_row_count = len(data)
                            result_data_structure = f"dict with 'data' key containing object with length {result_row_count}"
                    elif 'records' in result:
                        result_row_count = len(result['records'])
                        result_data_structure = f"dict with 'records' key containing {result_row_count} items"
                    elif 'rows' in result:
                        result_row_count = result['rows']
                        result_data_structure = f"dict with 'rows' key = {result_row_count}"
                    elif 'count' in result:
                        result_row_count = result['count']
                        result_data_structure = f"dict with 'count' key = {result_row_count}"
                    else:
                        # Result might just be a status/completion object, not the actual data
                        result_data_structure = f"dict with keys: {list(result.keys())} (no data/records/rows/count keys - likely just status)"
                        logger.debug(f"Job {job_id} result appears to be a status object, not data rows. This is expected when using StoreEmbedTask.")
                        result_row_count = 0  # Status objects have 0 data rows
                elif hasattr(result, '__len__') and not isinstance(result, str):
                    # Only count length for non-string objects (strings are handled above)
                    result_row_count = len(result)
                    result_data_structure = f"object with length {result_row_count}"
                
                if result_row_count is not None:
                    logger.info(f"Job {job_id} result contains {result_row_count} row(s) ({result_data_structure})")
                else:
                    logger.info(f"Job {job_id} result structure: {result_data_structure or type(result).__name__} (no row count available - this is normal when using StoreEmbedTask)")
            except Exception as e:
                logger.debug(f"Could not count rows in result: {e}")
                import traceback
                logger.debug(traceback.format_exc())
        
        logger.info(f"Processing completion for job {job_id} "
                   f"(file: {file_id}, batch_id: {batch_id} for tracking, trace_id: {trace_id}, result_rows: {result_row_count})")
        
        # has_processing_error and error_message are already set above (before row counting)
        
        try:
            # Save metadata for file
            self._save_file_metadata(file_id, job_id, result)
            
            # Trigger bulk insert from Objects to Milvus for this specific file
            # Each file has its own embeddings directory (embeddings_{bucket}_{object_name}/)
            # so we process each file individually when its job completes
            # Skip bulk insert if result_row_count is 0 (no content extracted/embedded) OR if there's a processing error
            if has_processing_error:
                logger.error(f"Skipping bulk insert for file {file_id} (job {job_id}): job failed with processing error. "
                           f"Error: {error_message} (trace_id: {trace_id})")
                # Handle as failure - move file to dead letter queue
                self.handle_failure(file_id, error_message or "Processing error detected in result", file_detail, trace_id=trace_id)
                # Clean up NeMo-Retriever parquet files left in objects bucket
                detail_file_id = file_detail.get('file_id', '') if file_detail else ''
                if detail_file_id:
                    self._cleanup_original_parquet_files([], batch_id, file_id=detail_file_id)
                return
            
            # Check if we have any results to process
            if result_row_count == 0:
                logger.info(f"Skipping bulk insert for file {file_id} (job {job_id}): job returned 0 rows "
                          f"(likely because extract_text=False or no content was extracted/embedded)")
                # Clean up NeMo-Retriever parquet files left in objects bucket (if any)
                detail_file_id = file_detail.get('file_id', '') if file_detail else ''
                if detail_file_id:
                    self._cleanup_original_parquet_files([], batch_id, file_id=detail_file_id)
                return
            
            try:
                # Trigger bulk insert for main collection (full results) for this specific file
                # Files are not cleaned up here - cleanup happens after filtered routing based on config
                bulk_insert_result = self._trigger_bulk_insert_from_objects(batch_id, job_id=job_id, file_detail=file_detail, trace_id=trace_id)
                original_files = bulk_insert_result.get('original_files', [])
                parquet_files = bulk_insert_result.get('parquet_files', [])
                local_files = bulk_insert_result.get('local_files', {})
                was_batched = bulk_insert_result.get('batched', False)
                
                # Parse and route filtered results to filtered collections if enabled
                # Use local files if available to avoid re-downloading
                temp_dir = bulk_insert_result.get('temp_dir')
                if self.config_manager:
                    storage_config = self.config_manager.get_storage_config()
                    if storage_config and storage_config.milvus_enable_filtered_collections:
                        if parquet_files:
                            self._route_filtered_results_to_collections(batch_id, job_id=job_id, parquet_files=parquet_files, local_files=local_files)
                        else:
                            logger.warning(f"No parquet files available for filtered routing (file: {file_id}, batch_id: {batch_id} for tracking)")
                    else:
                        logger.debug("Filtered collections are disabled, skipping filtered routing")
                
                # Cleanup temp directory after filtered routing is complete (or if disabled)
                if temp_dir and os.path.exists(temp_dir):
                    try:
                        shutil.rmtree(temp_dir, ignore_errors=True)
                        logger.debug(f"Cleaned up temp directory after filtered routing: {temp_dir}")
                    except Exception as e:
                        logger.warning(f"Error cleaning up temp directory {temp_dir}: {e}")
                
                # S3 parquet cleanup: when batched, deferred to batcher callback (after Milvus insert)
                if not was_batched:
                    detail_file_id = file_detail.get('file_id', '') if file_detail else ''
                    if original_files or detail_file_id:
                        self._cleanup_original_parquet_files(original_files, batch_id, file_id=detail_file_id)
            except Exception as e:
                logger.warning(f"Failed to trigger bulk insert for file {file_id} (batch_id: {batch_id} for tracking): {e}")
                # Don't fail the completion if bulk insert fails - parquet files are in Objects
                was_batched = False
            
            # Cleanup local file
            self._cleanup_local_file(file_id, file_detail)
            
            logger.info(f"Successfully completed processing for job {job_id}")
            
            # Record completion metrics
            if TELEMETRY_AVAILABLE:
                metrics = get_metrics()
                if metrics and metrics.enabled:
                    file_type = "unknown"
                    if file_detail:
                        s3_key = file_detail.get('s3_key', '')
                        file_type = os.path.splitext(s3_key)[1].lower()
                    
                    embeddings_count = result_row_count if result_row_count else 0
                    metrics.record_job_completed(
                        job_id=job_id,
                        file_type=file_type,
                        embeddings_count=embeddings_count
                    )
                    
                    s3_key = file_detail.get('s3_key', '')
                    file_size = file_detail.get('file_size', 0)
                    ft = os.path.splitext(s3_key)[1].lower() if s3_key else "unknown"
                    metrics.record_file_processed(file_type=ft, file_size_bytes=file_size)
            
            # Baseline notification: when batched, deferred to batcher callback
            # (after Milvus insert succeeds) to prevent data loss on crash.
            if not was_batched:
                if self.on_baseline_file_complete and file_detail.get('is_baseline'):
                    try:
                        self.on_baseline_file_complete(
                            s3_bucket=file_detail.get('s3_bucket'),
                            s3_key=file_detail.get('s3_key'),
                            version_id=file_detail.get('s3_version')
                        )
                    except Exception as e:
                        logger.warning(f"Failed to mark baseline file as processed: {e}")
        
        except Exception as e:
            logger.error(f"Error handling completion for job {job_id}: {e}")
            
            # Record job failure metrics
            if TELEMETRY_AVAILABLE:
                metrics = get_metrics()
                if metrics and metrics.enabled:
                    file_type = "unknown"
                    if file_detail:
                        s3_key = file_detail.get('s3_key', '')
                        file_type = os.path.splitext(s3_key)[1].lower()
                    metrics.record_job_failed(
                        job_id=job_id,
                        error_type="completion_error",
                        file_type=file_type
                    )
    
    def _save_file_metadata(self, file_id: str, job_id: str, result: Dict):
        """
        Save metadata for a completed file
        
        Args:
            file_id: File ID
            job_id: Job ID
            result: Result data from NeMo-Retriever
        """
        try:
            # Create metadata without database dependency
            metadata = {
                'file_id': file_id,
                'job_id': job_id,
                'result': result,
                'timestamp': time.time()
            }
            
            # Save to file
            metadata_file = os.path.join(
                self.completed_metadata_dir,
                f"{file_id}.json"
            )
            
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.debug(f"Saved metadata for {file_id} to {metadata_file}")
        
        except Exception as e:
            logger.error(f"Error saving metadata for {file_id}: {e}")
    
    def _cleanup_local_file(self, file_id: str, file_detail: Dict = None):
        """
        Delete local file after successful processing
        
        Args:
            file_id: File ID
            file_detail: File detail dictionary with s3_bucket, s3_key, kafka_offset
        """
        try:
            if file_detail:
                # Use filename based on bucket and object name only
                s3_bucket = file_detail.get('s3_bucket', '')
                s3_key = file_detail.get('s3_key', '')
                
                if s3_bucket and s3_key:
                    # Generate local filename
                    safe_key = s3_key.replace('/', '_').replace(':', '_').replace(' ', '_')
                    local_filename = f"{s3_bucket}_{safe_key}"
                    local_path = f"/tmp/nutanix-aidp-connector-downloads/{local_filename}"
                    
                    if os.path.exists(local_path):
                        try:
                            os.remove(local_path)
                            logger.debug(f"Deleted local file: {local_path}")
                        except FileNotFoundError:
                            # File was already deleted by another worker - this is expected in multi-worker setup
                            logger.debug(f"Local file already deleted by another worker: {local_path}")
                        except PermissionError:
                            # File might be in use by another worker - log and continue
                            logger.debug(f"Local file in use by another worker, skipping cleanup: {local_path}")
                    else:
                        # File doesn't exist - likely already cleaned up by another worker
                        logger.debug(f"Local file not found (already cleaned up): {local_path}")
            else:
                logger.warning(f"No file details available for cleanup of {file_id}")
        
        except Exception as e:
            logger.error(f"Error cleaning up file {file_id}: {e}")
    
    def handle_failure(self, file_id: str, error: str, file_detail: Optional[Dict] = None, trace_id: Optional[str] = None):
        """
        Handle a permanently failed file
        
        Args:
            file_id: File ID
            error: Error message
            file_detail: File detail dictionary with s3_bucket, s3_key, kafka_offset, etc.
            trace_id: Trace ID for debugging/correlation with NeMo-Retriever server logs (optional)
        """
        try:
            # Server never uploads invalid parquet (store_embed fails before writing), so no recovery for "illegal vector data".
            logger.error(f"Moving file {file_id} to dead letter queue: {error} (trace_id: {trace_id})")
            
            # Save to dead letter queue
            dead_letter_file = os.path.join(
                self.dead_letter_dir,
                f"{file_id}.json"
            )
            
            dead_letter_data = {
                'file_id': file_id,
                'error': str(error),
                'timestamp': time.time(),
                'file_detail': file_detail or {},
                'trace_id': trace_id  # Include trace_id for debugging/correlation with NeMo-Retriever server logs
            }
            
            with open(dead_letter_file, 'w') as f:
                json.dump(dead_letter_data, f, indent=2)
            
            logger.info(f"Saved dead letter for {file_id}")
            
            # Record failure metrics
            if TELEMETRY_AVAILABLE:
                metrics = get_metrics()
                if metrics and metrics.enabled:
                    file_type = "unknown"
                    if file_detail:
                        s3_key = file_detail.get('s3_key', '')
                        file_type = os.path.splitext(s3_key)[1].lower() if s3_key else "unknown"
                    metrics.record_file_failed(file_type=file_type, error_type="dead_letter")
            
            # Commit Kafka offset if available
            if file_detail and self.kafka_consumer:
                kafka_partition = file_detail.get('kafka_partition')
                kafka_offset = file_detail.get('kafka_offset')
                if kafka_partition is not None and kafka_offset is not None:
                    try:
                        self.kafka_consumer.commit_offset(kafka_partition, kafka_offset)
                        logger.debug(f"Committed Kafka offset for failed file {file_id}: partition={kafka_partition}, offset={kafka_offset}")
                    except Exception as commit_error:
                        logger.warning(f"Failed to commit Kafka offset for {file_id}: {commit_error}")
            
            # Notify baseline orchestrator so it does not deadlock waiting for this file
            if self.on_baseline_file_complete and file_detail and file_detail.get("is_baseline"):
                try:
                    self.on_baseline_file_complete(
                        s3_bucket=file_detail.get("s3_bucket"),
                        s3_key=file_detail.get("s3_key"),
                        version_id=file_detail.get("s3_version"),
                    )
                except Exception as baseline_err:
                    logger.warning(f"Failed to mark baseline file complete after failure: {baseline_err}")
            
            # Cleanup local file if exists
            if file_id:
                self._cleanup_local_file(file_id, file_detail)
        
        except Exception as e:
            logger.error(f"Error handling failure for {file_id}: {e}")
    
    def _trigger_bulk_insert_from_objects(self, batch_id: str, job_id: Optional[str] = None, file_detail: Dict = None, trace_id: Optional[str] = None):
        """
        Trigger bulk insert from Objects to Milvus after job completion.
        
        This is needed because NeMo-Retriever server creates parquet files in Objects
        but doesn't automatically trigger bulk insert to Milvus.
        
        Args:
            batch_id: Batch ID (for logging/tracking purposes only)
            job_id: Optional job ID (NeMo-Retriever job ID, typically "0" or similar)
            file_detail: File detail dictionary containing s3_bucket and s3_key
            trace_id: Optional trace ID for debugging/correlation with NeMo-Retriever server logs
        
        Returns:
            dict with keys:
                - 'original_files': list of parquet file keys in Objects bucket
                - 'parquet_files': list of parquet file S3 keys
                - 'local_files': dict mapping S3 keys to local file paths
        
        Note: NeMo-Retriever stores parquet files in embeddings_{bucket}_{object_name}/{UUID}/1.parquet
        where the path prefix is constructed from bucket and object name, and UUID is generated
        by RemoteBulkWriter. We search in the specific paths for each file instead of the entire
        embeddings/ directory.
        """
        if not BULK_INSERT_AVAILABLE:
            logger.warning("Bulk insert dependencies not available, skipping bulk insert")
            return {'original_files': [], 'parquet_files': [], 'local_files': {}}
        
        if not self.config_manager:
            logger.warning("Config manager not available, skipping bulk insert")
            return {'original_files': [], 'parquet_files': [], 'local_files': {}}
        
        try:
            # Get storage configuration
            storage_config = self.config_manager.get_storage_config()
            if not storage_config or not storage_config.milvus_enabled:
                logger.debug("Milvus is disabled, skipping bulk insert")
                return {'original_files': [], 'parquet_files': [], 'local_files': {}, 'total_rows': 0}
            
            logger.info(f"Triggering bulk insert for file (batch_id: {batch_id} for tracking)")
            t_start = time.perf_counter()
            stage_timings = {}
            # Parse Objects endpoint (format: host:port)
            objects_endpoint = storage_config.objects_endpoint
            if "://" in objects_endpoint:
                # Remove http:// or https:// prefix
                objects_endpoint = objects_endpoint.split("://")[1]
            
            # Determine protocol based on port
            if ":" in objects_endpoint:
                host, port = objects_endpoint.split(":", 1)
                port_num = int(port)
                if port_num == 443:
                    protocol = "https"
                else:
                    protocol = "http"
            else:
                # Default to HTTP if port not specified
                objects_endpoint = f"{objects_endpoint}:80"
                protocol = "http"
            
            # Build S3 endpoint URL (use HTTP for port 80)
            endpoint_url = f"{protocol}://{objects_endpoint}"
            
            # Get credentials from config, fallback to environment variables if empty
            objects_access_key = storage_config.objects_access_key or os.getenv('OBJECTS_ACCESS_KEY', '')
            objects_secret_key = storage_config.objects_secret_key or os.getenv('OBJECTS_SECRET_KEY', '')
            
            # Log Objects configuration being used
            # NOTE: Milvus server must be configured with the same Objects endpoint and credentials
            # for do_bulk_insert() to work, as it reads from Objects based on server-side configuration
            logger.info(f"Using Objects endpoint: {endpoint_url}")
            logger.info(f"Using Objects access key: {objects_access_key}")
            logger.debug(f"Using Objects secret key: {objects_secret_key[:10]}..." if objects_secret_key else "Using Objects secret key: (empty)")
            logger.info(f"Using Milvus-compatible Objects settings (endpoint={endpoint_url}, "
                       f"access_key={objects_access_key})")
            
            # Initialize S3 client using boto3
            s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=objects_access_key,
                aws_secret_access_key=objects_secret_key,
                verify=storage_config.objects_verify_ssl,
                config=boto3.session.Config(signature_version='s3v4')
            )
            
            bucket_name = storage_config.objects_bucket
            embeddings_prefix = "embeddings/"
            
            # Get the Milvus bucket name from storage config (should match milvus.yml)
            # The Milvus server reads from the bucket configured in its milvus.yml
            milvus_bucket = storage_config.milvus_bucket
            try:
                # Ensure Milvus bucket exists
                try:
                    s3_client.head_bucket(Bucket=milvus_bucket)
                    logger.debug(f"Bucket {milvus_bucket} already exists")
                except ClientError as e:
                    error_code = e.response.get('Error', {}).get('Code', '')
                    if error_code == '404':
                        # Bucket doesn't exist, create it
                        try:
                            s3_client.create_bucket(Bucket=milvus_bucket)
                            logger.info(f"Created bucket {milvus_bucket}")
                        except Exception as e:
                            logger.error(f"Failed to create bucket {milvus_bucket}: {e}")
                            return {'original_files': [], 'parquet_files': [], 'local_files': {}, 'total_rows': 0}
                    else:
                        logger.error(f"Error checking bucket {milvus_bucket}: {e}")
                        return {'original_files': [], 'parquet_files': [], 'local_files': {}, 'total_rows': 0}
            except Exception as e:
                logger.error(f"Error checking/creating Milvus bucket {milvus_bucket}: {e}")
                return {'original_files': [], 'parquet_files': [], 'local_files': {}, 'total_rows': 0}
            
            # Find parquet files in file-specific embeddings paths
            # Structure: embeddings_{bucket}_{object_name}/
            #            ├── {UUID1}/
            #            │   ├── 1.parquet
            #            │   ├── 2.parquet
            #            │   └── ... (multiple parquet files possible per UUID)
            #            ├── {UUID2}/
            #            │   ├── 1.parquet
            #            │   └── ...
            #            └── ... (multiple UUIDs possible)
            # 
            # Where the path prefix is constructed from bucket and object name, and UUIDs are generated
            # by RemoteBulkWriter. We recursively list ALL parquet files under the prefix to capture:
            # - All UUID directories
            # - All parquet files within each UUID directory
            # StoreEmbedTask may take a moment to write ALL files, so we use a stability check
            # to ensure we've captured all parquet files before proceeding.
            # Note: Each source file has a unique path, so different jobs won't conflict. Locking is used
            # to prevent duplicate processing if the same job is retried (new job_id, same path).
            t_parquet_discovery_start = time.perf_counter()
            all_parquet_files = []
            max_retries = 10  # Increased retries to allow for more files
            retry_delay = 2  # seconds
            stability_checks = 2  # Number of consecutive checks with same file count
            stability_delay = 1  # seconds between stability checks
            
            # Construct search prefix scoped by file_id to isolate this run's output
            # Path: {file_id}/embeddings_{bucket}_{object_name}/  (matches enhanced_tasks.py)
            search_prefix = None
            if file_detail:
                s3_bucket = file_detail.get('s3_bucket', '')
                s3_key = file_detail.get('s3_key', '')
                detail_file_id = file_detail.get('file_id', '')
                if s3_bucket and s3_key:
                    safe_bucket = s3_bucket.replace('/', '_').replace(':', '_').replace(' ', '_')
                    safe_object = s3_key.replace('/', '_').replace(':', '_').replace(' ', '_')
                    if len(safe_object) > 200:
                        safe_object = safe_object[-200:]
                    base_path = f"embeddings_{safe_bucket}_{safe_object}"
                    if detail_file_id:
                        search_prefix = f"{detail_file_id}/{base_path}"
                    else:
                        search_prefix = base_path
                    logger.debug(f"Will search for parquet files in {bucket_name}/{search_prefix}/")
            
            # If no file detail provided, fall back to searching entire embeddings/ directory
            if not search_prefix:
                logger.warning(f"No file detail provided for file (batch_id: {batch_id}), falling back to recursive search in embeddings/")
                search_prefix = embeddings_prefix
            
            logger.info(f"Searching for parquet files in {bucket_name}/{search_prefix}/ for file (batch_id: {batch_id} for tracking)")
            
            def list_parquet_files():
                """List all parquet files recursively under the search prefix.
                
                This lists ALL parquet files in ALL subdirectories under the prefix.
                Handles structure like:
                  embeddings_{bucket}_{object}/UUID1/1.parquet
                  embeddings_{bucket}_{object}/UUID1/2.parquet
                  embeddings_{bucket}_{object}/UUID2/1.parquet
                  ... (multiple UUIDs, multiple files per UUID)
                """
                files = []
                try:
                    paginator = s3_client.get_paginator('list_objects_v2')
                    # Add trailing slash to ensure we only match the directory, not similar prefixes
                    prefix_with_slash = search_prefix if search_prefix.endswith('/') else f"{search_prefix}/"
                    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix_with_slash)
                    
                    total_objects = 0
                    for page in pages:
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                total_objects += 1
                                key = obj['Key']
                                if key.endswith('.parquet'):
                                    files.append(key)
                                    logger.debug(f"Found parquet: {key}")
                    
                    logger.debug(f"Listed {total_objects} total objects, {len(files)} parquet files under {prefix_with_slash}")
                except Exception as e:
                    logger.error(f"Error listing objects in {bucket_name}/{search_prefix}: {e}")
                    import traceback
                    logger.debug(traceback.format_exc())
                return files
            
            # Wait for files to appear with retries
            for attempt in range(max_retries):
                all_parquet_files = list_parquet_files()
                
                if all_parquet_files:
                    logger.info(f"Found {len(all_parquet_files)} parquet file(s) on attempt {attempt + 1}")
                    
                    # Stability check: wait and re-list to ensure all files are written
                    # NeMo-Retriever may write multiple parquet files (1.parquet, 2.parquet, etc.)
                    stable_count = 0
                    prev_count = len(all_parquet_files)
                    
                    for stability_attempt in range(stability_checks):
                        time.sleep(stability_delay)
                        current_files = list_parquet_files()
                        current_count = len(current_files)
                        
                        logger.debug(f"Stability check {stability_attempt + 1}/{stability_checks}: "
                                   f"prev={prev_count}, current={current_count}")
                        
                        if current_count == prev_count:
                            stable_count += 1
                            if stable_count >= stability_checks:
                                # File count is stable, use the latest list
                                all_parquet_files = current_files
                                logger.info(f"File count stable at {current_count} file(s) after {stability_checks} checks")
                                break
                        else:
                            # More files appeared, reset stability counter
                            stable_count = 0
                            prev_count = current_count
                            all_parquet_files = current_files
                            logger.info(f"More files appeared: now {current_count} file(s)")
                    
                    logger.debug(f"All parquet files found ({len(all_parquet_files)}):")
                    for f in all_parquet_files:
                        logger.debug(f"  - {f}")
                    
                    break  # Exit retry loop
                
                if attempt < max_retries - 1:
                    logger.debug(f"No parquet files found yet (attempt {attempt + 1}/{max_retries}), waiting {retry_delay}s before retry...")
                    time.sleep(retry_delay)
            
            if not all_parquet_files:
                logger.warning(f"No parquet files found in {bucket_name}/{search_prefix}/ after {max_retries} attempts")
                logger.warning(f"This may indicate StoreEmbedTask did not write files, or files are in a different location")
                return {'original_files': [], 'parquet_files': [], 'local_files': {}, 'total_rows': 0}
            
            logger.info(f"Total parquet files found: {len(all_parquet_files)} in {bucket_name}/{search_prefix}/")
            stage_timings['parquet_discovery_sec'] = round(time.perf_counter() - t_parquet_discovery_start, 3)
            # Filter out already-processed files to prevent duplicate processing
            # This is primarily useful for job retries (new job_id searching same path)
            # We use a lock to atomically check and mark files as processed
            with self.parquet_files_lock:
                # Filter out files that have already been processed
                parquet_files = [f for f in all_parquet_files if f not in self.processed_parquet_files]
                
                if not parquet_files:
                    logger.info(f"All {len(all_parquet_files)} parquet file(s) have already been processed (likely from a previous job retry), skipping bulk insert for file (batch_id: {batch_id})")
                    return {'original_files': [], 'parquet_files': [], 'local_files': {}, 'total_rows': 0}
                
                # Mark files as processed BEFORE processing to prevent duplicate processing
                # This ensures that even if processing fails and job is retried, we don't reprocess the same files
                for f in parquet_files:
                    self.processed_parquet_files.add(f)
                
                logger.info(f"Found {len(parquet_files)} new parquet file(s) to bulk insert (out of {len(all_parquet_files)} total)")
                logger.info(f"Parquet files to process: {parquet_files[:5]}{'...' if len(parquet_files) > 5 else ''}")
                if len(all_parquet_files) > len(parquet_files):
                    logger.info(f"Skipped {len(all_parquet_files) - len(parquet_files)} already-processed file(s)")
            
            # Connect to Milvus
            milvus_uri = f"http://{storage_config.milvus_host}:{storage_config.milvus_port}"
            try:
                connections.connect(uri=milvus_uri, alias="default")
            except Exception:
                # Connection may already exist
                try:
                    connections.disconnect("default")
                except Exception:
                    pass
                connections.connect(uri=milvus_uri, alias="default")
            
            # Download parquet files locally first, then upload to Milvus bucket
            # This allows us to reuse local files for filtered routing
            task_ids = []
            uploaded_files = []  # Files uploaded to Milvus bucket (for cleanup)
            original_files = []  # Original files in Objects bucket (for cleanup)
            local_file_paths = {}  # Map S3 key to local file path for filtered routing
            temp_dir = None  # Temp directory for local downloads
            total_parquet_rows = 0  # Track total rows across all parquet files
            
            # Download files locally first
            temp_dir = tempfile.mkdtemp(prefix=f"nutanix_aidp_connector_bulk_insert_{batch_id}_")
            downloads_dir = os.path.join(temp_dir, "downloads")
            combined_dir = os.path.join(temp_dir, "combined")
            os.makedirs(downloads_dir, exist_ok=True)
            os.makedirs(combined_dir, exist_ok=True)
            logger.info(f"Downloading parquet files to local temp directory: {downloads_dir}")
            t_download_start = time.perf_counter()
            for parquet_file in parquet_files:
                try:
                    safe_filename = parquet_file.replace('/', '_').replace(':', '_')
                    local_path = os.path.join(downloads_dir, safe_filename)
                    
                    # Download from Objects bucket to local
                    logger.debug(f"Downloading {parquet_file} from {bucket_name} to {local_path}")
                    s3_client.download_file(bucket_name, parquet_file, local_path)
                    
                    # Verify download
                    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                        local_file_paths[parquet_file] = local_path
                        original_files.append(parquet_file)  # Track for Objects bucket cleanup
                        logger.debug(f"Downloaded {parquet_file} to {local_path} ({os.path.getsize(local_path)} bytes)")
                    else:
                        logger.error(f"Downloaded file {local_path} is empty or doesn't exist")
                except Exception as e:
                    logger.error(f"Error downloading {parquet_file} to local: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue
            
            if not local_file_paths:
                logger.error("No parquet files were downloaded locally")
                return {'original_files': [], 'parquet_files': [], 'local_files': {}, 'total_rows': 0}
            stage_timings['download_parquet_sec'] = round(time.perf_counter() - t_download_start, 3)
            logger.info(f"Downloaded {len(local_file_paths)} parquet file(s) locally")
            t_merge_start = time.perf_counter()
            # Read all parquet files, enrich metadata, and merge into one file (Milvus Parquet import accepts only one file per request).
            collection_name = storage_config.milvus_collection
            combined_key = None
            combined_path = None
            
            if PANDAS_AVAILABLE:
                dfs_list = []
                for parquet_file, local_path in local_file_paths.items():
                    df = None
                    try:
                        df = pd.read_parquet(local_path)
                        file_rows = len(df)
                        total_parquet_rows += file_rows
                        logger.debug(f"Parquet file {parquet_file} contains {file_rows} row(s)")
                        self._enrich_parquet_metadata(df, file_detail, parquet_file)
                        dfs_list.append(df)
                    except Exception as e:
                        logger.warning(f"Could not process parquet file {parquet_file}: {e}")
                        import traceback
                        logger.debug(traceback.format_exc())
                    finally:
                        if df is not None:
                            del df
                
                logger.info(f"Total rows across all parquet files: {total_parquet_rows}")
                
                if not dfs_list:
                    logger.warning("No parquet DataFrames to merge")
                else:
                    combined_df = pd.concat(dfs_list, ignore_index=True)
                    dfs_list.clear()
                    self._fix_v2_page_numbers(combined_df)
                    combined_filename = f"combined_{batch_id}.parquet"
                    combined_path = os.path.join(combined_dir, combined_filename)
                    combined_df.to_parquet(combined_path, index=False)
                    stage_timings['merge_enrich_sec'] = round(time.perf_counter() - t_merge_start, 3)

                    combined_key = f"{search_prefix}/{combined_filename}"
                    local_file_paths = {combined_key: combined_path}
                    parquet_files = [combined_key]

                    # ---- Batched path: queue DataFrame to MilvusBatchInserter ----
                    if self.milvus_batch_inserter is not None:
                        from completion.milvus_batch_inserter import MilvusInsertItem

                        # Build deferred callbacks that fire ONLY after successful Milvus insert.
                        # This prevents data loss on crash: S3 parquets stay until Milvus has the data,
                        # and the baseline marker is not advanced until the data is safe.
                        deferred_callbacks = []
                        detail_file_id = file_detail.get('file_id', '') if file_detail else ''
                        _orig_files = list(original_files)
                        _batch_id = batch_id
                        _file_id = detail_file_id
                        deferred_callbacks.append(
                            lambda _o=_orig_files, _b=_batch_id, _f=_file_id:
                                self._cleanup_original_parquet_files(_o, _b, file_id=_f)
                        )
                        if self.on_baseline_file_complete and file_detail.get('is_baseline'):
                            _fd = dict(file_detail)
                            deferred_callbacks.append(
                                lambda _fd=_fd: self.on_baseline_file_complete(
                                    s3_bucket=_fd.get('s3_bucket'),
                                    s3_key=_fd.get('s3_key'),
                                    version_id=_fd.get('s3_version'),
                                )
                            )

                        insert_item = MilvusInsertItem(
                            dataframe=combined_df,
                            total_rows=total_parquet_rows,
                            batch_id=batch_id,
                            job_id=job_id or '',
                            file_id=detail_file_id,
                            on_flush_complete=deferred_callbacks,
                        )
                        del combined_df
                        self.milvus_batch_inserter.add(insert_item)

                        total_bulk_sec = round(time.perf_counter() - t_start, 3)
                        logger.info(
                            "PIPELINE_LATENCY stage=completion_prepare batch_id=%s job_id=%s total_sec=%s %s",
                            batch_id, job_id, total_bulk_sec,
                            " ".join(f"{k}={v}" for k, v in sorted(stage_timings.items())),
                        )
                        if TELEMETRY_AVAILABLE:
                            metrics = get_metrics()
                            if metrics and metrics.enabled:
                                for stage_key, duration in stage_timings.items():
                                    metrics.record_pipeline_stage_duration(stage_key.replace('_sec', ''), duration)

                        gc.collect()
                        return {
                            'original_files': [],  # empty: cleanup deferred to batcher callback
                            'parquet_files': parquet_files,
                            'local_files': local_file_paths,
                            'total_rows': total_parquet_rows,
                            'temp_dir': temp_dir,
                            'batched': True,  # signal to caller: baseline notify deferred
                        }
                    del combined_df

                    # ---- Non-batched path: per-file upload + bulk insert ----
                    try:
                        logger.info(f"Uploading merged parquet ({total_parquet_rows} rows) to {milvus_bucket}/{combined_key}")
                        t_upload_start = time.perf_counter()
                        s3_client.upload_file(combined_path, milvus_bucket, combined_key)
                        s3_client.head_object(Bucket=milvus_bucket, Key=combined_key)
                        stage_timings['upload_milvus_sec'] = round(time.perf_counter() - t_upload_start, 3)
                        uploaded_files.append(combined_key)
                    except Exception as e:
                        logger.error(f"Error uploading merged parquet to {milvus_bucket}: {e}")
                        import traceback
                        logger.error(traceback.format_exc())
            
            if uploaded_files:
                file_batch = uploaded_files
                last_error = None
                t_bulk_insert_start = time.perf_counter()
                for attempt in range(BULK_INSERT_JOB_LIMIT_RETRIES):
                    try:
                        logger.info(f"Triggering bulk insert (1 file, {total_parquet_rows} rows)" + (f" (retry {attempt + 1}/{BULK_INSERT_JOB_LIMIT_RETRIES})" if attempt > 0 else ""))
                        task_id = utility.do_bulk_insert(
                            collection_name=collection_name,
                            files=file_batch,
                            consistency_level=CONSISTENCY,
                        )
                        logger.info(f"Created bulk insert task {task_id} for collection '{collection_name}' with 1 file")
                        task_ids.append(task_id)
                        stage_timings['bulk_insert_call_sec'] = round(time.perf_counter() - t_bulk_insert_start, 3)
                        if TELEMETRY_AVAILABLE:
                            metrics = get_metrics()
                            if metrics and metrics.enabled:
                                metrics.record_milvus_insert(collection=collection_name)
                        last_error = None
                        break
                    except Exception as e:
                        last_error = e
                        is_job_limit = (
                            (MilvusException is not None and isinstance(e, MilvusException) and getattr(e, "code", None) == MILVUS_JOB_LIMIT_ERROR_CODE)
                            or "number of jobs" in str(e).lower()
                            or "jobs has reached the limit" in str(e).lower()
                            or "accepts only one file" in str(e).lower()
                        )
                        if is_job_limit and attempt < BULK_INSERT_JOB_LIMIT_RETRIES - 1:
                            backoff = BULK_INSERT_JOB_LIMIT_BACKOFF_SEC * (2 ** attempt)
                            logger.warning(f"Milvus bulk insert limit hit (attempt {attempt + 1}/{BULK_INSERT_JOB_LIMIT_RETRIES}), retrying in {backoff}s: {e}")
                            time.sleep(backoff)
                        else:
                            logger.error(f"Error creating bulk insert task: {e}")
                            if TELEMETRY_AVAILABLE:
                                metrics = get_metrics()
                                if metrics and metrics.enabled:
                                    metrics.record_milvus_insert_failure(
                                        collection=collection_name,
                                        error_type=type(e).__name__
                                    )
                            import traceback
                            logger.error(traceback.format_exc())
                            for f in file_batch:
                                try:
                                    s3_client.delete_object(Bucket=milvus_bucket, Key=f)
                                except Exception:
                                    pass
                            break

            # Wait for bulk insert tasks to complete
            total_imported_rows = 0
            t_wait_bulk_start = time.perf_counter()
            while len(task_ids) > 0:
                time.sleep(1)
                for task_id in task_ids[:]:
                    try:
                        task = utility.get_bulk_insert_state(task_id=task_id)
                        state = task.state_name
                        if state == "Completed":
                            imported_rows = task.row_count
                            total_imported_rows += imported_rows
                            logger.info(f"Bulk insert task {task_id} completed successfully")
                            logger.info(f"Imported row count: {imported_rows}")
                            task_ids.remove(task_id)
                        elif task.state == BulkInsertState.ImportFailed:
                            logger.error(f"Bulk insert task {task_id} failed: {task.failed_reason}")
                            task_ids.remove(task_id)
                    except Exception as e:
                        logger.warning(f"Error checking bulk insert task {task_id}: {e}")
            stage_timings['wait_bulk_insert_sec'] = round(time.perf_counter() - t_wait_bulk_start, 3)
            total_bulk_sec = round(time.perf_counter() - t_start, 3)
            logger.info("PIPELINE_LATENCY stage=completion_bulk_insert batch_id=%s job_id=%s total_sec=%s %s",
                        batch_id, job_id, total_bulk_sec, " ".join(f"{k}={v}" for k, v in sorted(stage_timings.items())))
            if TELEMETRY_AVAILABLE:
                metrics = get_metrics()
                if metrics and metrics.enabled:
                    for stage_key, duration in stage_timings.items():
                        stage_name = stage_key.replace('_sec', '')
                        metrics.record_pipeline_stage_duration(stage_name, duration)
            # Cleanup Milvus bucket files after bulk insert
            cleanup_parquet_files = True
            if self.config_manager:
                storage_config = self.config_manager.get_storage_config()
                if storage_config:
                    cleanup_parquet_files = getattr(storage_config, 'milvus_cleanup_parquet_files', True)
                    if isinstance(cleanup_parquet_files, str):
                        cleanup_parquet_files = cleanup_parquet_files.lower() in ('true', '1', 'yes', 'on')
                    
            if cleanup_parquet_files and uploaded_files:
                try:
                    delete_objects = [{'Key': f} for f in uploaded_files]
                    s3_client.delete_objects(
                        Bucket=milvus_bucket,
                        Delete={'Objects': delete_objects}
                    )
                    logger.info(f"Cleaned up {len(uploaded_files)} files from {milvus_bucket}")
                except Exception as e:
                    logger.warning(f"Error cleaning up files from {milvus_bucket}: {e}")
            
            logger.info(f"Bulk insert for file completed (batch_id: {batch_id} for tracking)")
            
            gc.collect()
            
            return {
                'original_files': original_files,
                'parquet_files': parquet_files,
                'local_files': local_file_paths,
                'total_rows': total_parquet_rows,
                'temp_dir': temp_dir
            }
        
        except Exception as e:
            logger.error(f"Error triggering bulk insert for file (batch_id: {batch_id}): {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Don't re-raise - this is a best-effort operation
            # Cleanup any local files that were downloaded
            if 'local_file_paths' in locals():
                for local_path in local_file_paths.values():
                    try:
                        if os.path.exists(local_path):
                            os.unlink(local_path)
                    except Exception:
                        pass
                if 'temp_dir' in locals() and temp_dir and os.path.exists(temp_dir):
                    try:
                        # Use shutil.rmtree to ensure complete cleanup even if directory has files
                        shutil.rmtree(temp_dir, ignore_errors=True)
                        logger.debug(f"Cleaned up temp directory: {temp_dir}")
                    except Exception as e:
                        logger.warning(f"Error cleaning up temp directory {temp_dir}: {e}")
            return {'original_files': [], 'parquet_files': [], 'local_files': {}}
    
    def _cleanup_original_parquet_files(self, original_files: List[str], batch_id: str, file_id: str = None):
        """
        Clean up original parquet files from Objects bucket after filtered routing is complete.
        
        When file_id is provided, deletes the entire {file_id}/ prefix (catches all NeMo-Retriever
        output for this job, even files not discovered during listing). Falls back to deleting
        individual keys from original_files.
        
        Args:
            original_files: List of parquet file keys in Objects bucket to delete
            batch_id: Batch ID (for logging/tracking purposes only)
            file_id: Unique file processing ID; when set the entire {file_id}/ tree is removed
        """
        if not original_files and not file_id:
            return
        
        if not self.config_manager:
            logger.warning("Config manager not available, skipping cleanup")
            return
        
        try:
            storage_config = self.config_manager.get_storage_config()
            if not storage_config:
                logger.warning("Storage config not available, skipping cleanup")
                return
            
            cleanup_parquet_files = getattr(storage_config, 'milvus_cleanup_parquet_files', True)
            logger.info(f"Parquet file cleanup config value: {cleanup_parquet_files} (type: {type(cleanup_parquet_files)}, bool value: {bool(cleanup_parquet_files)})")
            
            if isinstance(cleanup_parquet_files, str):
                cleanup_parquet_files = cleanup_parquet_files.lower() in ('true', '1', 'yes', 'on')
                logger.info(f"Converted string value to boolean: {cleanup_parquet_files}")
            
            if not cleanup_parquet_files:
                count = len(original_files) if original_files else 0
                logger.info(f"Parquet file cleanup is disabled, retaining {count} file(s) in Objects bucket")
                return
            
            objects_endpoint = storage_config.objects_endpoint
            if "://" in objects_endpoint:
                objects_endpoint = objects_endpoint.split("://")[1]
            
            if ":" in objects_endpoint:
                host, port = objects_endpoint.split(":", 1)
                port_num = int(port)
                protocol = "https" if port_num == 443 else "http"
            else:
                objects_endpoint = f"{objects_endpoint}:80"
                protocol = "http"
            
            endpoint_url = f"{protocol}://{objects_endpoint}"
            
            objects_access_key = storage_config.objects_access_key or os.getenv('OBJECTS_ACCESS_KEY', '')
            objects_secret_key = storage_config.objects_secret_key or os.getenv('OBJECTS_SECRET_KEY', '')
            
            s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=objects_access_key,
                aws_secret_access_key=objects_secret_key,
                verify=storage_config.objects_verify_ssl,
                config=boto3.session.Config(signature_version='s3v4')
            )
            
            bucket_name = storage_config.objects_bucket
            
            # When file_id is available, wipe the entire {file_id}/ prefix tree
            if file_id:
                prefix = f"{file_id}/"
                try:
                    paginator = s3_client.get_paginator('list_objects_v2')
                    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
                    total_deleted = 0
                    for page in pages:
                        contents = page.get('Contents', [])
                        if not contents:
                            continue
                        delete_objects = [{'Key': obj['Key']} for obj in contents]
                        s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_objects})
                        total_deleted += len(delete_objects)
                    logger.info(f"Cleaned up {total_deleted} object(s) under {bucket_name}/{prefix} (file_id={file_id})")
                except Exception as e:
                    logger.warning(f"Error cleaning up {bucket_name}/{prefix}: {e}, falling back to individual file cleanup")
                    file_id = None  # fall through to individual cleanup below
            
            # Fallback: delete individual files from Objects bucket
            if not file_id and original_files:
                logger.info(f"Parquet file cleanup is enabled, will clean up {len(original_files)} file(s) from Objects bucket")
                try:
                    delete_objects = [{'Key': f} for f in original_files]
                    s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': delete_objects}
                    )
                    logger.info(f"Cleaned up {len(original_files)} original parquet file(s) from {bucket_name} after filtered routing")
                except Exception as e:
                    logger.warning(f"Error cleaning up original files from {bucket_name}: {e}")
        
        except Exception as e:
            logger.error(f"Error cleaning up original parquet files for file (batch_id: {batch_id}): {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _route_filtered_results_to_collections(self, batch_id: str, job_id: Optional[str] = None, parquet_files: Optional[List[str]] = None, local_files: Optional[Dict[str, str]] = None):
        """
        Parse parquet files, filter by document_type/subtype, and route to filtered collections.
        
        Filters:
        - text1: document_type == 'text'
        - tables1: content_metadata['subtype'] == 'table'
        - charts1: content_metadata['subtype'] == 'chart'
        
        Args:
            batch_id: Batch ID
            job_id: Optional job ID
            parquet_files: Optional list of parquet file S3 keys to process. If None, will search for files.
            local_files: Optional dict mapping S3 keys to local file paths. If provided, uses local files instead of downloading.
        """
        if not BULK_INSERT_AVAILABLE:
            logger.warning("Bulk insert dependencies not available, skipping filtered routing")
            return
        
        if not PANDAS_AVAILABLE:
            logger.warning("pandas not available, skipping filtered routing")
            return
        
        if not self.config_manager:
            logger.warning("Config manager not available, skipping filtered routing")
            return
        
        try:
            storage_config = self.config_manager.get_storage_config()
            if not storage_config or not storage_config.milvus_enabled:
                logger.debug("Milvus is disabled, skipping filtered routing")
                return
            
            logger.info(f"Routing filtered results for file (batch_id: {batch_id} for tracking)")
            
            # Get pipeline orchestrator to access filtered collection managers
            # We'll need to pass this or access it differently
            # For now, we'll create managers here if needed
            from storage.milvus_manager import MilvusManager
            
            # Parse Objects endpoint
            objects_endpoint = storage_config.objects_endpoint
            if "://" in objects_endpoint:
                objects_endpoint = objects_endpoint.split("://")[1]
            
            if ":" in objects_endpoint:
                host, port = objects_endpoint.split(":", 1)
                port_num = int(port)
                protocol = "https" if port_num == 443 else "http"
            else:
                objects_endpoint = f"{objects_endpoint}:80"
                protocol = "http"
            
            endpoint_url = f"{protocol}://{objects_endpoint}"
            
            # Get credentials from config, fallback to environment variables if empty
            objects_access_key = storage_config.objects_access_key or os.getenv('OBJECTS_ACCESS_KEY', '')
            objects_secret_key = storage_config.objects_secret_key or os.getenv('OBJECTS_SECRET_KEY', '')
            
            # Initialize S3 client
            s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=objects_access_key,
                aws_secret_access_key=objects_secret_key,
                verify=storage_config.objects_verify_ssl,
                config=boto3.session.Config(signature_version='s3v4')
            )
            
            bucket_name = storage_config.objects_bucket
            milvus_bucket = storage_config.milvus_bucket
            
            # Use provided parquet files list, or search for them
            if parquet_files is None:
                embeddings_prefix = "embeddings/"
                logger.info(f"Searching for parquet files in {bucket_name}/{embeddings_prefix}")
                parquet_files = []
                try:
                    paginator = s3_client.get_paginator('list_objects_v2')
                    pages = paginator.paginate(Bucket=bucket_name, Prefix=embeddings_prefix)
                    
                    for page in pages:
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                if obj['Key'].endswith('.parquet'):
                                    parquet_files.append(obj['Key'])
                except Exception as e:
                    logger.error(f"Error listing parquet files: {e}")
                    return
                
                if not parquet_files:
                    logger.warning(f"No parquet files found for filtered routing")
                    return
            else:
                logger.info(f"Using provided list of {len(parquet_files)} parquet file(s) for filtering")
            
            logger.info(f"Processing {len(parquet_files)} parquet file(s) for filtered routing")
            
            # Connect to Milvus
            milvus_uri = f"http://{storage_config.milvus_host}:{storage_config.milvus_port}"
            try:
                connections.connect(uri=milvus_uri, alias="default")
            except Exception:
                try:
                    connections.disconnect("default")
                except Exception:
                    pass
                connections.connect(uri=milvus_uri, alias="default")
            
            # Filtered collections configuration from config
            # Note: Parquet files from NeMo-Retriever have columns: pk, vector, text, source, content_metadata
            # We need to parse content_metadata (JSON) to get subtype
            filtered_collections = {}
            
            text_collection = storage_config.milvus_filtered_text_collection_name
            if text_collection and text_collection.strip():
                filtered_collections[text_collection.strip()] = {
                    'filter_func': lambda row: self._matches_text_filter(row)
                }
            
            tables_collection = storage_config.milvus_filtered_tables_collection_name
            if tables_collection and tables_collection.strip():
                filtered_collections[tables_collection.strip()] = {
                    'filter_func': lambda row: self._get_subtype(row) == 'table'
                }
            
            charts_collection = storage_config.milvus_filtered_charts_collection_name
            if charts_collection and charts_collection.strip():
                filtered_collections[charts_collection.strip()] = {
                    'filter_func': lambda row: self._get_subtype(row) == 'chart'
                }
            
            if not filtered_collections:
                logger.info("No filtered collections configured, skipping filtered routing")
                return
            
            # Track statistics
            stats = {
                'files_processed': 0,
                'files_failed': 0,
                'total_rows': 0,
                'collections': {name: {'rows': 0, 'bulk_inserts': 0} for name in filtered_collections.keys()}
            }
            
            # Process each parquet file
            for parquet_file in parquet_files:
                try:
                    logger.info(f"Processing parquet file: {parquet_file}")
                    
                    # Use local file if available, otherwise download from S3
                    if local_files and parquet_file in local_files:
                        tmp_path = local_files[parquet_file]
                        logger.info(f"Using local file: {tmp_path}")
                    else:
                        # Download parquet file to temp location
                        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                            tmp_path = tmp_file.name
                        
                        # Download from S3
                        logger.info(f"Downloading {parquet_file} from {bucket_name} to {tmp_path}")
                        s3_client.download_file(bucket_name, parquet_file, tmp_path)
                        logger.info(f"Downloaded {parquet_file} to {tmp_path}")
                    
                    # Read parquet file
                    logger.info(f"Reading parquet file: {tmp_path}")
                    df = pd.read_parquet(tmp_path)
                    logger.info(f"Read {len(df)} rows from {parquet_file}")
                    logger.info(f"Parquet file columns: {list(df.columns)}")
                    if len(df) > 0:
                        # Log sample of all relevant fields to understand structure
                        sample_row = df.iloc[0]
                        logger.info(f"Sample row data:")
                        for col in df.columns:
                            val = sample_row[col]
                            if col in ['source', 'content_metadata']:
                                logger.info(f"  {col} type: {type(val)}, value: {str(val)[:300]}")
                            elif col == 'text':
                                logger.info(f"  {col} type: {type(val)}, length: {len(str(val)) if val else 0}, preview: {str(val)[:100] if val else 'None'}")
                            else:
                                logger.info(f"  {col} type: {type(val)}, value: {str(val)[:100]}")
                    stats['total_rows'] += len(df)
                    stats['files_processed'] += 1
                    
                    # Route to each filtered collection
                    for collection_name, filter_config in filtered_collections.items():
                        filtered_df = None
                        filtered_tmp_path = None
                        try:
                            # Filter rows
                            filtered_df = df[df.apply(filter_config['filter_func'], axis=1)]
                            
                            if len(filtered_df) == 0:
                                logger.info(f"No rows match filter for {collection_name} from {parquet_file}")
                                continue
                            
                            logger.info(f"Filtered {len(filtered_df)} rows for {collection_name} from {parquet_file} (total rows: {len(df)})")
                            stats['collections'][collection_name]['rows'] += len(filtered_df)
                            
                            # Store row count before deleting DataFrame
                            filtered_row_count = len(filtered_df)
                            
                            # Write filtered parquet to temp file
                            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as filtered_tmp:
                                filtered_tmp_path = filtered_tmp.name
                            
                            filtered_df.to_parquet(filtered_tmp_path, index=False)
                            
                            # Explicitly delete filtered DataFrame to free memory immediately
                            del filtered_df
                            filtered_df = None
                            
                            # Upload filtered parquet to Milvus bucket
                            filtered_key = f"filtered/{collection_name}/{batch_id}/{os.path.basename(parquet_file)}"
                            logger.info(f"Uploading filtered parquet to {milvus_bucket}/{filtered_key}")
                            s3_client.upload_file(filtered_tmp_path, milvus_bucket, filtered_key)
                            logger.info(f"Uploaded filtered parquet to {milvus_bucket}/{filtered_key}")
                            
                            # Trigger bulk insert for filtered collection
                            try:
                                task_id = utility.do_bulk_insert(
                                    collection_name=collection_name,
                                    files=[filtered_key],
                                    consistency_level=CONSISTENCY,
                                )
                                logger.info(f"Created bulk insert task {task_id} for filtered collection '{collection_name}' with {filtered_row_count} rows")
                                stats['collections'][collection_name]['bulk_inserts'] += 1
                                
                                # Record Milvus insert metrics for filtered collection
                                if TELEMETRY_AVAILABLE:
                                    metrics = get_metrics()
                                    if metrics and metrics.enabled:
                                        metrics.record_milvus_insert(
                                            collection=collection_name,
                                            embeddings_count=filtered_row_count
                                        )
                            except Exception as e:
                                logger.error(f"Error creating bulk insert for {collection_name}: {e}")
                                import traceback
                                logger.error(traceback.format_exc())
                                
                                # Record Milvus insert failure metrics
                                if TELEMETRY_AVAILABLE:
                                    metrics = get_metrics()
                                    if metrics and metrics.enabled:
                                        metrics.record_milvus_insert_failure(
                                            collection=collection_name,
                                            error_type=type(e).__name__
                                        )
                            
                            # Cleanup temp file
                            if filtered_tmp_path:
                                try:
                                    os.unlink(filtered_tmp_path)
                                except Exception:
                                    pass
                                
                        except Exception as e:
                            logger.error(f"Error routing to {collection_name}: {e}")
                            import traceback
                            logger.error(traceback.format_exc())
                        finally:
                            # Ensure filtered DataFrame is deleted even on error
                            if filtered_df is not None:
                                del filtered_df
                    
                    # Explicitly delete main DataFrame to free memory
                    del df
                    df = None
                    # Suggest garbage collection after processing large DataFrames
                    gc.collect()
                    
                    # Cleanup temp file only if we downloaded it (not if using local file)
                    if not (local_files and parquet_file in local_files):
                        try:
                            os.unlink(tmp_path)
                        except Exception:
                            pass
                        
                except Exception as e:
                    logger.error(f"Error processing parquet file {parquet_file}: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    stats['files_failed'] += 1
            
            # Cleanup local files after processing (if they were provided)
            if local_files:
                logger.info(f"Cleaning up {len(local_files)} local parquet file(s)")
                temp_dir = None
                for s3_key, local_path in local_files.items():
                    try:
                        if os.path.exists(local_path):
                            os.unlink(local_path)
                            # Extract temp directory for cleanup
                            if temp_dir is None:
                                temp_dir = os.path.dirname(local_path)
                    except Exception as e:
                        logger.warning(f"Error cleaning up local file {local_path}: {e}")
                
                # Cleanup temp directory if it's empty
                if temp_dir and os.path.exists(temp_dir):
                    try:
                        if not os.listdir(temp_dir):
                            os.rmdir(temp_dir)
                            logger.debug(f"Removed empty temp directory: {temp_dir}")
                    except Exception:
                        pass
            
            # Log summary
            logger.info(f"Completed filtered routing for file (batch_id: {batch_id}): "
                       f"processed {stats['files_processed']} parquet file(s), "
                       f"failed {stats['files_failed']} parquet file(s), "
                       f"total rows: {stats['total_rows']}")
            for collection_name, collection_stats in stats['collections'].items():
                if collection_stats['rows'] > 0:
                    logger.info(f"  {collection_name}: {collection_stats['rows']} rows, {collection_stats['bulk_inserts']} bulk insert(s)")
                else:
                    logger.info(f"  {collection_name}: no matching rows")
            
        except Exception as e:
            logger.error(f"Error routing filtered results for file (batch_id: {batch_id}): {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _enrich_parquet_metadata(self, df: pd.DataFrame, file_detail: Optional[Dict] = None, parquet_file: Optional[str] = None):
        """
        Enrich parquet DataFrame's content_metadata with custom filterable fields.
        
        This adds custom metadata fields to the content_metadata JSON column that can be used
        for filtering queries in Milvus. Metadata is extracted from file_detail or other sources.
        
        Args:
            df: Pandas DataFrame with columns: pk, vector, text, source, content_metadata
            file_detail: File detail dictionary containing metadata (s3_bucket, s3_key, etc.)
            parquet_file: Parquet file path (for logging)
        """
        if df is None or len(df) == 0:
            return
        
        try:
            # Extract metadata from file_detail or other sources
            custom_metadata = self._extract_custom_metadata(file_detail, parquet_file)
            
            if not custom_metadata:
                logger.debug(f"No custom metadata to add for {parquet_file}")
                return
            
            # Enrich each row's content_metadata
            enriched_count = 0
            for idx in df.index:
                try:
                    content_metadata = df.at[idx, 'content_metadata']
                    
                    # Handle different content_metadata formats
                    if pd.isna(content_metadata):
                        # Create new metadata dict
                        metadata_dict = custom_metadata.copy()
                    elif isinstance(content_metadata, dict):
                        # Merge with existing metadata
                        metadata_dict = content_metadata.copy()
                        metadata_dict.update(custom_metadata)
                    elif isinstance(content_metadata, str):
                        # Parse JSON string
                        try:
                            metadata_dict = json.loads(content_metadata)
                            metadata_dict.update(custom_metadata)
                        except json.JSONDecodeError:
                            # If parsing fails, create new dict
                            metadata_dict = custom_metadata.copy()
                    else:
                        # Unknown format, create new dict
                        metadata_dict = custom_metadata.copy()
                    
                    # Convert to JSON string for Milvus compatibility
                    # Milvus expects content_metadata as JSON string, not struct/dict
                    df.at[idx, 'content_metadata'] = json.dumps(metadata_dict)
                    enriched_count += 1
                    
                except Exception as e:
                    logger.warning(f"Error enriching metadata for row {idx} in {parquet_file}: {e}")
                    continue
            
            logger.debug(f"Enriched {enriched_count} row(s) in {parquet_file} with custom metadata (as JSON strings): {list(custom_metadata.keys())}")
            
        except Exception as e:
            logger.error(f"Error enriching parquet metadata for {parquet_file}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
    
    _PAGE_SUFFIX_RE = re.compile(r'#page_(\d+)$')
    _PAGES_RANGE_SUFFIX_RE = re.compile(r'#pages_(\d+)-(\d+)$')

    def _fix_v2_page_numbers(self, df):
        """
        Fix page_number metadata for NeMo-Retriever V2 split-page parquet files.

        Handles two split formats produced by NeMo-Retriever V2:

        1. **Single-page splits** (``pdf_split_page_count=1``): each subjob produces
           a ``#page_N`` suffix (1-indexed). The chunk-local page_number is always 1;
           the correct page is extracted directly from the suffix.

        2. **Multi-page splits** (``pdf_split_page_count>1``): each subjob produces
           a ``#pages_X-Y`` suffix indicating the original page range. The chunk-local
           page_number (1-based within the chunk) must be offset by ``X-1`` to recover
           the correct global page number.

        This method:
        1. Extracts or computes the correct 1-indexed global page number
        2. Fixes content_metadata.page_number and hierarchy.page / page_count
        3. Strips the ``#page_N`` or ``#pages_X-Y`` suffix from source_name/source_id
        """
        if df is None or len(df) == 0:
            return

        pat_single = self._PAGE_SUFFIX_RE
        pat_range = self._PAGES_RANGE_SUFFIX_RE

        # --- First pass: collect page numbers per base source ---
        row_info = {}               # idx -> (page_num, base_source)
        base_source_pages = {}      # base_source -> set of page numbers

        for idx in df.index:
            source_raw = df.at[idx, 'source']
            try:
                source = json.loads(source_raw) if isinstance(source_raw, str) else source_raw
            except Exception:
                continue
            if not isinstance(source, dict):
                continue

            source_name = source.get('source_name') or source.get('source_id') or ''

            # Single-page split: #page_N
            m = pat_single.search(source_name)
            if m:
                page_num = int(m.group(1))
                base = source_name[:m.start()]
                row_info[idx] = (page_num, base)
                base_source_pages.setdefault(base, set()).add(page_num)
                continue

            # Multi-page range split: #pages_X-Y
            m = pat_range.search(source_name)
            if m:
                start_page = int(m.group(1))
                base = source_name[:m.start()]

                # Read chunk-local page_number from content_metadata
                cm_raw = df.at[idx, 'content_metadata']
                try:
                    cm = json.loads(cm_raw) if isinstance(cm_raw, str) else cm_raw
                except Exception:
                    cm = {}
                if not isinstance(cm, dict):
                    cm = {}
                local_page = cm.get('page_number', 1)

                page_num = start_page + (local_page - 1)
                row_info[idx] = (page_num, base)
                base_source_pages.setdefault(base, set()).add(page_num)

        if not row_info:
            return

        base_page_count = {b: max(pages) for b, pages in base_source_pages.items()}

        # --- Second pass: fix source and content_metadata ---
        fixed = 0
        for idx, (page_num, base) in row_info.items():
            total_pages = base_page_count[base]

            source_raw = df.at[idx, 'source']
            src_is_str = isinstance(source_raw, str)
            try:
                source = json.loads(source_raw) if src_is_str else source_raw
            except Exception:
                source = None

            if isinstance(source, dict):
                for key in ('source_name', 'source_id'):
                    val = source.get(key)
                    if isinstance(val, str):
                        val = pat_single.sub('', val)
                        val = pat_range.sub('', val)
                        source[key] = val
                df.at[idx, 'source'] = json.dumps(source) if src_is_str else source

            cm_raw = df.at[idx, 'content_metadata']
            cm_is_str = isinstance(cm_raw, str)
            try:
                cm = json.loads(cm_raw) if cm_is_str else cm_raw
            except Exception:
                cm = None

            if isinstance(cm, dict):
                cm['page_number'] = page_num
                hierarchy = cm.get('hierarchy')
                if isinstance(hierarchy, dict):
                    hierarchy['page'] = page_num
                    hierarchy['page_count'] = total_pages
                df.at[idx, 'content_metadata'] = json.dumps(cm) if cm_is_str else cm
                fixed += 1

        if fixed:
            logger.info(
                f"Fixed V2 page numbers for {fixed} row(s) across "
                f"{len(base_source_pages)} document(s)"
            )

    def _extract_custom_metadata(self, file_detail: Optional[Dict] = None, parquet_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract custom metadata from file_detail or other sources.
        
        This method can be customized to extract metadata from:
        - file_detail dictionary (s3_bucket, s3_key, etc.)
        - File path patterns
        - External metadata sources
        - Configuration
        
        Args:
            file_detail: File detail dictionary
            parquet_file: Parquet file path
            
        Returns:
            Dictionary of custom metadata fields to add to content_metadata
        """
        custom_metadata = {}
        
        try:
            if file_detail:
                # Extract metadata from file_detail
                s3_bucket = file_detail.get('s3_bucket', '')
                s3_key = file_detail.get('s3_key', '')
                
                # Example: Add bucket and key as metadata
                if s3_bucket:
                    custom_metadata['source_bucket'] = s3_bucket
                if s3_key:
                    custom_metadata['source_key'] = s3_key
                    
                    # Extract file extension as type
                    file_ext = os.path.splitext(s3_key)[1].lower()
                    if file_ext:
                        custom_metadata['file_type'] = file_ext.lstrip('.')
                    
                    # Extract directory path as category (example)
                    # You can customize this based on your S3 key structure
                    path_parts = s3_key.split('/')
                    if len(path_parts) > 1:
                        custom_metadata['source_path'] = '/'.join(path_parts[:-1])
                        # Use first directory as category
                        custom_metadata['category'] = path_parts[0]
                
                # Add timestamp if available
                if 'timestamp' in file_detail:
                    custom_metadata['ingestion_timestamp'] = file_detail['timestamp']
                else:
                    # Use current timestamp
                    custom_metadata['ingestion_timestamp'] = time.time()
                
                # Add batch_id if available
                if 'batch_id' in file_detail:
                    custom_metadata['batch_id'] = file_detail['batch_id']
                
                # Add S3 tags (if available)
                s3_tags = file_detail.get('s3_tags')
                if s3_tags:
                    # Add all S3 tags to custom_metadata with 's3_tag_' prefix to avoid conflicts
                    for tag_key, tag_value in s3_tags.items():
                        custom_metadata[f's3_tag_{tag_key}'] = tag_value
                    logger.debug(f"Added {len(s3_tags)} S3 tags to metadata")
                
                # Add S3 metadata (user-defined metadata headers, x-amz-meta-*)
                s3_metadata = file_detail.get('s3_metadata')
                if s3_metadata:
                    # Add all S3 metadata to custom_metadata
                    # Note: S3 metadata keys already have 'x-amz-meta-' prefix removed by boto3
                    for meta_key, meta_value in s3_metadata.items():
                        custom_metadata[f's3_meta_{meta_key}'] = meta_value
                    logger.debug(f"Added {len(s3_metadata)} S3 metadata entries to metadata")
                
                # Add any other fields from file_detail
                # Example: custom_metadata['project_id'] = file_detail.get('project_id', '')
                # Example: custom_metadata['department'] = file_detail.get('department', '')
            
            # Extract metadata from parquet_file path
            # Handles both old format: embeddings_{bucket}_{object}/UUID/1.parquet
            # and new format: {file_id}/embeddings_{bucket}_{object}/UUID/1.parquet
            if parquet_file:
                path_parts = parquet_file.split('/')
                embeddings_dir = None
                for part in path_parts:
                    if part.startswith('embeddings_'):
                        embeddings_dir = part
                        break
                if embeddings_dir:
                    parts = embeddings_dir.replace('embeddings_', '').split('_', 1)
                    if len(parts) >= 2:
                        custom_metadata['parquet_bucket'] = parts[0]
                        custom_metadata['parquet_object'] = parts[1]
            
            # Add any static metadata from config
            if self.config_manager:
                storage_config = self.config_manager.get_storage_config()
                if storage_config:
                    # Example: Add environment or deployment info
                    # custom_metadata['environment'] = getattr(storage_config, 'environment', 'production')
                    # custom_metadata['deployment_id'] = getattr(storage_config, 'deployment_id', '')
                    pass
            
            return custom_metadata
            
        except Exception as e:
            logger.warning(f"Error extracting custom metadata: {e}")
            return {}
    
    def _get_subtype(self, row: pd.Series) -> Optional[str]:
        """
        Extract subtype from row's content_metadata or source field.
        
        Based on NeMo-Retriever client API structure:
        - element['metadata']['content_metadata']['subtype'] for table/chart
        
        Args:
            row: Pandas Series (row from DataFrame)
            
        Returns:
            Subtype string or None
        """
        try:
            # Try content_metadata first
            content_metadata = row.get('content_metadata')
            if not pd.isna(content_metadata):
                if isinstance(content_metadata, dict):
                    subtype = content_metadata.get('subtype')
                    if subtype:
                        return subtype
                elif isinstance(content_metadata, str):
                    # Try to parse as JSON
                    try:
                        metadata = json.loads(content_metadata)
                        subtype = metadata.get('subtype')
                        if subtype:
                            return subtype
                    except Exception:
                        pass
            
            # Try source field - might have nested metadata structure
            source = row.get('source')
            if not pd.isna(source):
                if isinstance(source, dict):
                    # Check for nested metadata structure: source['metadata']['content_metadata']['subtype']
                    metadata = source.get('metadata')
                    if isinstance(metadata, dict):
                        content_metadata = metadata.get('content_metadata')
                        if isinstance(content_metadata, dict):
                            subtype = content_metadata.get('subtype')
                            if subtype:
                                return subtype
                elif isinstance(source, str):
                    try:
                        source_dict = json.loads(source)
                        metadata = source_dict.get('metadata')
                        if isinstance(metadata, dict):
                            content_metadata = metadata.get('content_metadata')
                            if isinstance(content_metadata, dict):
                                subtype = content_metadata.get('subtype')
                                if subtype:
                                    return subtype
                    except Exception:
                        pass
            
            return None
        except Exception as e:
            logger.debug(f"Error extracting subtype: {e}")
            return None
    
    def _matches_text_filter(self, row: pd.Series) -> bool:
        """
        Check if row matches text filter.
        
        Text rows are those that:
        1. Have document_type == 'text' (explicit text), OR
        2. Don't have a subtype (not 'table' or 'chart') - these are regular text content
        
        Based on NeMo-Retriever client API structure:
        - Text rows typically don't have a subtype, while tables/charts have subtype='table'/'chart'
        - Some text rows may have document_type == 'text'
        
        Args:
            row: Pandas Series (row from DataFrame)
            
        Returns:
            True if matches text filter
        """
        try:
            # First check if it's explicitly marked as text
            # Structure: {'document_type': 'text', 'metadata': {...}}
            source = row.get('source')
            if not pd.isna(source):
                if isinstance(source, dict):
                    # Check top-level document_type
                    document_type = source.get('document_type')
                    if document_type == 'text':
                        return True
                    # Also check nested in metadata if present
                    metadata = source.get('metadata')
                    if isinstance(metadata, dict):
                        document_type = metadata.get('document_type')
                        if document_type == 'text':
                            return True
                elif isinstance(source, str):
                    try:
                        source_dict = json.loads(source)
                        # Check top-level document_type
                        document_type = source_dict.get('document_type')
                        if document_type == 'text':
                            return True
                        # Also check nested in metadata if present
                        metadata = source_dict.get('metadata')
                        if isinstance(metadata, dict):
                            document_type = metadata.get('document_type')
                            if document_type == 'text':
                                return True
                    except Exception:
                        pass
            
            # Check content_metadata field for type or document_type
            # NeMo-Retriever uses 'type' field in content_metadata: {"type": "text", ...}
            content_metadata = row.get('content_metadata')
            if not pd.isna(content_metadata):
                if isinstance(content_metadata, dict):
                    # Check both 'type' and 'document_type' fields
                    content_type = content_metadata.get('type') or content_metadata.get('document_type')
                    if content_type == 'text':
                        return True
                elif isinstance(content_metadata, str):
                    try:
                        metadata = json.loads(content_metadata)
                        # Check both 'type' and 'document_type' fields
                        content_type = metadata.get('type') or metadata.get('document_type')
                        if content_type == 'text':
                            return True
                    except Exception:
                        pass
            
            # Check if there's a direct document_type column
            document_type = row.get('document_type')
            if not pd.isna(document_type):
                if document_type == 'text':
                    return True
            
            # If no explicit text marker, check if it's NOT a table or chart
            # Text rows are those without a subtype (or with subtype that's not 'table' or 'chart')
            subtype = self._get_subtype(row)
            if subtype is None:
                # No subtype means it's regular text content
                return True
            if subtype not in ['table', 'chart']:
                # Has a subtype but it's not table or chart, so it's text
                return True
            
            # Has subtype 'table' or 'chart', so it's not text
            return False
        except Exception as e:
            logger.debug(f"Error checking text filter: {e}")
            return False
    
    def stop(self, timeout: int = 30):
        """
        Stop completion processing
        
        Args:
            timeout: Timeout for stopping in seconds
        """
        if not self.running:
            logger.warning("Completion handler not running")
            return
        
        logger.info("Stopping completion handler...")
        self.running = False
        logger.info("Completion handler stopped")

