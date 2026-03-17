#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Async job status tracker for monitoring NeMo-Retriever submissions
"""

import logging
import time
import queue
from typing import Dict, List, Optional
from threading import Thread, Event, Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    from nv_ingest_client.client import NvIngestClient
except ImportError as e:
    logging.error(f"Failed to import nv-ingest client: {e}")
    logging.error("Make sure nv-ingest-client is installed: pip install nv-ingest-client")
    raise

# Import telemetry for metrics
try:
    from utils.telemetry import get_metrics
    TELEMETRY_AVAILABLE = True
except ImportError:
    TELEMETRY_AVAILABLE = False


logger = logging.getLogger(__name__)


class JobStatusTracker:
    """Track status of submitted NeMo-Retriever jobs"""
    
    def __init__(self, nemo_retriever_client: NvIngestClient,
                 poll_interval: int = 5, timeout: int = 300,
                 max_retries: int = 3, max_workers: int = 5,
                 completion_queue=None, failure_queue=None):
        """
        Initialize job status tracker
        
        Args:
            nemo_retriever_client: NeMo-Retriever client instance
            poll_interval: Status polling interval in seconds
            timeout: Job timeout in seconds
            max_retries: Maximum number of retries for failed jobs
            max_workers: Maximum number of concurrent status checks
            completion_queue: Queue for completed jobs
            failure_queue: Queue for permanently failed jobs
        """
        self.client = nemo_retriever_client
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.max_retries = max_retries
        self.completion_queue = completion_queue or queue.Queue()
        self.failure_queue = failure_queue or queue.Queue()
        
        # Thread pool for parallel status checking
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Tracking thread
        self.tracking_thread = None
        self.stop_event = Event()
        self.running = False
        
        # Job tracking
        self.active_jobs: Dict[str, Dict] = {}  # job_id -> job_info
        
        # Track jobs that returned empty results (indicating they might be failed)
        # When fetch_job_result returns empty list, we track it here
        # This helps us distinguish between:
        # - Jobs that succeeded (returned results, then disappeared) → completed
        # - Jobs that failed (returned empty results, then disappeared) → failed
        self.jobs_with_empty_results: Dict[str, float] = {}  # job_id -> timestamp when first seen with empty results
        
        # Completion tracking to prevent duplicate events
        self.completed_jobs = set()  # Track job_id+batch_id that have been completed
        self.completed_batches = set()  # Track batch_id for cleanup purposes (not used for deduplication - we process files individually)
        self.batch_jobs = {}  # Track batch_id -> set of job_ids in that batch
        self.batch_completed_jobs = {}  # Track batch_id -> set of completed job_ids
        self.completion_times = {}  # Track when jobs were completed
        self.completion_lock = Lock()
        
        # Validation failure pattern tracking
        # Track jobs that failed validation to detect patterns
        # Key: job signature (e.g., document_type/file_extension), Value: failure count
        # Validation failures are deterministic - if one job fails, all similar jobs will fail
        self.validation_failure_patterns: Dict[str, int] = {}
        self.validation_failure_lock = Lock()  # Lock for thread-safe pattern tracking  # Thread-safe access
        
        logger.info(f"JobStatusTracker initialized: poll_interval={poll_interval}s, "
                   f"timeout={timeout}s")
    
    def clear_active_jobs(self):
        """Clear all active jobs (useful after restart)"""
        cleared_count = len(self.active_jobs)
        self.active_jobs.clear()
        logger.info(f"Cleared {cleared_count} active jobs from tracker")
    
    def cleanup_completed_jobs(self):
        """Periodically clean up completed jobs tracking to prevent memory leaks"""
        with self.completion_lock:
            current_time = time.time()
            # Remove jobs completed more than 1 hour ago
            old_jobs = {
                job_batch_key for job_batch_key, completed_time in self.completion_times.items()
                if current_time - completed_time > 3600  # 1 hour
            }
            
            if old_jobs:
                self.completed_jobs -= old_jobs
                for job_batch_key in old_jobs:
                    del self.completion_times[job_batch_key]
                logger.debug(f"Cleaned up {len(old_jobs)} old completed jobs from tracking")
            
            # Also clean up old completed batches tracking (keep for 1 hour)
            # Note: We don't track batch completion times, so we'll clean up all batch tracking
            # that is older than the oldest job completion time
            if self.completion_times:
                oldest_completion = min(self.completion_times.values())
                if current_time - oldest_completion > 3600:  # 1 hour
                    # Clear all completed batches if oldest job is older than 1 hour
                    old_batch_count = len(self.completed_batches)
                    self.completed_batches.clear()
                    if old_batch_count > 0:
                        logger.debug(f"Cleaned up {old_batch_count} old completed batches from tracking")
    
    def add_job(self, job_id: str, batch_id: str, file_id: str, file_detail: Dict, trace_id: str):
        """
        Add job to tracking
        
        Args:
            job_id: Client-side job index (e.g., "0", "1", "2")
            batch_id: Batch ID
            file_id: File ID for this job (one job corresponds to one file)
            file_detail: File detail dictionary (s3_bucket, s3_key, kafka_offset, etc.)
            trace_id: Trace ID returned from submit_job
        
        Raises:
            ValueError: If trace_id or file_detail is None or empty
        """
        if not trace_id:
            raise ValueError(f"trace_id is required for job {job_id} in batch {batch_id}. Cannot track job without trace_id.")
        self.active_jobs[job_id] = {
            'job_id': job_id,  # Client-side job index
            'server_job_id': None,  # Server-assigned job ID (updated when we get first result)
            'trace_id': trace_id,  # Trace ID from submission
            'batch_id': batch_id,
            'file_id': file_id,  # Single file ID (one job = one file)
            'file_detail': file_detail,  # Single file detail dict
            'status': 'pending',
            'submit_time': time.time(),
            'created_at': time.time(),
            'retry_count': 0,
            'check_count': 0,
            'validation_error_checks': 0,  # Track how many times we've seen "does not exist in JobStates" error
            'ever_found_in_jobstates': False  # Track if we've ever successfully found this job in JobStates
        }
        
        # Track jobs per batch
        if batch_id not in self.batch_jobs:
            self.batch_jobs[batch_id] = set()
        self.batch_jobs[batch_id].add(job_id)
        
        # Initialize completed jobs tracking for this batch
        if batch_id not in self.batch_completed_jobs:
            self.batch_completed_jobs[batch_id] = set()
        
        logger.debug(f"Added job {job_id} to tracking (file: {file_id}, trace_id: {trace_id}, batch: {batch_id})")
    
    def _get_job_signature(self, job_info: Dict) -> str:
        """
        Get a signature for a job to track validation failure patterns.
        
        Validation failures are deterministic - if one job with a specific structure fails,
        all similar jobs will fail. We use this to fail fast for similar jobs.
        
        Args:
            job_info: Job tracking dictionary
            
        Returns:
            Job signature string (e.g., "pdf:ExtractTask,SplitTask,EmbedTask")
        """
        # Try to get document type and task types from file detail
        file_detail = job_info.get('file_detail', {})
        if file_detail:
            # Get document type from file's extension
            s3_key = file_detail.get('s3_key', '')
            if s3_key:
                file_ext = Path(s3_key).suffix.lower().lstrip('.')
                # For now, use file extension as signature
                # In the future, we could include task types if available
                return f"{file_ext}"
        
        # Fallback: use a generic signature
        return "unknown"
    
    def check_job_status(self, job_id: str) -> Optional[Dict]:
        """
        Check status of a single job
        
        Handles HTTP status codes from NeMo-Retriever server:
        - 200: Job completed successfully (returns result)
        - 202: Job still processing (returns None to retry later)
        - 404: Job not found or expired (distinguishes validation failure vs completed/expired)
        - 410: Job result expired/retrieved (treats as completed)
        - 503: Job processing failed STATE_FAILED (returns failure with error message)
        
        Args:
            job_id: Job ID to check
        
        Returns:
            Job result dictionary with status, or None if still processing
            Dictionary format:
            - status: 'completed', 'failed', 'timeout', or 'error'
            - result: Result data (for completed jobs) or None
            - error: Error message (for failed jobs)
            - note: Additional notes (for special cases like HTTP 410)
        """
        try:
            # Increment check count
            if job_id in self.active_jobs:
                self.active_jobs[job_id]['check_count'] = self.active_jobs[job_id].get('check_count', 0) + 1
            
            logger.debug(f"Checking status for job {job_id}")
            
            # Fetch job result with error details
            # Use fetch_job_result with return_failures=True to get error messages when jobs fail
            # This gives us better error information than fetch_job_result_cli
            # fetch_job_result returns:
            #   - If return_failures=False: List[Tuple[Optional[Dict], str]] (result, job_id)
            #   - If return_failures=True: Tuple[List[Tuple[Optional[Dict], str]], List[Tuple[str, str]]]
            #     where the second list contains (job_id, error_message) tuples
            try:
                # First try with return_failures=True to get error details
                result_with_failures = self.client.fetch_job_result(
                    job_id, 
                    timeout=100,  # HTTP timeout per request
                    return_failures=True
                )
                
                # Unpack the result
                if isinstance(result_with_failures, tuple) and len(result_with_failures) == 2:
                    successful_results, failed_results = result_with_failures
                    
                    # Check if this job failed
                    for failed_job_id, error_message in failed_results:
                        if failed_job_id == job_id:
                            logger.error(f"Job {job_id} failed with error: {error_message}")
                            # Record metrics for failed job (likely HTTP 503)
                            if TELEMETRY_AVAILABLE:
                                metrics = get_metrics()
                                if metrics and metrics.enabled:
                                    metrics.record_job_status_code(503, error_type="processing_failure")
                            return {
                                'job_id': job_id,
                                'status': 'failed',
                                'error': error_message,
                                'result': None
                            }
                    
                    # Job succeeded, get the result
                    if successful_results and len(successful_results) > 0:
                        result_tuple = successful_results[0]
                        if isinstance(result_tuple, tuple) and len(result_tuple) >= 1:
                            result = result_tuple[0]  # Extract the result dict
                        else:
                            result = result_tuple
                    else:
                        # No result yet, job still processing
                        logger.debug(f"Job {job_id} still processing (no result yet)")
                        return None
                else:
                    # Fallback: result format is different, try to extract
                    logger.debug(f"Unexpected result format from fetch_job_result: {type(result_with_failures)}")
                    if isinstance(result_with_failures, list) and len(result_with_failures) > 0:
                        result_tuple = result_with_failures[0]
                        if isinstance(result_tuple, tuple) and len(result_tuple) >= 1:
                            result = result_tuple[0]
                        else:
                            result = result_tuple
                    else:
                        logger.debug(f"Job {job_id} still processing")
                        return None
                        
            except TimeoutError:
                # HTTP 202: Job still processing - this is expected, return None to check again later
                logger.debug(f"Job {job_id} not ready yet (HTTP 202 - still processing)")
                # Record metrics for HTTP 202
                if TELEMETRY_AVAILABLE:
                    metrics = get_metrics()
                    if metrics and metrics.enabled:
                        metrics.record_job_status_code(202, error_type="none")
                return None
            except Exception as fetch_error:
                # Handle different HTTP status codes from fetch_job_result
                error_str = str(fetch_error)
                error_type = type(fetch_error).__name__
                
                # Try to extract HTTP status code from exception
                # Check multiple possible attributes where status code might be stored
                status_code = None
                if hasattr(fetch_error, 'status_code'):
                    status_code = fetch_error.status_code
                elif hasattr(fetch_error, 'code'):
                    status_code = fetch_error.code
                elif hasattr(fetch_error, 'response') and hasattr(fetch_error.response, 'status_code'):
                    status_code = fetch_error.response.status_code
                elif hasattr(fetch_error, 'status'):
                    status_code = fetch_error.status
                
                # Also try to extract status code from error message (e.g., "HTTP 404" or "404")
                if status_code is None:
                    import re
                    # Look for patterns like "HTTP 404", "404", "status 404", etc.
                    status_match = re.search(r'(?:HTTP|status|code)[\s:]*(\d{3})', error_str, re.IGNORECASE)
                    if status_match:
                        try:
                            status_code = int(status_match.group(1))
                        except ValueError:
                            pass
                    
                    # Also check for standalone 3-digit numbers that look like status codes
                    if status_code is None:
                        status_match = re.search(r'\b(200|202|404|410|503)\b', error_str)
                        if status_match:
                            try:
                                status_code = int(status_match.group(1))
                            except ValueError:
                                pass
                
                # Handle based on HTTP status code
                if status_code == 200:
                    # Job completed successfully - should have been handled above, but handle here as fallback
                    logger.debug(f"Job {job_id} completed successfully (HTTP 200)")
                    # Record metrics for HTTP 200
                    if TELEMETRY_AVAILABLE:
                        metrics = get_metrics()
                        if metrics and metrics.enabled:
                            metrics.record_job_status_code(200, error_type="none")
                    # This shouldn't happen as 200 should return results, but handle gracefully
                    return None
                
                elif status_code == 202:
                    # Job still processing - return None to check again later
                    logger.debug(f"Job {job_id} still processing (HTTP 202)")
                    # Record metrics for HTTP 202
                    if TELEMETRY_AVAILABLE:
                        metrics = get_metrics()
                        if metrics and metrics.enabled:
                            metrics.record_job_status_code(202, error_type="none")
                    return None
                
                elif status_code == 404:
                    # HTTP 404: Job not found or expired
                    # Need to distinguish between:
                    # 1. Processing failure (job failed during processing and was removed)
                    # 2. Validation failure (job never entered JobStates)
                    # 3. Job expired/cleaned up after completion
                    
                    # First check if this is a processing failure (error message indicates failure)
                    if ("failed" in error_str.lower() or 
                        "Error while processing job" in error_str or 
                        "Failed to process" in error_str or
                        "Embedding error" in error_str or
                        "hibernated endpoint" in error_str.lower()):
                        logger.error(f"Job {job_id} failed during processing (HTTP 404 with failure indicator): {error_str}")
                        # Record metrics for processing failure
                        if TELEMETRY_AVAILABLE:
                            metrics = get_metrics()
                            if metrics and metrics.enabled:
                                metrics.record_job_status_code(404, error_type="processing_failure")
                        return {
                            'job_id': job_id,
                            'status': 'failed',
                            'error': f'Job failed during processing (HTTP 404): {error_str}',
                            'result': None
                        }
                    
                    if job_id in self.active_jobs:
                        job_info = self.active_jobs[job_id]
                        check_count = job_info.get('check_count', 0)
                        ever_found = job_info.get('ever_found_in_jobstates', False)
                        
                        if ever_found:
                            # Job was found before but now gone - likely completed and cleaned up
                            logger.info(f"Job {job_id} not found (HTTP 404) but was previously in JobStates - likely completed and cleaned up")
                            # Record metrics for HTTP 404 (completed/expired)
                            if TELEMETRY_AVAILABLE:
                                metrics = get_metrics()
                                if metrics and metrics.enabled:
                                    metrics.record_job_status_code(404, error_type="expired")
                            return {
                                'job_id': job_id,
                                'status': 'completed',
                                'result': 'Job completed and result was cleaned up from JobStates',
                                'note': 'Job was found in JobStates previously but is now gone (HTTP 404)'
                            }
                        elif check_count >= 10:
                            # Job never found after many checks - likely validation failure
                            logger.warning(f"Job {job_id} not found (HTTP 404) after {check_count} checks - likely validation failure")
                            # Record metrics for HTTP 404 (validation failure)
                            if TELEMETRY_AVAILABLE:
                                metrics = get_metrics()
                                if metrics and metrics.enabled:
                                    metrics.record_job_status_code(404, error_type="validation_failure")
                            return {
                                'job_id': job_id,
                                'status': 'failed',
                                'error': f'Job not found (HTTP 404) after {check_count} checks. '
                                       f'This likely indicates a validation failure on the server. '
                                       f'Job was queued but rejected during validation and never entered JobStates. '
                                       f'Check NeMo-Retriever server logs for validation errors.',
                                'result': None
                            }
                        else:
                            # Not enough checks yet - might still be in queue
                            logger.debug(f"Job {job_id} not found (HTTP 404) after {check_count} checks - will retry")
                            # Record metrics for HTTP 404 (retrying)
                            if TELEMETRY_AVAILABLE:
                                metrics = get_metrics()
                                if metrics and metrics.enabled:
                                    metrics.record_job_status_code(404, error_type="not_found")
                            return None
                    else:
                        # Job not in tracking - treat as expired/not found
                        logger.warning(f"Job {job_id} not found (HTTP 404) and not in active tracking")
                        # Record metrics for HTTP 404 (not found)
                        if TELEMETRY_AVAILABLE:
                            metrics = get_metrics()
                            if metrics and metrics.enabled:
                                metrics.record_job_status_code(404, error_type="not_found")
                        return {
                            'job_id': job_id,
                            'status': 'failed',
                            'error': f'Job not found (HTTP 404): {error_str}',
                            'result': None
                        }
                
                elif status_code == 410:
                    # HTTP 410: Job result expired/retrieved
                    # Result was already retrieved or expired - treat as completed
                    logger.warning(f"Job {job_id} result expired or already retrieved (HTTP 410)")
                    # Record metrics for HTTP 410
                    if TELEMETRY_AVAILABLE:
                        metrics = get_metrics()
                        if metrics and metrics.enabled:
                            metrics.record_job_status_code(410, error_type="expired")
                    return {
                        'job_id': job_id,
                        'status': 'completed',
                        'result': 'Job result was expired or already retrieved',
                        'note': 'HTTP 410: Result was cleaned up or already retrieved'
                    }
                
                elif status_code == 503:
                    # HTTP 503: Job processing failed (STATE_FAILED)
                    # Extract error message from exception
                    logger.error(f"Job {job_id} processing failed (HTTP 503 - STATE_FAILED): {error_str}")
                    # Record metrics for HTTP 503
                    if TELEMETRY_AVAILABLE:
                        metrics = get_metrics()
                        if metrics and metrics.enabled:
                            metrics.record_job_status_code(503, error_type="processing_failure")
                    return {
                        'job_id': job_id,
                        'status': 'failed',
                        'error': f'Job processing failed (HTTP 503 - STATE_FAILED): {error_str}',
                        'result': None
                    }
                
                # If no status code but error message indicates failure
                elif "does not exist in JobStates" in error_str or "JobStates" in error_str:
                    # Check if we've seen this job fail before (tracked in jobs_with_empty_results)
                    if job_id in self.active_jobs:
                        job_info = self.active_jobs[job_id]
                        check_count = job_info.get('check_count', 0)
                        ever_found = job_info.get('ever_found_in_jobstates', False)
                        
                        if job_id in self.jobs_with_empty_results:
                            # We saw empty results before - this is a processing failure
                            logger.error(f"Job {job_id} failed during processing and was removed from JobStates: {error_str}")
                            # Record metrics for processing failure (likely HTTP 503)
                            if TELEMETRY_AVAILABLE:
                                metrics = get_metrics()
                                if metrics and metrics.enabled:
                                    metrics.record_job_status_code(503, error_type="processing_failure")
                            return {
                                'job_id': job_id,
                                'status': 'failed',
                                'error': f'Job failed during processing and was removed from JobStates. '
                                       f'Check NeMo-Retriever server logs for detailed error message. '
                                       f'Original exception: {error_str}',
                                'result': None
                            }
                        elif ever_found:
                            # Job was found before but now gone - likely completed
                            logger.info(f"Job {job_id} was previously in JobStates but is now gone - likely completed")
                            # Record metrics for completed job (likely HTTP 200, but now 404)
                            if TELEMETRY_AVAILABLE:
                                metrics = get_metrics()
                                if metrics and metrics.enabled:
                                    metrics.record_job_status_code(404, error_type="expired")
                            return {
                                'job_id': job_id,
                                'status': 'completed',
                                'result': 'Job completed and cleaned up from JobStates',
                                'note': 'Job was found in JobStates previously but is now gone'
                            }
                        elif check_count >= 10:
                            # Job never found after many checks - likely validation failure
                            logger.warning(f"Job {job_id} does not exist in JobStates after {check_count} checks - likely validation failure")
                            # Record metrics for validation failure (likely HTTP 404)
                            if TELEMETRY_AVAILABLE:
                                metrics = get_metrics()
                                if metrics and metrics.enabled:
                                    metrics.record_job_status_code(404, error_type="validation_failure")
                            return {
                                'job_id': job_id,
                                'status': 'failed',
                                'error': f'Job validation failed: Job was queued but rejected during validation '
                                       f'and never entered JobStates after {check_count} checks. '
                                       f'This may indicate a schema mismatch. '
                                       f'Check NeMo-Retriever server logs for detailed validation errors.',
                                'result': None
                            }
                        else:
                            # Not enough checks yet - re-raise to be handled by outer exception handler
                            raise
                    else:
                        # Re-raise to be handled by outer exception handler
                        raise
                
                # Check if the error message contains failure indicators
                elif "failed" in error_str.lower() or "Error while processing job" in error_str or "Failed to process" in error_str:
                    logger.error(f"Job {job_id} failed (detected from exception message): {error_str}")
                    # Record metrics for processing failure (likely HTTP 503)
                    if TELEMETRY_AVAILABLE:
                        metrics = get_metrics()
                        if metrics and metrics.enabled:
                            metrics.record_job_status_code(503, error_type="processing_failure")
                    return {
                        'job_id': job_id,
                        'status': 'failed',
                        'error': error_str,
                        'result': None
                    }
                
                # Other exceptions - log and re-raise
                logger.warning(f"Unexpected exception from fetch_job_result for job {job_id} ({error_type}, status_code={status_code}): {fetch_error}")
                raise
            
            # Extract data from result dict (similar to data_only=True in fetch_job_result_cli)
            # fetch_job_result returns the full result dict, so we need to extract the 'data' field
            if result and isinstance(result, dict):
                # Extract 'data' field if it exists (this contains the actual rows)
                if 'data' in result:
                    result_data = result['data']
                    logger.debug(f"Job {job_id} result contains 'data' field with {len(result_data) if isinstance(result_data, list) else 'non-list'} items")
                else:
                    # No 'data' field, use the full result
                    result_data = result
                    logger.debug(f"Job {job_id} result does not contain 'data' field, using full result")
            else:
                result_data = result
            
            # Update tracking - mark that we found the job in JobStates
            if job_id in self.active_jobs:
                job_info = self.active_jobs[job_id]
                if not job_info.get('ever_found_in_jobstates', False):
                    job_info['ever_found_in_jobstates'] = True
                    logger.debug(f"Job {job_id} found in JobStates")
            
            # If we got here, the job succeeded (failures are handled above)
            # Process the successful result
            logger.info(f"Job {job_id} completed successfully")
            # Record metrics for successful completion (HTTP 200)
            if TELEMETRY_AVAILABLE:
                metrics = get_metrics()
                if metrics and metrics.enabled:
                    metrics.record_job_status_code(200, error_type="none")
            
            if isinstance(result_data, (list, tuple)):
                logger.debug(f"Job {job_id} result contains {len(result_data)} row(s)")
                if len(result_data) > 0 and isinstance(result_data[0], dict):
                    logger.debug(f"Job {job_id} first row keys: {list(result_data[0].keys())[:10]}")
            else:
                logger.debug(f"Job {job_id} result type: {type(result_data)}")
            
            return {
                'job_id': job_id,
                'status': 'completed',
                'result': result_data,  # Return the data list
                'row_count': len(result_data) if isinstance(result_data, (list, tuple)) else 0
            }
        
        except TimeoutError:
            logger.warning(f"Job {job_id} timed out after {self.timeout}s")
            return {
                'job_id': job_id,
                'status': 'timeout',
                'error': 'Job timeout'
            }
        except KeyError as e:
            # Job doesn't exist in the client's job states
            # This could mean:
            # 1. Job completed successfully and was cleaned up
            # 2. Job failed and was cleaned up (failed jobs are removed from job states)
            # 
            # CRITICAL: We can only distinguish between these by checking if we saw empty results
            # If we saw empty results before the job disappeared → it's likely failed
            # If we never saw empty results → it's likely completed (returned results, then disappeared)
            error_str = str(e)
            if job_id in self.active_jobs:
                job_info = self.active_jobs[job_id]
                check_count = job_info.get('check_count', 0)
                job_age = time.time() - job_info.get('created_at', time.time())
                
                # Check if the error message indicates a processing failure
                # Sometimes fetch_job_result logs an error before raising KeyError
                # We check the exception message and also look for failure indicators
                if ("Error while processing job" in error_str or 
                    "Failed to process" in error_str or
                    "Embedding error" in error_str or
                    "hibernated endpoint" in error_str.lower() or
                    "failed" in error_str.lower()):
                    logger.error(f"Job {job_id} failed during processing and was removed from JobStates: {error_str}")
                    # Record metrics for processing failure
                    if TELEMETRY_AVAILABLE:
                        metrics = get_metrics()
                        if metrics and metrics.enabled:
                            metrics.record_job_status_code(404, error_type="processing_failure")
                    return {
                        'job_id': job_id,
                        'status': 'failed',
                        'error': f'Job failed during processing and was removed from JobStates: {error_str}',
                        'result': None
                    }
                
                # Only treat as failed if we actually saw empty results before it disappeared
                # This distinguishes between:
                # - Successful jobs: return results → disappear → completed
                # - Failed jobs: return empty results → disappear → failed
                if job_id in self.jobs_with_empty_results:
                    # We saw empty results, so this job likely failed
                    logger.warning(f"Job {job_id} disappeared from JobStates after {check_count} checks over {job_age:.1f}s with empty results - treating as failed")
                    return {
                        'job_id': job_id,
                        'status': 'failed',
                        'error': f'Job disappeared after {check_count} checks with empty results - likely failed',
                        'result': None
                    }
                else:
                    # We never saw empty results, so this job likely completed successfully
                    logger.info(f"Job {job_id} not found in job states after {check_count} checks - likely completed successfully (no empty results seen)")
                    return {
                        'job_id': job_id,
                        'status': 'completed',
                        'result': 'Job completed and cleaned up from client state'
                    }
            
            # Job not in active_jobs - this shouldn't happen, but treat as completed
            logger.info(f"Job {job_id} not found in job states - likely completed and cleaned up")
            return {
                'job_id': job_id,
                'status': 'completed',
                'result': 'Job completed and cleaned up from client state'
            }
        
        except Exception as e:
            error_msg = str(e)
            # Check if job doesn't exist in JobStates (common when job_id is invalid or validation failed)
            if "does not exist in JobStates" in error_msg or "JobStates" in error_msg:
                # Check if this is a tracked job
                if job_id in self.active_jobs:
                    job_info = self.active_jobs[job_id]
                    check_count = job_info.get('check_count', 0)
                    ever_found = job_info.get('ever_found_in_jobstates', False)
                    submit_time = job_info.get('submit_time', job_info.get('created_at', time.time()))
                    time_since_submit = time.time() - submit_time
                    
                    # Track that we've seen this error for this job
                    if 'validation_error_checks' not in job_info:
                        job_info['validation_error_checks'] = 0
                    job_info['validation_error_checks'] += 1
                    
                    # Determine failure reason based on whether we've ever found the job in JobStates
                    if ever_found:
                        # Job was previously found in JobStates but now gone - likely completed/failed and cleaned up
                        logger.info(f"Job {job_id} (server_job_id: {job_info.get('server_job_id', 'unknown')}) "
                                  f"was previously found in JobStates but is now gone after {check_count} checks. "
                                  f"This likely means the job completed (successfully or with failure) and was cleaned up. "
                                  f"Since we can't retrieve the final status, treating as completed.")
                        return {
                            'job_id': job_id,
                            'status': 'completed',
                            'result': 'Job was processed and cleaned up from JobStates (unable to retrieve final status)',
                            'note': 'Job was found in JobStates previously but is now gone, indicating it was processed and cleaned up'
                        }
                    else:
                        # Job was never found in JobStates - could be validation failure or still in queue
                        # Get job signature for pattern tracking (document_type + task types)
                        job_signature = self._get_job_signature(job_info)
                        
                        # Track validation failure patterns for monitoring/logging purposes
                        # Note: We don't fail fast for similar jobs - we let each job be checked individually
                        # This allows the system to continue processing even if some jobs fail validation
                        with self.validation_failure_lock:
                            pattern_failure_count = self.validation_failure_patterns.get(job_signature, 0)
                        
                        # If we've checked multiple times and job still not in JobStates, likely validation failure
                        # Note: We don't use time-based checks because queue processing time varies
                        # We use check_count because it represents actual polling attempts
                        # Threshold is set to 10 to account for queue delays and server load
                        if check_count >= 10 and job_info['validation_error_checks'] >= 10:
                            # Track this as a validation failure pattern for monitoring
                            with self.validation_failure_lock:
                                self.validation_failure_patterns[job_signature] = pattern_failure_count + 1
                            
                            # Log warning if we're seeing a pattern of validation failures
                            if pattern_failure_count >= 10:
                                logger.error(f"VALIDATION ERROR PATTERN DETECTED: Job {job_id} (pattern: {job_signature}) "
                                           f"does not exist in NeMo-Retriever JobStates after {check_count} checks. "
                                           f"This pattern has now failed {pattern_failure_count + 1} times. "
                                           f"This strongly indicates a validation failure on the server. "
                                           f"All jobs with this pattern (document_type/task structure) are likely failing validation. "
                                           f"Check NeMo-Retriever server logs for validation errors - likely schema mismatch "
                                           f"(e.g., tracing_options.ts_send format, missing required fields, etc.). "
                                           f"Jobs will continue to be processed and failed jobs will be added to dead letter queue.")
                            else:
                                logger.warning(f"Job {job_id} does not exist in NeMo-Retriever JobStates after {check_count} checks "
                                            f"({job_info['validation_error_checks']} validation error checks). "
                                            f"This likely indicates a validation failure on the server. "
                                            f"Pattern '{job_signature}' now has {pattern_failure_count + 1} failures.")
                            
                            return {
                                'job_id': job_id,
                                'status': 'failed',
                                'error': f'Job validation failed on server: Job was queued but rejected during validation '
                                       f'and never entered JobStates after {check_count} checks. '
                                       f'This may indicate a schema mismatch (e.g., tracing_options.ts_send format, missing required fields). '
                                       f'Check NeMo-Retriever server logs for detailed validation errors.',
                                'result': None
                            }
                        else:
                            # Not enough checks yet - job might still be in queue or processing
                            # Return None to check again (will retry)
                            logger.debug(f"Job {job_id} not found in JobStates (check {check_count}, "
                                       f"{job_info['validation_error_checks']} validation error checks). "
                                       f"Will retry - job may still be in queue or processing.")
                            return None
                else:
                    # Job not in active_jobs tracking - shouldn't happen, but treat as failed
                    logger.warning(f"Job {job_id} does not exist in NeMo-Retriever JobStates and is not in active tracking.")
                    return {
                        'job_id': job_id,
                        'status': 'failed',
                        'error': f'Job does not exist in NeMo-Retriever: {error_msg}',
                        'result': None
                    }
            logger.error(f"Error checking job {job_id}: {e}")
            return {
                'job_id': job_id,
                'status': 'error',
                'error': error_msg
            }
    
    def process_job_result(self, job_id: str, result: Dict):
        """
        Process job result and update state with completion deduplication
        
        Args:
            job_id: Job ID
            result: Result dictionary
        """
        status = result.get('status', 'unknown')
        
        if status == 'completed':
            # Get job info first to access batch_id
            if job_id not in self.active_jobs:
                logger.warning(f"Received result for unknown job {job_id}")
                return
            
            job_info = self.active_jobs[job_id]
            batch_id = job_info['batch_id']
            job_batch_key = f"{job_id}:{batch_id}"  # Unique key for job+batch
            
            # CRITICAL: Mark job as completed FIRST (atomic operation)
            # This prevents multiple threads from processing the same completion
            with self.completion_lock:
                if job_batch_key in self.completed_jobs:
                    logger.debug(f"Job {job_id} (batch {batch_id}) already completed by another thread, skipping duplicate processing")
                    return
                
                # Mark as completed immediately to prevent race conditions
                self.completed_jobs.add(job_batch_key)
                self.completion_times[job_batch_key] = time.time()
                job_info = job_info.copy()  # Copy before removing
                del self.active_jobs[job_id]  # Remove from active jobs
                
                # Track completed job in batch
                if batch_id in self.batch_jobs:
                    self.batch_jobs[batch_id].discard(job_id)
                    # Clean up empty batches
                    if not self.batch_jobs[batch_id]:
                        del self.batch_jobs[batch_id]
                        if batch_id in self.batch_completed_jobs:
                            del self.batch_completed_jobs[batch_id]
            
            # Now process completion (only one thread will reach here)
            logger.info(f"Job {job_id} completed successfully")
            
            # Track this job as completed for the batch
            with self.completion_lock:
                # Initialize batch_completed_jobs if it doesn't exist
                if batch_id not in self.batch_completed_jobs:
                    self.batch_completed_jobs[batch_id] = set()
                self.batch_completed_jobs[batch_id].add(job_id)
            
            # Add to completion queue - each job sends its own completion event
            # Each job corresponds to one file, so the completion handler processes each file independently
            # File-level deduplication (preventing reprocessing same parquet files) happens in the completion handler
            submit_time = job_info.get('submit_time') or job_info.get('created_at')
            nemo_retriever_duration_sec = round(time.time() - submit_time, 3) if submit_time else None
            self.completion_queue.put({
                'job_id': job_id,
                'batch_id': job_info['batch_id'],
                'file_id': job_info['file_id'],
                'file_detail': job_info.get('file_detail', {}),
                'trace_id': job_info.get('trace_id'),  # Pass trace_id for debugging/correlation with NeMo-Retriever server logs
                'result': result.get('result'),
                'nemo_retriever_duration_sec': nemo_retriever_duration_sec,
            })
        
        elif status in ['failed', 'timeout', 'error']:
            # Job failed - need to get job_info from active_jobs
            if job_id not in self.active_jobs:
                logger.warning(f"Received error result for unknown job {job_id}")
                return
            
            job_info = self.active_jobs[job_id]
            retry_count = job_info['retry_count']
            
            if retry_count < self.max_retries:
                # Retry the job
                job_info['retry_count'] += 1
                logger.warning(f"Job {job_id} failed, retrying "
                             f"({retry_count + 1}/{self.max_retries})")
                
                # Retry logic not implemented - mark as failed
                job_info['status'] = 'retry_pending'
            
            else:
                # Max retries exceeded
                logger.error(f"Job {job_id} permanently failed after {retry_count} retries")
                
                # Get error message from result
                error_message = result.get('error', 'Unknown error')
                if not error_message or error_message == 'Unknown error':
                    # Try to get more detailed error from result
                    error_message = str(result.get('result', {}).get('error', 'Job failed after max retries'))
                
                # If still no error message, try to extract from result structure
                if not error_message or error_message == 'Unknown error':
                    # NeMo-Retriever error format: result['result'] might contain error details
                    result_data = result.get('result', {})
                    if isinstance(result_data, dict):
                        error_message = str(result_data.get('error', result_data.get('message', 'Job failed')))
                    elif isinstance(result_data, str):
                        error_message = result_data
                    else:
                        error_message = f"Job failed with status: {status}"
                
                # Add to failure queue for dead letter queue processing
                self.failure_queue.put({
                    'job_id': job_id,
                    'batch_id': job_info['batch_id'],
                    'file_id': job_info['file_id'],
                    'file_detail': job_info.get('file_detail', {}),
                    'trace_id': job_info.get('trace_id'),  # Pass trace_id for debugging/correlation with NeMo-Retriever server logs
                    'error': error_message,
                    'retry_count': retry_count
                })
                
                logger.info(f"Added job {job_id} to failure queue for dead letter processing")
                
                # Clean up batch tracking
                batch_id = job_info['batch_id']
                if batch_id in self.batch_jobs:
                    self.batch_jobs[batch_id].discard(job_id)
                    # Clean up empty batches
                    if not self.batch_jobs[batch_id]:
                        del self.batch_jobs[batch_id]
                        if batch_id in self.batch_completed_jobs:
                            del self.batch_completed_jobs[batch_id]
                
                # Remove from active jobs
                del self.active_jobs[job_id]
    
    def tracking_loop(self):
        """Main tracking loop"""
        logger.info("Status tracking loop started")
        
        while not self.stop_event.is_set():
            try:
                if not self.active_jobs:
                    # No jobs to track, sleep and continue
                    time.sleep(self.poll_interval)
                    continue
                
                logger.debug(f"Checking status for {len(self.active_jobs)} active jobs")
                
                # Check status for all active jobs
                futures = {}
                for job_id in list(self.active_jobs.keys()):
                    future = self.executor.submit(self.check_job_status, job_id)
                    futures[future] = job_id
                
                # Process results as they complete
                for future in as_completed(futures):
                    job_id = futures[future]
                    try:
                        result = future.result()
                        if result:
                            self.process_job_result(job_id, result)
                    except Exception as e:
                        logger.error(f"Error processing result for job {job_id}: {e}", exc_info=True)
                
                # Cleanup old completed jobs more frequently to prevent memory buildup
                # Run cleanup every 5 minutes (instead of 10) when there are many active jobs
                if len(self.active_jobs) > 100:
                    # More frequent cleanup when under memory pressure
                    self.cleanup_completed_jobs()
                
                # Also cleanup jobs_with_empty_results periodically
                current_time = time.time()
                old_empty_jobs = {
                    job_id for job_id, timestamp in self.jobs_with_empty_results.items()
                    if current_time - timestamp > 3600  # 1 hour
                }
                for job_id in old_empty_jobs:
                    del self.jobs_with_empty_results[job_id]
                
                # Sleep before next poll
                time.sleep(self.poll_interval)
            
            except Exception as e:
                logger.error(f"Error in tracking loop: {e}")
                time.sleep(self.poll_interval)
        
        logger.info("Status tracking loop stopped")
    
    def start(self):
        """Start status tracking"""
        if self.running:
            logger.warning("Status tracker already running")
            return
        
        self.running = True
        self.stop_event.clear()
        
        self.tracking_thread = Thread(target=self.tracking_loop, daemon=True)
        self.tracking_thread.start()
        
        logger.info("Status tracker started")
    
    def stop(self, timeout: int = 30):
        """
        Stop status tracking
        
        Args:
            timeout: Timeout for stopping in seconds
        """
        if not self.running:
            logger.warning("Status tracker not running")
            return
        
        logger.info("Stopping status tracker...")
        self.stop_event.set()
        
        if self.tracking_thread:
            self.tracking_thread.join(timeout=timeout)
        
        self.running = False
        logger.info("Status tracker stopped")
    
    def shutdown(self, wait: bool = True, timeout: int = 30):
        """
        Shutdown the tracker
        
        Args:
            wait: Whether to wait for pending tasks
            timeout: Timeout for shutdown in seconds
        """
        self.stop(timeout=timeout)
        self.executor.shutdown(wait=wait, timeout=timeout)
    
    def count_active_files(self) -> int:
        """
        Count total number of files across all active jobs.
        
        Each job corresponds to exactly one file, so we count the number of active jobs.
        
        Returns:
            Total number of files in all active jobs
        """
        # Each job has exactly one file_id, so count active jobs
        return len(self.active_jobs)
    
    def get_stats(self) -> Dict:
        """Get tracking statistics"""
        return {
            'active_jobs': len(self.active_jobs),
            'active_files': self.count_active_files(),
            'jobs_by_status': {
                status: len([j for j in self.active_jobs.values() if j['status'] == status])
                for status in ['pending', 'processing', 'retry_pending']
            }
        }
    
    def wait_for_jobs(self, job_ids: List[str], timeout: int = 300, poll_interval: int = 2) -> bool:
        """
        Wait for specific jobs to complete.
        
        Args:
            job_ids: List of job IDs to wait for
            timeout: Maximum time to wait in seconds
            poll_interval: Time between status checks in seconds
            
        Returns:
            True if all jobs completed (success or failure), False if timeout
        """
        if not job_ids:
            return True
            
        start_time = time.time()
        remaining_jobs = set(job_ids)
        
        logger.info(f"Waiting for {len(remaining_jobs)} job(s) to complete (timeout={timeout}s)")
        
        while remaining_jobs and (time.time() - start_time) < timeout:
            # Check which jobs are still active
            with self.completion_lock:
                still_active = remaining_jobs & set(self.active_jobs.keys())
            
            # Jobs not in active_jobs are considered completed (either success or moved to failure queue)
            completed = remaining_jobs - still_active
            if completed:
                logger.debug(f"Jobs completed: {completed}")
                remaining_jobs = still_active
            
            if not remaining_jobs:
                elapsed = time.time() - start_time
                logger.info(f"All {len(job_ids)} job(s) completed in {elapsed:.1f}s")
                return True
            
            logger.debug(f"Still waiting for {len(remaining_jobs)} job(s): {list(remaining_jobs)[:5]}...")
            time.sleep(poll_interval)
        
        if remaining_jobs:
            elapsed = time.time() - start_time
            logger.warning(f"Timeout waiting for jobs after {elapsed:.1f}s. "
                          f"Remaining: {len(remaining_jobs)} job(s)")
            return False
        
        return True

