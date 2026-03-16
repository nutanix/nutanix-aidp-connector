#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Baseline Orchestrator for coordinating baseline operations across multiple buckets.
Manages the lifecycle of baseline processing including:
- Starting/stopping baseline for configured buckets
- Monitoring progress and completion
- Handling config changes dynamically
- Coordinating with main pipeline for Kafka pause/resume
"""

import logging
import time
import queue
import threading
import uuid
from typing import Dict, Optional, List, Callable, Set
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, Future

from config.settings import BaselineConfig, BaselineBucketConfig
from .state_manager import BaselineStateManager, BucketBaselineState
from .object_lister import BaselineObjectLister, ObjectInfo

logger = logging.getLogger(__name__)


@dataclass
class BucketBaselineTask:
    """Represents a baseline task for a bucket"""
    bucket_name: str
    prefix: Optional[str]
    enabled: bool
    future: Optional[Future] = None
    stop_requested: bool = False


class BaselineOrchestrator:
    """
    Orchestrates baseline operations across multiple buckets.
    Coordinates with the main pipeline for pausing Kafka during baseline.
    """
    
    def __init__(
        self,
        s3_client,
        config: BaselineConfig,
        message_queue: queue.Queue,
        status_tracker=None,
        max_outstanding_tasks: int = 10,
        on_baseline_complete: Optional[Callable[[], None]] = None,
        on_bucket_complete: Optional[Callable[[str], None]] = None
    ):
        """
        Initialize the baseline orchestrator.
        
        Args:
            s3_client: Boto3 S3 client for listing objects
            config: Baseline configuration
            message_queue: Queue to push objects for processing (same as Kafka messages)
            status_tracker: Job status tracker (to check active jobs count)
            max_outstanding_tasks: Maximum outstanding tasks before throttling listing
            on_baseline_complete: Callback when all baseline operations complete
            on_bucket_complete: Callback when a single bucket's baseline completes
        """
        self.s3_client = s3_client
        self.config = config
        self.message_queue = message_queue
        self.status_tracker = status_tracker
        self.max_outstanding_tasks = max_outstanding_tasks
        self.on_baseline_complete = on_baseline_complete
        self.on_bucket_complete = on_bucket_complete
        
        self.versioning_enabled = config.versioning_enabled

        # State management
        self.state_manager = BaselineStateManager(
            config.state_dir,
            versioning_enabled=config.versioning_enabled,
        )
        self.object_lister = BaselineObjectLister(
            s3_client=s3_client,
            state_manager=self.state_manager,
            list_batch_size=config.list_batch_size
        )
        
        # Task tracking
        self._tasks: Dict[str, BucketBaselineTask] = {}
        self._lock = threading.RLock()
        self._running = False
        self._executor: Optional[ThreadPoolExecutor] = None
        self._monitor_thread: Optional[threading.Thread] = None
        
        # Current config tracking (for detecting changes)
        self._current_bucket_configs: Dict[str, BaselineBucketConfig] = {}
        
        # Batch completion tracking for synchronous batch pattern
        # Key: object_key (bucket/key:version), Value: True when processed
        self._pending_objects: Dict[str, bool] = {}
        self._pending_lock = threading.Lock()
        self._batch_complete_event = threading.Event()
        
        logger.info(f"BaselineOrchestrator initialized: "
                   f"enabled={config.enabled}, "
                   f"buckets={len(config.buckets)}, "
                   f"max_concurrent={config.max_concurrent_listings}")
    
    def start(self):
        """Start baseline operations for all configured buckets"""
        if not self.config.enabled:
            logger.info("Baseline operations disabled in config")
            return
        
        if self._running:
            logger.warning("Baseline orchestrator already running")
            return
        
        self._running = True
        
        # Get enabled buckets from config
        enabled_buckets = [b for b in self.config.buckets if b.enabled]
        
        if not enabled_buckets:
            logger.info("No enabled buckets for baseline processing")
            self._complete_all_baselines()
            return
        
        logger.info(f"Starting baseline operations for {len(enabled_buckets)} buckets")
        
        # Store current config for change detection
        for bucket_config in self.config.buckets:
            self._current_bucket_configs[bucket_config.bucket_name] = bucket_config
        
        # Create thread pool for concurrent bucket processing
        self._executor = ThreadPoolExecutor(
            max_workers=self.config.max_concurrent_listings,
            thread_name_prefix="baseline-"
        )
        
        # Start baseline for each enabled bucket
        for bucket_config in enabled_buckets:
            self._start_bucket_baseline(bucket_config)
        
        # Start monitor thread
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="baseline-monitor"
        )
        self._monitor_thread.start()
        
        logger.info("Baseline orchestrator started")
    
    def stop(self):
        """Stop all baseline operations"""
        logger.info("Stopping baseline orchestrator...")
        self._running = False
        
        # Signal all tasks to stop
        with self._lock:
            for task in self._tasks.values():
                task.stop_requested = True
                self.object_lister.stop_listing(task.bucket_name)
        
        # Shutdown executor
        if self._executor:
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._executor = None
        
        # Wait for monitor thread
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5)
        
        logger.info("Baseline orchestrator stopped")
    
    def update_config(self, new_config: BaselineConfig):
        """
        Update configuration and handle bucket changes.
        Called when config is hot-reloaded.
        
        Args:
            new_config: New baseline configuration
        """
        if not self._running:
            self.config = new_config
            return
        
        logger.info("Updating baseline configuration...")
        
        # Build set of currently configured buckets
        new_bucket_names: Set[str] = set()
        new_enabled_buckets: Dict[str, BaselineBucketConfig] = {}
        
        for bucket_config in new_config.buckets:
            new_bucket_names.add(bucket_config.bucket_name)
            if bucket_config.enabled:
                new_enabled_buckets[bucket_config.bucket_name] = bucket_config
        
        with self._lock:
            # Find removed buckets (were in old config, not in new)
            current_bucket_names = set(self._current_bucket_configs.keys())
            removed_buckets = current_bucket_names - new_bucket_names
            
            # Find newly disabled buckets
            for bucket_name, old_config in self._current_bucket_configs.items():
                if bucket_name in new_bucket_names:
                    new_cfg = next((b for b in new_config.buckets if b.bucket_name == bucket_name), None)
                    if new_cfg and not new_cfg.enabled and old_config.enabled:
                        removed_buckets.add(bucket_name)
            
            # Stop and cleanup removed/disabled buckets
            for bucket_name in removed_buckets:
                self._stop_bucket_baseline(bucket_name, clear_state=True)
                logger.info(f"Stopped baseline for removed/disabled bucket: {bucket_name}")
            
            # Find newly enabled buckets
            for bucket_name, bucket_config in new_enabled_buckets.items():
                if bucket_name not in self._tasks:
                    old_config = self._current_bucket_configs.get(bucket_name)
                    if not old_config or not old_config.enabled:
                        self._start_bucket_baseline(bucket_config)
                        logger.info(f"Started baseline for newly enabled bucket: {bucket_name}")
            
            # Update tracked config
            self._current_bucket_configs = {b.bucket_name: b for b in new_config.buckets}
        
        self.config = new_config
    
    def get_status(self) -> Dict:
        """Get current status of all baseline operations"""
        status = {
            'enabled': self.config.enabled,
            'running': self._running,
            'buckets': {}
        }
        
        with self._lock:
            for bucket_name, task in self._tasks.items():
                state = self.state_manager.get_bucket_state(bucket_name, task.prefix)
                bucket_status = {
                    'enabled': task.enabled,
                    'stop_requested': task.stop_requested,
                    'running': task.future is not None and not task.future.done() if task.future else False
                }
                
                if state:
                    bucket_status.update({
                        'listing_complete': state.listing_complete,
                        'processing_complete': state.processing_complete,
                        'total_listed': state.total_objects_listed,
                        'total_processed': state.total_objects_processed,
                        'total_failed': state.total_objects_failed,
                        'started_at': state.started_at,
                        'last_updated': state.last_updated_at,
                        'completed_at': state.completed_at
                    })
                
                status['buckets'][bucket_name] = bucket_status
        
        return status
    
    def is_baseline_in_progress(self) -> bool:
        """Check if any baseline operation is still in progress"""
        if not self._running:
            return False
        
        with self._lock:
            for task in self._tasks.values():
                if task.future and not task.future.done():
                    return True
        
        return False
    
    def get_pending_buckets(self) -> List[str]:
        """Get list of buckets with pending baseline operations"""
        pending = []
        with self._lock:
            for bucket_name, task in self._tasks.items():
                state = self.state_manager.get_bucket_state(bucket_name, task.prefix)
                if state and not state.processing_complete:
                    pending.append(bucket_name)
        return pending
    
    def _start_bucket_baseline(self, bucket_config: BaselineBucketConfig):
        """Start baseline processing for a single bucket"""
        bucket_name = bucket_config.bucket_name
        
        # Test bucket access first
        success, error = self.object_lister.test_bucket_access(bucket_name)
        if not success:
            logger.error(f"Cannot access bucket {bucket_name}: {error}")
            return
        
        # Clear saved state if force_rerun is set
        if getattr(bucket_config, 'force_rerun', False):
            logger.info(f"force_rerun=True for bucket {bucket_name}, clearing saved state")
            self.state_manager.clear_bucket_state(bucket_name, bucket_config.prefix)

        # Check if already complete
        state = self.state_manager.get_bucket_state(bucket_name, bucket_config.prefix)
        if state and state.processing_complete:
            logger.info(f"Baseline already complete for bucket: {bucket_name}")
            return
        
        # Create task
        task = BucketBaselineTask(
            bucket_name=bucket_name,
            prefix=bucket_config.prefix,
            enabled=True
        )
        
        with self._lock:
            self._tasks[bucket_name] = task
        
        # Submit to executor
        if self._executor:
            task.future = self._executor.submit(
                self._process_bucket_baseline,
                bucket_config
            )
            logger.info(f"Started baseline task for bucket: {bucket_name}")
    
    def _stop_bucket_baseline(self, bucket_name: str, clear_state: bool = False):
        """Stop baseline processing for a bucket"""
        with self._lock:
            task = self._tasks.get(bucket_name)
            if task:
                task.stop_requested = True
                self.object_lister.stop_listing(bucket_name)
                
                # Cancel future if possible
                if task.future and not task.future.done():
                    task.future.cancel()
                
                del self._tasks[bucket_name]
        
        # Clear state if requested (bucket removed from config)
        if clear_state:
            prefix = task.prefix if task else None
            self.state_manager.clear_bucket_state(bucket_name, prefix)
    
    def _process_bucket_baseline(self, bucket_config: BaselineBucketConfig):
        """
        Process baseline for a single bucket using SYNCHRONOUS BATCH pattern.
        
        For each batch:
        1. List objects
        2. Push ALL objects in batch to queue
        3. WAIT for ALL objects to complete processing
        4. Update the next_key_marker (safe for resume)
        5. Move to next batch
        
        This ensures no data loss on crash - we only advance the marker
        after the entire batch is confirmed processed.
        """
        bucket_name = bucket_config.bucket_name
        prefix = bucket_config.prefix
        
        logger.info(f"Processing baseline for bucket: {bucket_name} (synchronous batch mode)")
        
        batch_count = 0
        total_processed = 0
        
        try:
            # List and process objects batch by batch
            for object_batch in self.object_lister.list_bucket_objects(
                bucket_name=bucket_name,
                prefix=prefix,
                resume=True
            ):
                # Check if we should stop
                task = self._tasks.get(bucket_name)
                if task and task.stop_requested:
                    logger.info(f"Baseline stopped for bucket: {bucket_name}")
                    return
                
                if not object_batch:
                    continue
                
                batch_count += 1
                batch_size = len(object_batch)
                
                logger.info(f"Processing batch {batch_count} for {bucket_name}: {batch_size} objects")
                
                # Track pending objects for this batch
                batch_object_keys = []
                with self._pending_lock:
                    self._batch_complete_event.clear()
                    for obj in object_batch:
                        object_key = self._make_object_key(obj.bucket, obj.key, obj.version_id)
                        self._pending_objects[object_key] = False  # Not yet processed
                        batch_object_keys.append(object_key)
                
                # Push ALL objects in batch to queue
                for obj in object_batch:
                    # Wait for pipeline capacity before pushing
                    self._wait_for_pipeline_capacity()
                    
                    # Check if we should stop
                    task = self._tasks.get(bucket_name)
                    if task and task.stop_requested:
                        logger.info(f"Baseline stopped for bucket: {bucket_name}")
                        # Clear pending objects
                        with self._pending_lock:
                            for key in batch_object_keys:
                                self._pending_objects.pop(key, None)
                        return
                    
                    message = self._create_message_from_object(obj)
                    self.message_queue.put(message)
                
                # WAIT for ALL objects in this batch to complete
                logger.info(f"Waiting for batch {batch_count} to complete ({batch_size} objects)...")
                self._wait_for_batch_complete(batch_object_keys, bucket_name)
                
                # Check if we should stop after waiting
                task = self._tasks.get(bucket_name)
                if task and task.stop_requested:
                    logger.info(f"Baseline stopped for bucket: {bucket_name}")
                    return
                
                # Batch complete - now it's safe to update the marker
                # Get the last object in the batch as the marker
                last_obj = object_batch[-1]
                self.state_manager.update_listing_progress(
                    bucket_name=bucket_name,
                    prefix=prefix,
                    next_key_marker=last_obj.key,
                    next_version_id_marker=last_obj.version_id,
                    is_truncated=True,  # Will be updated by lister if not truncated
                    objects_listed=0  # Already counted
                )
                
                total_processed += batch_size
                logger.info(f"Batch {batch_count} complete for {bucket_name}: "
                           f"processed={batch_size}, total={total_processed}")
                
                # Clean up batch tracking
                with self._pending_lock:
                    for key in batch_object_keys:
                        self._pending_objects.pop(key, None)
            
            # All batches complete - mark bucket as complete
            self.state_manager.mark_bucket_complete(bucket_name, prefix)
            
            # Notify callback
            if self.on_bucket_complete:
                self.on_bucket_complete(bucket_name)
            
            logger.info(f"Baseline complete for bucket: {bucket_name}: "
                       f"batches={batch_count}, total_objects={total_processed}")
            
        except Exception as e:
            logger.error(f"Error in baseline processing for {bucket_name}: {e}")
            raise
    
    def _make_object_key(self, bucket: str, key: str, version_id: Optional[str]) -> str:
        """Create a unique key for tracking an object.
        When versioning is disabled, version_id is omitted to avoid ':null' suffixes."""
        if self.versioning_enabled:
            return f"{bucket}/{key}:{version_id or 'null'}"
        return f"{bucket}/{key}"
    
    def _wait_for_batch_complete(self, batch_object_keys: List[str], bucket_name: str):
        """
        Wait for all objects in a batch to complete processing.
        Uses polling with timeout to check completion status.
        """
        poll_interval = 1  # Check every second
        timeout = 3600  # 1 hour max wait per batch
        start_time = time.time()
        
        while self._running:
            # Check if we should stop
            task = self._tasks.get(bucket_name)
            if task and task.stop_requested:
                return
            
            # Check if all objects in batch are processed
            with self._pending_lock:
                pending_count = sum(
                    1 for key in batch_object_keys 
                    if key in self._pending_objects and not self._pending_objects[key]
                )
            
            if pending_count == 0:
                # All objects processed
                return
            
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > timeout:
                logger.warning(f"Batch wait timeout for {bucket_name}: "
                             f"{pending_count} objects still pending after {elapsed:.0f}s")
                # Continue anyway - some objects may have failed
                return
            
            # Log progress periodically
            if int(elapsed) % 30 == 0 and elapsed > 0:
                logger.debug(f"Waiting for batch: {pending_count}/{len(batch_object_keys)} pending")
            
            # Wait before checking again
            time.sleep(poll_interval)
    
    def _wait_for_pipeline_capacity(self):
        """
        Wait until there's capacity in the pipeline to process more objects.
        Respects max_outstanding_tasks to prevent overwhelming the system.
        """
        if not self.status_tracker:
            return  # No tracker, don't throttle
        
        # Calculate max queue size (similar to download worker logic)
        # Allow 3x outstanding tasks in the queue
        max_queue_size = self.max_outstanding_tasks * 3
        
        wait_logged = False
        while self._running:
            # Check active jobs files from status tracker
            # max_outstanding_tasks is the number of FILES, not jobs
            active_jobs_files = self.status_tracker.count_active_files()
            queue_size = self.message_queue.qsize()
            
            # If we have room, proceed
            if active_jobs_files < self.max_outstanding_tasks and queue_size < max_queue_size:
                if wait_logged:
                    logger.debug(f"Baseline listing resumed: active_jobs_files={active_jobs_files}, "
                                f"queue_size={queue_size}")
                return
            
            # Log throttling (only once)
            if not wait_logged:
                logger.info(f"Baseline listing throttled: active_jobs_files={active_jobs_files}, "
                           f"queue_size={queue_size}, max_outstanding={self.max_outstanding_tasks}")
                wait_logged = True
            
            # Wait and check again
            time.sleep(1)
    
    def _create_message_from_object(self, obj: ObjectInfo) -> Dict:
        """
        Create a message in the same format as Kafka messages.
        This allows baseline objects to flow through the same pipeline.
        
        Must include all fields expected by download_worker:
        - file_id: Unique identifier for tracking
        - s3_bucket, s3_key: Object location
        - kafka_partition, kafka_offset: Use -1 for baseline (not from Kafka)
        """
        # When versioning is disabled, suppress the literal "null" version string
        # that S3 returns for non-versioned objects
        version = obj.version_id
        if not self.versioning_enabled and version in (None, 'null'):
            version = None

        return {
            'source': 'baseline',
            'event_type': 's3:ObjectCreated:Put',
            'file_id': str(uuid.uuid4()),
            's3_bucket': obj.bucket,
            's3_key': obj.key,
            's3_version': version,
            'file_size': obj.size,
            'e_tag': obj.e_tag,
            'kafka_partition': -1,
            'kafka_offset': -1,
            'is_baseline': True,
            'baseline_prefix': obj.bucket,
        }
    
    def mark_file_processed(self, s3_bucket: str, s3_key: str, version_id: Optional[str] = None):
        """
        Mark a baseline file as successfully processed.
        Called by completion handler when a baseline file completes.
        
        This is critical for the synchronous batch pattern - it signals
        that an object has completed processing, allowing the batch to
        be marked complete once all objects are done.
        
        Args:
            s3_bucket: S3 bucket name
            s3_key: S3 object key
            version_id: Object version ID (optional)
        """
        # Mark as processed in the pending objects tracker
        object_key = self._make_object_key(s3_bucket, s3_key, version_id)
        with self._pending_lock:
            if object_key in self._pending_objects:
                self._pending_objects[object_key] = True  # Mark as processed
                logger.debug(f"Marked baseline object as processed: {object_key}")
            else:
                # Object not in pending (might be from a previous run or already processed)
                logger.debug(f"Object not in pending list (already processed?): {object_key}")
        
        # Also mark in the state manager for persistence
        prefix = None
        for bucket_config in self.config.buckets:
            if bucket_config.bucket_name == s3_bucket:
                prefix = bucket_config.prefix
                break
        
        self.state_manager.mark_object_processed(
            bucket_name=s3_bucket,
            s3_key=s3_key,
            version_id=version_id,
            prefix=prefix
        )
    
    def _monitor_loop(self):
        """
        Monitor thread that checks for completion and handles cleanup.
        """
        logger.info("Baseline monitor started")
        
        while self._running:
            try:
                time.sleep(5)  # Check every 5 seconds
                
                completed_buckets = []
                all_done = True
                
                with self._lock:
                    for bucket_name, task in list(self._tasks.items()):
                        if task.future:
                            if task.future.done():
                                # Check for exceptions
                                try:
                                    task.future.result()
                                    completed_buckets.append(bucket_name)
                                except Exception as e:
                                    logger.error(f"Baseline task failed for {bucket_name}: {e}")
                                    completed_buckets.append(bucket_name)
                            else:
                                all_done = False
                        else:
                            all_done = False
                
                # Log progress
                if self._tasks:
                    pending = [b for b, t in self._tasks.items() 
                              if t.future and not t.future.done()]
                    if pending:
                        logger.debug(f"Baseline in progress for buckets: {pending}")
                
                # Check if all baselines are complete
                if all_done and self._tasks:
                    self._complete_all_baselines()
                    break
                
            except Exception as e:
                logger.error(f"Error in baseline monitor: {e}")
        
        logger.info("Baseline monitor stopped")
    
    def _complete_all_baselines(self):
        """Called when all baseline operations are complete"""
        logger.info("=" * 60)
        logger.info("ALL BASELINE OPERATIONS COMPLETE")
        logger.info("=" * 60)
        
        # Print summary
        for bucket_name in list(self._tasks.keys()):
            task = self._tasks.get(bucket_name)
            if task:
                state = self.state_manager.get_bucket_state(bucket_name, task.prefix)
                if state:
                    logger.info(f"  {bucket_name}: listed={state.total_objects_listed}, "
                               f"processed={state.total_objects_processed}, "
                               f"failed={state.total_objects_failed}")
        
        # Notify callback
        if self.on_baseline_complete:
            self.on_baseline_complete()
        
        self._running = False

