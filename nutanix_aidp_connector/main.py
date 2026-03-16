#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Main orchestrator for NeMo-Retriever pipeline service with enhanced task configuration
"""

import os
import signal
import logging
import time
import queue
import threading
import uuid

# Import configuration
from config.settings import get_settings
from config.pipeline_config_manager import pipeline_config_manager, ConfigChange
from utils.logging_config import setup_logging, log_with_context

# Import pipeline components
from kafka_consumer.consumer import PipelineKafkaConsumer
from s3_downloader.downloader import S3Downloader
from batch_manager.window_manager import BatchWindowManager, FileInfo
from nemo_retriever.job_submitter import NemoRetrieverJobSubmitter
from nemo_retriever.status_tracker import JobStatusTracker
from completion.handler import CompletionHandler
from delete_handler import DeleteHandler

# Import storage manager
from storage.milvus_manager import MilvusManager

# Import configuration API
from api.config_api import config_app

# Import configuration manager
from config.task_config_manager import config_manager

# Import telemetry
from utils.telemetry import init_metrics, get_metrics

# Import baseline module
from baseline.baseline_orchestrator import BaselineOrchestrator

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Main orchestrator for the NeMo-Retriever pipeline"""
    
    def __init__(self, config):
        """
        Initialize pipeline orchestrator
        
        Args:
            config: Settings configuration object (initial config, will be updated via hot-reload)
        """
        self.config = config
        self.running = False
        
        # Detect pod ordinal from StatefulSet pod name (e.g. "nutanix-aidp-connector-0" -> 0).
        # Ordinal 0 is the baseline leader; other pods skip baseline processing.
        pod_name = os.environ.get('POD_NAME', '')
        try:
            self._pod_ordinal = int(pod_name.rsplit('-', 1)[-1])
        except (ValueError, IndexError):
            self._pod_ordinal = 0
        self._is_baseline_leader = (self._pod_ordinal == 0)
        logger.info(f"Pod identity: name={pod_name or '(unknown)'}, ordinal={self._pod_ordinal}, "
                     f"baseline_leader={self._is_baseline_leader}")
        
        # Initialize telemetry/metrics
        self.metrics = init_metrics(config.telemetry)
        if self.metrics and self.metrics.enabled:
            logger.info("OpenTelemetry metrics initialized successfully")
        else:
            logger.info("Telemetry metrics are disabled")
        
        # Queues for inter-component communication
        self.message_queue = queue.Queue()
        self.batch_queue = queue.Queue()
        self.completion_queue = queue.Queue()
        self.delete_queue = queue.Queue()
        
        # Initialize components
        logger.info("Initializing pipeline components...")
        
        # Get initial config from hot-reload manager
        initial_config = pipeline_config_manager.get_config()
        if initial_config:
            self.config = initial_config
        
        # 1. Kafka consumer
        kafka_config = self.config.kafka
        self.kafka_consumer = PipelineKafkaConsumer(
            bootstrap_servers=kafka_config.bootstrap_servers,
            topic=kafka_config.topic,
            group_id=kafka_config.group_id,
            auto_offset_reset=getattr(kafka_config, 'auto_offset_reset', 'earliest'),
            message_queue=self.message_queue,
            delete_queue=self.delete_queue,
            allowed_event_types=kafka_config.allowed_event_types
        )
        
        # 2. S3 downloader
        s3_config = self.config.s3
        self.s3_downloader = S3Downloader(
            download_dir=s3_config.download_dir,
            max_workers=self.config.threading.download_workers,
            max_retries=self.config.processing.max_retries,
            retry_base_delay=self.config.processing.retry_base_delay,
            retry_max_delay=self.config.processing.retry_max_delay,
            endpoint_url=s3_config.endpoint_url,
            access_key=s3_config.access_key,
            secret_key=s3_config.secret_key,
            verify_ssl=s3_config.verify_ssl,
            region=s3_config.region
        )
        
        # Register config change callbacks
        self._register_config_change_callbacks()
        
        # 3. Batch window manager (uses config manager for hot-reload)
        self.batch_manager = BatchWindowManager(
            max_files=config.batching.max_files,  # Initial values, will be overridden by config manager
            max_bytes=config.batching.max_bytes,
            max_wait_seconds=config.batching.max_wait_seconds
        )
        
        # 4. NeMo-Retriever job submitter (V2 + pdf_split_page_count=1 so one subjob per page; one bad page only affects that page)
        self.job_submitter = NemoRetrieverJobSubmitter(
            host=config.nemo_retriever.host,
            port=config.nemo_retriever.port,
            task_queue=config.nemo_retriever.task_queue,
            timeout=config.nemo_retriever.timeout,
            max_workers=config.threading.submission_workers,
            text_depth=config.nemo_retriever.text_depth,
            api_version=config.nemo_retriever.api_version,
            pdf_split_page_count=config.nemo_retriever.pdf_split_page_count,
            circuit_breaker_threshold=config.processing.circuit_breaker_threshold,
            circuit_breaker_timeout=config.processing.circuit_breaker_timeout
        )
        
        # Failure queue for permanently failed jobs
        self.failure_queue = queue.Queue()
        
        # 5. Job status tracker
        self.status_tracker = JobStatusTracker(
            nemo_retriever_client=self.job_submitter.client,
            poll_interval=config.processing.status_poll_interval,
            timeout=config.nemo_retriever.timeout,
            max_retries=config.processing.max_retries,
            max_workers=config.threading.status_check_workers,
            completion_queue=self.completion_queue,
            failure_queue=self.failure_queue
        )
        
        # 6. Completion handler
        self.completion_handler = CompletionHandler(
            completed_metadata_dir=config.storage.completed_metadata,
            dead_letter_dir=config.storage.dead_letter,
            kafka_consumer=self.kafka_consumer,
            config_manager=config_manager
        )
        
        # 6b. Milvus batch inserter (amortizes bulk-insert wait across N files)
        self.milvus_batch_inserter = None
        milvus_cfg = config.storage.milvus
        batch_max_files = getattr(milvus_cfg, 'batch_insert_max_files', 0) if milvus_cfg else 0
        batch_max_wait = getattr(milvus_cfg, 'batch_insert_max_wait_seconds', 15.0) if milvus_cfg else 15.0
        if batch_max_files and batch_max_files > 0:
            from completion.milvus_batch_inserter import MilvusBatchInserter
            self.milvus_batch_inserter = MilvusBatchInserter(
                config_manager=config_manager,
                max_files=batch_max_files,
                max_wait_seconds=batch_max_wait,
            )
            self.completion_handler.milvus_batch_inserter = self.milvus_batch_inserter
            logger.info(f"Milvus batch inserter enabled: max_files={batch_max_files}, max_wait={batch_max_wait}s")
        else:
            logger.info("Milvus batch inserter disabled (batch_insert_max_files=0), using per-file bulk insert")
        
        # 7. Delete handler
        self.delete_handler = DeleteHandler(config_manager=config_manager)
        self.delete_handler.set_delete_queue(self.delete_queue)
        
        # 9. Configuration API server
        self.config_api_thread = None
        self.config_api_port = 5001
        
        # 10. Milvus manager (initialized later when config is available)
        self.milvus_manager = None
        self.last_milvus_collection_name = None
        self.last_milvus_endpoint = None  # Track endpoint separately (host:port)
        
        # 11. Additional Milvus managers for filtered collections
        self.milvus_managers = {}  # Dict of collection_name -> MilvusManager
        self.filtered_collections = {}  # Will be populated from config when filtered collections are initialized
        
        # 12. Baseline orchestrator for processing existing objects in buckets
        self.baseline_orchestrator = None
        self._baseline_in_progress = False
        self._kafka_paused_for_baseline = False
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("Pipeline orchestrator initialized successfully")
    
    def _register_config_change_callbacks(self):
        """Register callbacks for configuration changes"""
        # Kafka config change callback
        def on_kafka_config_change(change: ConfigChange):
            logger.info(f"Kafka configuration changed: {change.changed_fields}")
            new_kafka_config = pipeline_config_manager.get_kafka_config()
            
            # Update allowed event types if changed
            if 'allowed_event_types' in change.changed_fields:
                logger.info(f"Updating allowed event types: {new_kafka_config.allowed_event_types}")
                self.kafka_consumer.update_allowed_event_types(new_kafka_config.allowed_event_types)
            
            # Only reconnect if connection-related fields changed
            connection_fields = {'bootstrap_servers', 'topic', 'group_id'}
            if connection_fields.intersection(change.changed_fields):
                logger.info("Kafka connection parameters changed, reconnecting...")
                self.kafka_consumer.reconnect(
                    bootstrap_servers=new_kafka_config.bootstrap_servers,
                    topic=new_kafka_config.topic,
                    group_id=new_kafka_config.group_id
                )
        
        # S3 config change callback
        def on_s3_config_change(change: ConfigChange):
            logger.info(f"S3 configuration changed: {change.changed_fields}")
            new_s3_config = pipeline_config_manager.get_s3_config()
            
            # Only reconnect if connection-related fields changed
            connection_fields = {'endpoint_url', 'access_key', 'secret_key', 'verify_ssl', 'region'}
            if connection_fields.intersection(change.changed_fields):
                logger.info("S3 connection parameters changed, reconnecting...")
                self.s3_downloader.reconnect(
                    endpoint_url=new_s3_config.endpoint_url,
                    access_key=new_s3_config.access_key,
                    secret_key=new_s3_config.secret_key,
                    verify_ssl=new_s3_config.verify_ssl,
                    region=new_s3_config.region
                )
            
            # Update download directory if changed
            if 'download_dir' in change.changed_fields:
                logger.info(f"S3 download directory changed to: {new_s3_config.download_dir}")
                # Note: S3Downloader doesn't support changing download_dir dynamically
                # This would require reinitialization or a new method
        
        # Threading config change callback
        def on_threading_config_change(change: ConfigChange):
            logger.info(f"Threading configuration changed: {change.changed_fields}")
            new_threading_config = pipeline_config_manager.get_threading_config()
            
            # Update S3 downloader workers if changed
            if 'download_workers' in change.changed_fields:
                logger.info(f"Download workers changed to: {new_threading_config.download_workers}")
                # Note: ThreadPoolExecutor doesn't support dynamic worker count
                # Would need to recreate the executor
        
        # NeMo-Retriever config change callback
        def on_nemo_retriever_config_change(change: ConfigChange):
            logger.info(f"NeMo-Retriever configuration changed: {change.changed_fields}")
            new_nv_config = pipeline_config_manager.get_nemo_retriever_config()
            
            # Only reconnect if connection-related fields changed
            connection_fields = {'host', 'port', 'task_queue', 'timeout'}
            if connection_fields.intersection(change.changed_fields):
                logger.info("NeMo-Retriever connection parameters changed, reconnecting...")
                # Note: NeMo-Retriever client reconnection would need to be implemented
                # For now, just log the change
                logger.warning("NeMo-Retriever client reconnection not yet implemented")
        
        # Processing config change callback
        def on_processing_config_change(change: ConfigChange):
            logger.info(f"Processing configuration changed: {change.changed_fields}")
            new_processing_config = pipeline_config_manager.get_processing_config()
            
            # Update retry settings if changed
            if 'max_retries' in change.changed_fields or 'retry_base_delay' in change.changed_fields or 'retry_max_delay' in change.changed_fields:
                # Update S3 downloader retry settings
                self.s3_downloader.max_retries = new_processing_config.max_retries
                self.s3_downloader.retry_base_delay = new_processing_config.retry_base_delay
                self.s3_downloader.retry_max_delay = new_processing_config.retry_max_delay
                logger.info("Updated retry settings")
            
            # Update status poll interval if changed
            if 'status_poll_interval' in change.changed_fields:
                self.status_tracker.poll_interval = new_processing_config.status_poll_interval
                logger.info(f"Updated status poll interval to {new_processing_config.status_poll_interval} seconds")
            
            # Update circuit breaker settings if changed
            if 'circuit_breaker_threshold' in change.changed_fields or 'circuit_breaker_timeout' in change.changed_fields:
                if hasattr(self.job_submitter, 'circuit_breaker'):
                    self.job_submitter.circuit_breaker.failure_threshold = new_processing_config.circuit_breaker_threshold
                    self.job_submitter.circuit_breaker.timeout = new_processing_config.circuit_breaker_timeout
                    logger.info(f"Updated circuit breaker: threshold={new_processing_config.circuit_breaker_threshold}, timeout={new_processing_config.circuit_breaker_timeout}s")
        
        # Batching config change callback
        def on_batching_config_change(change: ConfigChange):
            logger.info(f"Batching configuration changed: {change.changed_fields}")
            # BatchWindowManager reads from config manager directly, but we need to trigger update
            flushed_batches = self.batch_manager.update_config_from_manager()
            # If batches were flushed due to max_outstanding_tasks violation, add them to queue
            for flushed_batch in flushed_batches:
                self.batch_queue.put(flushed_batch)
                self._record_batch_created_metrics(flushed_batch, batch_source="hot-reload flush")
        
        # NeMo-Retriever config change callback (enhanced)
        def on_nemo_retriever_config_change(change: ConfigChange):
            logger.info(f"NeMo-Retriever configuration changed: {change.changed_fields}")
            new_nv_config = pipeline_config_manager.get_nemo_retriever_config()
            
            # Update timeout in status tracker if changed
            if 'timeout' in change.changed_fields:
                self.status_tracker.timeout = new_nv_config.timeout
                logger.info(f"Updated status tracker timeout to {new_nv_config.timeout} seconds")
            
            # Only reconnect if connection-related fields changed
            connection_fields = {'host', 'port', 'task_queue'}
            if connection_fields.intersection(change.changed_fields):
                logger.info("NeMo-Retriever connection parameters changed, reconnecting...")
                # Note: NeMo-Retriever client reconnection would need to be implemented
                # For now, just log the change
                logger.warning("NeMo-Retriever client reconnection not yet implemented")
        
        # Storage/Milvus config change callback
        def on_storage_config_change(change: ConfigChange):
            logger.info(f"Storage/Milvus configuration changed: {change.changed_fields}")
            config_manager.reload_config()
            self._reinitialize_milvus_on_config_change()
        
        def on_milvus_config_change(change: ConfigChange):
            logger.info(f"Milvus configuration changed: {change.changed_fields}")
            config_manager.reload_config()
            self._reinitialize_milvus_on_config_change()
        
        # Baseline config change callback
        def on_baseline_config_change(change: ConfigChange):
            logger.info(f"Baseline configuration changed: {change.changed_fields}")
            new_config = pipeline_config_manager.get_config()
            if new_config and self.baseline_orchestrator:
                self.baseline_orchestrator.update_config(new_config.baseline)
        
        # Register callbacks
        pipeline_config_manager.register_change_callback('kafka', on_kafka_config_change)
        pipeline_config_manager.register_change_callback('s3', on_s3_config_change)
        pipeline_config_manager.register_change_callback('threading', on_threading_config_change)
        pipeline_config_manager.register_change_callback('nemo_retriever', on_nemo_retriever_config_change)
        pipeline_config_manager.register_change_callback('processing', on_processing_config_change)
        pipeline_config_manager.register_change_callback('batching', on_batching_config_change)
        pipeline_config_manager.register_change_callback('storage', on_storage_config_change)
        pipeline_config_manager.register_change_callback('milvus', on_milvus_config_change)
        pipeline_config_manager.register_change_callback('baseline', on_baseline_config_change)
        
        logger.info("Configuration change callbacks registered")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False
    
    def recover_state(self):
        """Recover state from previous crash using Kafka-based recovery"""
        logger.info("=" * 80)
        logger.info("CRASH RECOVERY: Using Kafka-based recovery...")
        logger.info("=" * 80)
        
        # Scan local download directory for existing files
        self._scan_existing_files()
        
        # Kafka will automatically resume from last committed offset
        logger.info("Kafka will resume from last committed offset")
        logger.info("Any duplicate processing will be handled gracefully")
        
        logger.info("=" * 80)
        logger.info("CRASH RECOVERY: Complete")
        logger.info("=" * 80)
    
    def _scan_existing_files(self):
        """Scan local directory for existing files to avoid re-downloading"""
        import glob
        
        download_dir = "/tmp/nutanix-aidp-connector-downloads"
        if not os.path.exists(download_dir):
            logger.info("Download directory doesn't exist, starting fresh")
            return
        
        # Find all existing files (format: {s3_bucket}_{s3_key})
        # Also check for old format (kafka_partition_offset_bucket_key) for backward compatibility
        pattern = os.path.join(download_dir, "*_*")
        old_pattern = os.path.join(download_dir, "*_*_*_*")
        existing_files = glob.glob(pattern) + glob.glob(old_pattern)
        
        logger.info(f"Found {len(existing_files)} existing files in download directory")
        
        # Store existing files for deduplication during processing
        self.existing_files = set()
        for file_path in existing_files:
            filename = os.path.basename(file_path)
            self.existing_files.add(filename)
        
        logger.info(f"Loaded {len(self.existing_files)} existing files for deduplication")
    
    def _get_local_filename(self, s3_bucket, s3_key):
        """Generate local filename based on bucket and object name only"""
        # Replace problematic characters in S3 key
        safe_key = s3_key.replace('/', '_').replace(':', '_').replace(' ', '_')
        return f"{s3_bucket}_{safe_key}"
    
    def _create_file_info(self, file_record, local_path=None, file_size=None):
        """
        Create a FileInfo object from file_record.
        
        Args:
            file_record: Dictionary containing file information
            local_path: Optional local path (if None, will be generated)
            file_size: Optional file size (if None, will be taken from file_record)
        
        Returns:
            FileInfo object
        """
        if local_path is None:
            expected_filename = self._get_local_filename(
                file_record['s3_bucket'],
                file_record['s3_key']
            )
            local_path = f"/tmp/nutanix-aidp-connector-downloads/{expected_filename}"
        
        if file_size is None:
            file_size = file_record.get('file_size', 0)
        
        # file_id should always be present (either from file_record or merged result)
        file_id = file_record.get('file_id') or file_record['file_id']
        
        return FileInfo(
            file_id=file_id,
            s3_bucket=file_record['s3_bucket'],
            s3_key=file_record['s3_key'],
            local_path=local_path,
            file_size=file_size,
            kafka_partition=file_record['kafka_partition'],
            kafka_offset=file_record['kafka_offset'],
            s3_version=file_record.get('s3_version'),
            s3_tags=file_record.get('s3_tags'),
            s3_metadata=file_record.get('s3_metadata'),
            is_baseline=file_record.get('is_baseline', False),
            baseline_prefix=file_record.get('baseline_prefix'),
        )
    
    def _record_batch_created_metrics(self, batch, batch_source="unknown"):
        """
        Record metrics and log when a batch is created.
        
        Args:
            batch: Batch dictionary
            batch_source: Source description for logging (e.g., "from existing file", "time-based flush")
        """
        logger.info(f"Created batch {batch['batch_id']} ({batch_source})")
        if self.metrics and self.metrics.enabled:
            self.metrics.record_batch_created(
                batch_size_files=len(batch.get('files', [])),
                batch_size_bytes=batch.get('total_bytes', 0)
            )
    
    def _add_file_to_batch(self, file_info, batch_source="unknown"):
        """
        Add file to batch manager and record metrics if batch is created.
        
        Args:
            file_info: FileInfo object to add
            batch_source: Source description for logging (e.g., "from existing file", "time-based flush")
        
        Returns:
            Batch dictionary if batch was created, None otherwise
        """
        batch = self.batch_manager.add_file(file_info)
        if batch:
            self.batch_queue.put(batch)
            self._record_batch_created_metrics(batch, batch_source)
        return batch
    
    def _count_files_in_batch_queue(self):
        """
        Count total number of files across all batches in the batch_queue.
        
        Since queue.Queue doesn't support peeking, we temporarily get all items,
        count files, and put them back. This is a best-effort count.
        
        Returns:
            Total number of files in all batches in the queue
        """
        total_files = 0
        batches = []
        
        # Temporarily drain the queue to count files
        try:
            while True:
                batch = self.batch_queue.get_nowait()
                batches.append(batch)
                # Count files in this batch
                file_count = batch.get('file_count', len(batch.get('files', [])))
                total_files += file_count
        except queue.Empty:
            pass
        
        # Put all batches back
        for batch in batches:
            self.batch_queue.put(batch)
        
        return total_files
    
    def _count_files_in_active_jobs(self):
        """
        Count total number of files across all active jobs in the status tracker.
        
        Each job corresponds to exactly one file (1:1 mapping), so we count active jobs.
        
        Returns:
            Total number of files in all active jobs
        """
        return self.status_tracker.count_active_files()
    
    def download_worker(self):
        """Worker for downloading files from message queue"""
        logger.info("Download worker started")
        
        download_futures = {}
        
        # Calculate download limit based on max_outstanding_tasks
        # Downloads should be limited to 3x the max outstanding tasks to prevent storage overflow
        max_outstanding_tasks = self.config.processing.max_outstanding_tasks
        max_pending_downloads = max_outstanding_tasks * 3
        logger.info(f"Download flow control: max_outstanding_tasks={max_outstanding_tasks}, "
                   f"max_pending_downloads={max_pending_downloads}")
        
        while self.running:
            try:
                # Calculate current pending items:
                # - Downloads in progress (count of files)
                # - Files in batch queue (waiting to be submitted) - sum of files across all batches
                # - Files in batch manager (accumulating, not yet a complete batch)
                # - Files in active jobs (submitted to NeMo-Retriever) - sum of files across all active jobs
                current_downloads = len(download_futures)
                batch_queue_files = self._count_files_in_batch_queue()  # Total files across all batches
                batch_manager_files = len(self.batch_manager.current_batch) if hasattr(self.batch_manager, 'current_batch') else 0
                active_jobs_files = self._count_files_in_active_jobs()  # Total files across all active jobs
                
                total_pending = current_downloads + batch_queue_files + batch_manager_files + active_jobs_files
                
                # Calculate how many more downloads we can start
                available_slots = max(0, max_pending_downloads - total_pending)
                
                # Also respect the download_workers limit
                download_worker_slots = max(0, self.config.threading.download_workers - current_downloads)
                can_download = min(available_slots, download_worker_slots)
                
                # Submit new downloads (respecting both limits)
                downloads_started = 0
                while not self.message_queue.empty() and downloads_started < can_download:
                    try:
                        file_record = self.message_queue.get_nowait()
                        enqueued_at = file_record.get('enqueued_at')
                        if enqueued_at is not None:
                            duration_sec = round(time.time() - enqueued_at, 3)
                            log_with_context(logger, 'info', "PIPELINE_LATENCY", context={
                                'stage': 'message_queue', 'file_id': file_record.get('file_id'),
                                'duration_sec': duration_sec
                            })
                            if self.metrics and self.metrics.enabled:
                                self.metrics.record_pipeline_stage_duration('message_queue', duration_sec)
                        # Check if file already exists locally (crash recovery)
                        expected_filename = self._get_local_filename(
                            file_record['s3_bucket'],
                            file_record['s3_key']
                        )
                        
                        if hasattr(self, 'existing_files') and expected_filename in self.existing_files:
                            logger.info(f"File already exists locally, skipping download: {expected_filename}")
                            # Add directly to batch manager
                            file_info = self._create_file_info(file_record)
                            self._add_file_to_batch(file_info, batch_source="from existing file")
                            continue
                        
                        # Submit async download
                        future = self.s3_downloader.download_async(
                            s3_bucket=file_record['s3_bucket'],
                            s3_key=file_record['s3_key'],
                            file_id=file_record['file_id']
                        )
                        
                        download_futures[future] = file_record
                        downloads_started += 1
                        
                    except queue.Empty:
                        break
                
                # Log throttling status periodically (when queue has items but we can't download)
                if not self.message_queue.empty() and can_download == 0:
                    logger.debug(f"Download throttled: total_pending={total_pending}, "
                               f"limit={max_pending_downloads}, "
                               f"downloads={current_downloads}, batch_queue_files={batch_queue_files}, "
                               f"batch_manager_files={batch_manager_files}, active_jobs_files={active_jobs_files}")
                    # Record throttle event
                    if self.metrics and self.metrics.enabled:
                        self.metrics.record_download_throttled(reason="flow_control")
                
                # Check completed downloads
                done_futures = [f for f in download_futures if f.done()]
                
                for future in done_futures:
                    file_record = download_futures.pop(future)
                    
                    try:
                        success, result = future.result()
                        
                        if success:
                            # Check if object was deleted/moved
                            if result.get('reason') == 'object_deleted':
                                logger.info(f"Object deleted/moved, marking as completed: {file_record['s3_key']}")
                                # Object deletion is already handled in the downloader
                                # Record as skipped in metrics
                                if self.metrics and self.metrics.enabled:
                                    file_ext = os.path.splitext(file_record['s3_key'])[1].lower()
                                    self.metrics.record_file_skipped(file_type=file_ext, reason="object_deleted")
                                continue
                            
                            # Record download metrics
                            if self.metrics and self.metrics.enabled:
                                file_ext = os.path.splitext(result['s3_key'])[1].lower()
                                download_time = result.get('download_time', 0)
                                self.metrics.record_file_downloaded(
                                    file_size_bytes=result['file_size'],
                                    duration_seconds=download_time,
                                    file_type=file_ext
                                )
                            download_time = result.get('download_time', 0)
                            if download_time is not None and download_time >= 0:
                                log_with_context(logger, 'info', "PIPELINE_LATENCY", context={
                                    'stage': 'download', 'file_id': file_record.get('file_id'),
                                    'duration_sec': round(download_time, 3)
                                })
                                if self.metrics and self.metrics.enabled:
                                    self.metrics.record_pipeline_stage_duration('download', download_time)
                            # Add to batch manager for normal downloads
                            # Merge result with file_record to get all fields
                            merged_record = {**file_record, **result}
                            file_info = self._create_file_info(
                                merged_record,
                                local_path=result['local_path'],
                                file_size=result['file_size']
                            )
                            self._add_file_to_batch(file_info, batch_source="from download")
                        else:
                            logger.error(f"Download failed for {file_record['s3_key']}: {result.get('error')}")
                            # Record download failure metrics
                            if self.metrics and self.metrics.enabled:
                                file_ext = os.path.splitext(file_record['s3_key'])[1].lower()
                                self.metrics.record_file_failed(
                                    file_type=file_ext,
                                    error_type="download_failed"
                                )
                    
                    except Exception as e:
                        logger.error(f"Error processing download result: {e}")
                
                # Check for time-based batch flush
                time_batch = self.batch_manager.check_time_based_flush()
                if time_batch:
                    self.batch_queue.put(time_batch)
                    self._record_batch_created_metrics(time_batch, batch_source="time-based flush")
                
                # Small sleep to prevent busy waiting
                time.sleep(1)
            
            except Exception as e:
                logger.error(f"Error in download worker: {e}")
        
        logger.info("Download worker stopped")
    
    def submission_worker(self):
        """Worker for submitting batches to NeMo-Retriever"""
        logger.info("Submission worker started")
        
        # Check if sequence execution is enabled
        enable_sequence_execution = self.config.processing.enable_sequence_execution
        max_outstanding_tasks = self.config.processing.max_outstanding_tasks
        
        if enable_sequence_execution:
            logger.info("Sequence execution ENABLED: Will wait for each job to complete before submitting next")
        else:
            logger.info("Sequence execution DISABLED: Jobs will be submitted without waiting for completion")
        
        logger.info(f"Max outstanding tasks limit: {max_outstanding_tasks}")
        
        while self.running:
            try:
                # Check if we've reached max_outstanding_tasks limit
                # max_outstanding_tasks is the number of FILES, not jobs
                active_jobs_files = self._count_files_in_active_jobs()
                if active_jobs_files >= max_outstanding_tasks:
                    logger.debug(f"Submission throttled: active_jobs_files={active_jobs_files}, "
                               f"max_outstanding_tasks={max_outstanding_tasks}")
                    # Record throttle event
                    if self.metrics and self.metrics.enabled:
                        self.metrics.record_submission_throttled(reason="max_outstanding_tasks")
                    time.sleep(0.5)  # Wait before checking again
                    continue
                
                # Get batch from queue with timeout
                try:
                    batch = self.batch_queue.get(timeout=1)
                    batch_created_at = batch.get('created_at')
                    if batch_created_at is not None:
                        batch_queue_duration_sec = round(time.time() - batch_created_at, 3)
                        log_with_context(logger, 'debug', "PIPELINE_LATENCY", context={
                            'stage': 'batch_queue', 'batch_id': batch.get('batch_id'),
                            'file_count': batch.get('file_count', 0), 'duration_sec': batch_queue_duration_sec
                        })
                        if self.metrics and self.metrics.enabled:
                            self.metrics.record_pipeline_stage_duration('batch_queue', batch_queue_duration_sec)
                    # Safety check: Ensure batch size doesn't exceed max_outstanding_tasks
                    batch_file_count = batch.get('file_count', len(batch.get('files', [])))
                    if batch_file_count > max_outstanding_tasks:
                        original_batch_id = batch['batch_id']
                        logger.warning(f"Batch {original_batch_id} has {batch_file_count} files, "
                                     f"which exceeds max_outstanding_tasks ({max_outstanding_tasks}). "
                                     f"Splitting batch into smaller batches.")
                        
                        # Split the batch into smaller batches
                        files = batch.get('files', [])
                        split_batches = []
                        
                        for i in range(0, len(files), max_outstanding_tasks):
                            chunk_files = files[i:i + max_outstanding_tasks]
                            chunk_bytes = sum(f.file_size if hasattr(f, 'file_size') else f.get('file_size', 0) for f in chunk_files)
                            
                            # First split keeps original batch_id, others get new UUIDs
                            split_index = i // max_outstanding_tasks
                            if split_index == 0:
                                split_batch_id = original_batch_id
                            else:
                                split_batch_id = str(uuid.uuid4())
                            
                            split_batch = {
                                'batch_id': split_batch_id,
                                'files': chunk_files,
                                'file_count': len(chunk_files),
                                'total_bytes': chunk_bytes,
                                'created_at': batch.get('created_at', time.time())
                            }
                            split_batches.append(split_batch)
                        
                        logger.info(f"Split batch {original_batch_id} into {len(split_batches)} smaller batches: "
                                  f"{[b['file_count'] for b in split_batches]} files each")
                        
                        # Use the first split batch for this iteration
                        batch = split_batches[0]
                        batch_file_count = batch['file_count']
                        
                        # Put remaining split batches back into the queue (in reverse order to maintain order)
                        # and record batch creation metrics for new split batches
                        for remaining_batch in reversed(split_batches[1:]):
                            self.batch_queue.put(remaining_batch)
                            self._record_batch_created_metrics(remaining_batch, batch_source="batch split")
                        
                        logger.debug(f"Requeued {len(split_batches) - 1} split batches for later processing")
                    
                    # Double-check before submitting (in case active_jobs changed)
                    active_jobs_files = self._count_files_in_active_jobs()
                    if active_jobs_files >= max_outstanding_tasks:
                        logger.debug(f"Submission throttled before batch submit: active_jobs_files={active_jobs_files}, "
                                   f"max_outstanding_tasks={max_outstanding_tasks}. Re-queuing batch.")
                        # Put batch back at front of queue
                        self.batch_queue.put(batch)
                        time.sleep(0.5)
                        continue
                    
                    # Check if submitting this batch would exceed max_outstanding_tasks
                    if active_jobs_files + batch_file_count > max_outstanding_tasks:
                        logger.debug(f"Submission throttled: Submitting batch {batch['batch_id']} with {batch_file_count} files "
                                   f"would exceed max_outstanding_tasks. active_jobs_files={active_jobs_files}, "
                                   f"would_be={active_jobs_files + batch_file_count}, limit={max_outstanding_tasks}. Re-queuing batch.")
                        # Put batch back at front of queue
                        self.batch_queue.put(batch)
                        time.sleep(0.5)
                        continue
                    
                    # Submit batch
                    logger.debug(f"Submitting batch {batch['batch_id']}: batch_files={batch_file_count}, "
                               f"active_jobs_files={active_jobs_files}, max_outstanding_tasks={max_outstanding_tasks}")
                    submit_start = time.time()
                    success, result = self.job_submitter.submit_batch(batch)
                    submission_duration_sec = round(time.time() - submit_start, 3)
                    log_with_context(logger, 'info', "PIPELINE_LATENCY", context={
                        'stage': 'submission', 'batch_id': batch.get('batch_id'),
                        'file_count': batch_file_count, 'duration_sec': submission_duration_sec
                    })
                    if self.metrics and self.metrics.enabled:
                        self.metrics.record_pipeline_stage_duration('submission', submission_duration_sec)
                    if success:
                        # Add jobs to status tracker
                        # Get job_to_files mapping from result (job_id -> [file_ids])
                        job_to_files = result.get('job_to_files', {})
                        
                        # Create file_id -> file_detail mapping for quick lookup
                        file_id_to_detail = {f.file_id: f.to_dict() for f in batch['files']}
                        
                        job_ids = result['job_ids']
                        job_id_to_trace_id = result.get('job_id_to_trace_id', {})
                        for job_id in job_ids:
                            # Get file ID for this specific job (one job = one file)
                            file_ids = job_to_files.get(job_id, [])
                            if not file_ids:
                                logger.error(f"No file_id found for job {job_id} in batch {batch['batch_id']}. Skipping job tracking.")
                                continue
                            file_id = file_ids[0]  # Each job has exactly one file_id
                            
                            # Get file detail for this file_id (required)
                            file_detail = file_id_to_detail.get(file_id)
                            if not file_detail:
                                logger.error(f"Missing file_detail for file_id {file_id} in job {job_id} in batch {batch['batch_id']}. Skipping job tracking.")
                                continue
                            
                            # Get trace_id for this job (required)
                            trace_id = job_id_to_trace_id.get(job_id)
                            if not trace_id:
                                logger.error(f"Missing trace_id for job {job_id} in batch {batch['batch_id']}. Skipping job tracking.")
                                continue
                            
                            self.status_tracker.add_job(job_id, batch['batch_id'], file_id, file_detail, trace_id)
                            
                            # Record job submission metrics
                            if self.metrics and self.metrics.enabled:
                                # Get file type from file detail if available
                                file_type = "unknown"
                                if file_detail:
                                    s3_key = file_detail.get('s3_key', '')
                                    file_type = os.path.splitext(s3_key)[1].lower()
                                self.metrics.record_job_submitted(job_id=job_id, file_type=file_type)
                        
                        # Get updated active jobs file count after adding new jobs
                        # max_outstanding_tasks is the number of FILES, not jobs
                        updated_active_jobs_files = self._count_files_in_active_jobs()
                        
                        # Warn if we exceeded max_outstanding_tasks (shouldn't happen due to throttling, but good to log)
                        if updated_active_jobs_files > max_outstanding_tasks:
                            logger.warning(f"Active jobs files ({updated_active_jobs_files}) exceeded max_outstanding_tasks "
                                         f"({max_outstanding_tasks}) after submitting batch {batch['batch_id']}")
                        
                        # Handle failed files (job creation failures)
                        failed_files = result.get('failed_files', [])
                        for failed_file_info in failed_files:
                            if isinstance(failed_file_info, tuple) and len(failed_file_info) == 3:
                                file_id, file_detail, error_msg = failed_file_info
                                # Add to dead letter queue
                                self.failure_queue.put({
                                    'file_id': file_id,
                                    'file_detail': file_detail,
                                    'error': error_msg,
                                    'failure_type': 'job_creation_failed'
                                })
                                logger.warning(f"Added failed job creation file {file_id} to dead letter queue: {error_msg}")
                            else:
                                # Backward compatibility: if it's just a file_id string
                                logger.warning(f"Failed file info format is not a tuple: {failed_file_info}")
                        
                        log_with_context(logger, 'info', 
                                       f"Submitted batch {batch['batch_id']}",
                                       context={
                                           'batch_id': batch['batch_id'],
                                           'job_ids': job_ids,
                                           'file_count': result['file_count'],
                                           'failed_files_count': len(failed_files),
                                           'active_jobs_files_before': active_jobs_files,
                                           'active_jobs_files_after': updated_active_jobs_files,
                                           'max_outstanding_tasks': max_outstanding_tasks
                                       })
                        
                        # Record batch submission metrics
                        if self.metrics and self.metrics.enabled:
                            self.metrics.record_batch_submitted(
                                batch_id=batch['batch_id'],
                                file_count=result['file_count']
                            )
                        
                        # If sequence execution is enabled, wait for jobs to complete before processing next batch
                        if enable_sequence_execution and job_ids:
                            logger.info(f"Sequence execution: Waiting for {len(job_ids)} job(s) in batch {batch['batch_id']} to complete...")
                            completed = self.status_tracker.wait_for_jobs(
                                job_ids, 
                                timeout=self.config.nemo_retriever.timeout,
                                poll_interval=self.config.processing.status_poll_interval
                            )
                            if completed:
                                logger.info(f"Sequence execution: Batch {batch['batch_id']} completed, ready for next batch")
                            else:
                                logger.warning(f"Sequence execution: Batch {batch['batch_id']} timed out, proceeding to next batch")
                    else:
                        logger.error(f"Failed to submit batch {batch['batch_id']}: {result.get('error')}")
                    
                    self.batch_queue.task_done()
                
                except queue.Empty:
                    continue
            
            except Exception as e:
                logger.error(f"Error in submission worker: {e}")
        
        logger.info("Submission worker stopped")
    
    def completion_worker(self):
        """Worker for handling completed jobs and failures"""
        logger.info("Completion worker started")
        
        while self.running:
            try:
                # Check completion queue
                try:
                    completion_data = self.completion_queue.get(timeout=0.5)
                    nemo_retriever_sec = completion_data.get('nemo_retriever_duration_sec')
                    if nemo_retriever_sec is not None:
                        log_with_context(logger, 'info', "PIPELINE_LATENCY", context={
                            'stage': 'nemo_retriever', 'file_id': completion_data.get('file_id'),
                            'job_id': completion_data.get('job_id'), 'duration_sec': nemo_retriever_sec
                        })
                        if self.metrics and self.metrics.enabled:
                            self.metrics.record_pipeline_stage_duration('nemo_retriever', nemo_retriever_sec)
                    completion_handle_start = time.time()
                    self.completion_handler.handle_completion(completion_data)
                    completion_handle_sec = round(time.time() - completion_handle_start, 3)
                    log_with_context(logger, 'info', "PIPELINE_LATENCY", context={
                        'stage': 'completion_handle', 'file_id': completion_data.get('file_id'),
                        'duration_sec': completion_handle_sec
                    })
                    if self.metrics and self.metrics.enabled:
                        self.metrics.record_pipeline_stage_duration('completion_handle', completion_handle_sec)
                    self.completion_queue.task_done()
                    continue
                except queue.Empty:
                    pass
                
                # Check failure queue
                try:
                    failure_data = self.failure_queue.get(timeout=0.5)
                    
                    # Handle permanent failures
                    # Single file format: {file_id, file_detail, error, trace_id} (from status_tracker)
                    file_id = failure_data.get('file_id')
                    error = failure_data.get('error', 'Unknown error')
                    file_detail = failure_data.get('file_detail')
                    trace_id = failure_data.get('trace_id')  # Trace ID for debugging/correlation
                    
                    if file_id:
                        self.completion_handler.handle_failure(file_id, error, file_detail, trace_id=trace_id)
                    else:
                        logger.warning(f"Failure data missing file_id: {failure_data}")
                    
                    self.failure_queue.task_done()
                    continue
                except queue.Empty:
                    pass
            
            except Exception as e:
                logger.error(f"Error in completion worker: {e}")
        
        logger.info("Completion worker stopped")
    
    def start(self):
        """Start the pipeline"""
        logger.info("=" * 80)
        logger.info("STARTING NV-INGEST PIPELINE")
        logger.info("=" * 80)
        
        # Initialize Milvus collection
        self._initialize_milvus_collection()
        
        # Recover state
        self.recover_state()
        
        # Debug: Check if we reach this point
        logger.info("Recovery completed, about to clear active jobs...")
        
        # Clear any stale active jobs from previous runs (after recovery)
        logger.info("Clearing stale active jobs from status tracker...")
        self.status_tracker.clear_active_jobs()
        
        # Start components
        self.running = True
        
        logger.info("Starting pipeline components...")
        
        # Initialize baseline orchestrator if enabled
        self._initialize_baseline_orchestrator()
        
        # Check if we should pause Kafka during baseline
        should_pause_kafka = (
            self.config.baseline.enabled and 
            self.config.baseline.pause_kafka_during_baseline and
            self.baseline_orchestrator and
            self.baseline_orchestrator.is_baseline_in_progress()
        )
        
        # Start Kafka consumer (unless paused for baseline)
        if should_pause_kafka:
            logger.info("Kafka consumer paused during baseline processing")
            self._kafka_paused_for_baseline = True
        elif self.kafka_consumer is not None:
            self.kafka_consumer.start()
        else:
            logger.warning("Kafka consumer not available, skipping")
        
        # Start delete handler
        if self.delete_handler is not None:
            self.delete_handler.start()
        else:
            logger.warning("Delete handler not available, skipping")
        
        # Start status tracker
        self.status_tracker.start()
        
        # Start configuration API server
        self._start_config_api()
        
        # Start workers
        download_thread = threading.Thread(target=self.download_worker, daemon=True)
        submission_thread = threading.Thread(target=self.submission_worker, daemon=True)
        
        download_thread.start()
        submission_thread.start()
        
        # Start Milvus batch inserter if enabled
        if self.milvus_batch_inserter is not None:
            self.milvus_batch_inserter.start()
        
        # Start multiple completion workers for parallel Milvus bulk inserts
        completion_workers_count = getattr(self.config.threading, 'completion_workers', 1)
        logger.info(f"Starting {completion_workers_count} completion worker(s) for parallel bulk inserts")
        completion_threads = []
        for i in range(completion_workers_count):
            completion_thread = threading.Thread(
                target=self.completion_worker, 
                daemon=True,
                name=f"completion-worker-{i}"
            )
            completion_thread.start()
            completion_threads.append(completion_thread)
        
        logger.info("=" * 80)
        logger.info("PIPELINE RUNNING")
        logger.info(f"Configuration API available at: http://localhost:{self.config_api_port}")
        logger.info("=" * 80)
        
        # Main monitoring loop
        try:
            while self.running:
                # Update config from hot-reload manager (already handled via callbacks)
                # But we still need to update components that don't have callbacks
                new_config = pipeline_config_manager.get_config()
                if new_config:
                    self.config = new_config
                
                # Milvus re-init is handled by on_storage/milvus_config_change callbacks
                self._reinitialize_milvus_on_config_change()
                
                # Update batching config from config manager (hot-reload)
                if self.batch_manager:
                    flushed_batches = self.batch_manager.update_config_from_manager()
                    # If batches were flushed due to max_outstanding_tasks violation, add them to queue
                    for flushed_batch in flushed_batches:
                        self.batch_queue.put(flushed_batch)
                        self._record_batch_created_metrics(flushed_batch, batch_source="hot-reload flush")
                
                # Log pipeline stats periodically
                time.sleep(10)
                
                if self.running:
                    stats = {
                        'message_queue': self.message_queue.qsize(),
                        'batch_queue': self.batch_queue.qsize(),
                        'completion_queue': self.completion_queue.qsize(),
                        'batch_stats': self.batch_manager.get_batch_stats(),
                        'tracker_stats': self.status_tracker.get_stats()
                    }
                    
                    log_with_context(logger, 'info', "Pipeline statistics", context=stats)
        
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        
        finally:
            self.shutdown()
    
    def _start_config_api(self):
        """Start the configuration API server in a separate thread"""
        def run_config_api():
            try:
                logger.info(f"Starting configuration API server on port {self.config_api_port}")
                config_app.run(host='0.0.0.0', port=self.config_api_port, debug=False, use_reloader=False)
            except Exception as e:
                logger.error(f"Configuration API server error: {e}")
        
        self.config_api_thread = threading.Thread(target=run_config_api, daemon=True)
        self.config_api_thread.start()
        logger.info("Configuration API server started")
    
    def _stop_config_api(self):
        """Stop the configuration API server"""
        if self.config_api_thread and self.config_api_thread.is_alive():
            logger.info("Stopping configuration API server...")
            # Note: Flask doesn't have a clean shutdown method, so we just log it
            # In production, you might want to use a proper WSGI server like gunicorn
            logger.info("Configuration API server stopped")
    
    def _initialize_baseline_orchestrator(self):
        """Initialize and start the baseline orchestrator if enabled.
        Only the baseline leader (pod ordinal 0) runs baseline processing."""
        if not self._is_baseline_leader:
            logger.info(f"Pod ordinal {self._pod_ordinal} is not the baseline leader — skipping baseline")
            return
        
        if not self.config.baseline.enabled:
            logger.info("Baseline processing is disabled")
            return
        
        enabled_buckets = [b for b in self.config.baseline.buckets if b.enabled]
        if not enabled_buckets:
            logger.info("No enabled buckets for baseline processing")
            return
        
        logger.info("=" * 60)
        logger.info("BASELINE PROCESSING: Initializing...")
        logger.info(f"Enabled buckets: {[b.bucket_name for b in enabled_buckets]}")
        logger.info("=" * 60)
        
        try:
            # Create baseline orchestrator with S3 client
            # Pass status_tracker and max_outstanding_tasks for throttling
            self.baseline_orchestrator = BaselineOrchestrator(
                s3_client=self.s3_downloader.s3_client,
                config=self.config.baseline,
                message_queue=self.message_queue,
                status_tracker=self.status_tracker,
                max_outstanding_tasks=self.config.processing.max_outstanding_tasks,
                on_baseline_complete=self._on_baseline_complete,
                on_bucket_complete=self._on_bucket_baseline_complete
            )
            
            # Register callback for baseline file completion tracking
            self.completion_handler.set_baseline_callback(
                self.baseline_orchestrator.mark_file_processed
            )
            
            # Start baseline processing
            self.baseline_orchestrator.start()
            self._baseline_in_progress = True
            
            logger.info("Baseline orchestrator started successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize baseline orchestrator: {e}")
            self.baseline_orchestrator = None
    
    def _on_baseline_complete(self):
        """Callback when all baseline operations are complete"""
        logger.info("=" * 60)
        logger.info("BASELINE PROCESSING: All buckets complete!")
        logger.info("=" * 60)
        
        self._baseline_in_progress = False
        
        # Resume Kafka if it was paused
        if self._kafka_paused_for_baseline:
            logger.info("Resuming Kafka consumer after baseline completion...")
            if self.kafka_consumer is not None:
                self.kafka_consumer.start()
                self._kafka_paused_for_baseline = False
                logger.info("Kafka consumer resumed")
    
    def _on_bucket_baseline_complete(self, bucket_name: str):
        """Callback when a single bucket's baseline is complete"""
        logger.info(f"Baseline complete for bucket: {bucket_name}")
        
        # If auto-disable is enabled, we could update the config here
        # For now, just log it
        if self.config.baseline.auto_disable_completed_buckets:
            logger.info(f"Bucket {bucket_name} can be disabled in config")
    
    def _query_embedding_dimension(self, model_name: str = "embed") -> int:
        """
        Query the embedding model to get its actual dimension
        
        Args:
            model_name: Name of the embedding model
            
        Returns:
            The dimension of the embedding model, or None if query fails
        """
        try:
            # Only query if we have a local NeMo-Retriever endpoint configured
            # Skip query if using NVIDIA cloud API (which requires API key and may not be available)
            nemo_retriever_host = getattr(self.config.nemo_retriever, 'host', None) if hasattr(self.config, 'nemo_retriever') else None
            nemo_retriever_port = getattr(self.config.nemo_retriever, 'port', None) if hasattr(self.config, 'nemo_retriever') else None
            
            if not nemo_retriever_host or not nemo_retriever_port or nemo_retriever_port == 0:
                logger.debug(f"Skipping embedding dimension query - no local NeMo-Retriever endpoint configured (host: {nemo_retriever_host}, port: {nemo_retriever_port})")
                return None
            
            # Try to query the NeMo-Retriever embedding endpoint
            # This is a best-effort approach - we'll make a test embedding call
            logger.info(f"Attempting to query embedding dimension from model '{model_name}' via local NeMo-Retriever endpoint...")
            
            # We'll use the nv_ingest_client to make a test embedding call
            try:
                from nv_ingest_client.util.transport import infer_microservice
                from nv_ingest_client.util.util import ClientConfigSchema
                
                client_config = ClientConfigSchema()
                
                # Only query if we have a local endpoint configured
                # ClientConfigSchema may default to NVIDIA cloud API, which we want to avoid
                if not client_config.embedding_nim_endpoint or 'integrate.api.nvidia.com' in client_config.embedding_nim_endpoint:
                    logger.debug(f"Skipping embedding dimension query - using NVIDIA cloud API endpoint (not configured for local use)")
                    return None
                
                test_data = [{"metadata": {"content": "test"}}]
                
                # Make a test embedding call
                result = infer_microservice(
                    data=test_data,
                    model_name=model_name,
                    embedding_endpoint=client_config.embedding_nim_endpoint,
                    nvidia_api_key=client_config.nvidia_api_key,
                    batch_size=1
                )
                
                # Extract dimension from result
                if result and len(result) > 0:
                    embedding = result[0]
                    if isinstance(embedding, dict):
                        embedding = embedding.get('embedding', embedding.get('data', None))
                    if embedding and hasattr(embedding, '__len__'):
                        dimension = len(embedding)
                        logger.info(f"Successfully queried embedding dimension: {dimension}")
                        return dimension
                    else:
                        logger.warning("Could not extract dimension from embedding result")
                else:
                    logger.warning("Empty result from embedding query")
                    
            except Exception as e:
                logger.debug(f"Failed to query embedding dimension via nv_ingest_client (this is expected if no local endpoint): {e}")
            
            # Fallback: return None to use config dimension
            return None
            
        except Exception as e:
            logger.debug(f"Error querying embedding dimension (using config dimension instead): {e}")
            return None
    
    def _initialize_milvus_collection(self):
        """Initialize Milvus collection - check if exists, create if not"""
        try:
            # Get storage configuration
            storage_config = config_manager.get_storage_config()
            
            # Check if storage config is valid
            if storage_config is None:
                logger.error("Storage configuration is None, cannot initialize Milvus")
                return False
            
            # Skip if Milvus is not enabled
            if not storage_config.milvus_enabled:
                logger.info("Milvus is disabled, skipping collection initialization")
                return True
            
            logger.info("=" * 80)
            logger.info("INITIALIZING MILVUS COLLECTION")
            logger.info("=" * 80)
            
            # Check if we should auto-create the collection or let NeMo-Retriever create it
            if not storage_config.milvus_auto_create_collection:
                logger.info("auto_create_collection is False - letting NeMo-Retriever create collection with correct dimension")
                # Still initialize the manager for connection purposes, but don't create collection
                self.milvus_manager = MilvusManager(
                    host=storage_config.milvus_host,
                    port=storage_config.milvus_port,
                    collection_name=storage_config.milvus_collection,
                    index_type=storage_config.milvus_index_type,
                    metric_type=storage_config.milvus_metric_type,
                    nlist=storage_config.milvus_nlist,
                    nprobe=storage_config.milvus_nprobe,
                    auto_id=storage_config.milvus_auto_id,
                    consistency_level=storage_config.milvus_consistency_level,
                    dimension=2048,  # Placeholder, NeMo-Retriever will use correct dimension
                    sparse=storage_config.milvus_sparse,
                    gpu_index=storage_config.milvus_gpu_index,
                    gpu_search=storage_config.milvus_gpu_search,
                    local_index=storage_config.milvus_local_index
                )
                # Just connect to verify connectivity
                if self.milvus_manager.connect():
                    logger.info(f"Connected to Milvus. Collection '{storage_config.milvus_collection}' will be created by NeMo-Retriever.")
                    self.last_milvus_collection_name = storage_config.milvus_collection
                    self.last_milvus_endpoint = f"{storage_config.milvus_host}:{storage_config.milvus_port}"
                    return True
                else:
                    logger.error("Failed to connect to Milvus")
                    return False
            
            # Auto-create collection enabled - proceed with dimension detection
            # Get embedding config to determine dimension
            dimension = 2048  # Default fallback
            try:
                # Try to get dimension from embed config
                embed_config = config_manager.get_embed_config("pdf")
                if embed_config and embed_config.enabled:
                    dimension = embed_config.dimensions
                    logger.info(f"Using dimension from config: {dimension}")
                    
                    # Try to query actual dimension from the model
                    actual_dimension = self._query_embedding_dimension(embed_config.model)
                    if actual_dimension and actual_dimension != dimension:
                        logger.warning(f"Config dimension ({dimension}) differs from model dimension ({actual_dimension}). Using model dimension.")
                        dimension = actual_dimension
                    elif actual_dimension:
                        logger.info(f"Confirmed dimension from model: {dimension}")
                        
            except Exception as e:
                logger.warning(f"Could not determine embedding dimension, using default {dimension}: {e}")
            
            # Initialize Milvus manager
            self.milvus_manager = MilvusManager(
                host=storage_config.milvus_host,
                port=storage_config.milvus_port,
                collection_name=storage_config.milvus_collection,
                index_type=storage_config.milvus_index_type,
                metric_type=storage_config.milvus_metric_type,
                nlist=storage_config.milvus_nlist,
                nprobe=storage_config.milvus_nprobe,
                auto_id=storage_config.milvus_auto_id,
                consistency_level=storage_config.milvus_consistency_level,
                dimension=dimension,
                sparse=storage_config.milvus_sparse,
                gpu_index=storage_config.milvus_gpu_index,
                gpu_search=storage_config.milvus_gpu_search,
                local_index=storage_config.milvus_local_index
            )
            
            # Ensure collection exists (create if not) - use NeMo-Retriever compatible schema
            if self.milvus_manager.ensure_collection():
                logger.info(f"Milvus collection '{storage_config.milvus_collection}' is ready (dimension: {dimension})")
                
                # Get collection info
                info = self.milvus_manager.get_collection_info()
                logger.info(f"Collection info: {info}")
                
                # Update last known collection name and endpoint
                self.last_milvus_collection_name = storage_config.milvus_collection
                self.last_milvus_endpoint = f"{storage_config.milvus_host}:{storage_config.milvus_port}"
                
                # Initialize additional filtered collections (text1, tables1, charts1) if enabled
                if storage_config.milvus_enable_filtered_collections:
                    self._initialize_filtered_collections(storage_config, dimension)
                else:
                    logger.info("Filtered collections are disabled, skipping initialization")
                
                return True
            else:
                logger.error(f"Failed to initialize Milvus collection '{storage_config.milvus_collection}'")
                return False
                
        except Exception as e:
            logger.error(f"Error initializing Milvus collection: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _initialize_filtered_collections(self, storage_config, dimension: int):
        """
        Initialize additional filtered collections based on configuration
        
        These collections use the same schema as the main collection but are used
        for filtered data based on document_type and subtype.
        
        Collection names are configurable via:
        - filtered_text_collection_name: for text content
        - filtered_tables_collection_name: for table content  
        - filtered_charts_collection_name: for chart content
        
        If a collection name is not configured (empty or None), that filtered collection
        will not be created or used.
        
        Args:
            storage_config: Storage configuration
            dimension: Vector dimension
        """
        try:
            logger.info("=" * 80)
            logger.info("INITIALIZING FILTERED MILVUS COLLECTIONS")
            logger.info("=" * 80)
            
            # Build filtered collections dict from config (only include if collection name is configured and not empty)
            filtered_collections = {}
            
            text_collection = storage_config.milvus_filtered_text_collection_name
            if text_collection and text_collection.strip():
                filtered_collections[text_collection.strip()] = {
                    'filter_type': 'document_type', 
                    'filter_value': 'text'
                }
            
            tables_collection = storage_config.milvus_filtered_tables_collection_name
            if tables_collection and tables_collection.strip():
                filtered_collections[tables_collection.strip()] = {
                    'filter_type': 'subtype', 
                    'filter_value': 'table'
                }
            
            charts_collection = storage_config.milvus_filtered_charts_collection_name
            if charts_collection and charts_collection.strip():
                filtered_collections[charts_collection.strip()] = {
                    'filter_type': 'subtype', 
                    'filter_value': 'chart'
                }
            
            if not filtered_collections:
                logger.warning(
                    "enable_filtered_collections is True but no filtered collection names are configured. "
                    "Please provide at least one of: filtered_text_collection_name, "
                    "filtered_tables_collection_name, or filtered_charts_collection_name"
                )
                return
            
            for collection_name, filter_config in filtered_collections.items():
                try:
                    logger.info(f"Initializing filtered collection: {collection_name}")
                    
                    # Create MilvusManager for this collection
                    manager = MilvusManager(
                        host=storage_config.milvus_host,
                        port=storage_config.milvus_port,
                        collection_name=collection_name,
                        index_type=storage_config.milvus_index_type,
                        metric_type=storage_config.milvus_metric_type,
                        nlist=storage_config.milvus_nlist,
                        nprobe=storage_config.milvus_nprobe,
                        auto_id=storage_config.milvus_auto_id,
                        consistency_level=storage_config.milvus_consistency_level,
                        dimension=dimension,
                        sparse=storage_config.milvus_sparse,
                        gpu_index=storage_config.milvus_gpu_index,
                        gpu_search=storage_config.milvus_gpu_search,
                        local_index=storage_config.milvus_local_index
                    )
                    
                    # Ensure collection exists with NeMo-Retriever compatible schema
                    if manager.ensure_collection():
                        info = manager.get_collection_info()
                        logger.info(f"Filtered collection '{collection_name}' is ready: {info}")
                        self.milvus_managers[collection_name] = manager
                    else:
                        logger.error(f"Failed to initialize filtered collection '{collection_name}'")
                        
                except Exception as e:
                    logger.error(f"Error initializing filtered collection '{collection_name}': {e}")
                    import traceback
                    logger.error(traceback.format_exc())
            
            logger.info(f"Initialized {len(self.milvus_managers)} filtered collection(s)")
            
        except Exception as e:
            logger.error(f"Error initializing filtered collections: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _reinitialize_milvus_on_config_change(self):
        """Re-initialize Milvus collection when configuration changes"""
        try:
            storage_config = config_manager.get_storage_config()
            
            # Check if storage config is valid
            if storage_config is None:
                logger.debug("Storage configuration is None, skipping re-initialization")
                return
            
            if not storage_config.milvus_enabled:
                if self.milvus_manager:
                    logger.info("Milvus disabled in config, disconnecting...")
                    self.milvus_manager.disconnect()
                    self.milvus_manager = None
                    self.last_milvus_collection_name = None
                    self.last_milvus_endpoint = None
                return
            
            # Check if host/port are valid (not empty)
            # Note: task_config_manager should use defaults if config has empty values, so this check
            # should rarely trigger. But if it does, we should handle it gracefully.
            if not storage_config.milvus_host or storage_config.milvus_port == 0:
                # Invalid configuration - only disconnect if we have a manager and endpoint actually changed
                if self.milvus_manager:
                    current_manager_endpoint = f"{self.milvus_manager.host}:{self.milvus_manager.port}"
                    # Only disconnect if endpoint actually changed from what we had before
                    # If last_milvus_endpoint is None, this might be initial load - don't disconnect unnecessarily
                    if self.last_milvus_endpoint and self.last_milvus_endpoint != current_manager_endpoint:
                        logger.warning("Milvus host/port not configured and endpoint changed, disconnecting existing connection...")
                        self.milvus_manager.disconnect()
                        self.milvus_manager = None
                        self.last_milvus_collection_name = None
                        self.last_milvus_endpoint = None
                    elif not self.last_milvus_endpoint:
                        # Initial load with empty config but manager exists - keep the connection
                        # This can happen if manager was initialized before config was fully loaded
                        logger.debug("Milvus config has empty values but existing connection exists, keeping it (will use manager's endpoint)")
                        # Initialize tracking from manager
                        self.last_milvus_endpoint = current_manager_endpoint
                        self.last_milvus_collection_name = self.milvus_manager.collection_name
                else:
                    # No manager and invalid config - nothing to disconnect
                    logger.debug("Milvus host/port not configured and no existing connection")
                return
            
            current_endpoint = f"{storage_config.milvus_host}:{storage_config.milvus_port}"
            current_collection_name = storage_config.milvus_collection
            
            # If last_milvus_endpoint is None but manager exists, initialize it from the manager
            if self.last_milvus_endpoint is None and self.milvus_manager is not None:
                self.last_milvus_endpoint = f"{self.milvus_manager.host}:{self.milvus_manager.port}"
                if self.last_milvus_collection_name is None:
                    self.last_milvus_collection_name = self.milvus_manager.collection_name
            
            # Skip if nothing changed and we have a valid connection
            if (self.last_milvus_endpoint == current_endpoint and 
                self.last_milvus_collection_name == current_collection_name and
                self.milvus_manager is not None):
                return
            
            # Check if endpoint changed - need to reconnect
            # Only reconnect if endpoint actually changed or if we don't have a manager
            endpoint_changed = self.last_milvus_endpoint != current_endpoint
            collection_changed = self.last_milvus_collection_name != current_collection_name
            
            # Only reconnect if endpoint actually changed or if we don't have a manager but have valid config
            if endpoint_changed or (self.milvus_manager is None):
                # Endpoint changed or no manager exists - need to connect/reconnect
                if self.milvus_manager:
                    old_endpoint = f"{self.milvus_manager.host}:{self.milvus_manager.port}"
                    logger.info(f"Milvus endpoint changed from '{old_endpoint}' to '{current_endpoint}', reconnecting...")
                    self.milvus_manager.disconnect()
                    self.milvus_manager = None
                
                # Re-initialize with new endpoint (may also have new collection name)
                self._initialize_milvus_collection()
                
            elif collection_changed and self.milvus_manager:
                # Only collection name changed - create new collection with correct dimension
                old_name = self.milvus_manager.collection_name
                logger.info(f"Collection name changed from '{old_name}' to '{current_collection_name}', creating new collection...")
                
                # Get embedding dimension (like during initialization)
                dimension = 2048  # Default fallback
                try:
                    # Try to get dimension from embed config
                    embed_config = config_manager.get_embed_config("pdf")
                    if embed_config and embed_config.enabled:
                        dimension = embed_config.dimensions
                        logger.info(f"Using dimension from config: {dimension}")
                        
                        # Try to query actual dimension from the model
                        actual_dimension = self._query_embedding_dimension(embed_config.model)
                        if actual_dimension and actual_dimension != dimension:
                            logger.warning(f"Config dimension ({dimension}) differs from model dimension ({actual_dimension}). Using model dimension.")
                            dimension = actual_dimension
                        elif actual_dimension:
                            logger.info(f"Confirmed dimension from model: {dimension}")
                            
                except Exception as e:
                    logger.warning(f"Could not determine embedding dimension, using default {dimension}: {e}")
                
                # Create new MilvusManager for the new collection with correct dimension
                new_manager = MilvusManager(
                    host=storage_config.milvus_host,
                    port=storage_config.milvus_port,
                    collection_name=current_collection_name,
                    index_type=storage_config.milvus_index_type,
                    metric_type=storage_config.milvus_metric_type,
                    nlist=storage_config.milvus_nlist,
                    nprobe=storage_config.milvus_nprobe,
                    auto_id=storage_config.milvus_auto_id,
                    consistency_level=storage_config.milvus_consistency_level,
                    dimension=dimension,
                    sparse=storage_config.milvus_sparse,
                    gpu_index=storage_config.milvus_gpu_index,
                    gpu_search=storage_config.milvus_gpu_search,
                    local_index=storage_config.milvus_local_index
                )
                
                # Ensure new collection exists (with NeMo-Retriever schema)
                if new_manager.ensure_collection():
                    logger.info(f"Successfully created/switched to collection '{current_collection_name}' (dimension: {dimension})")
                    info = new_manager.get_collection_info()
                    logger.info(f"Collection info: {info}")
                    
                    # Update manager reference
                    self.milvus_manager = new_manager
                    self.last_milvus_collection_name = current_collection_name
                    
                    # Initialize filtered collections if enabled
                    if storage_config.milvus_enable_filtered_collections:
                        logger.info("Filtered collections enabled, initializing filtered collections for new main collection...")
                        self._initialize_filtered_collections(storage_config, dimension)
                else:
                    logger.error(f"Failed to initialize collection '{current_collection_name}'")
                    
            elif not self.milvus_manager:
                # Manager doesn't exist - initialize it
                self._initialize_milvus_collection()
                
        except Exception as e:
            logger.error(f"Error re-initializing Milvus collection: {e}")
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("=" * 80)
        logger.info("INITIATING GRACEFUL SHUTDOWN")
        logger.info("=" * 80)
        
        self.running = False
        
        # Stop baseline orchestrator if running
        if self.baseline_orchestrator:
            logger.info("Stopping baseline orchestrator...")
            self.baseline_orchestrator.stop()
        
        # Stop Kafka consumer
        logger.info("Stopping Kafka consumer...")
        if self.kafka_consumer is not None:
            self.kafka_consumer.stop()
        
        # Flush any remaining batch
        logger.info("Flushing remaining batch...")
        final_batch = self.batch_manager.force_flush()
        if final_batch:
            logger.info(f"Submitting final batch: {final_batch['batch_id']}")
            success, result = self.job_submitter.submit_batch(final_batch)
            if success:
                job_id_to_trace_id = result.get('job_id_to_trace_id', {})
                job_to_files = result.get('job_to_files', {})
                file_id_to_detail = {f.file_id: f.to_dict() for f in final_batch['files']}
                for job_id in result['job_ids']:
                    # Get file ID for this specific job (one job = one file)
                    file_ids = job_to_files.get(job_id, [])
                    if not file_ids:
                        logger.error(f"No file_id found for job {job_id} in final batch {final_batch['batch_id']}. Skipping job tracking.")
                        continue
                    file_id = file_ids[0]  # Each job has exactly one file_id
                    
                    # Get file detail for this file_id (required)
                    file_detail = file_id_to_detail.get(file_id)
                    if not file_detail:
                        logger.error(f"Missing file_detail for file_id {file_id} in job {job_id} in final batch {final_batch['batch_id']}. Skipping job tracking.")
                        continue
                    
                    trace_id = job_id_to_trace_id.get(job_id)
                    if not trace_id:
                        logger.error(f"Missing trace_id for job {job_id} in final batch {final_batch['batch_id']}. Skipping job tracking.")
                        continue
                    self.status_tracker.add_job(job_id, final_batch['batch_id'], file_id, file_detail, trace_id)
        
        # Wait for active jobs to complete (with timeout)
        logger.info("Waiting for active jobs to complete...")
        time.sleep(5)
        
        # Stop configuration API server
        self._stop_config_api()
        
        # Disconnect from Milvus
        if self.milvus_manager:
            logger.info("Disconnecting from Milvus...")
            self.milvus_manager.disconnect()
            self.milvus_manager = None
        
        # Stop status tracker
        logger.info("Stopping status tracker...")
        self.status_tracker.stop()
        
        # Stop completion handler
        logger.info("Stopping completion handler...")
        self.completion_handler.stop()
        
        # Stop Milvus batch inserter (flushes remaining items)
        if self.milvus_batch_inserter is not None:
            logger.info("Stopping Milvus batch inserter (flushing remaining items)...")
            self.milvus_batch_inserter.stop()
        
        # Stop delete handler
        logger.info("Stopping delete handler...")
        if self.delete_handler is not None:
            self.delete_handler.stop()
        
        # Shutdown thread pools
        logger.info("Shutting down thread pools...")
        self.s3_downloader.shutdown(wait=True, timeout=30)
        self.job_submitter.shutdown(wait=True, timeout=60)
        
        logger.info("=" * 80)
        logger.info("SHUTDOWN COMPLETE")
        logger.info("=" * 80)


def main():
    """Main entry point"""
    # Get initial configuration (will be updated via hot-reload)
    initial_config = pipeline_config_manager.get_config()
    if not initial_config:
        # Fallback to old method if hot-reload manager doesn't have config
        initial_config = get_settings()
    
    # Setup logging
    logging_config = pipeline_config_manager.get_logging_config()
    setup_logging(
        level=logging_config.level,
        log_format=logging_config.format,
        log_file=logging_config.log_file
    )
    
    logger.info("NeMo-Retriever Pipeline Service")
    logger.info(f"Configuration loaded from: {os.getenv('PIPELINE_CONFIG', '/app/nutanix_aidp_connector/config-data/config.yaml')}")
    logger.info("Hot-reload enabled for config.yaml - changes will be applied automatically")
    
    # Create and start orchestrator
    orchestrator = PipelineOrchestrator(initial_config)
    orchestrator.start()


if __name__ == "__main__":
    main()

