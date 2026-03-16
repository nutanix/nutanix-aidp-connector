#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
OpenTelemetry instrumentation for Nutanix AIDP-Connector metrics with Prometheus export.

This module provides:
- Pipeline metrics (jobs, files, batches, downloads, etc.)
- Prometheus metrics endpoint for scraping
- Thread-safe metric updates
"""

import logging
import threading
from typing import Optional, Dict, Any
from functools import wraps
import time

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
from prometheus_client import start_http_server, REGISTRY

logger = logging.getLogger(__name__)


class PipelineMetrics:
    """
    OpenTelemetry-based metrics for the Nutanix AIDP-Connector.
    
    Provides counters, gauges, and histograms for monitoring pipeline health and performance.
    """
    
    _instance: Optional['PipelineMetrics'] = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """Singleton pattern for metrics instance"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config: Optional[Any] = None):
        """
        Initialize OpenTelemetry metrics with Prometheus exporter.
        
        Args:
            config: TelemetryConfig object with service name, version, and prometheus settings
        """
        # Skip re-initialization for singleton
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self._initialized = False
        self._enabled = False
        self._meter = None
        self._prometheus_server_started = False
        
        if config is None:
            logger.warning("Telemetry config not provided, metrics will be disabled")
            return
        
        if not config.enabled:
            logger.info("Telemetry is disabled by configuration")
            return
        
        try:
            self._setup_metrics(config)
            self._initialized = True
            self._enabled = True
            logger.info(f"OpenTelemetry metrics initialized for service: {config.service_name}")
        except Exception as e:
            logger.error(f"Failed to initialize OpenTelemetry metrics: {e}")
            self._enabled = False
    
    def _setup_metrics(self, config):
        """Set up OpenTelemetry metrics provider and instruments"""
        # Create resource with service information
        resource = Resource.create({
            SERVICE_NAME: config.service_name,
            SERVICE_VERSION: config.service_version,
        })
        
        # Create Prometheus metric reader
        self._prometheus_reader = PrometheusMetricReader()
        
        # Create meter provider with Prometheus exporter
        provider = MeterProvider(
            resource=resource,
            metric_readers=[self._prometheus_reader]
        )
        
        # Set as global meter provider
        metrics.set_meter_provider(provider)
        
        # Get meter for creating instruments
        self._meter = metrics.get_meter(
            name=config.service_name,
            version=config.service_version
        )
        
        # Start Prometheus HTTP server for metrics scraping
        self._start_prometheus_server(config.prometheus_port)
        
        # Create metric instruments
        self._create_instruments()
    
    def _start_prometheus_server(self, port: int):
        """Start Prometheus HTTP server for metrics endpoint"""
        if not self._prometheus_server_started:
            try:
                start_http_server(port, registry=REGISTRY)
                self._prometheus_server_started = True
                logger.info(f"Prometheus metrics server started on port {port}")
            except Exception as e:
                logger.error(f"Failed to start Prometheus server on port {port}: {e}")
                raise
    
    def _create_instruments(self):
        """Create all metric instruments"""
        
        # =============================================================================
        # COUNTERS - Cumulative metrics that only increase
        # =============================================================================
        
        # NOTE: Architecture uses 1:1 job-to-file mapping (each job processes exactly one file).
        # Therefore, job metrics (jobs_submitted_total, jobs_completed_total, jobs_failed_total)
        # should equal file metrics (files_processed_total + files_failed_total) for files that
        # reached the completion handler. Files that fail during download are tracked separately.
        
        # Job metrics
        self.jobs_submitted_total = self._meter.create_counter(
            name="nutanix_aidp_connector_jobs_submitted_total",
            description="Total number of jobs submitted to NeMo-Retriever (1 job = 1 file)",
            unit="1"
        )
        
        self.jobs_completed_total = self._meter.create_counter(
            name="nutanix_aidp_connector_jobs_completed_total",
            description="Total number of jobs completed successfully (1 job = 1 file)",
            unit="1"
        )
        
        self.jobs_failed_total = self._meter.create_counter(
            name="nutanix_aidp_connector_jobs_failed_total",
            description="Total number of jobs that failed (1 job = 1 file)",
            unit="1"
        )
        
        # Job error metrics by HTTP status code
        self.job_errors_by_status_code_total = self._meter.create_counter(
            name="nutanix_aidp_connector_job_errors_by_status_code_total",
            description="Total number of job errors by HTTP status code from NeMo-Retriever server",
            unit="1"
        )
        
        # Job status code metrics (for all status codes, not just errors)
        self.job_status_codes_total = self._meter.create_counter(
            name="nutanix_aidp_connector_job_status_codes_total",
            description="Total number of job status checks by HTTP status code (200, 202, 404, 410, 503)",
            unit="1"
        )
        
        # File metrics
        self.files_downloaded_total = self._meter.create_counter(
            name="nutanix_aidp_connector_files_downloaded_total",
            description="Total number of files downloaded from S3",
            unit="1"
        )
        
        self.files_processed_total = self._meter.create_counter(
            name="nutanix_aidp_connector_files_processed_total",
            description="Total number of files processed successfully",
            unit="1"
        )
        
        self.files_failed_total = self._meter.create_counter(
            name="nutanix_aidp_connector_files_failed_total",
            description="Total number of files that failed processing",
            unit="1"
        )
        
        self.files_skipped_total = self._meter.create_counter(
            name="nutanix_aidp_connector_files_skipped_total",
            description="Total number of files skipped (unsupported type, etc.)",
            unit="1"
        )
        
        # Batch metrics
        self.batches_created_total = self._meter.create_counter(
            name="nutanix_aidp_connector_batches_created_total",
            description="Total number of batches created",
            unit="1"
        )
        
        self.batches_submitted_total = self._meter.create_counter(
            name="nutanix_aidp_connector_batches_submitted_total",
            description="Total number of batches submitted",
            unit="1"
        )
        
        # Kafka metrics
        self.kafka_messages_received_total = self._meter.create_counter(
            name="nutanix_aidp_connector_kafka_messages_received_total",
            description="Total number of Kafka messages received",
            unit="1"
        )
        
        self.kafka_messages_processed_total = self._meter.create_counter(
            name="nutanix_aidp_connector_kafka_messages_processed_total",
            description="Total number of Kafka messages processed successfully",
            unit="1"
        )
        
        # Milvus metrics
        self.milvus_inserts_total = self._meter.create_counter(
            name="nutanix_aidp_connector_milvus_inserts_total",
            description="Total number of successful Milvus bulk inserts",
            unit="1"
        )
        
        self.milvus_insert_failures_total = self._meter.create_counter(
            name="nutanix_aidp_connector_milvus_insert_failures_total",
            description="Total number of failed Milvus bulk inserts",
            unit="1"
        )
        
        self.embeddings_stored_total = self._meter.create_counter(
            name="nutanix_aidp_connector_embeddings_stored_total",
            description="Total number of embeddings stored in Milvus",
            unit="1"
        )
        
        # Bytes processed
        self.bytes_downloaded_total = self._meter.create_counter(
            name="nutanix_aidp_connector_bytes_downloaded_total",
            description="Total bytes downloaded from S3",
            unit="By"
        )
        
        self.bytes_processed_total = self._meter.create_counter(
            name="nutanix_aidp_connector_bytes_processed_total",
            description="Total bytes processed by NeMo-Retriever",
            unit="By"
        )
        
        # Throttling/flow control
        self.submission_throttle_events_total = self._meter.create_counter(
            name="nutanix_aidp_connector_submission_throttle_events_total",
            description="Total number of times submission was throttled",
            unit="1"
        )
        
        self.download_throttle_events_total = self._meter.create_counter(
            name="nutanix_aidp_connector_download_throttle_events_total",
            description="Total number of times download was throttled",
            unit="1"
        )
        
        # =============================================================================
        # GAUGES - Point-in-time metrics that can go up or down
        # =============================================================================
        
        # Use UpDownCounters for gauges (OTel recommended approach)
        self.active_jobs = self._meter.create_up_down_counter(
            name="nutanix_aidp_connector_active_jobs",
            description="Current number of active jobs in NeMo-Retriever (1 job = 1 file, so this equals active files)",
            unit="1"
        )
        
        self.pending_downloads = self._meter.create_up_down_counter(
            name="nutanix_aidp_connector_pending_downloads",
            description="Current number of files pending download",
            unit="1"
        )
        
        self.batch_queue_size = self._meter.create_up_down_counter(
            name="nutanix_aidp_connector_batch_queue_size",
            description="Current number of batches in queue",
            unit="1"
        )
        
        self.download_queue_size = self._meter.create_up_down_counter(
            name="nutanix_aidp_connector_download_queue_size",
            description="Current number of files in download queue",
            unit="1"
        )
        
        # =============================================================================
        # HISTOGRAMS - Distribution of values
        # =============================================================================
        
        self.job_duration_seconds = self._meter.create_histogram(
            name="nutanix_aidp_connector_job_duration_seconds",
            description="Time taken to complete a job",
            unit="s"
        )
        
        self.download_duration_seconds = self._meter.create_histogram(
            name="nutanix_aidp_connector_download_duration_seconds",
            description="Time taken to download a file from S3",
            unit="s"
        )
        
        self.batch_processing_duration_seconds = self._meter.create_histogram(
            name="nutanix_aidp_connector_batch_processing_duration_seconds",
            description="Time taken to process a batch",
            unit="s"
        )
        
        self.file_size_bytes = self._meter.create_histogram(
            name="nutanix_aidp_connector_file_size_bytes",
            description="Distribution of file sizes processed",
            unit="By"
        )
        
        self.batch_size_files = self._meter.create_histogram(
            name="nutanix_aidp_connector_batch_size_files",
            description="Distribution of batch sizes (number of files)",
            unit="1"
        )
        
        self.embeddings_per_file = self._meter.create_histogram(
            name="nutanix_aidp_connector_embeddings_per_file",
            description="Distribution of embeddings generated per file",
            unit="1"
        )
        
        # Pipeline stage latencies (one histogram with stage attribute for Prometheus querying)
        self.pipeline_stage_duration_seconds = self._meter.create_histogram(
            name="nutanix_aidp_connector_pipeline_stage_duration_seconds",
            description="Duration in seconds per pipeline stage (message_queue, download, batch_queue, submission, nemo_retriever, completion_handle, parquet_discovery, download_parquet, merge_enrich, upload_milvus, bulk_insert_call, wait_bulk_insert)",
            unit="s"
        )
        
        logger.debug("All metric instruments created")
    
    # =============================================================================
    # PUBLIC METHODS - For recording metrics
    # =============================================================================
    
    def record_job_submitted(self, job_id: str = "", file_type: str = "unknown"):
        """Record a job submission"""
        if self._enabled:
            self.jobs_submitted_total.add(1, {"file_type": file_type})
            self.active_jobs.add(1)
    
    def record_job_completed(self, job_id: str = "", duration_seconds: float = 0, 
                            file_type: str = "unknown", embeddings_count: int = 0):
        """Record a successful job completion"""
        if self._enabled:
            self.jobs_completed_total.add(1, {"file_type": file_type})
            self.active_jobs.add(-1)
            if duration_seconds > 0:
                self.job_duration_seconds.record(duration_seconds, {"file_type": file_type})
            if embeddings_count > 0:
                self.embeddings_stored_total.add(embeddings_count, {"file_type": file_type})
                self.embeddings_per_file.record(embeddings_count, {"file_type": file_type})
    
    def record_job_failed(self, job_id: str = "", error_type: str = "unknown", 
                         file_type: str = "unknown"):
        """Record a job failure"""
        if self._enabled:
            self.jobs_failed_total.add(1, {"error_type": error_type, "file_type": file_type})
            self.active_jobs.add(-1)
    
    def record_file_downloaded(self, file_size_bytes: int = 0, duration_seconds: float = 0,
                               file_type: str = "unknown"):
        """Record a file download"""
        if self._enabled:
            self.files_downloaded_total.add(1, {"file_type": file_type})
            self.bytes_downloaded_total.add(file_size_bytes, {"file_type": file_type})
            if duration_seconds > 0:
                self.download_duration_seconds.record(duration_seconds, {"file_type": file_type})
            if file_size_bytes > 0:
                self.file_size_bytes.record(file_size_bytes, {"file_type": file_type})
    
    def record_file_processed(self, file_type: str = "unknown", file_size_bytes: int = 0):
        """Record a successfully processed file"""
        if self._enabled:
            self.files_processed_total.add(1, {"file_type": file_type})
            if file_size_bytes > 0:
                self.bytes_processed_total.add(file_size_bytes, {"file_type": file_type})
    
    def record_file_failed(self, file_type: str = "unknown", error_type: str = "unknown"):
        """Record a file processing failure"""
        if self._enabled:
            self.files_failed_total.add(1, {"file_type": file_type, "error_type": error_type})
    
    def record_file_skipped(self, file_type: str = "unknown", reason: str = "unsupported"):
        """Record a skipped file"""
        if self._enabled:
            self.files_skipped_total.add(1, {"file_type": file_type, "reason": reason})
    
    def record_batch_created(self, batch_size_files: int = 0, batch_size_bytes: int = 0):
        """Record a batch creation"""
        if self._enabled:
            self.batches_created_total.add(1)
            if batch_size_files > 0:
                self.batch_size_files.record(batch_size_files)
    
    def record_batch_submitted(self, batch_id: str = "", file_count: int = 0):
        """Record a batch submission"""
        if self._enabled:
            self.batches_submitted_total.add(1)
    
    def record_kafka_message_received(self, event_type: str = "unknown"):
        """Record a Kafka message received"""
        if self._enabled:
            self.kafka_messages_received_total.add(1, {"event_type": event_type})
    
    def record_kafka_message_processed(self, event_type: str = "unknown"):
        """Record a Kafka message processed"""
        if self._enabled:
            self.kafka_messages_processed_total.add(1, {"event_type": event_type})
    
    def record_milvus_insert(self, collection: str = "default", embeddings_count: int = 0):
        """Record a successful Milvus insert"""
        if self._enabled:
            self.milvus_inserts_total.add(1, {"collection": collection})
            if embeddings_count > 0:
                self.embeddings_stored_total.add(embeddings_count, {"collection": collection})
    
    def record_milvus_insert_failure(self, collection: str = "default", error_type: str = "unknown"):
        """Record a Milvus insert failure"""
        if self._enabled:
            self.milvus_insert_failures_total.add(1, {"collection": collection, "error_type": error_type})
    
    def record_submission_throttled(self, reason: str = "max_outstanding_tasks"):
        """Record a submission throttle event"""
        if self._enabled:
            self.submission_throttle_events_total.add(1, {"reason": reason})
    
    def record_download_throttled(self, reason: str = "flow_control"):
        """Record a download throttle event"""
        if self._enabled:
            self.download_throttle_events_total.add(1, {"reason": reason})
    
    def record_pipeline_stage_duration(self, stage: str, duration_seconds: float):
        """
        Record pipeline stage latency for Prometheus.
        
        Args:
            stage: One of message_queue, download, batch_queue, submission, nemo_retriever,
                   completion_handle, parquet_discovery, download_parquet, merge_enrich,
                   upload_milvus, bulk_insert_call, wait_bulk_insert
            duration_seconds: Duration in seconds
        """
        if self._enabled and duration_seconds >= 0:
            self.pipeline_stage_duration_seconds.record(duration_seconds, {"stage": stage})
    
    def record_job_status_code(self, status_code: int, error_type: str = "none"):
        """
        Record a job status check by HTTP status code
        
        Args:
            status_code: HTTP status code (200, 202, 404, 410, 503)
            error_type: Type of error if status code indicates failure ("none", "validation_failure", "processing_failure", "expired", "not_found")
        """
        if self._enabled:
            status_code_str = str(status_code)
            self.job_status_codes_total.add(1, {"status_code": status_code_str, "error_type": error_type})
            
            # Also record errors separately for easier querying
            if status_code >= 400:  # 4xx and 5xx are errors
                self.job_errors_by_status_code_total.add(1, {"status_code": status_code_str, "error_type": error_type})
    
    def update_batch_queue_size(self, delta: int):
        """Update batch queue size gauge"""
        if self._enabled:
            self.batch_queue_size.add(delta)
    
    def update_download_queue_size(self, delta: int):
        """Update download queue size gauge"""
        if self._enabled:
            self.download_queue_size.add(delta)
    
    def update_pending_downloads(self, delta: int):
        """Update pending downloads gauge"""
        if self._enabled:
            self.pending_downloads.add(delta)
    
    @property
    def enabled(self) -> bool:
        """Check if metrics are enabled"""
        return self._enabled


# Global metrics instance (singleton)
_metrics_instance: Optional[PipelineMetrics] = None


def init_metrics(config) -> PipelineMetrics:
    """
    Initialize the global metrics instance.
    
    Args:
        config: TelemetryConfig object
        
    Returns:
        PipelineMetrics instance
    """
    global _metrics_instance
    _metrics_instance = PipelineMetrics(config)
    return _metrics_instance


def get_metrics() -> Optional[PipelineMetrics]:
    """
    Get the global metrics instance.
    
    Returns:
        PipelineMetrics instance or None if not initialized
    """
    return _metrics_instance


def timed_operation(metric_name: str = None, file_type: str = "unknown"):
    """
    Decorator to time an operation and record it to a histogram.
    
    Args:
        metric_name: Name of the histogram metric to record to
        file_type: File type for labeling
        
    Usage:
        @timed_operation("download")
        def download_file(self, file_info):
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                metrics = get_metrics()
                if metrics and metrics.enabled:
                    if metric_name == "download":
                        metrics.download_duration_seconds.record(duration, {"file_type": file_type})
                    elif metric_name == "job":
                        metrics.job_duration_seconds.record(duration, {"file_type": file_type})
                    elif metric_name == "batch":
                        metrics.batch_processing_duration_seconds.record(duration)
        return wrapper
    return decorator



