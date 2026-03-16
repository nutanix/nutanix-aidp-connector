#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Configuration settings using Pydantic for validation
"""

import os
import yaml
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, model_validator, field_validator, model_validator


class KafkaConfig(BaseModel):
    """Kafka configuration"""
    bootstrap_servers: str = Field(default="")
    topic: str = Field(default="OSSEvents")
    group_id: str = Field(default="nutanix-aidp-connector-group")
    auto_offset_reset: str = Field(
        default="earliest",
        description="When consumer group has no offset: 'earliest'=replay from start, 'latest'=only new messages"
    )
    min_topic_partitions: int = Field(
        default=1,
        description="Expected minimum partitions on the Kafka topic. A warning is logged at startup if actual count is lower."
    )
    auto_commit: bool = Field(default=False)
    consumer_timeout_ms: int = Field(default=1000)
    allowed_event_types: List[str] = Field(
        default=["s3:ObjectCreated:Put", "s3:ObjectTagging:Put"],
        description="List of S3 event types to process from Kafka notifications"
    )


class S3Config(BaseModel):
    """S3 configuration"""
    region: str = Field(default="us-east-1")
    download_dir: str = Field(default="/tmp/nutanix-aidp-connector-downloads")
    endpoint_url: Optional[str] = Field(default="")
    verify_ssl: bool = Field(default=False)
    access_key: Optional[str] = Field(default=None)
    secret_key: Optional[str] = Field(default=None)


class ThreadingConfig(BaseModel):
    """Threading configuration"""
    download_workers: int = Field(default=20)
    submission_workers: int = Field(default=10)
    status_check_workers: int = Field(default=5)
    completion_workers: int = Field(default=5, description="Number of parallel workers for processing completions and Milvus bulk inserts")


class BatchingConfig(BaseModel):
    """Batching configuration"""
    max_files: int = Field(default=20)
    max_bytes: int = Field(default=2000000000)  # 2GB
    max_wait_seconds: int = Field(default=30)


class NemoRetrieverExtractConfig(BaseModel):
    """NeMo-Retriever extract task configuration for documents"""
    extract_text: bool = Field(default=True)
    extract_images: bool = Field(default=True)
    extract_tables: bool = Field(default=True)
    extract_charts: bool = Field(default=True)
    extract_metadata: bool = Field(default=True)
    ocr_enabled: bool = Field(default=True)
    ocr_language: str = Field(default="eng")
    extract_tables_method: str = Field(default="yolox")
    table_output_format: str = Field(default="pseudo_markdown")


class NemoRetrieverSplitConfig(BaseModel):
    """NeMo-Retriever split task configuration for documents"""
    enabled: bool = Field(default=True)
    split_by_page: bool = Field(default=True)
    split_by_size: bool = Field(default=True)
    max_character_length: int = Field(default=1024)
    sentence_window_size: int = Field(default=0)


class NemoRetrieverEmbedConfig(BaseModel):
    """NeMo-Retriever embed task configuration for documents"""
    enabled: bool = Field(default=True)
    dimensions: int = Field(default=2048, description="Embedding dimension (default: 2048 to match NeMo-Retriever server's default model)")


class NemoRetrieverConfig(BaseModel):
    """NeMo-Retriever configuration"""
    host: str = Field(default="")
    port: int = Field(default=0)
    task_queue: str = Field(default="morpheus_task_queue")
    timeout: int = Field(default=300)
    text_depth: str = Field(default="page")
    api_version: str = Field(
        default="v2",
        description="NeMo-Retriever API version: 'v1' (no server-side PDF splitting) or 'v2' (splits PDFs; use with pdf_split_page_count).",
    )
    pdf_split_page_count: int = Field(
        default=1,
        ge=1,
        le=128,
        description="V2 only: pages per chunk (1–128). Use 1 so each page is one subjob; one bad page only affects that page.",
    )

    # Supported document types (only these file extensions are processed)
    supported_extensions: List[str] = Field(
        default=["pdf", "docx", "doc", "txt", "md", "rtf", "pptx", "ppt"],
        description="Supported file extensions. Images, audio, video, etc. are NOT supported."
    )
    
    # Task configurations for documents
    extract: NemoRetrieverExtractConfig = Field(default_factory=NemoRetrieverExtractConfig)
    split: NemoRetrieverSplitConfig = Field(default_factory=NemoRetrieverSplitConfig)
    embed: NemoRetrieverEmbedConfig = Field(default_factory=NemoRetrieverEmbedConfig)


class MilvusStorageConfig(BaseModel):
    """Milvus storage configuration"""
    enabled: bool = Field(default=True)
    host: str = Field(default="")
    port: int = Field(default=0)
    collection_name: str = Field(default="nutanix_aidp_connector_objects_nemo_retriever")
    bucket: str = Field(default="aidp-milvus")
    index_type: str = Field(default="GPU_CAGRA")
    metric_type: str = Field(default="L2")
    nlist: int = Field(default=1024)
    nprobe: int = Field(default=10)
    auto_id: bool = Field(default=True)
    consistency_level: str = Field(default="Strong")
    auto_create_collection: bool = Field(default=True)
    # NeMo-Retriever compatible schema options
    sparse: bool = Field(default=False, description="Enable sparse vector field and BM25 function for hybrid search")
    gpu_index: bool = Field(default=True, description="Use GPU_CAGRA index for dense vectors (requires GPU-enabled Milvus)")
    gpu_search: bool = Field(default=True, description="Enable GPU-accelerated search")
    local_index: bool = Field(default=False, description="Use local index (FLAT) instead of server-side index")
    enable_filtered_collections: bool = Field(default=True, description="Enable creation and routing to filtered collections")
    filtered_text_collection_name: Optional[str] = Field(default=None, description="Collection name for filtered text content (optional)")
    filtered_tables_collection_name: Optional[str] = Field(default=None, description="Collection name for filtered table content (optional)")
    filtered_charts_collection_name: Optional[str] = Field(default=None, description="Collection name for filtered chart content (optional)")
    cleanup_parquet_files: bool = Field(default=True, description="Clean up parquet files from Milvus and Objects buckets after successful bulk insert")
    max_bulk_insert_files: int = Field(default=128, description="Max parquet files per do_bulk_insert request (0 = no limit). Batching reduces Milvus job count.")
    batch_insert_max_files: int = Field(default=10, description="Number of source files to accumulate before a combined Milvus bulk insert (0 = disable batching)")
    batch_insert_max_wait_seconds: float = Field(default=15.0, description="Max seconds to wait before flushing an incomplete Milvus insert batch")
    
    @model_validator(mode='after')
    def validate_filtered_collections(self):
        """Validate that if filtered collections are enabled, at least one collection name is provided"""
        if self.enable_filtered_collections:
            collection_names = [
                self.filtered_text_collection_name,
                self.filtered_tables_collection_name,
                self.filtered_charts_collection_name
            ]
            # Check if at least one collection name is provided (non-empty after stripping)
            has_collection = any(
                name and name.strip() 
                for name in collection_names
            )
            if not has_collection:
                raise ValueError(
                    "enable_filtered_collections is True but no filtered collection names are provided. "
                    "Please provide at least one of: filtered_text_collection_name, "
                    "filtered_tables_collection_name, or filtered_charts_collection_name"
                )
        return self


class ObjectsStorageConfig(BaseModel):
    """Objects storage configuration (S3-compatible service for Milvus/NeMo-Retriever)"""
    enabled: bool = Field(default=True)
    endpoint_url: str = Field(default="")
    verify_ssl: bool = Field(default=False)
    access_key: str = Field(default="")
    secret_key: str = Field(default="")
    bucket: str = Field(default="veer-test-bucket")
    region: str = Field(default="us-east-1")


class MilvusConfig(BaseModel):
    """Milvus configuration (deprecated - use storage.milvus instead)"""
    host: str = Field(default="")
    port: int = Field(default=0)


class ProcessingConfig(BaseModel):
    """Processing configuration"""
    max_retries: int = Field(default=3)
    retry_base_delay: int = Field(default=1)
    retry_max_delay: int = Field(default=60)
    status_poll_interval: int = Field(default=5)
    circuit_breaker_threshold: int = Field(default=5)
    circuit_breaker_timeout: int = Field(default=60)
    enable_sequence_execution: bool = Field(default=False, description="Wait for previous job to complete before submitting next job")
    max_outstanding_tasks: int = Field(default=10, description="Maximum number of tasks that can be pending with NeMo-Retriever server. Downloads are limited to 3x this value.")


class StorageConfig(BaseModel):
    """Storage configuration"""
    completed_metadata: str = Field(default="/var/lib/nutanix-aidp-connector/completed/")
    dead_letter: str = Field(default="/var/lib/nutanix-aidp-connector/dead_letter/")
    milvus: Optional[MilvusStorageConfig] = Field(default=None)
    objects: Optional[ObjectsStorageConfig] = Field(default=None)


class LoggingConfig(BaseModel):
    """Logging configuration"""
    level: str = Field(default="INFO")
    format: str = Field(default="json")
    log_file: Optional[str] = Field(default="/var/log/pipeline/pipeline.log")


class TelemetryConfig(BaseModel):
    """OpenTelemetry configuration for metrics and tracing"""
    enabled: bool = Field(default=True, description="Enable OpenTelemetry instrumentation")
    service_name: str = Field(default="nutanix-aidp-connector", description="Service name for telemetry")
    service_version: str = Field(default="1.0.0", description="Service version")
    prometheus_port: int = Field(default=9090, description="Port for Prometheus metrics endpoint")
    prometheus_endpoint: str = Field(default="/metrics", description="Endpoint path for Prometheus metrics")
    metric_export_interval_ms: int = Field(default=5000, description="Metric export interval in milliseconds")


class BaselineBucketConfig(BaseModel):
    """Configuration for a single baseline bucket"""
    bucket_name: str = Field(..., description="Name of the bucket to process for baseline")
    enabled: bool = Field(default=True, description="Whether to process this bucket for baseline")
    prefix: Optional[str] = Field(default=None, description="Optional key prefix to filter objects")
    force_rerun: bool = Field(default=False, description="Clear saved state and re-run baseline from scratch on next startup")


class BaselineConfig(BaseModel):
    """Baseline operation configuration for processing existing objects in buckets"""
    enabled: bool = Field(default=False, description="Enable baseline operation mode")
    buckets: List[BaselineBucketConfig] = Field(default_factory=list, description="List of buckets to process for baseline")
    state_dir: str = Field(default="/var/lib/nutanix-aidp-connector/baseline/", description="Directory to store baseline state files")
    list_batch_size: int = Field(default=1000, description="Number of objects to list per API call (max 1000)")
    max_concurrent_listings: int = Field(default=3, description="Maximum concurrent bucket listing operations")
    pause_kafka_during_baseline: bool = Field(default=True, description="Whether to pause Kafka consumption during baseline processing")
    auto_disable_completed_buckets: bool = Field(default=True, description="Automatically disable buckets in config after baseline completion")
    versioning_enabled: bool = Field(default=False, description="Whether S3 bucket versioning is enabled. When false, version IDs are omitted from object tracking keys.")


class Settings(BaseModel):
    """Main settings"""
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    s3: S3Config = Field(default_factory=S3Config)
    threading: ThreadingConfig = Field(default_factory=ThreadingConfig)
    batching: BatchingConfig = Field(default_factory=BatchingConfig)
    nemo_retriever: NemoRetrieverConfig = Field(default_factory=NemoRetrieverConfig)
    milvus: MilvusConfig = Field(default_factory=MilvusConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    telemetry: TelemetryConfig = Field(default_factory=TelemetryConfig)
    baseline: BaselineConfig = Field(default_factory=BaselineConfig)

    @classmethod
    def load_from_yaml(cls, config_path: str) -> "Settings":
        """Load settings from YAML file"""
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        # Handle nested storage.milvus and storage.objects if present
        if config_data and 'storage' in config_data:
            storage_data = config_data['storage']
            if 'milvus' in storage_data and isinstance(storage_data['milvus'], dict):
                storage_data['milvus'] = MilvusStorageConfig(**storage_data['milvus'])
            if 'objects' in storage_data and isinstance(storage_data['objects'], dict):
                storage_data['objects'] = ObjectsStorageConfig(**storage_data['objects'])
        
        # Credentials are NEVER stored in ConfigMap - always read from environment variables (set from Secrets)
        # S3 credentials from environment variables
        if config_data and 's3' in config_data:
            s3_config = config_data.get('s3', {})
            # Remove any access_key/secret_key from config if present (shouldn't be there)
            s3_config.pop('access_key', None)
            s3_config.pop('secret_key', None)
        
        # Objects storage credentials from environment variables
        if config_data and 'storage' in config_data and 'objects' in config_data['storage']:
            objects_config = config_data['storage'].get('objects', {})
            if isinstance(objects_config, dict):
                # Remove any access_key/secret_key from config if present (shouldn't be there)
                objects_config.pop('access_key', None)
                objects_config.pop('secret_key', None)
        
        # Create Settings instance
        settings = cls(**config_data)
        
        # Always read credentials from environment variables (set from Secrets)
        # Credentials are NEVER stored in ConfigMap for security reasons
        if settings.s3:
            s3_access_key = os.getenv('S3_ACCESS_KEY')
            s3_secret_key = os.getenv('S3_SECRET_KEY')
            if s3_access_key:
                settings.s3.access_key = s3_access_key
            if s3_secret_key:
                settings.s3.secret_key = s3_secret_key
        
        if settings.storage and settings.storage.objects:
            objects_access_key = os.getenv('OBJECTS_ACCESS_KEY')
            objects_secret_key = os.getenv('OBJECTS_SECRET_KEY')
            if objects_access_key:
                settings.storage.objects.access_key = objects_access_key
            if objects_secret_key:
                settings.storage.objects.secret_key = objects_secret_key
        
        return settings

    @classmethod
    def load_from_env(cls) -> "Settings":
        """Load settings from environment variables with defaults"""
        # Try to load from config file first
        config_path = os.getenv('PIPELINE_CONFIG', '/app/pipeline/config/config.yaml')
        
        if os.path.exists(config_path):
            return cls.load_from_yaml(config_path)
        
        # Fall back to environment variables
        return cls(
            kafka=KafkaConfig(
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', ''),
                topic=os.getenv('KAFKA_TOPIC', 'OSSEvents'),
                group_id=os.getenv('KAFKA_GROUP_ID', 'nutanix-aidp-connector-group'),
            ),
            s3=S3Config(
                endpoint_url=os.getenv('S3_ENDPOINT_URL', ''),
                access_key=os.getenv('AWS_ACCESS_KEY_ID'),
                secret_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                download_dir=os.getenv('DOWNLOAD_DIR', '/tmp/nutanix-aidp-connector-downloads'),
            ),
        )


def get_settings() -> Settings:
    """Get application settings"""
    return Settings.load_from_env()

