# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
NeMo-Retriever Task Configuration Manager
Handles dynamic configuration loading and hot-reloading
"""

import logging
from typing import Optional, Set
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class ExtractConfig:
    """Extract task configuration for documents"""
    extract_text: bool = True
    extract_images: bool = True
    extract_tables: bool = True
    extract_charts: bool = True
    extract_metadata: bool = True
    ocr_enabled: bool = True
    ocr_language: str = "eng"
    extract_tables_method: str = "yolox"
    table_output_format: str = "pseudo_markdown"
    # Legacy fields (kept for compatibility but not used for documents)
    extract_links: bool = False
    extract_footnotes: bool = False
    extract_headers: bool = False
    extract_footers: bool = False
    preserve_formatting: bool = False
    ocr_confidence_threshold: float = 0.7
    image_analysis: bool = False
    face_detection: bool = False
    object_detection: bool = False
    transcription_enabled: bool = False
    transcription_language: str = "en"
    transcription_model: str = "whisper"
    speaker_diarization: bool = False
    audio_analysis: bool = False
    frame_extraction: bool = False
    frame_interval: int = 30
    video_analysis: bool = False
    preserve_cell_formatting: bool = False
    extract_formulas: bool = False
    data_validation: bool = False
    recursive_extraction: bool = False
    max_depth: int = 5
    max_file_size: int = 104857600
    extract_infographics: bool = False
    extract_page_as_image: bool = False
    extract_images_method: str = "group"


@dataclass
class SplitConfig:
    """Split task configuration for documents"""
    enabled: bool = True
    split_by_page: bool = True
    split_by_size: bool = True
    max_character_length: int = 1024
    sentence_window_size: int = 0
    # Legacy fields (kept for compatibility)
    strategy: str = "semantic"
    chunk_size: int = 1000
    chunk_overlap: int = 200
    tokenizer: str = "tiktoken"
    model: str = "gpt-3.5-turbo"
    min_chunk_size: int = 100
    max_chunk_size: int = 2000
    preserve_paragraphs: bool = True
    preserve_sections: bool = True
    split_by_headers: bool = True
    split_by_pages: bool = False
    custom_separators: list = None
    split_by_silence: bool = False
    silence_threshold: float = 0.1
    min_silence_duration: float = 2.0
    split_by_scenes: bool = False
    scene_threshold: float = 0.3
    split_by_sheets: bool = False
    split_by_columns: bool = False
    preserve_headers: bool = True


@dataclass
class EmbedConfig:
    """Embed task configuration for documents"""
    enabled: bool = True
    # Legacy fields (kept for compatibility)
    model: str = None
    dimensions: int = 2048  # Default to 2048 to match NeMo-Retriever server's default embedding model (e.g., embedqa-1b-v2)
    batch_size: int = 100
    normalize: bool = True
    metadata_fields: list = None
    custom_metadata: bool = False
    extract_features: list = None


@dataclass
class StorageConfig:
    """Storage configuration"""
    milvus_enabled: bool = True
    milvus_host: str = ""
    milvus_port: int = 19530
    milvus_collection: str = "nutanix_aidp_connector_objects_nemo_retriever"
    milvus_bucket: str = "aidp-milvus"
    milvus_index_type: str = "GPU_CAGRA"
    milvus_metric_type: str = "L2"
    milvus_nlist: int = 1024
    milvus_nprobe: int = 10
    milvus_auto_id: bool = True
    milvus_consistency_level: str = "Strong"
    milvus_auto_create_collection: bool = True
    milvus_sparse: bool = False
    milvus_gpu_index: bool = True
    milvus_gpu_search: bool = True
    milvus_local_index: bool = False
    milvus_enable_filtered_collections: bool = True
    milvus_filtered_text_collection_name: Optional[str] = None
    milvus_filtered_tables_collection_name: Optional[str] = None
    milvus_filtered_charts_collection_name: Optional[str] = None
    milvus_cleanup_parquet_files: bool = True
    milvus_max_bulk_insert_files: int = 128  # Max parquet files per do_bulk_insert (0 = no limit)
    milvus_batch_insert_max_files: int = 10  # Source files to accumulate before combined Milvus bulk insert (0 = disable)
    milvus_batch_insert_max_wait_seconds: float = 15.0  # Max seconds before flushing incomplete Milvus insert batch
    objects_enabled: bool = True
    objects_endpoint: str = ""
    objects_verify_ssl: bool = False
    objects_access_key: str = ""
    objects_secret_key: str = ""
    objects_bucket: str = "veer-test-bucket"
    objects_region: str = "us-east-1"


class TaskConfigManager:
    """
    Simplified task configuration manager.
    Reads configuration from the main config.yaml via pipeline_config_manager.
    """
    
    # Supported document extensions
    SUPPORTED_EXTENSIONS = {'pdf', 'docx', 'doc', 'txt', 'md', 'rtf', 'pptx', 'ppt'}
    
    # MIME type mappings for documents only
    FILE_TYPES = {
        'pdf': 'application/pdf',
        'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'doc': 'application/msword',
        'txt': 'text/plain',
        'md': 'text/markdown',
        'rtf': 'application/rtf',
        'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        'ppt': 'application/vnd.ms-powerpoint',
    }
    
    def __init__(self, config_path: str = None):
        """Initialize the task config manager."""
        self._extract_config: Optional[ExtractConfig] = None
        self._split_config: Optional[SplitConfig] = None
        self._embed_config: Optional[EmbedConfig] = None
        self._storage_config: Optional[StorageConfig] = None
        self._supported_extensions: Set[str] = self.SUPPORTED_EXTENSIONS.copy()
        
        # Load config on initialization
        self._load_config()
        logger.info(f"TaskConfigManager initialized. Supported extensions: {self._supported_extensions}")
    
    def _load_config(self):
        """Load configuration from pipeline_config_manager."""
        try:
            from config.pipeline_config_manager import pipeline_config_manager
            config = pipeline_config_manager.get_config()
            
            if config and config.nemo_retriever:
                nv_config = config.nemo_retriever
                
                # Update supported extensions from config
                if nv_config.supported_extensions:
                    self._supported_extensions = set(ext.lower() for ext in nv_config.supported_extensions)
                
                # Load extract config
                if nv_config.extract:
                    self._extract_config = ExtractConfig(
                        extract_text=nv_config.extract.extract_text,
                        extract_images=nv_config.extract.extract_images,
                        extract_tables=nv_config.extract.extract_tables,
                        extract_charts=nv_config.extract.extract_charts,
                        extract_metadata=nv_config.extract.extract_metadata,
                        ocr_enabled=nv_config.extract.ocr_enabled,
                        ocr_language=nv_config.extract.ocr_language,
                        extract_tables_method=nv_config.extract.extract_tables_method,
                        table_output_format=nv_config.extract.table_output_format,
                    )
                else:
                    self._extract_config = ExtractConfig()
                
                # Load split config
                if nv_config.split:
                    self._split_config = SplitConfig(
                        enabled=nv_config.split.enabled,
                        split_by_page=nv_config.split.split_by_page,
                        split_by_size=nv_config.split.split_by_size,
                        max_character_length=nv_config.split.max_character_length,
                        sentence_window_size=nv_config.split.sentence_window_size,
                    )
                else:
                    self._split_config = SplitConfig()
                
                # Load embed config
                if nv_config.embed:
                    self._embed_config = EmbedConfig(
                        enabled=nv_config.embed.enabled,
                        dimensions=nv_config.embed.dimensions  # Defaults to 2048 in NemoRetrieverEmbedConfig
                    )
                else:
                    self._embed_config = EmbedConfig()
            else:
                self._extract_config = ExtractConfig()
                self._split_config = SplitConfig()
                self._embed_config = EmbedConfig()
            
            # Load storage config
            self._load_storage_config(config)
            
        except Exception as e:
            logger.warning(f"Failed to load config from pipeline_config_manager: {e}. Using defaults.")
            self._extract_config = ExtractConfig()
            self._split_config = SplitConfig()
            self._embed_config = EmbedConfig()
            self._storage_config = StorageConfig()
    
    def _load_storage_config(self, config):
        """Load storage configuration."""
        try:
            if config and config.storage:
                storage = config.storage
                milvus = storage.milvus
                objects = storage.objects
                
                self._storage_config = StorageConfig(
                    milvus_enabled=milvus.enabled if milvus else True,
                    milvus_host=milvus.host if milvus and milvus.host else '',
                    milvus_port=milvus.port if milvus and milvus.port else 19530,
                    milvus_collection=milvus.collection_name if milvus else 'nutanix_aidp_connector_objects_nemo_retriever',
                    milvus_bucket=milvus.bucket if milvus else 'aidp-milvus',
                    milvus_index_type=milvus.index_type if milvus else 'GPU_CAGRA',
                    milvus_metric_type=milvus.metric_type if milvus else 'L2',
                    milvus_nlist=milvus.nlist if milvus else 1024,
                    milvus_nprobe=milvus.nprobe if milvus else 10,
                    milvus_auto_id=milvus.auto_id if milvus else True,
                    milvus_consistency_level=milvus.consistency_level if milvus else 'Strong',
                    milvus_auto_create_collection=milvus.auto_create_collection if milvus else True,
                    milvus_sparse=getattr(milvus, 'sparse', False) if milvus else False,
                    milvus_gpu_index=getattr(milvus, 'gpu_index', True) if milvus else True,
                    milvus_gpu_search=getattr(milvus, 'gpu_search', True) if milvus else True,
                    milvus_local_index=getattr(milvus, 'local_index', False) if milvus else False,
                    milvus_enable_filtered_collections=getattr(milvus, 'enable_filtered_collections', True) if milvus else True,
                    milvus_filtered_text_collection_name=getattr(milvus, 'filtered_text_collection_name', None) if milvus else None,
                    milvus_filtered_tables_collection_name=getattr(milvus, 'filtered_tables_collection_name', None) if milvus else None,
                    milvus_filtered_charts_collection_name=getattr(milvus, 'filtered_charts_collection_name', None) if milvus else None,
                    milvus_cleanup_parquet_files=getattr(milvus, 'cleanup_parquet_files', True) if milvus else True,
                    milvus_max_bulk_insert_files=getattr(milvus, 'max_bulk_insert_files', 128) if milvus else 128,
                    milvus_batch_insert_max_files=getattr(milvus, 'batch_insert_max_files', 10) if milvus else 10,
                    milvus_batch_insert_max_wait_seconds=getattr(milvus, 'batch_insert_max_wait_seconds', 15.0) if milvus else 15.0,
                    objects_enabled=objects.enabled if objects else True,
                    objects_endpoint=objects.endpoint_url if objects else '',
                    objects_verify_ssl=getattr(objects, 'verify_ssl', False) if objects else False,
                    objects_access_key=objects.access_key if objects else '',
                    objects_secret_key=objects.secret_key if objects else '',
                    objects_bucket=objects.bucket if objects else 'veer-test-bucket',
                    objects_region=objects.region if objects else 'us-east-1',
                )
            else:
                self._storage_config = StorageConfig()
        except Exception as e:
            logger.warning(f"Failed to load storage config: {e}. Using defaults.")
            self._storage_config = StorageConfig()
    
    def get_supported_extensions(self) -> Set[str]:
        """Get set of supported file extensions."""
        return self._supported_extensions
    
    def is_file_supported(self, filename: str) -> bool:
        """Check if file extension is supported."""
        ext = Path(filename).suffix.lower().lstrip('.')
        return ext in self._supported_extensions
    
    def get_file_type(self, filename: str) -> str:
        """Get MIME type from filename."""
        ext = Path(filename).suffix.lower().lstrip('.')
        return self.FILE_TYPES.get(ext, 'unknown')
    
    def get_extract_config(self, file_type: str = None) -> ExtractConfig:
        """Get extract configuration (same for all document types)."""
        return self._extract_config or ExtractConfig()
    
    def get_split_config(self, file_type: str = None) -> SplitConfig:
        """Get split configuration (same for all document types)."""
        return self._split_config or SplitConfig()
    
    def get_embed_config(self, file_type: str = None) -> EmbedConfig:
        """Get embed configuration (same for all document types)."""
        return self._embed_config or EmbedConfig()
    
    def get_storage_config(self) -> StorageConfig:
        """Get storage configuration."""
        return self._storage_config or StorageConfig()
    
    def reload_config(self):
        """Reload configuration from pipeline config manager."""
        logger.info("Reloading task configuration...")
        self._load_config()
        logger.info(f"Task configuration reloaded. Supported extensions: {self._supported_extensions}")
    
    def stop(self):
        """Stop the configuration manager (no-op for simplified version)."""
        logger.info("TaskConfigManager stopped")


# Global configuration manager instance
config_manager = TaskConfigManager()
