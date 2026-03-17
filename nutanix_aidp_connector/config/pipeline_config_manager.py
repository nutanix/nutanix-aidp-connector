# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Pipeline Configuration Manager
Handles hot-reloadable configuration for config.yaml with change detection
"""

import os
import yaml
import time
import threading
import logging
import hashlib
from typing import Dict, Any, Optional, Callable, Set
from dataclasses import dataclass, asdict
from config.settings import Settings, KafkaConfig, S3Config, ThreadingConfig, BatchingConfig, NemoRetrieverConfig, MilvusConfig, ProcessingConfig, StorageConfig, LoggingConfig, MilvusStorageConfig, ObjectsStorageConfig

logger = logging.getLogger(__name__)


@dataclass
class ConfigChange:
    """Configuration change notification"""
    section: str  # e.g., 'kafka', 's3', 'threading', etc.
    old_value: Any
    new_value: Any
    changed_fields: Set[str]  # Fields that changed within this section


class PipelineConfigManager:
    """Manages main pipeline configuration with hot-reloading and change detection"""
    
    def __init__(self, config_path: str = "/app/nutanix_aidp_connector/config-data/config.yaml"):
        """
        Initialize pipeline config manager
        
        Args:
            config_path: Path to config.yaml file
        """
        self.config_path = config_path
        self.config: Optional[Settings] = None
        self.last_modified = 0
        self.last_content_hash = None  # Content hash for change detection (works with ConfigMap subPath)
        self.reload_interval = 30  # seconds
        self._lock = threading.Lock()
        self._reload_thread = None
        self._stop_reload = threading.Event()
        
        # Change callbacks: section -> list of callbacks
        self._change_callbacks: Dict[str, list[Callable[[ConfigChange], None]]] = {}
        
        # Track last config values for change detection
        self._last_config_dict: Optional[Dict[str, Any]] = None
        
        # Load initial configuration
        self._load_config()
        
        # Initialize content hash after first load
        if os.path.exists(self.config_path):
            self.last_modified = os.path.getmtime(self.config_path)
            self.last_content_hash = self._get_file_hash()
        
        # Start hot-reload thread
        self._start_reload_thread()
    
    def _load_config(self) -> bool:
        """Load configuration from YAML file"""
        try:
            if not os.path.exists(self.config_path):
                logger.error(f"Configuration file not found: {self.config_path}")
                # Initialize with defaults if file doesn't exist
                self.config = Settings.load_from_env()
                self._last_config_dict = self._config_to_dict(self.config)
                return False
            
            # Load new config
            new_config = Settings.load_from_yaml(self.config_path)
            
            # Detect changes
            changes = None
            if self.config is not None:
                changes = self._detect_changes(self.config, new_config)
                if changes:
                    logger.info(f"Configuration changes detected: {len(changes)} sections changed")
            
            # Update config before notifying so callbacks see the new values
            self.config = new_config
            self._last_config_dict = self._config_to_dict(self.config)
            
            if changes:
                self._notify_changes(changes)
            
            # Update content hash after successful load
            self.last_content_hash = self._get_file_hash()
            
            logger.info(f"Configuration loaded successfully from {self.config_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # If we have a previous config, keep using it
            if self.config is None:
                self.config = Settings.load_from_env()
                self._last_config_dict = self._config_to_dict(self.config)
            return False
    
    def _config_to_dict(self, config: Settings) -> Dict[str, Any]:
        """Convert Settings object to dictionary for comparison"""
        # Pydantic models have .dict() method, not asdict()
        def pydantic_to_dict(obj):
            if hasattr(obj, 'dict'):
                return obj.dict()
            elif hasattr(obj, '__dict__'):
                return {k: v for k, v in obj.__dict__.items() if not k.startswith('_')}
            else:
                return asdict(obj)
        
        return {
            'kafka': pydantic_to_dict(config.kafka),
            's3': pydantic_to_dict(config.s3),
            'threading': pydantic_to_dict(config.threading),
            'batching': pydantic_to_dict(config.batching),
            'nemo_retriever': pydantic_to_dict(config.nemo_retriever),
            'milvus': pydantic_to_dict(config.milvus),
            'processing': pydantic_to_dict(config.processing),
            'storage': pydantic_to_dict(config.storage),
            'logging': pydantic_to_dict(config.logging),
        }
    
    def _detect_changes(self, old_config: Settings, new_config: Settings) -> list[ConfigChange]:
        """Detect configuration changes between old and new config"""
        changes = []
        
        old_dict = self._config_to_dict(old_config)
        new_dict = self._config_to_dict(new_config)
        
        # Compare each section
        for section in ['kafka', 's3', 'threading', 'batching', 'nemo_retriever', 'milvus', 'processing', 'storage', 'logging']:
            old_section = old_dict.get(section, {})
            new_section = new_dict.get(section, {})
            
            # Find changed fields
            changed_fields = set()
            for key in set(old_section.keys()) | set(new_section.keys()):
                old_val = old_section.get(key)
                new_val = new_section.get(key)
                if old_val != new_val:
                    changed_fields.add(key)
            
            if changed_fields:
                changes.append(ConfigChange(
                    section=section,
                    old_value=old_section,
                    new_value=new_section,
                    changed_fields=changed_fields
                ))
                logger.debug(f"Section '{section}' changed. Fields: {changed_fields}")
        
        return changes
    
    def _notify_changes(self, changes: list[ConfigChange]):
        """Notify registered callbacks about configuration changes"""
        for change in changes:
            # Notify section-specific callbacks
            callbacks = self._change_callbacks.get(change.section, [])
            for callback in callbacks:
                try:
                    callback(change)
                except Exception as e:
                    logger.error(f"Error in config change callback for section '{change.section}': {e}")
            
            # Notify global callbacks (for 'all' section)
            global_callbacks = self._change_callbacks.get('all', [])
            for callback in global_callbacks:
                try:
                    callback(change)
                except Exception as e:
                    logger.error(f"Error in global config change callback: {e}")
    
    def register_change_callback(self, section: str, callback: Callable[[ConfigChange], None]):
        """
        Register a callback for configuration changes
        
        Args:
            section: Section to monitor ('kafka', 's3', 'threading', etc.) or 'all' for all sections
            callback: Callback function that receives ConfigChange object
        """
        if section not in self._change_callbacks:
            self._change_callbacks[section] = []
        self._change_callbacks[section].append(callback)
        logger.debug(f"Registered change callback for section '{section}'")
    
    def unregister_change_callback(self, section: str, callback: Callable[[ConfigChange], None]):
        """Unregister a change callback"""
        if section in self._change_callbacks:
            try:
                self._change_callbacks[section].remove(callback)
            except ValueError:
                pass
    
    def _start_reload_thread(self):
        """Start hot-reload thread"""
        self._reload_thread = threading.Thread(target=self._reload_loop, daemon=True)
        self._reload_thread.start()
        logger.info("Pipeline configuration hot-reload thread started")
    
    def _get_file_hash(self) -> Optional[str]:
        """Get content hash of the configuration file"""
        try:
            if not os.path.exists(self.config_path):
                return None
            with open(self.config_path, 'rb') as f:
                content = f.read()
                return hashlib.md5(content).hexdigest()
        except Exception as e:
            logger.debug(f"Failed to compute file hash: {e}")
            return None
    
    def _reload_loop(self):
        """Hot-reload loop with both mtime and content hash detection"""
        while not self._stop_reload.is_set():
            try:
                if os.path.exists(self.config_path):
                    current_modified = os.path.getmtime(self.config_path)
                    current_hash = self._get_file_hash()
                    
                    # Check both modification time and content hash
                    # This handles Kubernetes ConfigMap subPath limitation where mtime may not update
                    mtime_changed = current_modified > self.last_modified
                    content_changed = current_hash is not None and current_hash != self.last_content_hash
                    
                    if mtime_changed or content_changed:
                        logger.info("Configuration file changed, reloading...")
                        with self._lock:
                            if self._load_config():
                                self.last_modified = current_modified
                                self.last_content_hash = current_hash
                                logger.info("Configuration reloaded successfully")
                            else:
                                logger.warning("Configuration reload failed, keeping previous config")
                    elif current_hash is None and self.last_content_hash is not None:
                        # File might have been deleted/recreated, reload anyway
                        logger.warning("File hash unavailable, forcing reload...")
                        with self._lock:
                            if self._load_config():
                                self.last_modified = current_modified
                                self.last_content_hash = self._get_file_hash()
                
                time.sleep(self.reload_interval)
            except Exception as e:
                logger.error(f"Error in configuration reload loop: {e}")
                time.sleep(5)  # Short sleep on error
    
    def get_config(self) -> Settings:
        """Get current configuration (thread-safe)"""
        with self._lock:
            return self.config
    
    def get_kafka_config(self) -> KafkaConfig:
        """Get Kafka configuration"""
        with self._lock:
            return self.config.kafka if self.config else KafkaConfig()
    
    def get_s3_config(self) -> S3Config:
        """Get S3 configuration"""
        with self._lock:
            return self.config.s3 if self.config else S3Config()
    
    def get_threading_config(self) -> ThreadingConfig:
        """Get threading configuration"""
        with self._lock:
            return self.config.threading if self.config else ThreadingConfig()
    
    def get_batching_config(self) -> BatchingConfig:
        """Get batching configuration"""
        with self._lock:
            return self.config.batching if self.config else BatchingConfig()
    
    def get_nemo_retriever_config(self) -> NemoRetrieverConfig:
        """Get NeMo-Retriever configuration"""
        with self._lock:
            return self.config.nemo_retriever if self.config else NemoRetrieverConfig()
    
    def get_milvus_config(self) -> MilvusConfig:
        """Get Milvus configuration"""
        with self._lock:
            return self.config.milvus if self.config else MilvusConfig()
    
    def get_processing_config(self) -> ProcessingConfig:
        """Get processing configuration"""
        with self._lock:
            return self.config.processing if self.config else ProcessingConfig()
    
    def get_storage_config(self) -> StorageConfig:
        """Get storage configuration"""
        with self._lock:
            return self.config.storage if self.config else StorageConfig()
    
    def get_logging_config(self) -> LoggingConfig:
        """Get logging configuration"""
        with self._lock:
            return self.config.logging if self.config else LoggingConfig()
    
    def reload(self) -> bool:
        """Manually trigger configuration reload"""
        logger.info("Manual configuration reload triggered")
        with self._lock:
            if os.path.exists(self.config_path):
                self.last_modified = os.path.getmtime(self.config_path) - 1  # Force reload
            return self._load_config()
    
    def stop(self):
        """Stop the hot-reload thread"""
        self._stop_reload.set()
        if self._reload_thread and self._reload_thread.is_alive():
            self._reload_thread.join(timeout=5)
        logger.info("Pipeline configuration hot-reload thread stopped")


# Global instance
pipeline_config_manager = PipelineConfigManager(
    config_path=os.getenv('PIPELINE_CONFIG', '/app/nutanix_aidp_connector/config-data/config.yaml')
)

