#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Batch window manager for time/size/count-based windowing with hot-reload support
"""

import time
import uuid
import logging
from typing import Dict, List, Optional
from threading import Lock
from dataclasses import dataclass, asdict

# Import configuration managers for hot-reload
try:
    from config.pipeline_config_manager import pipeline_config_manager
except ImportError:
    # Fallback if config manager not available
    pipeline_config_manager = None

logger = logging.getLogger(__name__)


@dataclass
class FileInfo:
    """File information for batching"""
    file_id: str
    s3_bucket: str
    s3_key: str
    local_path: str
    file_size: int
    kafka_partition: int
    kafka_offset: int
    s3_version: str = None  # S3 object version (if versioning enabled)
    s3_tags: Dict = None  # S3 object tags (key-value pairs)
    s3_metadata: Dict = None  # S3 object metadata (user-defined metadata headers)
    is_baseline: bool = False
    baseline_prefix: str = None
    created_at: float = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)


class BatchWindowManager:
    """
    Manage batching of files based on time, size, and count thresholds
    """
    
    def __init__(self, max_files: int = None, max_bytes: int = None, 
                 max_wait_seconds: int = None):
        """
        Initialize batch window manager with hot-reload support
        
        Args:
            max_files: Maximum number of files per batch (overrides config if provided)
            max_bytes: Maximum total bytes per batch (overrides config if provided)
            max_wait_seconds: Maximum time to wait before flushing batch (overrides config if provided)
        """
        self.use_config_manager = pipeline_config_manager is not None
        
        # Initialize with provided values or from config
        self._update_config(max_files, max_bytes, max_wait_seconds)
        
        self.current_batch: List[FileInfo] = []
        self.current_batch_size = 0
        self.last_batch_time = time.time()
        self.lock = Lock()
        
        logger.info(f"BatchWindowManager initialized: max_files={self.max_files}, "
                   f"max_bytes={self.max_bytes}, max_wait_seconds={self.max_wait_seconds}, "
                   f"use_config_manager={self.use_config_manager}")
    
    def _update_config(self, max_files: Optional[int] = None, 
                      max_bytes: Optional[int] = None,
                      max_wait_seconds: Optional[int] = None):
        """
        Update configuration from config manager or provided values
        
        Args:
            max_files: Override max_files if provided
            max_bytes: Override max_bytes if provided
            max_wait_seconds: Override max_wait_seconds if provided
        """
        if self.use_config_manager:
            try:
                batching_config = pipeline_config_manager.get_batching_config()
                processing_config = pipeline_config_manager.get_processing_config()
                
                # Get max_outstanding_tasks to ensure max_files doesn't exceed it
                max_outstanding_tasks = processing_config.max_outstanding_tasks if processing_config else None
                
                # Use provided values if given, otherwise use config
                requested_max_files = max_files if max_files is not None else batching_config.max_files
                self.max_bytes = max_bytes if max_bytes is not None else batching_config.max_bytes
                self.max_wait_seconds = max_wait_seconds if max_wait_seconds is not None else batching_config.max_wait_seconds
                
                # Ensure max_files doesn't exceed max_outstanding_tasks
                if max_outstanding_tasks is not None and requested_max_files > max_outstanding_tasks:
                    logger.warning(f"max_files ({requested_max_files}) exceeds max_outstanding_tasks ({max_outstanding_tasks}). "
                                 f"Capping max_files to {max_outstanding_tasks} to prevent batch size violations.")
                    self.max_files = max_outstanding_tasks
                else:
                    self.max_files = requested_max_files
                
            except Exception as e:
                logger.warning(f"Failed to get batching config, using defaults: {e}")
                self.max_files = max_files or 20
                self.max_bytes = max_bytes or 2000000000
                self.max_wait_seconds = max_wait_seconds or 30
        else:
            # Fallback to provided values or defaults
            self.max_files = max_files or 20
            self.max_bytes = max_bytes or 2000000000
            self.max_wait_seconds = max_wait_seconds or 30
    
    def update_config_from_manager(self) -> List[Dict]:
        """
        Update configuration from config manager (hot-reload)
        
        Returns:
            List of batch dictionaries that were flushed due to max_outstanding_tasks violation.
            Empty list if no batches were flushed. Caller should add returned batches to batch queue.
        """
        flushed_batches = []
        if self.use_config_manager:
            try:
                old_max_files = self.max_files
                old_max_bytes = self.max_bytes
                old_max_wait = self.max_wait_seconds
                
                self._update_config()
                
                # Log if values changed
                if (old_max_files != self.max_files or 
                    old_max_bytes != self.max_bytes or 
                    old_max_wait != self.max_wait_seconds):
                    logger.info(f"Batching config updated: "
                              f"max_files={old_max_files}->{self.max_files}, "
                              f"max_bytes={old_max_bytes}->{self.max_bytes}, "
                              f"max_wait_seconds={old_max_wait}->{self.max_wait_seconds}")
                    
                    # Check against max_outstanding_tasks as well
                    max_outstanding_tasks = self._get_max_outstanding_tasks()
                    if max_outstanding_tasks is not None:
                        if self.max_files > max_outstanding_tasks:
                            logger.warning(f"max_files ({self.max_files}) exceeds max_outstanding_tasks "
                                        f"({max_outstanding_tasks}) after hot-reload. Capping to {max_outstanding_tasks}.")
                            self.max_files = max_outstanding_tasks
                        
                        # If current batch exceeds max_outstanding_tasks, flush multiple batches until compliant
                        current_batch_size = len(self.current_batch)
                        if current_batch_size > max_outstanding_tasks:
                            # Calculate number of batches to flush: we need to flush until remaining <= max_outstanding_tasks
                            batches_to_flush = (current_batch_size - 1) // max_outstanding_tasks
                            remaining_after_flush = current_batch_size - (batches_to_flush * max_outstanding_tasks)
                            
                            logger.warning(f"Current batch has {current_batch_size} files, "
                                        f"which exceeds max_outstanding_tasks ({max_outstanding_tasks}). "
                                        f"Flushing {batches_to_flush} batch(es) immediately, "
                                        f"keeping {remaining_after_flush} files in current batch.")
                            
                            # Flush multiple batches until compliant
                            flushed_batches = self._flush_batches_until_compliant(max_outstanding_tasks)
                        elif current_batch_size > self.max_files:
                            logger.warning(f"Current batch has {current_batch_size} files, "
                                        f"which exceeds new max_files limit ({self.max_files}). "
                                        f"Batch will be flushed on next add_file() call.")
                    else:
                        # Fallback: just check against max_files
                        current_batch_size = len(self.current_batch)
                        if current_batch_size > self.max_files:
                            logger.warning(f"Current batch has {current_batch_size} files, "
                                         f"which exceeds new max_files limit ({self.max_files}). "
                                         f"Batch will be flushed on next add_file() call.")
            except Exception as e:
                logger.error(f"Error updating batching config: {e}")
        
        return flushed_batches
    
    def add_file(self, file_info: FileInfo) -> Optional[Dict]:
        """
        Add file to current batch and flush if threshold met
        
        Args:
            file_info: File information
        
        Returns:
            Batch dictionary if flushed, None otherwise
        """
        with self.lock:
            # Check max_outstanding_tasks limit before adding file
            # This ensures batches never exceed the limit
            max_outstanding_tasks = self._get_max_outstanding_tasks()
            flushed_batch = None
            
            if max_outstanding_tasks is not None:
                # If adding this file would exceed max_outstanding_tasks, flush current batch first
                if len(self.current_batch) >= max_outstanding_tasks:
                    logger.debug(f"Batch size ({len(self.current_batch)}) reached max_outstanding_tasks "
                               f"({max_outstanding_tasks}), flushing before adding new file")
                    flushed_batch = self._flush_batch()
            
            # Add file to batch
            self.current_batch.append(file_info)
            self.current_batch_size += file_info.file_size
            
            logger.debug(f"Added file {file_info.file_id} to batch "
                        f"(files: {len(self.current_batch)}, "
                        f"bytes: {self.current_batch_size})")
            
            # Check if batch should be flushed (time/size/count thresholds)
            if self._should_flush():
                new_flushed_batch = self._flush_batch()
                # Return the batch that was flushed (either from max_outstanding_tasks check or threshold check)
                return flushed_batch if flushed_batch else new_flushed_batch
            
            # Return batch if we flushed due to max_outstanding_tasks, otherwise None
            return flushed_batch
    
    def _get_max_outstanding_tasks(self) -> Optional[int]:
        """
        Get max_outstanding_tasks from config manager
        
        Returns:
            max_outstanding_tasks value or None if not available
        """
        if self.use_config_manager:
            try:
                processing_config = pipeline_config_manager.get_processing_config()
                return processing_config.max_outstanding_tasks if processing_config else None
            except Exception as e:
                logger.debug(f"Could not get max_outstanding_tasks from config: {e}")
                return None
        return None
    
    def _should_flush(self) -> bool:
        """Check if batch should be flushed based on thresholds"""
        time_expired = (time.time() - self.last_batch_time) >= self.max_wait_seconds
        count_exceeded = len(self.current_batch) >= self.max_files
        size_exceeded = self.current_batch_size >= self.max_bytes
        
        if time_expired:
            logger.debug(f"Time threshold exceeded ({self.max_wait_seconds}s)")
        if count_exceeded:
            logger.debug(f"File count threshold exceeded ({self.max_files} files)")
        if size_exceeded:
            logger.debug(f"Size threshold exceeded ({self.max_bytes} bytes)")
        
        return time_expired or count_exceeded or size_exceeded
    
    def _flush_batches_until_compliant(self, max_outstanding_tasks: int) -> List[Dict]:
        """
        Flush multiple batches until current batch size is <= max_outstanding_tasks.
        Used when hot-reload reduces max_outstanding_tasks below current batch size.
        
        Args:
            max_outstanding_tasks: Maximum allowed files in a batch
            
        Returns:
            List of batch dictionaries that were flushed
        """
        flushed_batches = []
        
        while len(self.current_batch) > max_outstanding_tasks:
            # Flush one batch of max_outstanding_tasks size
            flushed_batch = self._flush_batch(max_files=max_outstanding_tasks)
            if flushed_batch:
                flushed_batches.append(flushed_batch)
            else:
                # Should not happen, but break to avoid infinite loop
                logger.error(f"Failed to flush batch, but current_batch still has {len(self.current_batch)} files")
                break
        
        if flushed_batches:
            logger.info(f"Flushed {len(flushed_batches)} batch(es) to comply with max_outstanding_tasks={max_outstanding_tasks}. "
                       f"Remaining files in current batch: {len(self.current_batch)}")
        
        return flushed_batches
    
    def _flush_batch(self, max_files: Optional[int] = None) -> Optional[Dict]:
        """
        Flush current batch and create new one
        
        Args:
            max_files: Optional limit on number of files to flush. If provided, only flush
                      up to max_files files, leaving the rest in current_batch.
        
        Returns:
            Dictionary containing batch information, or None if no files to flush
        """
        if not self.current_batch:
            logger.debug("No files in batch to flush")
            return None
        
        # If max_files is specified and current batch exceeds it, flush only max_files
        if max_files is not None and len(self.current_batch) > max_files:
            files_to_flush = self.current_batch[:max_files]
            files_to_keep = self.current_batch[max_files:]
            
            # Calculate bytes for flushed portion
            flushed_bytes = sum(f.file_size for f in files_to_flush)
            
            batch_id = str(uuid.uuid4())
            batch = {
                'batch_id': batch_id,
                'files': files_to_flush,
                'file_count': len(files_to_flush),
                'total_bytes': flushed_bytes,
                'created_at': time.time()
            }
            
            logger.info(f"Flushing partial batch {batch_id}: "
                       f"{batch['file_count']} files (keeping {len(files_to_keep)} files in current batch), "
                       f"{batch['total_bytes']} bytes "
                       f"({batch['total_bytes'] / (1024*1024):.2f} MB)")
            
            # Keep remaining files in current batch
            self.current_batch = files_to_keep
            self.current_batch_size = sum(f.file_size for f in files_to_keep)
            self.last_batch_time = time.time()
            
            return batch
        
        # Normal flush: flush all files
        batch_id = str(uuid.uuid4())
        batch = {
            'batch_id': batch_id,
            'files': self.current_batch.copy(),
            'file_count': len(self.current_batch),
            'total_bytes': self.current_batch_size,
            'created_at': time.time()
        }
        
        logger.info(f"Flushing batch {batch_id}: "
                   f"{batch['file_count']} files, "
                   f"{batch['total_bytes']} bytes "
                   f"({batch['total_bytes'] / (1024*1024):.2f} MB)")
        
        # Reset batch
        self.current_batch = []
        self.current_batch_size = 0
        self.last_batch_time = time.time()
        
        return batch
    
    def force_flush(self) -> Optional[Dict]:
        """
        Force flush current batch regardless of thresholds
        
        Returns:
            Batch dictionary if any files present, None otherwise
        """
        with self.lock:
            if self.current_batch:
                logger.info("Force flushing current batch")
                return self._flush_batch()
            return None
    
    def get_batch_stats(self) -> Dict:
        """
        Get current batch statistics
        
        Returns:
            Dictionary with batch stats
        """
        with self.lock:
            time_elapsed = time.time() - self.last_batch_time
            return {
                'file_count': len(self.current_batch),
                'total_bytes': self.current_batch_size,
                'time_elapsed': time_elapsed,
                'time_remaining': max(0, self.max_wait_seconds - time_elapsed),
                'files_remaining': max(0, self.max_files - len(self.current_batch)),
                'bytes_remaining': max(0, self.max_bytes - self.current_batch_size)
            }
    
    def check_time_based_flush(self) -> Optional[Dict]:
        """
        Check if time-based flush is needed (call periodically)
        
        Returns:
            Batch dictionary if flushed, None otherwise
        """
        with self.lock:
            if self.current_batch and (time.time() - self.last_batch_time) >= self.max_wait_seconds:
                logger.info("Time-based flush triggered")
                return self._flush_batch()
            return None

