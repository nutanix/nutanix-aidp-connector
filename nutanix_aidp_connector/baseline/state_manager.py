#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Baseline State Manager for persisting baseline operation state.
Handles crash recovery by storing:
- Next continuation token for each bucket
- Processed object keys to avoid reprocessing
- Baseline completion status per bucket
"""

import os
import json
import logging
import hashlib
from pathlib import Path
from threading import RLock
from typing import Dict, Optional, Set, List
from dataclasses import dataclass, asdict, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class BucketBaselineState:
    """State for a single bucket's baseline operation"""
    bucket_name: str
    prefix: Optional[str] = None
    # Pagination state
    next_key_marker: Optional[str] = None
    next_version_id_marker: Optional[str] = None
    is_truncated: bool = True
    # Progress tracking
    total_objects_listed: int = 0
    total_objects_processed: int = 0
    total_objects_failed: int = 0
    # Completion status
    listing_complete: bool = False
    processing_complete: bool = False
    # Timestamps
    started_at: Optional[str] = None
    last_updated_at: Optional[str] = None
    completed_at: Optional[str] = None
    # Processed object tracking (for deduplication)
    processed_keys: List[str] = field(default_factory=list)
    failed_keys: List[str] = field(default_factory=list)


class BaselineStateManager:
    """
    Manages persistent state for baseline operations.
    Provides crash recovery by storing state to disk.
    """
    
    def __init__(self, state_dir: str, versioning_enabled: bool = False):
        """
        Initialize the baseline state manager.
        
        Args:
            state_dir: Directory to store state files
            versioning_enabled: Whether S3 versioning is enabled; when False,
                                version IDs are omitted from object tracking keys
        """
        self.state_dir = state_dir
        self.versioning_enabled = versioning_enabled
        self.lock = RLock()
        self._bucket_states: Dict[str, BucketBaselineState] = {}
        
        # Create state directory
        Path(state_dir).mkdir(parents=True, exist_ok=True)
        
        # Load existing states from disk
        self._load_all_states()
        
        logger.info(f"BaselineStateManager initialized: state_dir={state_dir}, "
                   f"versioning_enabled={versioning_enabled}, "
                   f"loaded_buckets={len(self._bucket_states)}")
    
    def _make_object_key(self, s3_key: str, version_id: Optional[str] = None) -> str:
        """Build the tracking key for an object, omitting version_id when versioning is off."""
        if self.versioning_enabled:
            return f"{s3_key}:{version_id or 'null'}"
        return s3_key

    def _get_state_file_path(self, bucket_name: str, prefix: Optional[str] = None) -> str:
        """Get the state file path for a bucket"""
        # Create a unique filename based on bucket name and prefix
        key = f"{bucket_name}:{prefix or ''}"
        hash_suffix = hashlib.md5(key.encode()).hexdigest()[:8]
        safe_bucket = bucket_name.replace('/', '_').replace(':', '_')
        filename = f"baseline_state_{safe_bucket}_{hash_suffix}.json"
        return os.path.join(self.state_dir, filename)
    
    def _get_processed_keys_file_path(self, bucket_name: str, prefix: Optional[str] = None) -> str:
        """Get the processed keys file path for a bucket"""
        key = f"{bucket_name}:{prefix or ''}"
        hash_suffix = hashlib.md5(key.encode()).hexdigest()[:8]
        safe_bucket = bucket_name.replace('/', '_').replace(':', '_')
        filename = f"baseline_processed_{safe_bucket}_{hash_suffix}.txt"
        return os.path.join(self.state_dir, filename)
    
    def _load_all_states(self):
        """Load all existing state files from disk"""
        if not os.path.exists(self.state_dir):
            return
        
        for filename in os.listdir(self.state_dir):
            if filename.startswith('baseline_state_') and filename.endswith('.json'):
                state_path = os.path.join(self.state_dir, filename)
                try:
                    with open(state_path, 'r') as f:
                        data = json.load(f)
                    state = BucketBaselineState(**data)
                    state_key = f"{state.bucket_name}:{state.prefix or ''}"
                    self._bucket_states[state_key] = state
                    logger.info(f"Loaded baseline state for bucket: {state.bucket_name}")
                except Exception as e:
                    logger.warning(f"Failed to load state file {filename}: {e}")
    
    def get_bucket_state(self, bucket_name: str, prefix: Optional[str] = None) -> Optional[BucketBaselineState]:
        """
        Get the current state for a bucket.
        
        Args:
            bucket_name: Name of the bucket
            prefix: Optional key prefix
            
        Returns:
            BucketBaselineState or None if not found
        """
        state_key = f"{bucket_name}:{prefix or ''}"
        with self.lock:
            return self._bucket_states.get(state_key)
    
    def initialize_bucket_state(self, bucket_name: str, prefix: Optional[str] = None) -> BucketBaselineState:
        """
        Initialize state for a new bucket or return existing state.
        
        Args:
            bucket_name: Name of the bucket
            prefix: Optional key prefix
            
        Returns:
            BucketBaselineState
        """
        state_key = f"{bucket_name}:{prefix or ''}"
        
        with self.lock:
            if state_key in self._bucket_states:
                existing_state = self._bucket_states[state_key]
                logger.info(f"Resuming baseline for bucket {bucket_name}: "
                           f"listed={existing_state.total_objects_listed}, "
                           f"processed={existing_state.total_objects_processed}")
                return existing_state
            
            # Create new state
            state = BucketBaselineState(
                bucket_name=bucket_name,
                prefix=prefix,
                started_at=datetime.utcnow().isoformat()
            )
            self._bucket_states[state_key] = state
            self._save_state(state)
            logger.info(f"Initialized new baseline state for bucket: {bucket_name}")
            return state
    
    def update_listing_progress(
        self,
        bucket_name: str,
        prefix: Optional[str] = None,
        next_key_marker: Optional[str] = None,
        next_version_id_marker: Optional[str] = None,
        is_truncated: bool = True,
        objects_listed: int = 0
    ):
        """
        Update the listing progress for a bucket.
        
        Args:
            bucket_name: Name of the bucket
            prefix: Optional key prefix
            next_key_marker: Next key marker for pagination
            next_version_id_marker: Next version ID marker for pagination
            is_truncated: Whether more objects exist
            objects_listed: Number of objects listed in this batch
        """
        state_key = f"{bucket_name}:{prefix or ''}"
        
        with self.lock:
            state = self._bucket_states.get(state_key)
            if not state:
                logger.warning(f"No state found for bucket {bucket_name}, initializing...")
                state = self.initialize_bucket_state(bucket_name, prefix)
            
            # Only update markers if provided (to avoid losing progress on partial updates)
            # This is important for crash recovery - we only advance the marker when
            # objects are actually processed, not just listed
            if next_key_marker is not None:
                state.next_key_marker = next_key_marker
            if next_version_id_marker is not None:
                state.next_version_id_marker = next_version_id_marker
            
            state.is_truncated = is_truncated
            state.total_objects_listed += objects_listed
            state.last_updated_at = datetime.utcnow().isoformat()
            
            if not is_truncated:
                state.listing_complete = True
                logger.info(f"Listing complete for bucket {bucket_name}: "
                           f"total_listed={state.total_objects_listed}")
            
            self._save_state(state)
    
    def mark_object_processed(
        self,
        bucket_name: str,
        s3_key: str,
        version_id: Optional[str] = None,
        prefix: Optional[str] = None
    ):
        """
        Mark an object as successfully processed.
        
        Args:
            bucket_name: Name of the bucket
            s3_key: Object key
            version_id: Object version ID (optional)
            prefix: Optional key prefix filter
        """
        state_key = f"{bucket_name}:{prefix or ''}"
        object_key = self._make_object_key(s3_key, version_id)
        
        with self.lock:
            state = self._bucket_states.get(state_key)
            if not state:
                return
            
            state.total_objects_processed += 1
            state.last_updated_at = datetime.utcnow().isoformat()
            
            # Append to processed keys file (don't keep all in memory)
            self._append_processed_key(bucket_name, prefix, object_key)
            
            # Periodically save state
            if state.total_objects_processed % 100 == 0:
                self._save_state(state)
                logger.debug(f"Baseline progress for {bucket_name}: "
                            f"processed={state.total_objects_processed}")
    
    def mark_object_failed(
        self,
        bucket_name: str,
        s3_key: str,
        version_id: Optional[str] = None,
        prefix: Optional[str] = None,
        error: Optional[str] = None
    ):
        """
        Mark an object as failed.
        
        Args:
            bucket_name: Name of the bucket
            s3_key: Object key
            version_id: Object version ID (optional)
            prefix: Optional key prefix filter
            error: Error message (optional)
        """
        state_key = f"{bucket_name}:{prefix or ''}"
        object_key = self._make_object_key(s3_key, version_id)
        
        with self.lock:
            state = self._bucket_states.get(state_key)
            if not state:
                return
            
            state.total_objects_failed += 1
            state.failed_keys.append(object_key)
            state.last_updated_at = datetime.utcnow().isoformat()
            
            # Keep failed keys list manageable
            if len(state.failed_keys) > 10000:
                state.failed_keys = state.failed_keys[-5000:]
            
            self._save_state(state)
            logger.warning(f"Baseline object failed: {bucket_name}/{s3_key} - {error}")
    
    def mark_bucket_complete(self, bucket_name: str, prefix: Optional[str] = None):
        """
        Mark baseline processing complete for a bucket.
        
        Args:
            bucket_name: Name of the bucket
            prefix: Optional key prefix
        """
        state_key = f"{bucket_name}:{prefix or ''}"
        
        with self.lock:
            state = self._bucket_states.get(state_key)
            if not state:
                return
            
            state.processing_complete = True
            state.completed_at = datetime.utcnow().isoformat()
            state.last_updated_at = datetime.utcnow().isoformat()
            self._save_state(state)
            
            logger.info(f"Baseline complete for bucket {bucket_name}: "
                       f"listed={state.total_objects_listed}, "
                       f"processed={state.total_objects_processed}, "
                       f"failed={state.total_objects_failed}")
    
    def is_object_processed(
        self,
        bucket_name: str,
        s3_key: str,
        version_id: Optional[str] = None,
        prefix: Optional[str] = None
    ) -> bool:
        """
        Check if an object has already been processed.
        
        Args:
            bucket_name: Name of the bucket
            s3_key: Object key
            version_id: Object version ID (optional)
            prefix: Optional key prefix filter
            
        Returns:
            True if already processed
        """
        object_key = self._make_object_key(s3_key, version_id)
        processed_file = self._get_processed_keys_file_path(bucket_name, prefix)
        
        if not os.path.exists(processed_file):
            return False
        
        # Check in file (line by line to handle large files)
        try:
            with open(processed_file, 'r') as f:
                for line in f:
                    if line.strip() == object_key:
                        return True
        except Exception as e:
            logger.warning(f"Error checking processed keys file: {e}")
        
        return False
    
    def clear_bucket_state(self, bucket_name: str, prefix: Optional[str] = None):
        """
        Clear all state for a bucket (when removed from config).
        
        Args:
            bucket_name: Name of the bucket
            prefix: Optional key prefix
        """
        state_key = f"{bucket_name}:{prefix or ''}"
        
        with self.lock:
            if state_key in self._bucket_states:
                del self._bucket_states[state_key]
            
            # Remove state files
            state_file = self._get_state_file_path(bucket_name, prefix)
            processed_file = self._get_processed_keys_file_path(bucket_name, prefix)
            
            for file_path in [state_file, processed_file]:
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        logger.info(f"Removed baseline state file: {file_path}")
                    except Exception as e:
                        logger.warning(f"Failed to remove state file {file_path}: {e}")
    
    def get_all_bucket_states(self) -> Dict[str, BucketBaselineState]:
        """Get all bucket states"""
        with self.lock:
            return dict(self._bucket_states)
    
    def get_incomplete_buckets(self) -> List[str]:
        """Get list of buckets with incomplete baseline processing"""
        incomplete = []
        with self.lock:
            for state_key, state in self._bucket_states.items():
                if not state.processing_complete:
                    incomplete.append(state.bucket_name)
        return incomplete
    
    def _save_state(self, state: BucketBaselineState):
        """Save bucket state to disk"""
        state_file = self._get_state_file_path(state.bucket_name, state.prefix)
        try:
            # Convert dataclass to dict, excluding large lists
            state_dict = asdict(state)
            # Don't save all processed keys in JSON (they're in separate file)
            state_dict['processed_keys'] = []
            
            with open(state_file, 'w') as f:
                json.dump(state_dict, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save state for bucket {state.bucket_name}: {e}")
    
    def _append_processed_key(self, bucket_name: str, prefix: Optional[str], object_key: str):
        """Append a processed key to the processed keys file"""
        processed_file = self._get_processed_keys_file_path(bucket_name, prefix)
        try:
            with open(processed_file, 'a') as f:
                f.write(object_key + '\n')
        except Exception as e:
            logger.warning(f"Failed to append processed key: {e}")

