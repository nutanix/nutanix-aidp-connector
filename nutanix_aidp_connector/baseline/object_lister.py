#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Baseline Object Lister for listing S3 objects with pagination.
Handles paginated listing of bucket objects and supports crash recovery.
"""

import logging
import time
import queue
from typing import Optional, List, Dict, Any, Generator, Tuple
from dataclasses import dataclass
from threading import Event
import boto3
from botocore.exceptions import ClientError

from .state_manager import BaselineStateManager, BucketBaselineState

logger = logging.getLogger(__name__)


@dataclass
class ObjectInfo:
    """Information about a listed S3 object"""
    bucket: str
    key: str
    version_id: Optional[str] = None
    size: int = 0
    last_modified: Optional[str] = None
    e_tag: Optional[str] = None
    is_latest: bool = True
    is_delete_marker: bool = False


class BaselineObjectLister:
    """
    Lists objects from S3 buckets with pagination support.
    Integrates with state manager for crash recovery.
    """
    
    def __init__(
        self,
        s3_client,
        state_manager: BaselineStateManager,
        list_batch_size: int = 1000
    ):
        """
        Initialize the baseline object lister.
        
        Args:
            s3_client: Boto3 S3 client
            state_manager: Baseline state manager for persistence
            list_batch_size: Number of objects to list per API call (max 1000)
        """
        self.s3_client = s3_client
        self.state_manager = state_manager
        self.list_batch_size = min(list_batch_size, 1000)  # S3 max is 1000
        self._stop_events: Dict[str, Event] = {}  # bucket_name -> stop event
        
        logger.info(f"BaselineObjectLister initialized: batch_size={self.list_batch_size}")
    
    def create_stop_event(self, bucket_name: str) -> Event:
        """Create a stop event for a bucket to enable graceful cancellation"""
        event = Event()
        self._stop_events[bucket_name] = event
        return event
    
    def stop_listing(self, bucket_name: str):
        """Signal to stop listing for a specific bucket"""
        if bucket_name in self._stop_events:
            self._stop_events[bucket_name].set()
            logger.info(f"Stop signal sent for bucket: {bucket_name}")
    
    def is_stopped(self, bucket_name: str) -> bool:
        """Check if listing should stop for a bucket"""
        event = self._stop_events.get(bucket_name)
        return event.is_set() if event else False
    
    def list_bucket_objects(
        self,
        bucket_name: str,
        prefix: Optional[str] = None,
        output_queue: Optional[queue.Queue] = None,
        resume: bool = True
    ) -> Generator[List[ObjectInfo], None, None]:
        """
        List all objects in a bucket with pagination.
        
        Args:
            bucket_name: Name of the bucket to list
            prefix: Optional key prefix to filter objects
            output_queue: Optional queue to push objects to (for async processing)
            resume: Whether to resume from previous state
            
        Yields:
            List of ObjectInfo for each batch
        """
        logger.info(f"Starting baseline listing for bucket: {bucket_name}, prefix: {prefix}")
        
        # Initialize or get existing state
        state = self.state_manager.initialize_bucket_state(bucket_name, prefix)
        
        # Check if already complete
        if state.listing_complete and resume:
            logger.info(f"Baseline listing already complete for bucket: {bucket_name}")
            return
        
        # Create stop event
        self.create_stop_event(bucket_name)
        
        # Build initial list parameters
        list_kwargs = self._build_list_params(bucket_name, prefix, state, resume)
        
        total_listed = 0
        batch_count = 0
        
        while True:
            # Check for stop signal
            if self.is_stopped(bucket_name):
                logger.info(f"Listing stopped for bucket: {bucket_name} after {total_listed} objects")
                break
            
            try:
                # List objects
                start_time = time.time()
                response = self.s3_client.list_object_versions(**list_kwargs)
                list_time = time.time() - start_time
                
                # Process versions (current and non-current)
                versions = response.get('Versions', [])
                delete_markers = response.get('DeleteMarkers', [])
                
                # Filter out delete markers (we only process actual objects)
                objects = []
                for ver in versions:
                    if not ver.get('IsLatest', True) and ver.get('VersionId') == 'null':
                        continue  # Skip null versions that aren't latest
                    
                    obj = ObjectInfo(
                        bucket=bucket_name,
                        key=ver['Key'],
                        version_id=ver.get('VersionId'),
                        size=ver.get('Size', 0),
                        last_modified=ver.get('LastModified', '').isoformat() if ver.get('LastModified') else None,
                        e_tag=ver.get('ETag'),
                        is_latest=ver.get('IsLatest', True)
                    )
                    
                    # Skip if already processed (for resume scenario)
                    if resume and self.state_manager.is_object_processed(
                        bucket_name, obj.key, obj.version_id, prefix
                    ):
                        continue
                    
                    objects.append(obj)
                
                batch_count += 1
                total_listed += len(objects)
                
                # Log progress
                logger.info(f"Listed batch {batch_count} for {bucket_name}: "
                           f"{len(objects)} objects in {list_time:.2f}s, "
                           f"total={total_listed}")
                
                # Get pagination info for next iteration
                is_truncated = response.get('IsTruncated', False)
                next_key_marker = response.get('NextKeyMarker')
                next_version_marker = response.get('NextVersionIdMarker')
                
                # NOTE: We DO NOT update next_key_marker here during listing!
                # The orchestrator will update the marker AFTER the entire batch
                # is confirmed processed (synchronous batch pattern).
                # This ensures no data loss on crash.
                
                # Just update the count for progress tracking (not the marker)
                self.state_manager.update_listing_progress(
                    bucket_name=bucket_name,
                    prefix=prefix,
                    next_key_marker=None,  # Orchestrator updates this after batch completes
                    next_version_id_marker=None,
                    is_truncated=is_truncated,
                    objects_listed=len(objects)
                )
                
                # Push to queue if provided
                if output_queue and objects:
                    for obj in objects:
                        output_queue.put(obj)
                
                # Yield batch
                if objects:
                    yield objects
                
                # Check if we've reached the end
                if not is_truncated:
                    logger.info(f"Completed listing for bucket {bucket_name}: "
                               f"total_objects={total_listed}, batches={batch_count}")
                    break
                
                # Update list params for next iteration
                list_kwargs['KeyMarker'] = next_key_marker
                if next_version_marker:
                    list_kwargs['VersionIdMarker'] = next_version_marker
                elif 'VersionIdMarker' in list_kwargs:
                    # Remove VersionIdMarker if not provided in response
                    del list_kwargs['VersionIdMarker']
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                error_message = e.response['Error']['Message']
                logger.error(f"S3 ClientError listing {bucket_name}: {error_code} - {error_message}")
                
                if error_code in ['NoSuchBucket', 'AccessDenied']:
                    logger.error(f"Cannot list bucket {bucket_name}: {error_code}")
                    break
                
                # Retry with backoff for other errors
                time.sleep(5)
                continue
                
            except Exception as e:
                logger.error(f"Error listing bucket {bucket_name}: {e}")
                time.sleep(5)
                continue
        
        # Cleanup stop event
        if bucket_name in self._stop_events:
            del self._stop_events[bucket_name]
    
    def _build_list_params(
        self,
        bucket_name: str,
        prefix: Optional[str],
        state: BucketBaselineState,
        resume: bool
    ) -> Dict[str, Any]:
        """Build parameters for list_object_versions API call"""
        params = {
            'Bucket': bucket_name,
            'MaxKeys': self.list_batch_size
        }
        
        if prefix:
            params['Prefix'] = prefix
        
        # Resume from previous state if applicable
        if resume and state.next_key_marker:
            params['KeyMarker'] = state.next_key_marker
            if state.next_version_id_marker:
                params['VersionIdMarker'] = state.next_version_id_marker
            else:
                # Default version marker
                params['VersionIdMarker'] = 'null'
            logger.info(f"Resuming listing from key: {state.next_key_marker}")
        
        return params
    
    def count_bucket_objects(
        self,
        bucket_name: str,
        prefix: Optional[str] = None
    ) -> Tuple[int, int]:
        """
        Count total objects in a bucket (for progress estimation).
        
        Args:
            bucket_name: Name of the bucket
            prefix: Optional key prefix
            
        Returns:
            Tuple of (object_count, total_size_bytes)
        """
        total_count = 0
        total_size = 0
        
        paginator = self.s3_client.get_paginator('list_object_versions')
        params = {'Bucket': bucket_name}
        if prefix:
            params['Prefix'] = prefix
        
        try:
            for page in paginator.paginate(**params):
                versions = page.get('Versions', [])
                for ver in versions:
                    total_count += 1
                    total_size += ver.get('Size', 0)
        except Exception as e:
            logger.error(f"Error counting objects in {bucket_name}: {e}")
        
        return total_count, total_size
    
    def test_bucket_access(self, bucket_name: str) -> Tuple[bool, Optional[str]]:
        """
        Test if we have access to list the bucket.
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            Tuple of (success, error_message)
        """
        try:
            # Try to list a single object
            self.s3_client.list_object_versions(
                Bucket=bucket_name,
                MaxKeys=1
            )
            return True, None
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            return False, f"{error_code}: {error_message}"
        except Exception as e:
            return False, str(e)

