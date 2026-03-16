#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
S3 downloader with thread pool and retry logic
"""

import os
import uuid
import time
import logging
from typing import Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, Future
from pathlib import Path
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


class S3Downloader:
    """Download S3 objects with parallel processing and retry logic"""
    
    def __init__(self, download_dir: str, max_workers: int = 20,
                 max_retries: int = 3, retry_base_delay: int = 1,
                 retry_max_delay: int = 60, endpoint_url: str = None,
                 access_key: str = None, secret_key: str = None,
                 verify_ssl: bool = True, region: str = "us-east-1"):
        """
        Initialize S3 downloader
        
        Args:
            download_dir: Directory to download files to
            max_workers: Maximum number of concurrent downloads
            max_retries: Maximum number of retry attempts
            retry_base_delay: Base delay for exponential backoff (seconds)
            retry_max_delay: Maximum delay between retries (seconds)
            endpoint_url: S3 endpoint URL
            access_key: AWS access key
            secret_key: AWS secret key
            verify_ssl: Whether to verify SSL certificates
            region: AWS region for S3 operations
        """
        self.download_dir = download_dir
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.retry_base_delay = retry_base_delay
        self.retry_max_delay = retry_max_delay
        self.region = region
        
        # Create download directory
        Path(download_dir).mkdir(parents=True, exist_ok=True)
        
        # Create S3 client
        self.s3_client = self._create_s3_client(
            endpoint_url, access_key, secret_key, verify_ssl, region
        )
        
        # Thread pool for parallel downloads
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        logger.info(f"S3Downloader initialized: workers={max_workers}, "
                   f"download_dir={download_dir}")
    
    def reconnect(self, endpoint_url: str = None, access_key: str = None,
                  secret_key: str = None, verify_ssl: bool = None, region: str = None):
        """
        Reconnect S3 client with new configuration
        
        Args:
            endpoint_url: New endpoint URL (optional)
            access_key: New access key (optional)
            secret_key: New secret key (optional)
            verify_ssl: New verify_ssl setting (optional)
            region: New region (optional)
        """
        logger.info("Reconnecting S3 client with new configuration...")
        
        # Update configuration
        if region:
            self.region = region
        
        # Use provided values or get from environment
        current_endpoint = endpoint_url
        current_access_key = access_key
        current_secret_key = secret_key
        current_verify_ssl = verify_ssl if verify_ssl is not None else True
        
        # Create new S3 client with new configuration
        self.s3_client = self._create_s3_client(
            current_endpoint, current_access_key, current_secret_key,
            current_verify_ssl, self.region
        )
        
        logger.info("S3 client reconnected successfully")
    
    def _create_s3_client(self, endpoint_url: str, access_key: str, 
                         secret_key: str, verify_ssl: bool, region: str):
        """Create S3 client"""
        try:
            # Get credentials from environment variables (set from Secrets)
            # Credentials are NEVER stored in ConfigMap for security reasons
            access_key = access_key or os.getenv('S3_ACCESS_KEY')
            secret_key = secret_key or os.getenv('S3_SECRET_KEY')
            
            # boto3 requires a full URL with scheme; auto-prepend https:// if missing
            if endpoint_url and not endpoint_url.startswith(('http://', 'https://')):
                endpoint_url = f"https://{endpoint_url}"
            
            s3_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                verify=verify_ssl,
                region_name=region,
                # Add specific configuration for this S3 endpoint
                config=boto3.session.Config(
                    signature_version='s3v4',
                    s3={
                        'addressing_style': 'path'
                    }
                )
            )
            
            logger.info(f"S3 client created (endpoint: {endpoint_url}, region: {region}, SSL: {verify_ssl})")
            logger.info(f"S3 client config: signature_version=s3v4, addressing_style=path")
            return s3_client
        except Exception as e:
            logger.error(f"Failed to create S3 client: {e}")
            raise
    
    def download_with_retry(self, s3_bucket: str, s3_key: str, 
                          file_id: str = None) -> Tuple[bool, Dict]:
        """
        Download S3 object with exponential backoff retry
        
        Args:
            s3_bucket: S3 bucket name
            s3_key: S3 object key
            file_id: Unique file identifier (generated if not provided)
        
        Returns:
            Tuple of (success, result_dict)
        
        Note:
            Local files are named as: {s3_bucket}_{s3_key} (with special characters replaced)
        """
        file_id = file_id or str(uuid.uuid4())
        
        for attempt in range(self.max_retries):
            try:
                # Generate local file path using bucket and object name only
                safe_key = s3_key.replace('/', '_').replace(':', '_').replace(' ', '_')
                local_filename = f"{s3_bucket}_{safe_key}"
                
                local_path = os.path.join(self.download_dir, local_filename)
                
                logger.info(f"Downloading {s3_key} from {s3_bucket} "
                           f"(attempt {attempt + 1}/{self.max_retries})")
                
                # Get object metadata, tags, and size first
                s3_tags = {}
                s3_metadata = {}
                try:
                    head_response = self.s3_client.head_object(
                        Bucket=s3_bucket, Key=s3_key
                    )
                    file_size = head_response['ContentLength']
                    
                    # Extract user-defined metadata (x-amz-meta-* headers)
                    for key, value in head_response.get('Metadata', {}).items():
                        s3_metadata[key] = value
                    
                    # Also extract other metadata headers that might be useful
                    # Note: Standard headers like ContentType, LastModified are in head_response directly
                    # We focus on user-defined metadata (x-amz-meta-*)
                    logger.debug(f"Found {len(s3_metadata)} metadata entries for {s3_key}")
                    
                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    error_message = e.response['Error'].get('Message', 'No message')
                    logger.error(f"S3 ClientError: {error_code} - {error_message}")
                    logger.error(f"Full error response: {e.response}")
                    
                    if error_code == '404':
                        logger.info(f"Object not found (deleted): {s3_key} - {error_code}")
                        # Object was deleted - return success with special status
                        return True, {'status': 'completed', 'reason': 'object_deleted', 'file_id': file_id}
                    elif error_code == '403':
                        logger.warning(f"Access denied for object: {s3_key} - {error_code}")
                        # Access denied - return failure
                        return False, {'error': f'Access denied: {error_code}', 'file_id': file_id}
                    raise
                
                # Download the object
                start_time = time.time()
                self.s3_client.download_file(s3_bucket, s3_key, local_path)
                download_time = time.time() - start_time
                
                # Verify download
                if os.path.exists(local_path):
                    downloaded_size = os.path.getsize(local_path)
                    
                    if downloaded_size != file_size:
                        logger.warning(f"Size mismatch: expected {file_size}, "
                                     f"got {downloaded_size}")
                    
                    logger.info(f"Successfully downloaded {s3_key} "
                               f"({downloaded_size} bytes in {download_time:.2f}s)")
                    
                    # Fetch S3 object tags
                    try:
                        tag_response = self.s3_client.get_object_tagging(
                            Bucket=s3_bucket, Key=s3_key
                        )
                        # Convert tag list to dictionary
                        for tag in tag_response.get('TagSet', []):
                            s3_tags[tag['Key']] = tag['Value']
                        logger.debug(f"Found {len(s3_tags)} tags for {s3_key}")
                    except ClientError as tag_error:
                        error_code = tag_error.response.get('Error', {}).get('Code', '')
                        if error_code == '404':
                            logger.debug(f"No tags found for {s3_key} (object may not have tags)")
                        else:
                            logger.warning(f"Could not fetch tags for {s3_key}: {tag_error}")
                    except Exception as tag_error:
                        logger.warning(f"Unexpected error fetching tags for {s3_key}: {tag_error}")
                    
                    result = {
                        'file_id': file_id,
                        's3_bucket': s3_bucket,
                        's3_key': s3_key,
                        'local_path': local_path,
                        'file_size': downloaded_size,
                        'download_time': download_time,
                        's3_tags': s3_tags if s3_tags else None,
                        's3_metadata': s3_metadata if s3_metadata else None
                    }
                    
                    return True, result
                else:
                    logger.error(f"File not found after download: {local_path}")
                    raise Exception("Download verification failed")
                
            except Exception as e:
                logger.error(f"Download attempt {attempt + 1} failed: {e}")
                
                # Check if it's a 404 error (object deleted) or 403 error (access denied)
                if hasattr(e, 'response') and hasattr(e.response, 'get'):
                    error_code = e.response.get('Error', {}).get('Code')
                    if error_code == '404':
                        logger.info(f"Object not found (deleted): {s3_key} - {error_code}")
                        # Object was deleted - return success with special status
                        return True, {'status': 'completed', 'reason': 'object_deleted', 'file_id': file_id}
                    elif error_code == '403':
                        logger.warning(f"Access denied for object: {s3_key} - {error_code}")
                        # Access denied - return failure
                        return False, {'error': f'Access denied: {error_code}', 'file_id': file_id}
                
                # Calculate exponential backoff delay
                if attempt < self.max_retries - 1:
                    delay = min(
                        self.retry_base_delay * (2 ** attempt),
                        self.retry_max_delay
                    )
                    logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"All {self.max_retries} download attempts failed")
                    return False, {
                        'error': str(e),
                        'file_id': file_id,
                        's3_bucket': s3_bucket,
                        's3_key': s3_key
                    }
        
        return False, {'error': 'Max retries exceeded', 'file_id': file_id}
    
    def download_async(self, s3_bucket: str, s3_key: str, 
                      file_id: str = None) -> Future:
        """
        Download S3 object asynchronously
        
        Args:
            s3_bucket: S3 bucket name
            s3_key: S3 object key
            file_id: Unique file identifier
        
        Returns:
            Future object for the download task
        """
        return self.executor.submit(
            self.download_with_retry, s3_bucket, s3_key, file_id
        )
    
    def shutdown(self, wait: bool = True, timeout: int = 30):
        """
        Shutdown the thread pool
        
        Args:
            wait: Whether to wait for pending tasks
            timeout: Timeout for shutdown in seconds
        """
        logger.info(f"Shutting down S3 downloader (wait={wait}, timeout={timeout})")
        self.executor.shutdown(wait=wait, timeout=timeout)

