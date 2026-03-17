#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
NeMo-Retriever job submitter using official client with enhanced task configuration
"""

import os
import logging
import time
import base64
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, Future
from pathlib import Path

try:
    from nv_ingest_client.client import NvIngestClient
    from nv_ingest_client.primitives.jobs import JobSpec
    from nv_ingest_client.primitives.tasks import ExtractTask, SplitTask, EmbedTask, StoreTask, StoreEmbedTask
    from nv_ingest_client.util.file_processing.extract import extract_file_content
    # Try to import DocumentTypeEnum if available
    try:
        from nv_ingest_client.primitives.jobs import DocumentTypeEnum
    except ImportError:
        DocumentTypeEnum = None
except ImportError as e:
    logging.error(f"Failed to import nv-ingest client: {e}")
    logging.error("Make sure nv-ingest-client is installed: pip install nv-ingest-client")
    raise

# Import our enhanced configuration system
from config.task_config_manager import config_manager
from nemo_retriever.enhanced_tasks import create_tasks_for_file_type

logger = logging.getLogger(__name__)


class CircuitBreaker:
    """Circuit breaker pattern for NeMo-Retriever service"""
    
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        """
        Initialize circuit breaker
        
        Args:
            failure_threshold: Number of failures before opening circuit
            timeout: Timeout before attempting to close circuit (seconds)
        """
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'closed'  # closed, open, half-open
    
    def record_success(self):
        """Record successful request"""
        self.failure_count = 0
        if self.state == 'half-open':
            self.state = 'closed'
            logger.info("Circuit breaker closed after successful request")
    
    def record_failure(self):
        """Record failed request"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'open'
            logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
    
    def can_execute(self) -> bool:
        """Check if request can be executed"""
        if self.state == 'closed':
            return True
        
        if self.state == 'open':
            # Check if timeout has passed
            if time.time() - self.last_failure_time >= self.timeout:
                self.state = 'half-open'
                logger.info("Circuit breaker half-open, attempting request")
                return True
            return False
        
        # half-open state
        return True
    
    def get_state(self) -> str:
        """Get current circuit breaker state"""
        return self.state


class NemoRetrieverJobSubmitter:
    """Submit jobs to NeMo-Retriever service"""
    
    def __init__(self, host: str, port: int, task_queue: str,
                 timeout: int = 300, max_workers: int = 10,
                 text_depth: str = "page",
                 api_version: str = "v2",
                 pdf_split_page_count: int = 1,
                 circuit_breaker_threshold: int = 5,
                 circuit_breaker_timeout: int = 60):
        """
        Initialize NeMo-Retriever job submitter

        Args:
            host: NeMo-Retriever server hostname
            port: NeMo-Retriever server port
            task_queue: Task queue name
            timeout: Job timeout in seconds
            max_workers: Maximum number of concurrent submissions
            text_depth: Text extraction depth (page/document)
            api_version: NeMo-Retriever API version ('v1' or 'v2'). Use 'v2' with pdf_split_page_count=1
                so each page is one subjob (one bad page only affects that page).
            pdf_split_page_count: V2 only: pages per chunk (1-128). Use 1 for one subjob per page.
            circuit_breaker_threshold: Circuit breaker failure threshold
            circuit_breaker_timeout: Circuit breaker timeout
        """
        self.host = host
        self.port = port
        self.task_queue = task_queue
        self.timeout = timeout
        self.text_depth = text_depth
        self.api_version = api_version
        self.pdf_split_page_count = pdf_split_page_count
        
        # Get supported extensions from config manager
        self.supported_extensions = config_manager.get_supported_extensions()
        
        # Initialize NeMo-Retriever client (message_client_kwargs sets RestClient api_version -> /v1/ or /v2/ endpoints)
        try:
            self.client = NvIngestClient(
                message_client_hostname=host,
                message_client_port=port,
                message_client_kwargs={"api_version": api_version},
                worker_pool_size=max_workers
            )
            logger.info(f"NeMo-Retriever client initialized: {host}:{port}, api_version={api_version}, pdf_split_page_count={pdf_split_page_count}")
        except Exception as e:
            logger.error(f"Failed to initialize NeMo-Retriever client: {e}")
            raise
        
        # Thread pool for parallel job submissions
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Circuit breaker
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_breaker_threshold,
            timeout=circuit_breaker_timeout
        )
        
        logger.info(f"NemoRetrieverJobSubmitter initialized: workers={max_workers}, "
                   f"supported_extensions={self.supported_extensions}")
    
    def is_file_type_supported(self, file_path: str) -> Tuple[bool, str]:
        """
        Check if file type is supported for processing.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Tuple of (is_supported, file_extension)
        """
        file_ext = Path(file_path).suffix.lower().lstrip('.')
        is_supported = file_ext in self.supported_extensions
        return is_supported, file_ext
    
    def create_job_spec(self, file_path: str, source_id: str, s3_bucket: str = None, s3_version: str = None, file_id: str = None) -> Tuple[Optional[JobSpec], Optional[str]]:
        """
        Create job specification for a file with enhanced task configuration
        
        Args:
            file_path: Path to local file
            source_id: Source identifier (S3 key/object name)
            s3_bucket: S3 bucket name (optional, for constructing full source_name)
            s3_version: S3 object version (optional, for constructing full source_name)
            file_id: Unique file processing ID (scopes S3 output to avoid cross-run contamination)
        
        Returns:
            Tuple of (JobSpec, file_type) or (None, None) on error
        
        Note:
            source_name format: bucket_name/object_name/version (or bucket_name/object_name if version not available)
        """
        try:
            # Get file extension for configuration lookup
            file_ext = Path(file_path).suffix.lower().lstrip('.')
            
            # Check if file type is supported (documents only)
            is_supported, ext = self.is_file_type_supported(file_path)
            if not is_supported:
                logger.error(f"Unsupported file type '{ext}' for {source_id}. "
                           f"Only document types are supported: {self.supported_extensions}. "
                           f"File will be sent to dead letter queue.")
                return None, None
            
            config_file_type = config_manager.get_file_type(file_path)
            
            # Try to extract file content and type using nv_ingest_client
            # If it fails (e.g., for CSV files), fall back to extension-based detection
            file_content = None
            file_type = None
            
            try:
                file_content, file_type = extract_file_content(file_path)
                logger.debug(f"Extracted file content and type using extract_file_content: {file_type}")
            except Exception as extract_error:
                # Fallback: handle files that extract_file_content doesn't support (e.g., CSV)
                logger.warning(f"extract_file_content failed for {source_id}: {extract_error}. Using fallback method.")
                
                # Read file content manually and encode as base64 for JSON serialization
                try:
                    with open(file_path, 'rb') as f:
                        file_bytes = f.read()
                    # Encode bytes to base64 string for JSON serialization
                    # extract_file_content likely returns base64 or a similar format
                    file_content = base64.b64encode(file_bytes).decode('utf-8')
                except Exception as read_error:
                    logger.error(f"Failed to read file {file_path}: {read_error}")
                    raise
                
                # Map file extension to DocumentTypeEnum value
                # Only document types are supported
                extension_to_type = {
                    'txt': 'TEXT',
                    'md': 'TEXT',
                    'rtf': 'TEXT',
                    'pdf': 'PDF',
                    'docx': 'DOCX',
                    'doc': 'DOC',
                    'pptx': 'PPTX',
                    'ppt': 'PPT',
                }
                
                file_type_str = extension_to_type.get(file_ext)
                if file_type_str is None:
                    # This should not happen as we already validated, but handle gracefully
                    logger.error(f"Unsupported file extension '{file_ext}' for {source_id}")
                    return None, None
                
                # Try to convert to DocumentTypeEnum if available, otherwise use string
                if DocumentTypeEnum and hasattr(DocumentTypeEnum, file_type_str):
                    file_type = getattr(DocumentTypeEnum, file_type_str)
                    logger.debug(f"Mapped file extension '{file_ext}' to DocumentTypeEnum.{file_type_str}")
                else:
                    # Use string value (will work if JobSpec accepts string document types)
                    file_type = file_type_str
                    logger.debug(f"Mapped file extension '{file_ext}' to document type '{file_type}' (string)")
            
            logger.debug(f"Creating job spec for {source_id} (type: {file_type}, ext: {file_ext}, config_type: {config_file_type})")
            
            # Construct source_name as bucket_name/object_name/version format
            # Check for both None and empty string
            if s3_bucket and s3_bucket.strip():
                if s3_version:
                    source_name = f"{s3_bucket}/{source_id}/{s3_version}"
                else:
                    source_name = f"{s3_bucket}/{source_id}"
            else:
                # Fallback to just object name if bucket not provided
                source_name = source_id
                logger.warning(f"s3_bucket is None/empty for {source_id}, using fallback format: {source_name}")
            
            logger.info(f"Using source_name: {source_name} (bucket: {s3_bucket}, version: {s3_version})")
            
            # NeMo-Retriever uses source_id for the source_name field in parquet files
            # So we pass the full path (bucket_name/object_name/version) as source_id to ensure consistency
            # Create job specification
            # Note: tracing_options.ts_send must be an integer (1 or 0), not boolean (True/False)
            # Server schema: TracingOptionsSchema(ts_send: int, trace: bool = False, trace_id: Optional[str] = None)
            # V2: pdf_config.split_page_count (e.g. 1) so server splits PDF into one subjob per page
            extended_options = {
                'tracing_options': {
                    'ts_send': 1  # Server expects int (1 or 0), not bool (True/False)
                }
            }
            if self.api_version == "v2" and file_ext == "pdf":
                extended_options['pdf_config'] = {'split_page_count': self.pdf_split_page_count}
            job_spec = JobSpec(
                document_type=file_type,
                payload=file_content,
                source_id=source_name,  # Use bucket_name/object_name/version format so NeMo-Retriever uses it for source_name in parquet
                source_name=source_name,  # Also set source_name for consistency
                extended_options=extended_options
            )
            
            # Get configurations for this file type
            # Use file_ext (lowercase extension) instead of config_file_type (MIME type)
            # because extract_configs keys are file extensions like "pdf", "docx", etc.
            extract_config = config_manager.get_extract_config(file_ext)
            split_config = config_manager.get_split_config(file_ext)
            embed_config = config_manager.get_embed_config(config_file_type)  # Keep MIME type for embed config
            storage_config = config_manager.get_storage_config()
            
            # Convert configurations to dictionaries
            # Note: text_depth is passed from job_submitter initialization, not from ExtractConfig
            extract_dict = {
                'extract_text': extract_config.extract_text,
                'extract_images': extract_config.extract_images,
                'extract_tables': extract_config.extract_tables,
                'extract_charts': extract_config.extract_charts,
                'extract_metadata': extract_config.extract_metadata,
                'extract_links': extract_config.extract_links,
                'extract_footnotes': extract_config.extract_footnotes,
                'extract_headers': extract_config.extract_headers,
                'extract_footers': extract_config.extract_footers,
                'preserve_formatting': extract_config.preserve_formatting,
                'ocr_enabled': extract_config.ocr_enabled,
                'ocr_language': extract_config.ocr_language,
                'ocr_confidence_threshold': extract_config.ocr_confidence_threshold,
                'image_analysis': extract_config.image_analysis,
                'face_detection': extract_config.face_detection,
                'object_detection': extract_config.object_detection,
                'transcription_enabled': extract_config.transcription_enabled,
                'transcription_language': extract_config.transcription_language,
                'transcription_model': extract_config.transcription_model,
                'speaker_diarization': extract_config.speaker_diarization,
                'audio_analysis': extract_config.audio_analysis,
                'frame_extraction': extract_config.frame_extraction,
                'frame_interval': extract_config.frame_interval,
                'video_analysis': extract_config.video_analysis,
                'preserve_cell_formatting': extract_config.preserve_cell_formatting,
                'extract_formulas': extract_config.extract_formulas,
                'data_validation': extract_config.data_validation,
                'recursive_extraction': extract_config.recursive_extraction,
                'max_depth': extract_config.max_depth,
                'max_file_size': extract_config.max_file_size,
                'text_depth': self.text_depth,  # Use text_depth from job_submitter initialization (defaults to "page")
                # Additional NeMo-Retriever extraction parameters
                'extract_infographics': extract_config.extract_infographics,
                'extract_page_as_image': extract_config.extract_page_as_image,
                'extract_images_method': extract_config.extract_images_method,
                'extract_tables_method': extract_config.extract_tables_method,
                'table_output_format': extract_config.table_output_format
            }
            
            # Map max_character_length to chunk_size if chunk_size is not explicitly set
            # This ensures config.yaml's max_character_length is properly used
            chunk_size = split_config.chunk_size
            if chunk_size == 1000 and hasattr(split_config, 'max_character_length') and split_config.max_character_length:
                # Use max_character_length if chunk_size is still at default value
                chunk_size = split_config.max_character_length
            
            split_dict = {
                'enabled': split_config.enabled,
                'strategy': split_config.strategy,
                'chunk_size': chunk_size,  # Use mapped chunk_size
                'chunk_overlap': split_config.chunk_overlap,
                'tokenizer': split_config.tokenizer,
                'model': split_config.model,
                'min_chunk_size': split_config.min_chunk_size,
                'max_chunk_size': split_config.max_chunk_size,
                'preserve_paragraphs': split_config.preserve_paragraphs,
                'preserve_sections': split_config.preserve_sections,
                'split_by_headers': split_config.split_by_headers,
                'split_by_pages': split_config.split_by_pages,
                'split_by_page': getattr(split_config, 'split_by_page', split_config.split_by_pages),  # Support both naming conventions
                'split_by_size': getattr(split_config, 'split_by_size', False),  # Add split_by_size support
                'max_character_length': getattr(split_config, 'max_character_length', None),  # Pass through max_character_length
                'custom_separators': split_config.custom_separators or [],
                'split_by_silence': split_config.split_by_silence,
                'silence_threshold': split_config.silence_threshold,
                'min_silence_duration': split_config.min_silence_duration,
                'split_by_scenes': split_config.split_by_scenes,
                'scene_threshold': split_config.scene_threshold,
                'split_by_sheets': split_config.split_by_sheets,
                'split_by_columns': split_config.split_by_columns,
                'preserve_headers': split_config.preserve_headers
            }
            
            embed_dict = {
                'enabled': embed_config.enabled,
                'model': embed_config.model,
                'dimensions': embed_config.dimensions,
                'batch_size': embed_config.batch_size,
                'normalize': embed_config.normalize,
                'metadata_fields': embed_config.metadata_fields or [],
                'custom_metadata': embed_config.custom_metadata,
                'extract_features': embed_config.extract_features or []
            }
            
            storage_dict = {
                'milvus_enabled': storage_config.milvus_enabled,
                'milvus_host': storage_config.milvus_host,
                'milvus_port': storage_config.milvus_port,
                'milvus_collection': storage_config.milvus_collection,
                'milvus_index_type': storage_config.milvus_index_type,
                'milvus_metric_type': storage_config.milvus_metric_type,
                'milvus_nlist': storage_config.milvus_nlist,
                'milvus_nprobe': storage_config.milvus_nprobe,
                'milvus_auto_id': storage_config.milvus_auto_id,
                'milvus_consistency_level': storage_config.milvus_consistency_level,
                'objects_enabled': storage_config.objects_enabled,
                'objects_endpoint': storage_config.objects_endpoint,
                'objects_access_key': storage_config.objects_access_key,
                'objects_secret_key': storage_config.objects_secret_key,
                'objects_bucket': storage_config.objects_bucket,
                'objects_region': storage_config.objects_region
            }
            
            # Create all tasks for this file type, passing file-specific info for custom path construction
            tasks = create_tasks_for_file_type(file_type, extract_dict, split_dict, embed_dict, storage_dict,
                                              s3_bucket=s3_bucket, source_id=source_id, file_id=file_id)
            
            # Add tasks to job spec
            for task in tasks:
                if task:  # Skip None tasks (disabled tasks)
                    job_spec.add_task(task)
            
            logger.info(f"Added {len(tasks)} tasks to job spec for {source_name}")
            # Log task types for debugging
            task_types = [type(t).__name__ for t in tasks if t is not None]
            logger.info(f"Task types: {task_types}")
            
            return job_spec, file_type
        
        except Exception as e:
            logger.error(f"Failed to create job spec for {file_path}: {e}")
            return None, None
    
    def submit_batch(self, batch: Dict) -> Tuple[bool, Dict]:
        """
        Submit a batch of files to NeMo-Retriever
        
        Args:
            batch: Batch dictionary with file information
        
        Returns:
            Tuple of (success, result_dict)
        """
        batch_id = batch['batch_id']
        files = batch['files']
        
        logger.info(f"Submitting batch {batch_id} with {len(files)} files to NeMo-Retriever")
        
        # Check circuit breaker
        if not self.circuit_breaker.can_execute():
            logger.warning(f"Circuit breaker is {self.circuit_breaker.get_state()}, "
                         f"delaying batch submission")
            return False, {
                'error': 'Circuit breaker open',
                'batch_id': batch_id,
                'state': self.circuit_breaker.get_state()
            }
        
        job_ids = []
        failed_files = []  # List of tuples: (file_id, file_detail, error_message)
        job_to_files = {}  # Track job_id -> file_ids mapping
        
        try:
            # Create and add jobs for each file
            for file_info in files:
                file_id = file_info.file_id
                local_path = file_info.local_path
                s3_key = file_info.s3_key
                s3_bucket = file_info.s3_bucket
                s3_version = getattr(file_info, 's3_version', None)  # Get version if available
                
                # Check if file type is supported before creating job spec
                is_supported, file_ext = self.is_file_type_supported(local_path)
                if not is_supported:
                    error_msg = (f"Unsupported file type '{file_ext}' for {file_id}. "
                               f"Only document types supported: {self.supported_extensions}")
                    logger.error(error_msg)
                    # Store file details for dead letter queue
                    failed_files.append((file_id, file_info.to_dict(), error_msg))
                    continue
                
                # Create job specification with bucket and version for consistent source_name
                job_spec, file_type = self.create_job_spec(local_path, s3_key, s3_bucket=s3_bucket, s3_version=s3_version, file_id=file_id)
                
                if job_spec is None:
                    error_msg = f"Failed to create job spec for {file_id}"
                    logger.error(error_msg)
                    # Store file details for dead letter queue
                    failed_files.append((file_id, file_info.to_dict(), error_msg))
                    continue
                
                try:
                    # Get source_name from job_spec for logging
                    source_name = getattr(job_spec, 'source_name', getattr(job_spec, 'source_id', s3_key))
                    
                    # Add job to client
                    # Note: add_job() returns a client-side job index (e.g., "0", "1", "2"), not the server job ID
                    # The actual server job ID is assigned after submit_job() is called
                    logger.debug(f"Calling add_job for file {file_id} (source: {source_name})")
                    job_id = self.client.add_job(job_spec)
                    logger.debug(f"add_job returned job_index: {job_id} (type: {type(job_id)})")
                    
                    # Validate job_id is not None or empty (but "0" is valid as it's the first client-side index)
                    if job_id is None or (isinstance(job_id, str) and not job_id.strip()):
                        error_msg = f"NeMo-Retriever client returned empty job index for file {file_id}. Job may not have been added properly."
                        logger.error(error_msg)
                        failed_files.append((file_id, file_info.to_dict(), error_msg))
                        continue
                    
                    # Convert job_id to string for consistency
                    job_id = str(job_id)
                    job_ids.append(job_id)
                    
                    # Track file for this job
                    if job_id not in job_to_files:
                        job_to_files[job_id] = []
                    job_to_files[job_id].append(file_id)
                    
                    logger.info(f"Added job (index: {job_id}) for file {file_id} (source: {source_name})")
                
                except Exception as e:
                    error_msg = f"Failed to add job for {file_id}: {e}"
                    logger.error(error_msg, exc_info=True)
                    source_name = getattr(job_spec, 'source_name', getattr(job_spec, 'source_id', s3_key)) if 'job_spec' in locals() else s3_key
                    logger.error(f"Job spec that failed: source_name={source_name}")
                    # Store file details for dead letter queue
                    failed_files.append((file_id, file_info.to_dict(), error_msg))
            
            if not job_ids:
                logger.error(f"No jobs created for batch {batch_id}")
                self.circuit_breaker.record_failure()
                return False, {
                    'error': 'No jobs created',
                    'batch_id': batch_id,
                    'failed_files': failed_files
                }
            
            # Submit all jobs
            logger.info(f"Submitting {len(job_ids)} jobs (IDs: {job_ids}) to queue '{self.task_queue}'")
            start_time = time.time()
            
            try:
                # Submit jobs to queue
                # Note: submit_job() sends to message queue, which accepts immediately
                # Server validation happens asynchronously when consuming from queue
                trace_ids = self.client.submit_job(job_ids, self.task_queue)
                submission_time = time.time() - start_time
                
                # Log trace IDs for debugging
                logger.debug(f"submit_job returned trace_ids: {trace_ids}")
                
                # Note: We don't add a delay here because:
                # 1. Queues are used when there's delay/speed difference - jobs might sit in queue for minutes/hours
                # 2. Validation happens asynchronously when server consumes from queue - timing is unpredictable
                # 3. Validation failures are deterministic - if one fails, all similar jobs will fail
                # 4. We use pattern-based detection in status_tracker to fail fast for validation failures
                
                logger.info(f"Batch {batch_id} submitted successfully: "
                           f"{len(job_ids)} jobs in {submission_time:.2f}s (trace_ids: {trace_ids})")
            except Exception as e:
                submission_time = time.time() - start_time
                error_msg = f"Failed to submit jobs to NeMo-Retriever queue: {e}"
                logger.error(error_msg, exc_info=True)
                # Add all files to failed_files since submission failed
                for job_id in job_ids:
                    if job_id in job_to_files:
                        for file_id in job_to_files[job_id]:
                            failed_files.append((file_id, {}, error_msg))
                self.circuit_breaker.record_failure()
                return False, {
                    'error': 'Job submission failed',
                    'batch_id': batch_id,
                    'failed_files': failed_files,
                    'submission_error': error_msg
                }
            
            # Record success in circuit breaker
            self.circuit_breaker.record_success()
            
            # Map client-side job_id to trace_id
            # trace_ids is a list returned by submit_job, one per job_id in the same order
            job_id_to_trace_id = {}
            if trace_ids and len(trace_ids) == len(job_ids):
                for idx, job_id in enumerate(job_ids):
                    job_id_to_trace_id[job_id] = trace_ids[idx] if idx < len(trace_ids) else None
            elif trace_ids:
                logger.warning(f"Trace IDs count ({len(trace_ids)}) doesn't match job IDs count ({len(job_ids)})")
            
            return True, {
                'batch_id': batch_id,
                'job_ids': job_ids,
                'job_to_files': job_to_files,  # Map job_id -> [file_ids]
                'job_id_to_trace_id': job_id_to_trace_id,  # Map client job_id -> trace_id
                'file_count': len(job_ids),
                'failed_files': failed_files,  # List of (file_id, file_detail, error_msg) tuples
                'submission_time': submission_time
            }
        
        except Exception as e:
            logger.error(f"Failed to submit batch {batch_id}: {e}")
            self.circuit_breaker.record_failure()
            
            return False, {
                'error': str(e),
                'batch_id': batch_id,
                'job_ids': job_ids,
                'job_to_files': job_to_files,  # Map job_id -> [file_ids]
                'failed_files': failed_files
            }
    
    def submit_batch_async(self, batch: Dict) -> Future:
        """
        Submit batch asynchronously
        
        Args:
            batch: Batch dictionary
        
        Returns:
            Future object for the submission task
        """
        return self.executor.submit(self.submit_batch, batch)
    
    def shutdown(self, wait: bool = True, timeout: int = 60):
        """
        Shutdown the thread pool
        
        Args:
            wait: Whether to wait for pending tasks
            timeout: Timeout for shutdown in seconds
        """
        logger.info(f"Shutting down NeMo-Retriever job submitter (wait={wait}, timeout={timeout})")
        self.executor.shutdown(wait=wait, timeout=timeout)

