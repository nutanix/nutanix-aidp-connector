# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Enhanced NeMo-Retriever Task Classes
Supports dynamic configuration based on file types
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass
import logging

# Import actual Task classes from nv_ingest_client
try:
    from nv_ingest_client.primitives.tasks import ExtractTask as NvExtractTask
    from nv_ingest_client.primitives.tasks import SplitTask as NvSplitTask
    from nv_ingest_client.primitives.tasks import EmbedTask as NvEmbedTask
    from nv_ingest_client.primitives.tasks import StoreTask as NvStoreTask
    from nv_ingest_client.primitives.tasks import StoreEmbedTask as NvStoreEmbedTask
except ImportError as e:
    logging.error(f"Failed to import NeMo-Retriever task classes: {e}")
    NvExtractTask = None
    NvSplitTask = None
    NvEmbedTask = None
    NvStoreTask = None
    NvStoreEmbedTask = None

logger = logging.getLogger(__name__)


def create_tasks_for_file_type(file_type: str, extract_config: Dict[str, Any], 
                              split_config: Dict[str, Any], embed_config: Dict[str, Any],
                              storage_config: Dict[str, Any], 
                              s3_bucket: str = None, source_id: str = None,
                              file_id: str = None) -> List[Any]:
    """
    Create all tasks for a specific file type using actual Task classes from nv_ingest_client
    
    Returns:
        List of Task objects (not dictionaries)
    """
    tasks = []
    
    if not NvExtractTask:
        logger.error("NeMo-Retriever Task classes not available")
        return tasks
    
    # Create ExtractTask (required)
    try:
        # Map our config to nv_ingest_client ExtractTask parameters
        # ExtractTask requires document_type (should be DocumentTypeEnum or compatible value)
        # file_type should already be a DocumentTypeEnum from extract_file_content
        extract_text_val = extract_config.get('extract_text', True)
        extract_images_val = extract_config.get('extract_images', False)
        extract_tables_val = extract_config.get('extract_tables', False)
        extract_charts_val = extract_config.get('extract_charts', False)
        extract_method_val = extract_config.get('extract_method', 'pdfium')
        text_depth_val = extract_config.get('text_depth', 'page')
        # Additional NeMo-Retriever extraction parameters
        extract_infographics_val = extract_config.get('extract_infographics', False)
        extract_page_as_image_val = extract_config.get('extract_page_as_image', False)
        extract_images_method_val = extract_config.get('extract_images_method', 'group')
        extract_tables_method_val = extract_config.get('extract_tables_method', 'yolox')
        table_output_format_val = extract_config.get('table_output_format', 'pseudo_markdown')
        
        extract_task = NvExtractTask(
            document_type=file_type,  # Should be DocumentTypeEnum (e.g., DocumentTypeEnum.PDF)
            extract_text=extract_text_val,
            extract_images=extract_images_val,
            extract_tables=extract_tables_val,
            extract_charts=extract_charts_val,
            extract_method=extract_method_val,
            text_depth=text_depth_val,
            # Additional NeMo-Retriever extraction parameters
            extract_infographics=extract_infographics_val,
            extract_page_as_image=extract_page_as_image_val,
            extract_images_method=extract_images_method_val,
            extract_tables_method=extract_tables_method_val,
            table_output_format=table_output_format_val
        )
        tasks.append(extract_task)
        logger.info(f"Created ExtractTask for document_type={file_type}: extract_text={extract_text_val}, extract_tables={extract_tables_val}, extract_charts={extract_charts_val}, extract_images={extract_images_val}, text_depth={text_depth_val}, extract_tables_method={extract_tables_method_val}, table_output_format={table_output_format_val}")
    except Exception as e:
        logger.error(f"Failed to create ExtractTask: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    # Create SplitTask (if enabled)
    if split_config.get('enabled', True):
        try:
            # Build params dict with all split configuration parameters
            # Parameters like strategy, min_chunk_size, max_chunk_size, etc. go in params dict
            split_params = split_config.get('params', {}).copy() if split_config.get('params') else {}
            
            # Handle split_by_page and split_by_size from config.yaml
            # These map to split_by_pages in params
            split_by_page = split_config.get('split_by_page', False) or split_config.get('split_by_pages', False)
            split_by_size = split_config.get('split_by_size', False)
            
            if split_by_page:
                split_params['split_by_pages'] = True
            if split_by_size:
                # When split_by_size is true, use max_character_length as chunk_size limit
                max_char_length = split_config.get('max_character_length')
                if max_char_length:
                    # Use max_character_length as the effective chunk_size when splitting by size
                    effective_chunk_size = max_char_length
                else:
                    effective_chunk_size = split_config.get('chunk_size', 1024)
            else:
                effective_chunk_size = split_config.get('chunk_size', 1024)
            
            # Add split configuration parameters to params dict
            if 'strategy' in split_config:
                split_params['strategy'] = split_config['strategy']
            if 'min_chunk_size' in split_config:
                split_params['min_chunk_size'] = split_config['min_chunk_size']
            if 'max_chunk_size' in split_config:
                split_params['max_chunk_size'] = split_config['max_chunk_size']
            elif split_config.get('max_character_length'):
                # Use max_character_length as max_chunk_size if not explicitly set
                split_params['max_chunk_size'] = split_config.get('max_character_length')
            if 'preserve_paragraphs' in split_config:
                split_params['preserve_paragraphs'] = split_config['preserve_paragraphs']
            if 'preserve_sections' in split_config:
                split_params['preserve_sections'] = split_config['preserve_sections']
            if 'split_by_headers' in split_config:
                split_params['split_by_headers'] = split_config['split_by_headers']
            if 'custom_separators' in split_config:
                split_params['custom_separators'] = split_config['custom_separators']
            if 'split_by_silence' in split_config:
                split_params['split_by_silence'] = split_config['split_by_silence']
            if 'silence_threshold' in split_config:
                split_params['silence_threshold'] = split_config['silence_threshold']
            if 'min_silence_duration' in split_config:
                split_params['min_silence_duration'] = split_config['min_silence_duration']
            if 'split_by_scenes' in split_config:
                split_params['split_by_scenes'] = split_config['split_by_scenes']
            if 'scene_threshold' in split_config:
                split_params['scene_threshold'] = split_config['scene_threshold']
            if 'split_by_sheets' in split_config:
                split_params['split_by_sheets'] = split_config['split_by_sheets']
            if 'split_by_columns' in split_config:
                split_params['split_by_columns'] = split_config['split_by_columns']
            if 'preserve_headers' in split_config:
                split_params['preserve_headers'] = split_config['preserve_headers']
            if 'model' in split_config:
                split_params['model'] = split_config['model']
            
            # Determine chunk_overlap - use 0 for page-based splitting, otherwise use config or default
            chunk_overlap = split_config.get('chunk_overlap', 0)
            if split_by_page and not split_by_size:
                # Pure page-based splitting doesn't need overlap
                chunk_overlap = 0
            
            split_task = NvSplitTask(
                tokenizer=split_config.get('tokenizer') or None,
                chunk_size=effective_chunk_size,
                chunk_overlap=chunk_overlap,
                params=split_params
            )
            logger.debug(f"Created SplitTask with chunk_size={effective_chunk_size}, chunk_overlap={chunk_overlap}, "
                        f"split_by_page={split_by_page}, split_by_size={split_by_size}, "
                        f"strategy={split_params.get('strategy', 'default')}, params={split_params}")
            tasks.append(split_task)
        except Exception as e:
            logger.error(f"Failed to create SplitTask: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    # Create EmbedTask (if enabled)
    # Note: model_name is not specified - let NeMo-Retriever server decide which model to use
    if embed_config.get('enabled', True):
        try:
            embed_task = NvEmbedTask(
                endpoint_url=embed_config.get('endpoint_url') or None,
                model_name=None,  # Let NeMo-Retriever server decide which model to use
                api_key=embed_config.get('api_key') or None,
                filter_errors=embed_config.get('filter_errors', False)
            )
            tasks.append(embed_task)
        except Exception as e:
            logger.error(f"Failed to create EmbedTask: {e}")
    
    # Create StoreEmbedTask for Milvus (vector storage)
    if storage_config.get('milvus_enabled', True):
        try:
            # StoreEmbedTask takes params (dict) - Milvus and MinIO parameters must be in params dict
            # According to NeMo-Retriever API, milvus_address should be in format "host:port"
            # MinIO is required for intermediate storage before bulk upload to Milvus
            milvus_host = storage_config.get('milvus_host')
            milvus_port = storage_config.get('milvus_port')
            milvus_collection = storage_config.get('milvus_collection')
            
            # Validate Milvus configuration
            if not milvus_host or not milvus_port:
                logger.error(f"Milvus host/port not configured: host={milvus_host}, port={milvus_port}. Skipping StoreEmbedTask creation.")
                return tasks
            
            # Prepare params dict
            params = {
                # Milvus parameters
                'milvus_address': f"{milvus_host}:{milvus_port}",  # Format: "host:port"
                'milvus_uri': f"http://{milvus_host}:{milvus_port}",  # Full URI format
                'milvus_host': milvus_host,  # Also provide separately for compatibility
                'milvus_port': milvus_port,  # Also provide separately for compatibility
                'milvus_collection_name': milvus_collection,  # Full parameter name
                'collection_name': milvus_collection,  # Alternative parameter name (NeMo-Retriever API may use this)
                'milvus_index_type': storage_config.get('milvus_index_type'),
                'milvus_metric_type': storage_config.get('milvus_metric_type')
            }
            
            # Add Objects parameters (required for intermediate storage before Milvus bulk upload)
            # Note: NeMo-Retriever API still uses "minio_" prefix for these parameters
            if storage_config.get('objects_enabled', True):
                # Parse endpoint to determine if HTTPS is used
                objects_endpoint = storage_config.get('objects_endpoint', '')
                # Remove protocol prefix if present (NeMo-Retriever expects host:port format)
                if '://' in objects_endpoint:
                    objects_endpoint = objects_endpoint.split('://')[1]
                
                # Determine if secure (HTTPS) based on port
                # Milvus uses HTTPS on port 443, but NeMo-Retriever's RemoteBulkWriter has secure=False hardcoded
                # We'll set secure based on port, but NeMo-Retriever will use HTTP internally
                objects_secure = False
                if ':' in objects_endpoint:
                    port = int(objects_endpoint.split(':')[1])
                    objects_secure = (port == 443)
                    # Note: NeMo-Retriever's RemoteBulkWriter has secure=False hardcoded, so it will use HTTP
                    # even if we pass minio_secure=True. The Objects client will use secure if specified.
                
                # Construct custom minio_bucket_path: {file_id}/embeddings_{bucket}_{object_name}
                # file_id scopes each job's output so previous runs' leftovers are never mixed in.
                # Sanitize bucket and object name to be filesystem-safe
                if s3_bucket and source_id:
                    safe_bucket = s3_bucket.replace('/', '_').replace(':', '_').replace(' ', '_')
                    safe_object = source_id.replace('/', '_').replace(':', '_').replace(' ', '_')
                    if len(safe_object) > 200:
                        safe_object = safe_object[-200:]
                    base_path = f"embeddings_{safe_bucket}_{safe_object}"
                else:
                    base_path = 'embeddings'
                
                if file_id:
                    minio_bucket_path = f"{file_id}/{base_path}"
                else:
                    minio_bucket_path = base_path
                
                params.update({
                    'minio_endpoint': objects_endpoint,  # Format: host:port (no protocol prefix) - NeMo-Retriever API uses minio_ prefix
                    'minio_access_key': storage_config.get('objects_access_key'),
                    'minio_secret_key': storage_config.get('objects_secret_key'),
                    'minio_bucket_name': storage_config.get('objects_bucket'),
                    'minio_bucket_path': minio_bucket_path,  # Custom path with bucket and object name
                    'minio_region': storage_config.get('objects_region'),
                    'minio_secure': objects_secure  # Set based on port (True for 443, False otherwise)
                })
                
                logger.info(f"StoreEmbedTask Objects config: endpoint={objects_endpoint}, secure={objects_secure}, "
                            f"bucket={storage_config.get('objects_bucket')}, access_key={storage_config.get('objects_access_key')[:10]}...")
            
            store_task = NvStoreEmbedTask(params=params)
            tasks.append(store_task)
            logger.info(f"Created StoreEmbedTask with Milvus params: host={storage_config.get('milvus_host')}, collection={storage_config.get('milvus_collection')}, "
                       f"Objects bucket={storage_config.get('objects_bucket')}, Objects endpoint={objects_endpoint}")
        except Exception as e:
            logger.error(f"Failed to create StoreEmbedTask: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    # Create StoreTask for Objects (if enabled and Milvus is not the primary storage)
    # Note: StoreTask is for object storage, not embedding storage
    # We use StoreEmbedTask for Milvus, but can add Objects params to StoreEmbedTask if needed
    # Note: NeMo-Retriever API still uses "minio" as store_method and "minio_" prefix for parameters
    if storage_config.get('objects_enabled', False) and not storage_config.get('milvus_enabled', True):
        try:
            # StoreTask for object storage (not embeddings)
            store_task = NvStoreTask(
                structured=True,
                images=False,
                store_method='minio' if storage_config.get('objects_enabled') else None,  # NeMo-Retriever API uses 'minio'
                params={
                    'minio_endpoint': storage_config.get('objects_endpoint'),  # NeMo-Retriever API uses minio_ prefix
                    'minio_access_key': storage_config.get('objects_access_key'),
                    'minio_secret_key': storage_config.get('objects_secret_key'),
                    'minio_bucket_name': storage_config.get('objects_bucket'),
                    'minio_region': storage_config.get('objects_region')
                } if storage_config.get('objects_enabled') else None
            )
            tasks.append(store_task)
        except Exception as e:
            logger.error(f"Failed to create StoreTask: {e}")
    
    return tasks
