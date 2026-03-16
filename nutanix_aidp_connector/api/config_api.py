# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Configuration API for hot-reloading NeMo-Retriever task configurations
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify, Response
from pathlib import Path

# Import our configuration manager
from config.task_config_manager import config_manager

logger = logging.getLogger(__name__)

def create_config_app() -> Flask:
    """Create Flask app for configuration API"""
    app = Flask(__name__)

    @app.after_request
    def _add_cache_headers(response: Response) -> Response:
        """Prevent caching of API responses (mitigates session/Vary: Cookie issues behind proxies)."""
        response.headers["Cache-Control"] = "private, no-store, no-cache, must-revalidate"
        response.headers["Vary"] = "Cookie"
        return response

    @app.route('/health', methods=['GET'])
    def health_check():
        """Health check endpoint"""
        return jsonify({
            'status': 'healthy',
            'service': 'nutanix-aidp-connector-config-api',
            'version': '1.0.0'
        })
    
    @app.route('/metrics/status', methods=['GET'])
    def metrics_status():
        """Get telemetry/metrics status"""
        try:
            from utils.telemetry import get_metrics
            metrics = get_metrics()
            
            if metrics and metrics.enabled:
                return jsonify({
                    'status': 'success',
                    'telemetry': {
                        'enabled': True,
                        'prometheus_endpoint': 'Prometheus metrics available on configured prometheus_port (default: 9090)',
                        'message': 'Metrics are being exported to Prometheus'
                    }
                })
            else:
                return jsonify({
                    'status': 'success',
                    'telemetry': {
                        'enabled': False,
                        'message': 'Telemetry is disabled or not initialized'
                    }
                })
        except Exception as e:
            logger.error(f"Failed to get metrics status: {e}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to get metrics status: {str(e)}'
            }), 500
    
    @app.route('/config/reload', methods=['POST'])
    def reload_config():
        """Reload configuration from file"""
        try:
            config_manager.reload_config()
            return jsonify({
                'status': 'success',
                'message': 'Configuration reloaded successfully'
            })
        except Exception as e:
            logger.error(f"Failed to reload configuration: {e}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to reload configuration: {str(e)}'
            }), 500
    
    @app.route('/config/status', methods=['GET'])
    def get_config_status():
        """Get current configuration status"""
        try:
            storage_config = config_manager.get_storage_config()
            
            # Get batching config from pipeline_config_manager
            from config.pipeline_config_manager import pipeline_config_manager
            pipeline_batching = pipeline_config_manager.get_batching_config()
            
            # Get supported extensions
            supported_extensions = list(config_manager.get_supported_extensions())
            
            return jsonify({
                'status': 'success',
                'config': {
                    'supported_extensions': supported_extensions,
                    'supported_extensions_count': len(supported_extensions),
                    'batching': {
                        'max_files': pipeline_batching.max_files,
                        'max_bytes': pipeline_batching.max_bytes,
                        'max_wait_seconds': pipeline_batching.max_wait_seconds
                    },
                    'storage': {
                        'milvus_enabled': storage_config.milvus_enabled,
                        'milvus_host': storage_config.milvus_host,
                        'milvus_port': storage_config.milvus_port,
                        'milvus_collection': storage_config.milvus_collection,
                        'objects_enabled': storage_config.objects_enabled,
                        'objects_endpoint': storage_config.objects_endpoint,
                        'objects_bucket': storage_config.objects_bucket
                    }
                }
            })
        except Exception as e:
            logger.error(f"Failed to get configuration status: {e}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to get configuration status: {str(e)}'
            }), 500
    
    @app.route('/config/file-types', methods=['GET'])
    def get_file_types():
        """Get supported file types (document types only)"""
        try:
            supported_extensions = list(config_manager.get_supported_extensions())
            file_types = {ext: config_manager.FILE_TYPES.get(ext, 'unknown') for ext in supported_extensions}
            return jsonify({
                'status': 'success',
                'supported_extensions': supported_extensions,
                'file_types': file_types
            })
        except Exception as e:
            logger.error(f"Failed to get file types: {e}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to get file types: {str(e)}'
            }), 500
    
    @app.route('/config/file-types-legacy', methods=['GET'])
    def get_file_types_legacy():
        """Get supported file types (legacy endpoint for compatibility)"""
        try:
            return jsonify({
                'status': 'success',
                'file_types': config_manager.FILE_TYPES
            })
        except Exception as e:
            logger.error(f"Failed to get file types: {e}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to get file types: {str(e)}'
            }), 500
    
    @app.route('/config/extract/<file_type>', methods=['GET'])
    def get_extract_config(file_type: str):
        """Get extract configuration for a file type"""
        try:
            config = config_manager.get_extract_config(file_type)
            return jsonify({
                'status': 'success',
                'file_type': file_type,
                'config': {
                    'extract_text': config.extract_text,
                    'extract_images': config.extract_images,
                    'extract_tables': config.extract_tables,
                    'extract_metadata': config.extract_metadata,
                    'extract_links': config.extract_links,
                    'extract_footnotes': config.extract_footnotes,
                    'extract_headers': config.extract_headers,
                    'extract_footers': config.extract_footers,
                    'preserve_formatting': config.preserve_formatting,
                    'ocr_enabled': config.ocr_enabled,
                    'ocr_language': config.ocr_language,
                    'ocr_confidence_threshold': config.ocr_confidence_threshold,
                    'image_analysis': config.image_analysis,
                    'face_detection': config.face_detection,
                    'object_detection': config.object_detection,
                    'transcription_enabled': config.transcription_enabled,
                    'transcription_language': config.transcription_language,
                    'transcription_model': config.transcription_model,
                    'speaker_diarization': config.speaker_diarization,
                    'audio_analysis': config.audio_analysis,
                    'frame_extraction': config.frame_extraction,
                    'frame_interval': config.frame_interval,
                    'video_analysis': config.video_analysis,
                    'preserve_cell_formatting': config.preserve_cell_formatting,
                    'extract_formulas': config.extract_formulas,
                    'extract_charts': config.extract_charts,
                    'data_validation': config.data_validation,
                    'recursive_extraction': config.recursive_extraction,
                    'max_depth': config.max_depth,
                    'max_file_size': config.max_file_size
                }
            })
        except Exception as e:
            logger.error(f"Failed to get extract config for {file_type}: {e}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to get extract config: {str(e)}'
            }), 500
    
    @app.route('/config/split/<file_type>', methods=['GET'])
    def get_split_config(file_type: str):
        """Get split configuration for a file type"""
        try:
            config = config_manager.get_split_config(file_type)
            return jsonify({
                'status': 'success',
                'file_type': file_type,
                'config': {
                    'enabled': config.enabled,
                    'strategy': config.strategy,
                    'chunk_size': config.chunk_size,
                    'chunk_overlap': config.chunk_overlap,
                    'tokenizer': config.tokenizer,
                    'model': config.model,
                    'min_chunk_size': config.min_chunk_size,
                    'max_chunk_size': config.max_chunk_size,
                    'preserve_paragraphs': config.preserve_paragraphs,
                    'preserve_sections': config.preserve_sections,
                    'split_by_headers': config.split_by_headers,
                    'split_by_pages': config.split_by_pages,
                    'custom_separators': config.custom_separators,
                    'split_by_silence': config.split_by_silence,
                    'silence_threshold': config.silence_threshold,
                    'min_silence_duration': config.min_silence_duration,
                    'split_by_scenes': config.split_by_scenes,
                    'scene_threshold': config.scene_threshold,
                    'split_by_sheets': config.split_by_sheets,
                    'split_by_columns': config.split_by_columns,
                    'preserve_headers': config.preserve_headers
                }
            })
        except Exception as e:
            logger.error(f"Failed to get split config for {file_type}: {e}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to get split config: {str(e)}'
            }), 500
    
    @app.route('/config/embed/<file_type>', methods=['GET'])
    def get_embed_config(file_type: str):
        """Get embed configuration for a file type"""
        try:
            config = config_manager.get_embed_config(file_type)
            return jsonify({
                'status': 'success',
                'file_type': file_type,
                'config': {
                    'enabled': config.enabled,
                    'model': config.model,
                    'dimensions': config.dimensions,
                    'batch_size': config.batch_size,
                    'normalize': config.normalize,
                    'metadata_fields': config.metadata_fields,
                    'custom_metadata': config.custom_metadata,
                    'extract_features': config.extract_features
                }
            })
        except Exception as e:
            logger.error(f"Failed to get embed config for {file_type}: {e}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to get embed config: {str(e)}'
            }), 500
    
    @app.route('/config/batching', methods=['GET'])
    def get_batching_config():
        """Get batching configuration from config.yaml"""
        try:
            from config.pipeline_config_manager import pipeline_config_manager
            pipeline_batching = pipeline_config_manager.get_batching_config()
            
            return jsonify({
                'status': 'success',
                'config': {
                    'max_files': pipeline_batching.max_files,
                    'max_bytes': pipeline_batching.max_bytes,
                    'max_wait_seconds': pipeline_batching.max_wait_seconds
                }
            })
        except Exception as e:
            logger.error(f"Failed to get batching config: {e}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to get batching config: {str(e)}'
            }), 500
    
    @app.route('/config/storage', methods=['GET'])
    def get_storage_config():
        """Get storage configuration"""
        try:
            config = config_manager.get_storage_config()
            return jsonify({
                'status': 'success',
                'config': {
                    'milvus': {
                        'enabled': config.milvus_enabled,
                        'host': config.milvus_host,
                        'port': config.milvus_port,
                        'collection': config.milvus_collection,
                        'index_type': config.milvus_index_type,
                        'metric_type': config.milvus_metric_type,
                        'nlist': config.milvus_nlist,
                        'nprobe': config.milvus_nprobe,
                        'auto_id': config.milvus_auto_id,
                        'consistency_level': config.milvus_consistency_level
                    },
                    'objects': {
                        'enabled': config.objects_enabled,
                        'endpoint': config.objects_endpoint,
                        'access_key': config.objects_access_key,
                        'secret_key': '***hidden***',
                        'bucket': config.objects_bucket,
                        'region': config.objects_region
                    }
                }
            })
        except Exception as e:
            logger.error(f"Failed to get storage config: {e}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to get storage config: {str(e)}'
            }), 500
    
    @app.route('/config/update', methods=['POST'])
    def update_config():
        """Update configuration (write to file)"""
        try:
            data = request.get_json()
            if not data:
                return jsonify({
                    'status': 'error',
                    'message': 'No configuration data provided'
                }), 400
            
            # Validate required fields
            required_sections = ['file_types', 'extract_tasks', 'split_tasks', 'embed_tasks', 'storage']
            for section in required_sections:
                if section not in data:
                    return jsonify({
                        'status': 'error',
                        'message': f'Missing required section: {section}'
                    }), 400
            
            # Write to configuration file
            config_path = config_manager.config_path
            with open(config_path, 'w') as f:
                import yaml
                yaml.dump(data, f, default_flow_style=False, indent=2)
            
            # Reload configuration
            config_manager.reload_config()
            
            return jsonify({
                'status': 'success',
                'message': 'Configuration updated and reloaded successfully'
            })
            
        except Exception as e:
            logger.error(f"Failed to update configuration: {e}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to update configuration: {str(e)}'
            }), 500
    
    @app.route('/config/validate', methods=['POST'])
    def validate_config():
        """Validate configuration without applying it"""
        try:
            data = request.get_json()
            if not data:
                return jsonify({
                    'status': 'error',
                    'message': 'No configuration data provided'
                }), 400
            
            # Basic validation
            validation_errors = []
            
            # Check required sections
            required_sections = ['file_types', 'extract_tasks', 'split_tasks', 'embed_tasks', 'storage']
            for section in required_sections:
                if section not in data:
                    validation_errors.append(f'Missing required section: {section}')
            
            # Validate file types
            if 'file_types' in data:
                file_types = data['file_types']
                if not isinstance(file_types, dict):
                    validation_errors.append('file_types must be a dictionary')
            
            # Validate extract tasks (only document types are supported)
            if 'extract_tasks' in data:
                extract_tasks = data['extract_tasks']
                # Only document_config is required since we only support document types
                if 'document_config' not in extract_tasks:
                    validation_errors.append('Missing extract task config: document_config')
            
            # Similar validation for split_tasks, embed_tasks, storage
            
            if validation_errors:
                return jsonify({
                    'status': 'error',
                    'message': 'Configuration validation failed',
                    'errors': validation_errors
                }), 400
            
            return jsonify({
                'status': 'success',
                'message': 'Configuration is valid'
            })
            
        except Exception as e:
            logger.error(f"Failed to validate configuration: {e}")
            return jsonify({
                'status': 'error',
                'message': f'Failed to validate configuration: {str(e)}'
            }), 500
    
    return app

# Create the Flask app instance
config_app = create_config_app()

if __name__ == '__main__':
    # Run the configuration API server
    config_app.run(host='0.0.0.0', port=5001, debug=True)
