#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Test script for the enhanced NeMo-Retriever task configuration system
"""

import os
import sys
import logging
import time
import requests
import json

# Add the nutanix_aidp_connector directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'nutanix_aidp_connector'))

from config.task_config_manager import config_manager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_config_manager():
    """Test the configuration manager"""
    print("=" * 80)
    print("TESTING CONFIGURATION MANAGER")
    print("=" * 80)
    
    # Test file type detection
    test_files = [
        "document.pdf",
        "image.jpg", 
        "audio.mp3",
        "video.mp4",
        "spreadsheet.xlsx",
        "archive.zip"
    ]
    
    print("\n1. Testing file type detection:")
    for filename in test_files:
        file_type = config_manager.get_file_type(filename)
        print(f"  {filename} -> {file_type}")
    
    # Test extract configurations
    print("\n2. Testing extract configurations:")
    for file_type in ["pdf", "jpg", "mp3", "mp4", "xlsx", "zip"]:
        config = config_manager.get_extract_config(file_type)
        print(f"  {file_type}: text={config.extract_text}, images={config.extract_images}, tables={config.extract_tables}")
    
    # Test split configurations
    print("\n3. Testing split configurations:")
    for file_type in ["pdf", "jpg", "mp3", "mp4", "xlsx", "zip"]:
        config = config_manager.get_split_config(file_type)
        print(f"  {file_type}: enabled={config.enabled}, strategy={config.strategy}, chunk_size={config.chunk_size}")
    
    # Test embed configurations
    print("\n4. Testing embed configurations:")
    for file_type in ["pdf", "jpg", "mp3", "mp4", "xlsx", "zip"]:
        config = config_manager.get_embed_config(file_type)
        print(f"  {file_type}: enabled={config.enabled}, model={config.model}, dimensions={config.dimensions}")
    
    # Test storage configuration
    print("\n5. Testing storage configuration:")
    storage_config = config_manager.get_storage_config()
    print(f"  Milvus: {storage_config.milvus_host}:{storage_config.milvus_port}")
    print(f"  MinIO: {storage_config.minio_endpoint}/{storage_config.minio_bucket}")

def test_config_api():
    """Test the configuration API"""
    print("\n" + "=" * 80)
    print("TESTING CONFIGURATION API")
    print("=" * 80)
    
    base_url = "http://localhost:5001"
    
    # Test health check
    try:
        response = requests.get(f"{base_url}/health", timeout=5)
        print(f"Health check: {response.status_code} - {response.json()}")
    except requests.exceptions.ConnectionError:
        print("Configuration API not running. Start the pipeline to test the API.")
        return
    except Exception as e:
        print(f"Error testing health check: {e}")
        return
    
    # Test configuration status
    try:
        response = requests.get(f"{base_url}/config/status", timeout=5)
        print(f"Config status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"  File types: {data['config']['file_types_count']}")
            print(f"  Extract configs: {data['config']['extract_configs_count']}")
            print(f"  Split configs: {data['config']['split_configs_count']}")
            print(f"  Embed configs: {data['config']['embed_configs_count']}")
    except Exception as e:
        print(f"Error testing config status: {e}")
    
    # Test file types endpoint
    try:
        response = requests.get(f"{base_url}/config/file-types", timeout=5)
        print(f"File types: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"  Supported types: {len(data['file_types'])}")
    except Exception as e:
        print(f"Error testing file types: {e}")
    
    # Test extract config for PDF
    try:
        response = requests.get(f"{base_url}/config/extract/pdf", timeout=5)
        print(f"PDF extract config: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            config = data['config']
            print(f"  Extract text: {config['extract_text']}")
            print(f"  Extract images: {config['extract_images']}")
            print(f"  Extract tables: {config['extract_tables']}")
            print(f"  OCR enabled: {config['ocr_enabled']}")
    except Exception as e:
        print(f"Error testing PDF extract config: {e}")
    
    # Test split config for PDF
    try:
        response = requests.get(f"{base_url}/config/split/pdf", timeout=5)
        print(f"PDF split config: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            config = data['config']
            print(f"  Enabled: {config['enabled']}")
            print(f"  Strategy: {config['strategy']}")
            print(f"  Chunk size: {config['chunk_size']}")
            print(f"  Chunk overlap: {config['chunk_overlap']}")
    except Exception as e:
        print(f"Error testing PDF split config: {e}")
    
    # Test embed config for PDF
    try:
        response = requests.get(f"{base_url}/config/embed/pdf", timeout=5)
        print(f"PDF embed config: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            config = data['config']
            print(f"  Enabled: {config['enabled']}")
            print(f"  Model: {config['model']}")
            print(f"  Dimensions: {config['dimensions']}")
            print(f"  Batch size: {config['batch_size']}")
    except Exception as e:
        print(f"Error testing PDF embed config: {e}")
    
    # Test storage config
    try:
        response = requests.get(f"{base_url}/config/storage", timeout=5)
        print(f"Storage config: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            config = data['config']
            print(f"  Milvus: {config['milvus']['host']}:{config['milvus']['port']}")
            print(f"  MinIO: {config['minio']['endpoint']}/{config['minio']['bucket']}")
    except Exception as e:
        print(f"Error testing storage config: {e}")

def test_hot_reload():
    """Test configuration hot-reload"""
    print("\n" + "=" * 80)
    print("TESTING HOT-RELOAD")
    print("=" * 80)
    
    # Test manual reload
    print("Testing manual configuration reload...")
    config_manager.reload_config()
    print("Configuration reloaded successfully")
    
    # Test API reload
    base_url = "http://localhost:5001"
    try:
        response = requests.post(f"{base_url}/config/reload", timeout=5)
        print(f"API reload: {response.status_code} - {response.json()}")
    except requests.exceptions.ConnectionError:
        print("Configuration API not running. Start the pipeline to test hot-reload.")
    except Exception as e:
        print(f"Error testing API reload: {e}")

if __name__ == "__main__":
    print("Enhanced NeMo-Retriever Task Configuration System Test")
    print("=" * 80)
    
    # Test configuration manager
    test_config_manager()
    
    # Test configuration API (if running)
    test_config_api()
    
    # Test hot-reload
    test_hot_reload()
    
    print("\n" + "=" * 80)
    print("TEST COMPLETED")
    print("=" * 80)
    print("\nTo test the full system:")
    print("1. Start the pipeline: python nutanix_aidp_connector/main.py")
    print("2. Run this test again to test the API endpoints")
    print("3. Modify nutanix_aidp_connector/config/config.yaml and test hot-reload")
    print("4. Use the API to update configurations on-the-fly")
