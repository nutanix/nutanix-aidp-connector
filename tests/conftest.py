#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Pytest configuration and fixtures
"""

import pytest
import sys
import os

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


@pytest.fixture(scope="session")
def mock_s3_credentials():
    """Mock S3 credentials for testing"""
    return {
        'access_key': 'test-access-key',
        'secret_key': 'test-secret-key',
        'endpoint_url': 'http://localhost:4566'
    }


@pytest.fixture(scope="session")
def mock_kafka_config():
    """Mock Kafka configuration for testing"""
    return {
        'bootstrap_servers': 'localhost:9092',
        'topic': 'test-topic',
        'group_id': 'test-group'
    }

