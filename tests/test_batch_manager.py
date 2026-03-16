#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Unit tests for batch window manager
"""

import pytest
import time
from pipeline.batch_manager.window_manager import BatchWindowManager, FileInfo


class TestBatchWindowManager:
    """Test cases for BatchWindowManager"""
    
    def test_initialization(self):
        """Test batch manager initialization"""
        manager = BatchWindowManager(
            max_files=10,
            max_bytes=1000,
            max_wait_seconds=30
        )
        
        assert manager.max_files == 10
        assert manager.max_bytes == 1000
        assert manager.max_wait_seconds == 30
        assert len(manager.current_batch) == 0
        assert manager.current_batch_size == 0
    
    def test_file_count_threshold(self):
        """Test batching based on file count"""
        manager = BatchWindowManager(
            max_files=3,
            max_bytes=10000,
            max_wait_seconds=300
        )
        
        # Add files below threshold
        file1 = FileInfo("id1", "bucket", "key1", "/path1", 100, 0, 0)
        batch = manager.add_file(file1)
        assert batch is None  # No flush yet
        
        file2 = FileInfo("id2", "bucket", "key2", "/path2", 100, 0, 1)
        batch = manager.add_file(file2)
        assert batch is None  # No flush yet
        
        # Add third file to trigger flush
        file3 = FileInfo("id3", "bucket", "key3", "/path3", 100, 0, 2)
        batch = manager.add_file(file3)
        assert batch is not None  # Should flush
        assert batch['file_count'] == 3
        assert batch['total_bytes'] == 300
    
    def test_size_threshold(self):
        """Test batching based on total size"""
        manager = BatchWindowManager(
            max_files=100,
            max_bytes=500,
            max_wait_seconds=300
        )
        
        # Add files below threshold
        file1 = FileInfo("id1", "bucket", "key1", "/path1", 200, 0, 0)
        batch = manager.add_file(file1)
        assert batch is None
        
        file2 = FileInfo("id2", "bucket", "key2", "/path2", 200, 0, 1)
        batch = manager.add_file(file2)
        assert batch is None
        
        # Add file that exceeds size threshold
        file3 = FileInfo("id3", "bucket", "key3", "/path3", 200, 0, 2)
        batch = manager.add_file(file3)
        assert batch is not None
        assert batch['file_count'] == 3
        assert batch['total_bytes'] == 600
    
    def test_time_threshold(self):
        """Test batching based on time"""
        manager = BatchWindowManager(
            max_files=100,
            max_bytes=10000,
            max_wait_seconds=1  # 1 second for fast test
        )
        
        # Add a file
        file1 = FileInfo("id1", "bucket", "key1", "/path1", 100, 0, 0)
        batch = manager.add_file(file1)
        assert batch is None
        
        # Wait for time threshold
        time.sleep(1.1)
        
        # Check time-based flush
        batch = manager.check_time_based_flush()
        assert batch is not None
        assert batch['file_count'] == 1
    
    def test_force_flush(self):
        """Test force flush functionality"""
        manager = BatchWindowManager(
            max_files=100,
            max_bytes=10000,
            max_wait_seconds=300
        )
        
        # Add files
        file1 = FileInfo("id1", "bucket", "key1", "/path1", 100, 0, 0)
        manager.add_file(file1)
        file2 = FileInfo("id2", "bucket", "key2", "/path2", 100, 0, 1)
        manager.add_file(file2)
        
        # Force flush
        batch = manager.force_flush()
        assert batch is not None
        assert batch['file_count'] == 2
        
        # Verify batch is empty after flush
        assert len(manager.current_batch) == 0
        assert manager.current_batch_size == 0
    
    def test_batch_stats(self):
        """Test batch statistics"""
        manager = BatchWindowManager(
            max_files=10,
            max_bytes=1000,
            max_wait_seconds=30
        )
        
        file1 = FileInfo("id1", "bucket", "key1", "/path1", 100, 0, 0)
        manager.add_file(file1)
        
        stats = manager.get_batch_stats()
        assert stats['file_count'] == 1
        assert stats['total_bytes'] == 100
        assert stats['files_remaining'] == 9
        assert stats['bytes_remaining'] == 900


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

