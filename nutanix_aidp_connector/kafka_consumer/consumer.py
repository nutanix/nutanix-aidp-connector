#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Kafka consumer with manual offset management using confluent-kafka
"""

import json
import logging
import queue
import time
import uuid
from typing import Dict, Optional, List
from threading import Thread, Event
from confluent_kafka import Consumer, KafkaError, KafkaException

logger = logging.getLogger(__name__)


class PipelineKafkaConsumer:
    """Kafka consumer for pipeline with manual offset management"""
    
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str,
                 auto_offset_reset: str = 'earliest',
                 message_queue: queue.Queue = None,
                 delete_queue: queue.Queue = None,
                 allowed_event_types: List[str] = None):
        """
        Initialize Kafka consumer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic to consume
            group_id: Consumer group ID
            auto_offset_reset: Auto offset reset strategy
            message_queue: Queue for passing messages to processing (for create events)
            delete_queue: Queue for passing delete events to delete handler
            allowed_event_types: List of allowed S3 event types to process
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.message_queue = message_queue or queue.Queue()
        self.delete_queue = delete_queue
        self.allowed_event_types = allowed_event_types or [
            "s3:ObjectCreated:Put",
            "s3:ObjectTagging:Put",
            "s3:ObjectRemoved:Delete",
            "s3:ObjectRemoved:DeleteMarkerCreated"
        ]
        
        self.consumer = None
        self.consumer_thread = None
        self.stop_event = Event()
        self.running = False
        
        logger.info(f"PipelineKafkaConsumer initialized: topic={topic}, "
                   f"bootstrap_servers={bootstrap_servers}, "
                   f"allowed_event_types={self.allowed_event_types}")
    
    def _create_consumer(self):
        """Create Kafka consumer with built-in offset management"""
        try:
            config = {
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': self.group_id,
                'auto.offset.reset': self.auto_offset_reset,
                'enable.auto.commit': True,  # Use Kafka's built-in offset management
                'auto.commit.interval.ms': 1000,  # Commit offsets every 1 second
                'session.timeout.ms': 30000,
                'heartbeat.interval.ms': 10000,
                'max.poll.interval.ms': 300000,
                'fetch.wait.max.ms': 500,
                'fetch.min.bytes': 1,
                'fetch.max.bytes': 52428800,  # 50MB
                'api.version.request': True,
                'api.version.fallback.ms': 0,
                'broker.version.fallback': '0.10.0.0',
                'log.connection.close': False,
                'socket.timeout.ms': 10000,
                'metadata.max.age.ms': 300000,
            }
            
            self.consumer = Consumer(config)
            self.consumer.subscribe([self.topic], on_assign=self._on_assign, on_revoke=self._on_revoke)
            
            logger.info(f"Kafka consumer created for topic '{self.topic}' with built-in offset management")
            return True
        
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            return False
    
    def _on_assign(self, consumer, partitions):
        """Callback when partitions are assigned"""
        logger.info(f"Partitions assigned: {[p.partition for p in partitions]}")
        if not partitions:
            logger.warning(
                "No partitions assigned to this consumer. If running multiple replicas, "
                "ensure the Kafka topic has at least as many partitions as the replica count."
            )
        for p in partitions:
            low, high = consumer.get_watermark_offsets(p)
            logger.info(f"Partition {p.partition}: low={low}, high={high}, messages={high-low}")
    
    def _on_revoke(self, consumer, partitions):
        """Callback when partitions are revoked"""
        logger.warning(f"Partitions revoked: {[p.partition for p in partitions]}")
    
    def parse_s3_notification(self, message_value: str) -> Optional[Dict]:
        """
        Parse S3 notification message
        
        Supports event types configured via allowed_event_types.
        Default: s3:ObjectCreated:Put, s3:ObjectTagging:Put
        Event types can be updated via hot-reload without restarting the consumer.
        
        Args:
            message_value: JSON message value
        
        Returns:
            Parsed notification dict or None if event type is not supported
        """
        try:
            notification = json.loads(message_value)
            
            # Extract S3 information from Records array
            records = notification.get('Records', [])
            if not records:
                logger.warning("No records in notification")
                return None
            
            record = records[0]
            
            # Extract event type from record (standard S3 notification format)
            # Try eventName first (standard S3 format), then EventType (custom format)
            event_type = record.get('eventName') or notification.get('EventType')
            
            if not event_type:
                logger.warning(f"No event type found in notification. Record keys: {list(record.keys())}")
                return None
            
            # Normalize: some S3-compatible stores send "ObjectRemoved:Delete" without "s3:" prefix
            if event_type in ("ObjectRemoved:Delete", "s3:ObjectRemoved:Delete"):
                event_type = "s3:ObjectRemoved:Delete"
            elif event_type in ("ObjectRemoved:DeleteMarkerCreated", "s3:ObjectRemoved:DeleteMarkerCreated"):
                event_type = "s3:ObjectRemoved:DeleteMarkerCreated"
            
            logger.debug(f"Parsed event type: {event_type} from notification")
            
            # Process only allowed event types (configurable via hot-reload)
            if event_type not in self.allowed_event_types:
                logger.debug(f"Skipping unsupported event: {event_type} "
                           f"(allowed: {self.allowed_event_types})")
                return None
            
            s3_info = record.get('s3', {})
            
            bucket_name = s3_info.get('bucket', {}).get('name')
            object_key = s3_info.get('object', {}).get('key')
            object_size = s3_info.get('object', {}).get('size', 0)
            # Extract version if available (S3 versioning)
            # Version can be in: s3.object.versionId or s3.objectVersionId
            object_version = s3_info.get('object', {}).get('versionId') or s3_info.get('objectVersionId')
            
            if not bucket_name or not object_key:
                logger.warning("Missing bucket or key in notification")
                return None
            
            return {
                'bucket': bucket_name,
                'key': object_key,
                'size': object_size,
                'version': object_version,  # May be None if versioning not enabled
                'event_type': event_type
            }
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing notification: {e}")
            return None
    
    def consume_loop(self):
        """Main consume loop with Kafka's built-in offset management"""
        logger.info("Kafka consume loop started")
        
        # Create consumer
        if not self._create_consumer():
            logger.error("Failed to create consumer, exiting")
            return
        
        # Wait for partition assignment (poll a few times to trigger assignment)
        logger.info("Waiting for partition assignment...")
        for i in range(10):  # Wait up to 10 seconds
            partitions = self.consumer.assignment()
            if partitions:
                logger.info(f"Partitions assigned: {[p.partition for p in partitions]}")
                for p in partitions:
                    low, high = self.consumer.get_watermark_offsets(p)
                    logger.info(f"Partition {p.partition}: low={low}, high={high}, messages={high-low}")
                break
            self.consumer.poll(timeout=1.0)  # Poll to trigger assignment
            time.sleep(0.5)
        
        if not self.consumer.assignment():
            logger.warning("No partitions assigned after waiting - consumer may not be receiving messages")
        
        message_count = 0
        
        try:
            # Start processing messages immediately - Kafka handles offset management
            while not self.stop_event.is_set():
                try:
                    # Poll for messages
                    msg = self.consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition event
                            logger.debug(f"End of partition reached for {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")
                            continue
                        elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                            # Topic/partition temporarily unavailable, retry
                            logger.warning(f"Topic/partition temporarily unavailable: {msg.error()}, retrying...")
                            time.sleep(1)  # Brief pause before retry
                            continue
                        elif msg.error().code() == KafkaError.NETWORK_EXCEPTION:
                            # Network issue, retry
                            logger.warning(f"Network error: {msg.error()}, retrying...")
                            time.sleep(1)  # Brief pause before retry
                            continue
                        else:
                            logger.error(f"Kafka error: {msg.error()}")
                            # Only break on critical errors, not temporary ones
                            if msg.error().code() in [KafkaError.AUTHENTICATION, KafkaError.AUTHORIZATION]:
                                logger.error("Authentication/authorization error, stopping consumer")
                                break
                            else:
                                logger.warning("Non-critical error, continuing...")
                                continue
                    
                    if self.stop_event.is_set():
                        break
                    
                    message_count += 1
                    
                    logger.debug(f"Received message #{message_count} "
                               f"(partition: {msg.partition()}, "
                               f"offset: {msg.offset()})")
                    
                    # Parse S3 notification
                    message_value = msg.value().decode('utf-8') if msg.value() else None
                    s3_info = self.parse_s3_notification(message_value)
                    
                    if s3_info:
                        # Process message based on event type
                        event_type = s3_info.get('event_type')
                        if event_type and event_type.startswith('s3:ObjectRemoved'):
                            # Route delete events to delete queue
                            self._process_s3_delete(s3_info, msg)
                        else:
                            # Route create/update events to message queue
                            self._process_s3_object(s3_info, msg)
                    else:
                        # Non-supported event or invalid notification - Kafka auto-commits offset
                        logger.debug(f"Skipping unsupported event at offset {msg.offset()}")
                
                except KafkaException as e:
                    logger.error(f"Kafka exception: {e}")
                    # Reconnection will happen automatically on next poll
                    break
                except Exception as e:
                    logger.error(f"Error in consume loop: {e}")
        
        finally:
            logger.info(f"Processed {message_count} messages")
            self._close_consumer()
    
    def _process_s3_object(self, s3_info: dict, msg=None):
        """
        Process S3 object creation/update event (single-pod deployment, no locking needed)
        
        Args:
            s3_info: S3 object information
            msg: Kafka message (optional, for fallback processing)
        """
        try:
            # Create file record
            file_id = str(uuid.uuid4())
            file_record = {
                'file_id': file_id,
                's3_bucket': s3_info['bucket'],
                's3_key': s3_info['key'],
                'file_size': s3_info['size'],
                's3_version': s3_info.get('version'),  # Include version if available
                'kafka_partition': s3_info.get('partition', 0),
                'kafka_offset': s3_info.get('offset', 0),
                'kafka_topic': s3_info.get('topic', self.topic),
                'enqueued_at': time.time(),
            }
            
            # Add to processing queue
            self.message_queue.put(file_record)
            
            logger.info(f"Queued file for download: "
                      f"{s3_info['key']} from {s3_info['bucket']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing S3 object {s3_info.get('key', 'unknown')}: {e}")
            return False
    
    def _process_s3_delete(self, s3_info: dict, msg=None):
        """
        Process S3 object delete event
        
        Args:
            s3_info: S3 object information
            msg: Kafka message (optional, for fallback processing)
        """
        try:
            if not self.delete_queue:
                logger.warning("Delete queue not configured, cannot process delete event")
                return False
            
            # Create delete record
            delete_record = {
                's3_bucket': s3_info['bucket'],
                's3_key': s3_info['key'],
                's3_version': s3_info.get('version'),  # Include version if available
                'event_type': s3_info.get('event_type'),
                'kafka_partition': s3_info.get('partition', 0),
                'kafka_offset': s3_info.get('offset', 0),
                'kafka_topic': s3_info.get('topic', self.topic)
            }
            
            # Add to delete queue
            self.delete_queue.put(delete_record)
            
            logger.info(f"Queued file for deletion from Milvus: "
                      f"s3://{s3_info['bucket']}/{s3_info['key']} "
                      f"(event: {s3_info.get('event_type')})")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing S3 delete event {s3_info.get('key', 'unknown')}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def commit_offset(self, partition: int, offset: int):
        """
        Manually commit a specific offset for a partition
        
        Args:
            partition: Kafka partition number
            offset: Offset to commit
        """
        if not self.consumer:
            logger.warning("Consumer not initialized, cannot commit offset")
            return False
        
        try:
            from confluent_kafka import TopicPartition
            
            # Create TopicPartition for the specific offset
            tp = TopicPartition(self.topic, partition, offset + 1)  # Commit next offset
            
            # Commit the offset
            self.consumer.commit(offsets=[tp], asynchronous=False)
            logger.debug(f"Committed offset {offset + 1} for partition {partition}")
            return True
        except Exception as e:
            logger.error(f"Failed to commit offset {offset} for partition {partition}: {e}")
            return False
    
    def _close_consumer(self):
        """Close Kafka consumer"""
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")
    
    def start(self):
        """Start consuming"""
        if self.running:
            logger.warning("Consumer already running")
            return
        
        self.running = True
        self.stop_event.clear()
        
        self.consumer_thread = Thread(target=self.consume_loop, daemon=True)
        self.consumer_thread.start()
        
        logger.info("Kafka consumer started")
    
    def stop(self, timeout: int = 30):
        """
        Stop consuming
        
        Args:
            timeout: Timeout for stopping in seconds
        """
        if not self.running:
            logger.warning("Consumer not running")
            return
        
        logger.info("Stopping Kafka consumer...")
        self.stop_event.set()
        
        if self.consumer_thread:
            self.consumer_thread.join(timeout=timeout)
        
        self.running = False
        logger.info("Kafka consumer stopped")
    
    def update_allowed_event_types(self, allowed_event_types: List[str]):
        """
        Update allowed event types for hot-reloading
        
        Args:
            allowed_event_types: New list of allowed S3 event types
        """
        old_types = self.allowed_event_types.copy()
        self.allowed_event_types = allowed_event_types
        logger.info(f"Updated allowed event types: {old_types} -> {allowed_event_types}")
    
    def reconnect(self, bootstrap_servers: str = None, topic: str = None, group_id: str = None):
        """
        Reconnect Kafka consumer with new configuration
        
        Args:
            bootstrap_servers: New bootstrap servers (optional)
            topic: New topic (optional)
            group_id: New group ID (optional)
        """
        logger.info("Reconnecting Kafka consumer with new configuration...")
        
        # Stop current consumer
        was_running = self.running
        if was_running:
            self.stop(timeout=5)
        
        # Update configuration
        if bootstrap_servers:
            self.bootstrap_servers = bootstrap_servers
        if topic:
            self.topic = topic
        if group_id:
            self.group_id = group_id
        
        # Close old consumer
        self._close_consumer()
        
        # Restart if it was running
        if was_running:
            self.start()
        
        logger.info(f"Kafka consumer reconnected: topic={self.topic}, bootstrap_servers={self.bootstrap_servers}")