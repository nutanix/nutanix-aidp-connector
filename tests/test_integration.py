#!/usr/bin/env python
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

"""
Integration test script for Kafka Notification Reader with S3 Integration

This script demonstrates how to:
1. Send test messages to Kafka topic  
2. Verify the integrated service processes them correctly
"""

import json
import time
import argparse
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_producer(bootstrap_servers):
    """Create Kafka producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(','),
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        logger.info(f"Created Kafka producer for servers: {bootstrap_servers}")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None

def send_test_message(producer, topic, bucket_name, object_name, key=None):
    """Send a test notification message to Kafka"""
    message = {
        "bucket_name": bucket_name,
        "object_name": object_name
    }
    
    try:
        future = producer.send(topic, value=message, key=key)
        result = future.get(timeout=10)
        
        logger.info(f"Successfully sent message to topic '{topic}':")
        logger.info(f"  Message: {json.dumps(message, indent=2)}")
        logger.info(f"  Partition: {result.partition}, Offset: {result.offset}")
        return True
        
    except KafkaError as e:
        logger.error(f"Failed to send message to Kafka: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending message: {e}")
        return False

def main():
    """Main function to send test messages"""
    parser = argparse.ArgumentParser(description='Send test messages for Kafka S3 Integration')
    parser.add_argument('--bootstrap-servers', '-b',
                       default='localhost:9092',
                       help='Comma-separated list of Kafka bootstrap servers')
    parser.add_argument('--topic', '-t',
                       required=True,
                       help='Kafka topic to send test messages to')
    parser.add_argument('--bucket', '-B',
                       default='src-bucket-micron',
                       help='S3 bucket name for test messages')
    parser.add_argument('--object', '-o',
                       default='test-file.txt',
                       help='S3 object name for test messages')
    parser.add_argument('--count', '-c',
                       type=int,
                       default=1,
                       help='Number of test messages to send')
    parser.add_argument('--interval', '-i',
                       type=float,
                       default=1.0,
                       help='Interval between messages in seconds')
    
    args = parser.parse_args()
    
    logger.info(f"Starting integration test - sending {args.count} messages to topic '{args.topic}'")
    
    # Create producer
    producer = create_producer(args.bootstrap_servers)
    if not producer:
        logger.error("Failed to create Kafka producer")
        return False
    
    success_count = 0
    
    try:
        for i in range(args.count):
            # Create unique object name for multiple messages
            object_name = f"{args.object}" if args.count == 1 else f"{args.object}-{i+1}"
            key = f"test-key-{i+1}"
            
            logger.info(f"Sending message {i+1}/{args.count}")
            
            if send_test_message(producer, args.topic, args.bucket, object_name, key):
                success_count += 1
                
                # Wait between messages if sending multiple
                if i < args.count - 1 and args.interval > 0:
                    logger.info(f"Waiting {args.interval} seconds...")
                    time.sleep(args.interval)
            else:
                logger.error(f"Failed to send message {i+1}")
                
    finally:
        producer.close()
        
    logger.info(f"Integration test completed - {success_count}/{args.count} messages sent successfully")
    return success_count == args.count

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
