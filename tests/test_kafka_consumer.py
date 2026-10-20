#!/usr/bin/env python
# Copyright (c) 2026 Nutanix Inc. All rights reserved.

"""Regression tests for Kafka coordinates in queued S3 events."""

import json
import queue
from unittest.mock import Mock

import pytest
from confluent_kafka import Message

from nutanix_aidp_connector.kafka_consumer.consumer import PipelineKafkaConsumer


@pytest.fixture
def pipeline(mock_kafka_config):
    return PipelineKafkaConsumer(
        **mock_kafka_config,
        message_queue=queue.Queue(),
        delete_queue=queue.Queue(),
    )


@pytest.fixture(params=[
    ("s3:ObjectCreated:Put", "_process_s3_object", "message_queue"),
    ("s3:ObjectTagging:Put", "_process_s3_object", "message_queue"),
    ("s3:ObjectRemoved:Delete", "_process_s3_delete", "delete_queue"),
    ("s3:ObjectRemoved:DeleteMarkerCreated", "_process_s3_delete", "delete_queue"),
], ids=["create", "tag", "delete", "delete-marker"])
def event_case(pipeline, request):
    event_type, handler_name, queue_name = request.param
    notification = {
        "Records": [{
            "eventName": event_type,
            "s3": {
                "bucket": {"name": "documents"},
                "object": {
                    "key": "report.pdf",
                    "size": 1024,
                    "versionId": "version-1",
                },
            },
        }],
    }
    s3_info = pipeline.parse_s3_notification(json.dumps(notification))
    assert s3_info is not None
    return s3_info, getattr(pipeline, handler_name), getattr(pipeline, queue_name)


@pytest.mark.parametrize("topic,partition,offset", [
    ("source-events", 3, 42),
    ("source-events", 0, 0),
    ("another-topic", 1, 7),
])
def test_queued_record_preserves_message_coordinates(event_case, topic, partition, offset):
    s3_info, process, records = event_case
    original_info = s3_info.copy()
    message = Mock(spec=Message)
    message.topic.return_value = topic
    message.partition.return_value = partition
    message.offset.return_value = offset

    assert process(s3_info, message) is True

    record = records.get_nowait()
    assert (record["kafka_topic"], record["kafka_partition"], record["kafka_offset"]) == (
        topic, partition, offset,
    )
    assert record["s3_bucket"] == "documents"
    assert record["s3_key"] == "report.pdf"
    assert record["s3_version"] == "version-1"
    assert s3_info == original_info
    assert records.empty()


def test_message_coordinates_override_fallback_values(event_case):
    s3_info, process, records = event_case
    s3_info.update(partition=9, offset=99, topic="fallback-events")
    original_info = s3_info.copy()
    message = Mock(spec=Message)
    message.topic.return_value = "source-events"
    message.partition.return_value = 0
    message.offset.return_value = 0

    assert process(s3_info, message) is True

    record = records.get_nowait()
    assert (record["kafka_topic"], record["kafka_partition"], record["kafka_offset"]) == (
        "source-events", 0, 0,
    )
    assert s3_info == original_info


@pytest.mark.parametrize("metadata,expected", [
    ({}, ("test-topic", 0, 0)),
    (
        {"topic": "fallback-events", "partition": 2, "offset": 15},
        ("fallback-events", 2, 15),
    ),
], ids=["defaults", "explicit"])
def test_processing_without_message_preserves_fallback(event_case, metadata, expected):
    s3_info, process, records = event_case
    s3_info.update(metadata)
    original_info = s3_info.copy()

    assert process(s3_info) is True

    record = records.get_nowait()
    assert (record["kafka_topic"], record["kafka_partition"], record["kafka_offset"]) == expected
    assert s3_info == original_info
