<!--
Copyright (c) 2025 Nutanix Inc. All rights reserved.

Author: veer.kumar@nutanix.com
-->

# Nutanix AIDP-Connector Architecture

Technical deep-dive for the Nutanix AIDP-Connector pipeline. For quick start and deployment see [README.md](README.md); for Helm values reference see [helm/nutanix-aidp-connector/README.md](helm/nutanix-aidp-connector/README.md).

## Table of Contents

- [System Overview](#system-overview)
- [Architecture Diagram](#architecture-diagram)
- [Processing Modes](#processing-modes)
- [Core Components](#core-components)
- [Flow Control & Throttling](#flow-control--throttling)
- [Threading Model](#threading-model)
- [NeMo-Retriever Task Pipeline](#nemo-retriever-task-pipeline)
- [State Management](#state-management)
- [Crash Recovery](#crash-recovery)
- [Error Handling](#error-handling)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Metrics & Observability](#metrics--observability)
- [Resource Configuration](#resource-configuration)
- [Performance Characteristics](#performance-characteristics)
- [Security](#security)
- [Troubleshooting](#troubleshooting)
- [Design Decisions](#design-decisions)

---

## System Overview

Nutanix AIDP-Connector is a Kubernetes-native, single-pod service that:

1. Consumes S3 event notifications from Kafka (real-time) **and** lists existing objects (baseline).
2. Downloads objects, batches them, and submits to NeMo-Retriever for GPU-accelerated extraction, splitting, and embedding.
3. Merges NeMo-Retriever parquet output, enriches with S3 tags/metadata, and bulk-inserts into Milvus.
4. Handles S3 delete events by removing corresponding Milvus records.

Key characteristics:
- In-memory state with Kafka-based recovery
- Multi-level flow control (download → submission → baseline)
- Hot-reloadable configuration (30 s detection)
- Prometheus metrics via OpenTelemetry

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Nutanix AIDP-Connector Pod                              │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                       Input Sources                               │  │
│  │  ┌─────────────────┐            ┌──────────────────────────────┐  │  │
│  │  │  Kafka Consumer │            │   Baseline Orchestrator      │  │  │
│  │  │  (Real-time)    │            │   (Existing Objects)         │  │  │
│  │  └────────┬────────┘            └──────────────┬───────────────┘  │  │
│  │           └──────────────┬─────────────────────┘                  │  │
│  │                          ▼                                        │  │
│  │                    Message Queue                                  │  │
│  │                          ▼                                        │  │
│  │              Download Workers (Throttled)                         │  │
│  │              Max = 3 × maxOutstandingTasks                        │  │
│  │                          ▼                                        │  │
│  │                    Batch Manager                                  │  │
│  │              (maxFiles / maxBytes / maxWait)                      │  │
│  │                          ▼                                        │  │
│  │              Submission Workers (Throttled)                       │  │
│  │              Active ≤ maxOutstandingTasks                         │  │
│  │                          ▼                                        │  │
│  │                    Status Tracker                                 │  │
│  │              (Poll NeMo-Retriever for results)                         │  │
│  │                          ▼                                        │  │
│  │              Completion Handler                                   │  │
│  │              (Merge parquet → Enrich → Milvus bulk insert)        │  │
│  │                          ▼                                        │  │
│  │              Milvus Batch Inserter                                │  │
│  │              (Amortise bulk insert across files)                   │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────┐    │
│  │  Config API    │  │  Prometheus    │  │  Baseline State Mgr    │    │
│  │  (Port 5001)   │  │  (Port 9090)   │  │  (PVC persistence)    │    │
│  └────────────────┘  └────────────────┘  └────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
         │                    │                    │                │
    ┌────┴────┐         ┌────┴────┐         ┌────┴────┐     ┌────┴────┐
    │  Kafka  │         │   S3    │         │NeMo-Retriever│     │ Milvus  │
    │ Server  │         │ Objects │         │  (GPU)  │     │VectorDB │
    └─────────┘         └─────────┘         └─────────┘     └─────────┘
```

---

## Processing Modes

### Real-time Processing (Kafka)

```
┌────────────┐    ┌─────────┐    ┌──────────┐    ┌─────────┐    ┌───────────┐
│ S3 Bucket  │───►│  Kafka  │───►│ Consumer │───►│ Message │───►│ Download  │
│ (Put/Tag)  │    │  Topic  │    │          │    │  Queue  │    │  Workers  │
└────────────┘    └─────────┘    └──────────┘    └─────────┘    └─────┬─────┘
                                                                      │
     ┌────────────────────────────────────────────────────────────────┘
     ▼
┌─────────┐    ┌─────────┐    ┌───────────┐    ┌──────────┐    ┌─────────┐
│  Batch  │───►│  Batch  │───►│Submission │───►│NeMo-Retriever │───►│ Status  │
│ Manager │    │  Queue  │    │  Workers  │    │  Server  │    │ Tracker │
└─────────┘    └─────────┘    └───────────┘    └──────────┘    └────┬────┘
                                                                     │
     ┌───────────────────────────────────────────────────────────────┘
     ▼
┌────────────┐    ┌────────────┐    ┌─────────┐
│ Completion │───►│   Milvus   │    │ Metrics │
│  Handler   │    │ Bulk Insert│    │ Export  │
└────────────┘    └────────────┘    └─────────┘
```

**Flow:**
1. S3 bucket operations generate events
2. Kafka receives S3 notifications
3. Consumer parses messages, routes creates to message queue and deletes to delete queue
4. Download workers fetch objects from S3
5. Batch manager aggregates files by count/size/time
6. Submission workers send to NeMo-Retriever
7. Status tracker polls for completion
8. Completion handler triggers Milvus insert

Kafka offsets are auto-committed every 1 s. On restart, uncommitted messages are reprocessed.

### Baseline Processing (Existing Objects)

```
┌────────────────┐    ┌──────────────────┐    ┌────────────────┐
│   S3 Bucket    │───►│     Baseline     │───►│   List Objects │
│  (Configured)  │    │   Orchestrator   │    │  (Paginated)   │
└────────────────┘    └──────────────────┘    └───────┬────────┘
                                                      │
                      ┌───────────────────────────────┘
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Synchronous Batch Pattern                        │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐   │
│  │ List Batch N │───►│Process Batch │───►│ Wait for Completion  │   │
│  │ (1000 objs)  │    │ (Download,   │    │ (All files in batch) │   │
│  └──────────────┘    │  Submit)     │    └──────────┬───────────┘   │
│                      └──────────────┘               │               │
│                            ▲                        │               │
│                            └────────────────────────┘               │
│                       Update State → Next Batch                     │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      State Persistence (PVC)                        │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  /var/lib/nutanix-aidp-connector/baseline/baseline_state_*.json               │   │
│  │  - next_key_marker (resume point)                            │   │
│  │  - total_objects_listed / processed                          │   │
│  │  - listing_complete / processing_complete                    │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

**Key Features:**
- **Synchronous Batch Pattern**: Lists a batch, processes it, waits for completion, then moves to next
- **State Persistence**: `next_key_marker` only updated after batch completion (PVC-backed)
- **Crash Recovery**: On restart, resumes from last committed marker
- **No Data Loss**: Jobs in-flight at crash time are re-listed and reprocessed
- **Kafka Pause**: Consumer is paused during baseline to avoid resource contention
- **force_rerun**: Per-bucket config to re-trigger baseline after completion

### Delete Handling

`s3:ObjectRemoved:Delete` and `DeleteMarkerCreated` events are routed to a separate delete queue. The delete handler performs batched Milvus record removal by `source_name`, processing deletes in batches of 50 files at a time.

---

## Core Components

| Component | Purpose |
|-----------|---------|
| **Kafka Consumer** | Consumes S3 events, routes creates to message queue and deletes to delete queue |
| **Baseline Orchestrator** | Paginated S3 listing with crash-safe state persistence on PVC |
| **Download Workers** | Parallel S3 downloads with retry and throttling |
| **Batch Manager** | Aggregates files by count / bytes / time window (hot-reloadable) |
| **Submission Workers** | Submits batches to NeMo-Retriever with flow control |
| **Status Tracker** | Polls NeMo-Retriever for job completion; batch-level tracking |
| **Completion Handler** | Downloads NeMo-Retriever parquets from S3, merges, enriches with tags/metadata, fixes V2 page numbers |
| **Milvus Batch Inserter** | Accumulates DataFrames across files, performs a single combined bulk insert |
| **Delete Handler** | Batched Milvus record deletion on S3 remove events |
| **Config API** | Flask REST API on port 5001 for health, config status, and reload |
| **Telemetry** | OpenTelemetry → Prometheus exporter on port 9090 |

---

## Flow Control & Throttling

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Flow Control System                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Configuration:                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  maxOutstandingTasks: 150   # Max concurrent NeMo-Retriever jobs     │   │
│  │  maxFiles (batch): 5        # Files per batch                   │   │
│  │  maxBytes: 2GB              # Max batch size                    │   │
│  │  maxWaitSeconds: 30         # Batch flush timeout               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Download Throttling:                                                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Max downloaded files = 3 × maxOutstandingTasks = 450 files     │   │
│  │  If pending downloads > limit: WAIT                             │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Submission Throttling:                                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  If active_jobs (by file count) >= maxOutstandingTasks: WAIT    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Baseline Throttling:                                                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Synchronous batch pattern: Process batch → Wait → Next batch   │   │
│  │  No new listings until current batch fully processed            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

All gates count **files**, not batches, for accurate throttling.

### Throttling Pseudocode

```python
# Download throttling
max_pending_downloads = 3 * config.max_outstanding_tasks
if status_tracker.get_pending_downloads() >= max_pending_downloads:
    wait_for_capacity()

# Submission throttling
if status_tracker.get_active_jobs_file_count() >= config.max_outstanding_tasks:
    wait_for_slots()

# Baseline throttling
for batch in list_objects():
    process_batch(batch)
    wait_for_batch_completion()  # Synchronous
    update_state_marker()        # Only after completion
```

---

## Threading Model

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Thread Architecture                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Main Thread                                                            │
│  └── Orchestrates startup, shutdown, signal handling, config reload     │
│                                                                         │
│  Kafka Consumer Thread (1)                                              │
│  └── Consumes messages, routes to message queue / delete queue          │
│                                                                         │
│  Download Worker Pool (configurable, default: 5)                        │
│  └── Parallel S3 downloads with retry logic                             │
│                                                                         │
│  Batch Manager Thread (1)                                               │
│  └── Aggregates files into batches by count/size/time                   │
│                                                                         │
│  Submission Worker Pool (configurable, default: 10)                     │
│  └── Submits batches to NeMo-Retriever                                       │
│                                                                         │
│  Status Tracker Thread (1)                                              │
│  └── Polls NeMo-Retriever for job status                                     │
│                                                                         │
│  Completion Worker Pool (configurable, default: 5)                      │
│  └── Processes completed jobs, triggers Milvus insert                   │
│                                                                         │
│  Delete Worker Thread (1)                                               │
│  └── Batched Milvus deletes from delete queue                           │
│                                                                         │
│  Baseline Orchestrator Thread (1, if enabled)                           │
│  └── Lists S3 objects, manages baseline state                           │
│                                                                         │
│  Config API Thread (1)                                                  │
│  └── Flask server for REST API (port 5001)                              │
│                                                                         │
│  Config Watcher Thread (1)                                              │
│  └── Monitors config files for hot-reload (30s interval)                │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Thread Summary

| Thread / Pool | Count | Responsibility |
|---------------|-------|----------------|
| Main | 1 | Startup, shutdown, signal handling, config reload loop |
| Kafka Consumer | 1 | Consume messages, route to queues |
| Download Workers | configurable | Parallel S3 downloads with retry |
| Batch Manager | 1 | Aggregate files into batches |
| Submission Workers | configurable | Submit batches to NeMo-Retriever |
| Status Tracker | 1 | Poll NeMo-Retriever, route to completion queue |
| Completion Workers | configurable | Process results, trigger Milvus insert |
| Delete Worker | 1 | Batched Milvus deletes |
| Baseline Orchestrator | 1 | S3 listing, state persistence (if enabled) |
| Config API | 1 | Flask server (port 5001) |
| Config Watcher | 1 | File-change detection for hot-reload |

### Thread Pools

```python
download_pool = ThreadPoolExecutor(max_workers=config.download_workers)
submission_pool = ThreadPoolExecutor(max_workers=config.submission_workers)
status_pool = ThreadPoolExecutor(max_workers=config.status_check_workers)
```

---

## NeMo-Retriever Task Pipeline

Each file submitted to NeMo-Retriever goes through a configurable task pipeline:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        NeMo-Retriever Task Pipeline                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌──────────┐ │
│  │ ExtractTask │───►│  SplitTask  │───►│  EmbedTask  │───►│  Store   │ │
│  │             │    │             │    │             │    │ EmbedTask│ │
│  └─────────────┘    └─────────────┘    └─────────────┘    └──────────┘ │
│        │                  │                  │                  │       │
│        ▼                  ▼                  ▼                  ▼       │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌──────────┐ │
│  │ Text/Tables │    │   Chunks    │    │ Embeddings  │    │ Parquet  │ │
│  │ Charts/Imgs │    │ (1024 tok)  │    │ (2048 dim)  │    │  Files   │ │
│  └─────────────┘    └─────────────┘    └─────────────┘    └──────────┘ │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Task Details

| Task | Purpose | Output |
|------|---------|--------|
| **ExtractTask** | Parse document content | Text, tables, charts, images |
| **SplitTask** | Chunk text for embedding | Text chunks (~1024 tokens) |
| **EmbedTask** | Generate vector embeddings | Embeddings (2048 dimensions) |
| **StoreEmbedTask** | Store in Objects S3 | Parquet files for Milvus bulk insert |

### Task Addition Flow

```
1. ExtractTask (always added first)
   ↓
2. SplitTask (if enabled in config)
   ↓
3. EmbedTask (if enabled in config)
   ↓
4. StoreEmbedTask (if Milvus enabled — most common)
   ↓
5. StoreTask (only if Objects enabled AND Milvus disabled)
```

### Extraction Parameters (configurable in config.yaml)

```yaml
extract_text: true
extract_images: false
extract_tables: false
extract_charts: false
extract_metadata: false
ocr_enabled: false
extract_tables_method: "yolox"
table_output_format: "pseudo_markdown"
```

PDF files are submitted with `pdf_split_page_count=1` so each page is a separate NeMo-Retriever subjob — a single bad page only affects that page.

### Completion Handler Processing

After NeMo-Retriever completes, the Completion Handler performs:

1. **Parquet Discovery**: Lists parquet files from the NeMo-Retriever output prefix (recursive, with stability check to avoid racing with NeMo-Retriever writes).
2. **Download & Merge**: Downloads all parquet files and concatenates into a single DataFrame.
3. **V2 Page Fix**: Fixes page numbers (1-indexed → 0-indexed) and strips `#page_N` suffixes from `source_name`/`source_id`.
4. **Metadata Enrichment**: Injects S3 tags (`s3_tag_*`), S3 metadata (`s3_meta_*`), and standard fields (`s3_bucket`, `s3_key`, `file_type`, `batch_id`, `ingestion_timestamp`).
5. **Combined Parquet Write**: Writes the enriched DataFrame to S3 as a combined parquet.
6. **Milvus Bulk Insert**: Triggers bulk insert (or queues for the Milvus Batch Inserter).
7. **Deferred Cleanup**: S3 intermediate files and baseline notification only cleaned up **after** successful Milvus insert.

---

## State Management

### In-Memory State

```python
active_downloads = {}           # Future -> FileInfo
active_batches = {}             # batch_id -> BatchInfo
active_jobs = {}                # job_id -> {trace_id, file_info, batch_id, ...}
completed_jobs = set()          # job_id:batch_id for deduplication
completed_batches = set()       # batch_id for batch completion deduplication
existing_files = set()          # local filenames for crash recovery
```

### File Naming Strategy

Files are named deterministically by S3 coordinates for idempotent crash recovery:

```python
def get_local_filename(kafka_partition, kafka_offset, s3_bucket, s3_key):
    safe_key = s3_key.replace('/', '_').replace(':', '_').replace(' ', '_')
    return f"{kafka_partition}_{kafka_offset}_{s3_bucket}_{safe_key}"
```

### Kafka Offset Management

- **Auto-Commit**: Enabled (`enable.auto.commit: True`)
- **Commit Interval**: 1 second (`auto.commit.interval.ms: 1000`)
- **Recovery**: Kafka automatically resumes from last committed offset
- **No Manual Tracking**: All offset management handled by Kafka

### Baseline State Persistence

State file stored on PVC at `/var/lib/nutanix-aidp-connector/baseline/baseline_state_*.json`:

```json
{
  "bucket_name": "aidp-docs",
  "next_key_marker": "documents/report-500.pdf",
  "next_version_id_marker": null,
  "total_objects_listed": 500,
  "total_objects_processed": 500,
  "listing_complete": false,
  "processing_complete": false,
  "last_updated": "2025-01-15T10:30:00Z"
}
```

---

## Crash Recovery

| Scenario | Mechanism |
|----------|-----------|
| Kafka processing | Auto-committed offsets; uncommitted messages replayed on restart |
| Baseline processing | `next_key_marker` persisted to PVC after each batch completes |
| In-flight NeMo-Retriever jobs | Orphaned jobs timeout; files re-submitted on restart |
| Milvus batch insert | Deferred callbacks — S3 cleanup and baseline notification only fire **after** successful insert |
| Downloaded files | Deterministic naming; duplicate downloads overwrite safely |

**Recovery Process:**
1. On restart, scan local download directory for existing files
2. Kafka resumes from last committed offset (auto-commit)
3. Skip files that already exist locally (deterministic naming)
4. Baseline resumes from last persisted `next_key_marker`
5. Process remaining files normally

---

## Error Handling

### Retry Logic

| Operation | Attempts | Backoff |
|-----------|----------|---------|
| S3 Downloads | 3 | Exponential (1 s, 2 s, 4 s) |
| NeMo-Retriever Jobs | 3 | Exponential (1 s, 2 s, 4 s) |
| S3 Operations | 3 | Exponential (1 s, 2 s, 4 s) |

### Circuit Breaker (NeMo-Retriever)

- **Threshold**: 5 consecutive failures → circuit opens
- **Timeout**: 60 seconds before half-open retry
- **States**: Closed → Open → Half-Open → Closed
- **Recovery**: Automatic after timeout

### Dead Letter Queue

- **Location**: `/var/lib/nutanix-aidp-connector/dead_letter/`
- **Format**: JSON with error details, file_id, timestamp
- **Purpose**: Store permanently failed messages for manual investigation

---

## Configuration

### File Layout

| File | Hot-reload | Purpose |
|------|-----------|---------|
| `config.yaml` (mounted from ConfigMap) | Yes (30 s) | Kafka, S3, threading, batching, NeMo-Retriever, storage, baseline, telemetry |
| `values.yaml` (Helm) | No (needs upgrade) | All the above + image, resources, probes, PVC sizes |

### Key Sections

```yaml
kafka:
  bootstrapServers: ""
  topic: "OSSEvents"
  groupId: "nutanix-aidp-connector-group"
  autoOffsetReset: "latest"   # "earliest" to replay history; "latest" for new messages only
  allowedEventTypes:
    - "s3:ObjectCreated:Put"
    - "s3:ObjectTagging:Put"
    - "s3:ObjectCreated:CompleteMultipartUpload"
    - "s3:ObjectRemoved:Delete"
    - "s3:ObjectRemoved:DeleteMarkerCreated"

batching:
  maxFiles: 5
  maxBytes: 2000000000      # 2 GB
  maxWaitSeconds: 30

processing:
  maxOutstandingTasks: 150

threading:
  downloadWorkers: 5
  submissionWorkers: 10
  statusCheckWorkers: 10
  completionWorkers: 5

storage:
  milvus:
    enabled: true
    host: "milvus-vectordb.milvus"
    port: 19530
    collectionName: "docs_collection"
    bucket: "milvus-store"
  objects:
    enabled: true
    endpointUrl: ""
    bucket: "nutanix-aidp-connector-staging"

baseline:
  enabled: true
  stateDir: "/var/lib/nutanix-aidp-connector/baseline/"
  buckets:
    - bucketName: "fd-source"
      enabled: true
      forceRerun: false
```

### Hot-Reload Behaviour

- File modification time is checked every 30 s.
- Only changed sections trigger updates.
- **Connection-related changes** (Kafka `bootstrap_servers`/`topic`/`group_id`, S3 endpoint/credentials, Milvus host/port) recreate the relevant client.
- **Non-connection changes** (batching thresholds, event types, thread counts) apply immediately without reconnection.
- Manual trigger: `POST /config/reload`.

### S3 Tags & Metadata Enrichment

During download, Nutanix AIDP-Connector fetches S3 object tags (`get_object_tagging`) and user-defined metadata headers (`head_object`). Before Milvus insert these are injected into `content_metadata`:

| Prefix | Source | Example |
|--------|--------|---------|
| `s3_tag_` | S3 object tags | `s3_tag_project`, `s3_tag_environment` |
| `s3_meta_` | `x-amz-meta-*` headers | `s3_meta_author`, `s3_meta_category` |

Milvus collections use `enable_dynamic_field=True` so new tag keys are automatically queryable:

```python
results = collection.search(
    data=[query_vector], anns_field="vector",
    param={"metric_type": "L2", "params": {"nprobe": 10}},
    limit=10,
    expr='content_metadata["s3_tag_project"] == "engineering"'
)
```

---

## API Reference

Base URL: `http://nutanix-aidp-connector:5001` (in-cluster) or `http://localhost:5001` after `kubectl port-forward`.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Liveness check |
| GET | `/metrics/status` | Telemetry status |
| GET | `/config/status` | Config overview (file types, batching, storage) |
| GET | `/config/file-types` | Supported file type mappings |
| GET | `/config/extract/{type}` | Extract config for a file type |
| GET | `/config/split/{type}` | Split config for a file type |
| GET | `/config/embed/{type}` | Embed config for a file type |
| GET | `/config/batching` | Batching thresholds |
| GET | `/config/storage` | Milvus + Objects config |
| POST | `/config/reload` | Trigger config reload |
| POST | `/config/update` | Update config (JSON body) |
| POST | `/config/validate` | Validate config without applying |

All responses include `Cache-Control: private, no-store` and `Vary: Cookie` headers.

---

## Metrics & Observability

Prometheus metrics are exported on port 9090 at `/metrics`.

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `nutanix_aidp_connector_pipeline_stage_duration_seconds` | Histogram | Per-stage latency (labels: `stage`) |
| `nutanix_aidp_connector_job_duration_seconds` | Histogram | End-to-end job duration |
| `nutanix_aidp_connector_jobs_submitted_total` | Counter | Jobs submitted to NeMo-Retriever |
| `nutanix_aidp_connector_jobs_completed_total` | Counter | Jobs completed successfully |
| `nutanix_aidp_connector_jobs_failed_total` | Counter | Jobs that failed |
| `nutanix_aidp_connector_files_processed_total` | Counter | Files processed |
| `nutanix_aidp_connector_active_jobs` | Gauge | Current in-flight NeMo-Retriever jobs |
| `nutanix_aidp_connector_job_status_codes_total` | Counter | Status checks by HTTP code + error type |
| `nutanix_aidp_connector_submission_throttled` | Counter | Submission throttle events |
| `nutanix_aidp_connector_download_throttled` | Counter | Download throttle events |

### Pipeline Stages (histogram labels)

`submission`, `nemo_retriever`, `completion_handle`, `milvus_batch_insert`

### Example PromQL

```promql
# Per-stage p95 latency
histogram_quantile(0.95, rate(nutanix_aidp_connector_pipeline_stage_duration_seconds_bucket[5m]))

# Throughput (files/min)
rate(nutanix_aidp_connector_files_processed_total[5m]) * 60

# NeMo-Retriever error rate
sum(rate(nutanix_aidp_connector_job_status_codes_total{status_code!="200",status_code!="202"}[5m]))
  / sum(rate(nutanix_aidp_connector_job_status_codes_total[5m]))
```

### ServiceMonitor

The Helm chart creates a `ServiceMonitor` in the configured `serviceMonitorNamespace` (default `aidp`). The name includes `Release.Namespace` so multiple Nutanix AIDP-Connector releases can coexist.

---

## Troubleshooting

### Pod not starting

- Check credentials: `kubectl describe pod -l app=nutanix-aidp-connector`
- Verify external service connectivity (Kafka, S3, NeMo-Retriever, Milvus)
- Review logs: `kubectl logs deployment/nutanix-aidp-connector -n <ns>`

### Config not reloading

- Verify ConfigMap is updated: `kubectl get cm nutanix-aidp-connector-config -o yaml`
- Manual trigger: `curl -X POST http://localhost:5001/config/reload`
- Check logs for `config.*reload` messages

### Files not processing

- Confirm Kafka topic is receiving events: check consumer lag
- Verify S3 credentials: attempt a manual `aws s3 ls`
- Check flow control: `nutanix_aidp_connector_active_jobs` gauge may be at `maxOutstandingTasks`

### NeMo-Retriever jobs stuck

- Check NeMo-Retriever Redis task queue: `redis-cli LLEN morpheus_task_queue`
- Inspect job state: `redis-cli GET job_state:<trace-id-uuid>`
- If jobs are stuck in SUBMITTED, restart NeMo-Retriever pods and clear Redis state

### Milvus insert failures

- Verify Milvus connectivity: `kubectl exec ... -- python -c "from pymilvus import connections; connections.connect(host='...', port=19530)"`
- Check that the `milvus-store` bucket exists and is accessible by both Milvus and Nutanix AIDP-Connector
- Review completion handler logs for parquet merge / upload errors

### Useful kubectl commands

```bash
kubectl logs -f deployment/nutanix-aidp-connector -n <ns>
kubectl describe pod -l app=nutanix-aidp-connector -n <ns>
kubectl get events --sort-by=.metadata.creationTimestamp -n <ns>
kubectl top pods -l app=nutanix-aidp-connector -n <ns>
kubectl port-forward svc/nutanix-aidp-connector 5001:5001 -n <ns>
```

---

## Resource Configuration

### Pod Resources (defaults in values.yaml)

| Resource | Request | Limit |
|----------|---------|-------|
| CPU | 1000m | 2000m |
| Memory | 2Gi | 8Gi |

### Storage

| Volume | Type | Size | Purpose |
|--------|------|------|---------|
| Logs | PVC | 5Gi | Application logs |
| Baseline state | PVC | 1Gi | Baseline state persistence (crash recovery) |
| Downloads | emptyDir | — | Temporary S3 downloads (cleared on pod restart) |
| Completed metadata | Local | — | `/var/lib/nutanix-aidp-connector/completed/` |
| Dead letter | Local | — | `/var/lib/nutanix-aidp-connector/dead_letter/` |

---

## Performance Characteristics

> For a comprehensive performance tuning guide with per-stage latency monitoring, PromQL queries, bottleneck diagnosis, and tuning recommendations, see [docs/PERFORMANCE_TUNING.md](docs/PERFORMANCE_TUNING.md).

### Throughput (observed, 767-file run)

| Metric | Value |
|--------|-------|
| Wall-clock (767 docs) | ~53 min |
| Throughput | ~14.4 docs/min |
| Average per-document | ~4.2 s (with parallelism) |

### Per-Stage Latency (observed)

| Stage | Mean | P95 |
|-------|------|-----|
| `nemo_retriever` (batch) | ~260 s | ~459 s |
| `completion_handle` | ~33 s | ~75 s |
| `submission` | ~4 s | ~6 s |

NeMo-Retriever dominates (~80 % of wall-clock). The main tuning knobs ranked by impact:

1. `batching.maxFiles` — more files per NeMo-Retriever batch → fewer API calls
2. `processing.maxOutstandingTasks` — more concurrency to NeMo-Retriever
3. `threading.downloadWorkers` — faster S3 prefetch to keep pipeline fed

### Optimisations

- **Parallel processing**: Configurable worker pools for download, submission, completion
- **Milvus batch inserter**: Amortises bulk-insert overhead (767 files → ~28 Milvus API calls)
- **Connection pooling**: S3, NeMo-Retriever, and Milvus clients reused across operations
- **Unique S3 prefixes**: Each file gets a UUID-scoped NeMo-Retriever output path, preventing parquet collisions

---

## Security

### Authentication

| Service | Method |
|---------|--------|
| S3 (object reads) | AWS access key / secret key (Kubernetes Secret) |
| Objects (Milvus storage) | Separate access key / secret key (Kubernetes Secret) |
| Kafka | SASL/SSL (if configured) |
| Milvus | Credentials in config (if enabled) |
| NeMo-Retriever | Unauthenticated (cluster-internal) |

### Credential Handling

- Credentials are **never** stored in ConfigMap; they are injected via Kubernetes Secrets as environment variables.
- The `s3:` section (for reading objects after Kafka notifications) uses separate credentials from `storage.objects:` (for Milvus/NeMo-Retriever intermediate storage).

### Network

- All inter-service communication is pod-to-pod within the cluster.
- Config API (port 5001) and Prometheus (port 9090) should be restricted via NetworkPolicy in production.
- TLS recommended for external-facing endpoints.

---

## Design Decisions

| Decision | Rationale | Trade-off |
|----------|-----------|-----------|
| Single-pod deployment | No distributed locking; simpler state management | No horizontal scaling (sufficient for current throughput) |
| In-memory job tracking | Fast; minimal external dependencies | State lost on crash, but Kafka replays uncommitted work |
| Kafka auto-offset commit | Reliable, low-overhead offset management | No per-message ack, but 1 s commit interval is acceptable |
| Milvus batch inserter | Amortises bulk-insert overhead across files (767 files → ~28 API calls) | Small delay before vectors are queryable |
| Deferred callbacks | S3 cleanup and baseline notification only after Milvus insert succeeds | Slight complexity; prevents data loss on crash |
| V2 page-number fix in Nutanix AIDP-Connector | NeMo-Retriever V2 persists 1-indexed pages to S3 parquet; normalisation happens too late for Nutanix AIDP-Connector | Extra post-processing step per file |
| `autoOffsetReset` configurable | New consumer groups can choose to skip topic history (`latest`) or replay (`earliest`) | Must be set correctly at first deploy |
| ServiceMonitor per namespace | Multiple Nutanix AIDP-Connector releases coexist in the same Prometheus-watched namespace | Slightly longer resource name |
