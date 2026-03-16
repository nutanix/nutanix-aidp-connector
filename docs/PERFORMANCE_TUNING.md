<!--
Copyright (c) 2025 Nutanix Inc. All rights reserved.

Author: veer.kumar@nutanix.com
-->

# Nutanix AIDP-Connector Performance Tuning

Guide for monitoring pipeline latency, diagnosing bottlenecks, and tuning each stage.

For architecture details see [ARCHITECTURE.md](../ARCHITECTURE.md); for Helm values reference see [helm/nutanix-aidp-connector/README.md](../helm/nutanix-aidp-connector/README.md).

---

## Table of Contents

- [Pipeline Stages](#pipeline-stages)
- [Viewing Pipeline Latency](#viewing-pipeline-latency)
  - [Structured Log Lines](#structured-log-lines)
  - [Prometheus Metrics](#prometheus-metrics)
  - [Useful PromQL Queries](#useful-promql-queries)
  - [Periodic Statistics Log](#periodic-statistics-log)
- [Stage-by-Stage Breakdown](#stage-by-stage-breakdown)
  - [1. message_queue](#1-message_queue)
  - [2. download](#2-download)
  - [3. batch_queue](#3-batch_queue)
  - [4. submission](#4-submission)
  - [5. nemo_retriever](#5-nemo_retriever)
  - [6. completion_handle (sub-stages)](#6-completion_handle-sub-stages)
  - [7. milvus_batch_insert (sub-stages)](#7-milvus_batch_insert-sub-stages)
- [Tuning Knobs Ranked by Impact](#tuning-knobs-ranked-by-impact)
- [Common Bottleneck Patterns](#common-bottleneck-patterns)
- [NeMo-Retriever Overload Prevention](#nemo-retriever-overload-prevention)
- [Observed Baselines](#observed-baselines)

---

## Pipeline Stages

Every document flows through these stages in order:

```
Kafka ──► message_queue ──► download ──► batch_queue ──► submission
              │                                              │
              │  (S3 download)                               │  (NeMo-Retriever REST API)
              ▼                                              ▼
         batch_queue                                   nemo_retriever
                                                             │
                                                             │  (GPU: extract, split, embed, store)
                                                             ▼
                                                     completion_handle
                                                             │
                                                             │  (parquet merge, metadata enrich)
                                                             ▼
                                                     milvus_batch_insert
                                                             │
                                                             │  (bulk insert to Milvus)
                                                             ▼
                                                          Done
```

---

## Viewing Pipeline Latency

### Structured Log Lines

Every stage emits a `PIPELINE_LATENCY` log line with the stage name, file/batch ID, and duration:

```json
{
  "message": "PIPELINE_LATENCY",
  "context": {
    "stage": "nemo_retriever",
    "file_id": "2b561f24-ba28-464f-83ca-d32ce9eb769b",
    "duration_sec": 42.7
  }
}
```

To extract latencies from pod logs:

```bash
# All latency lines
kubectl logs nutanix-aidp-connector-0 -n <ns> | grep PIPELINE_LATENCY

# Filter by stage
kubectl logs nutanix-aidp-connector-0 -n <ns> | grep PIPELINE_LATENCY | grep '"stage": "nemo_retriever"'
```

### Prometheus Metrics

All stage durations are recorded in the histogram `nutanix_aidp_connector_pipeline_stage_duration_seconds` with a `stage` label.

| Metric | Type | Description |
|--------|------|-------------|
| `nutanix_aidp_connector_pipeline_stage_duration_seconds` | Histogram | Per-stage latency (label: `stage`) |
| `nutanix_aidp_connector_job_duration_seconds` | Histogram | End-to-end job duration |
| `nutanix_aidp_connector_jobs_submitted_total` | Counter | Total jobs submitted |
| `nutanix_aidp_connector_jobs_completed_total` | Counter | Total jobs completed |
| `nutanix_aidp_connector_jobs_failed_total` | Counter | Total jobs failed |
| `nutanix_aidp_connector_active_jobs` | Gauge | Currently in-flight jobs |
| `nutanix_aidp_connector_submission_throttled` | Counter | Download-worker throttle events |
| `nutanix_aidp_connector_download_throttled` | Counter | Submission-worker throttle events |

### Useful PromQL Queries

```promql
# Per-stage P95 latency (last 5 min)
histogram_quantile(0.95,
  sum by (le, stage)(rate(nutanix_aidp_connector_pipeline_stage_duration_seconds_bucket[5m]))
)

# Per-stage average latency
sum by (stage)(rate(nutanix_aidp_connector_pipeline_stage_duration_seconds_sum[5m]))
  / sum by (stage)(rate(nutanix_aidp_connector_pipeline_stage_duration_seconds_count[5m]))

# Throughput: completed files per minute
rate(nutanix_aidp_connector_files_processed_total[5m]) * 60

# NeMo-Retriever error rate
sum(rate(nutanix_aidp_connector_job_status_codes_total{status_code!="200",status_code!="202"}[5m]))
  / sum(rate(nutanix_aidp_connector_job_status_codes_total[5m]))

# Milvus bulk-insert average time
sum(rate(nutanix_aidp_connector_pipeline_stage_duration_seconds_sum{stage="milvus_batch_total"}[5m]))
  / sum(rate(nutanix_aidp_connector_pipeline_stage_duration_seconds_count{stage="milvus_batch_total"}[5m]))
```

### Periodic Statistics Log

Every 10 seconds, Nutanix AIDP-Connector logs `Pipeline statistics` with live queue depths and job counts:

```json
{
  "message": "Pipeline statistics",
  "context": {
    "message_queue": 0,
    "batch_queue": 5,
    "completion_queue": 0,
    "tracker_stats": {
      "active_jobs": 12,
      "jobs_by_status": { "pending": 10, "processing": 2, "retry_pending": 0 }
    }
  }
}
```

Key fields to watch:

| Field | Healthy | Concern |
|-------|---------|---------|
| `batch_queue` | 0–10 | > 30 means NeMo-Retriever can't keep up |
| `active_jobs` | 1–50 | > 100 risks overloading NeMo-Retriever |
| `pending` | Low | All jobs stuck in `pending` means NeMo-Retriever API is unresponsive |
| `processing` | > 0 | Constant 0 while `pending` > 0 indicates NeMo-Retriever is stalled |

---

## Stage-by-Stage Breakdown

### 1. message_queue

**What it measures**: Time a Kafka message sits in the in-memory queue before the download worker picks it up.

**Typical latency**: < 1 second

**If slow** (queue building up):
- Download workers are saturated — increase `threading.downloadWorkers`
- Flow control is throttling downloads — check `max_outstanding_tasks` or reduce submission rate

| Tuning Parameter | Helm Key | Default | Effect |
|---|---|---|---|
| Download workers | `threading.downloadWorkers` | 5 | More workers drain the queue faster |

### 2. download

**What it measures**: Time to download a single file from S3.

**Typical latency**: 0.01–2 seconds (depends on file size and S3 latency)

**If slow**:
- Network latency to S3 endpoint
- Large file sizes
- S3 throttling (rate limiting)

| Tuning Parameter | Helm Key | Default | Effect |
|---|---|---|---|
| Download workers | `threading.downloadWorkers` | 5 | Parallel S3 downloads |
| S3 endpoint | `s3.endpointUrl` | — | Use a closer/faster endpoint |

### 3. batch_queue

**What it measures**: Time a downloaded file waits in the batch window before being grouped into a batch.

**Typical latency**: 0–30 seconds (bounded by `maxWaitSeconds`)

**If slow** (always hitting max wait):
- Not enough files arriving to fill a batch — reduce `batching.maxFiles` or `batching.maxWaitSeconds`
- Reduce `maxWaitSeconds` if you prefer lower latency over batch efficiency

| Tuning Parameter | Helm Key | Default | Effect |
|---|---|---|---|
| Max files per batch | `batching.maxFiles` | 5 | Smaller = faster flush, larger = more efficient NeMo-Retriever calls |
| Max wait | `batching.maxWaitSeconds` | 30 | Upper bound on queue wait |
| Max bytes | `batching.maxBytes` | 2 GB | Byte-based batch limit |

### 4. submission

**What it measures**: Time to construct NeMo-Retriever task specs and submit the batch via REST API.

**Typical latency**: 1–6 seconds

**If slow**:
- NeMo-Retriever REST endpoint is slow to accept jobs (overloaded)
- Network latency between Nutanix AIDP-Connector pod and NeMo-Retriever service
- Too many outstanding tasks — submission worker waits for `max_outstanding_tasks` backpressure

| Tuning Parameter | Helm Key | Default | Effect |
|---|---|---|---|
| Submission workers | `threading.submissionWorkers` | 10 | Parallel submission threads |
| Max outstanding tasks | `processing.maxOutstandingTasks` | 150 | Backpressure limit before submission blocks |

### 5. nemo_retriever

**What it measures**: End-to-end time from job submission to NeMo-Retriever returning results. This includes GPU extraction, splitting, embedding, and embedding storage (parquet upload to S3).

**Typical latency**: 30–460+ seconds per batch (dominates ~80% of wall-clock)

**If slow**:
- PDF page count (more pages = longer processing with `pdfSplitPageCount=1`)
- NeMo-Retriever GPU saturation
- Embedding storage bottleneck (page-by-page parquet S3 upload inside NeMo-Retriever)
- Too many concurrent jobs overwhelming NeMo-Retriever

| Tuning Parameter | Helm Key | Default | Effect |
|---|---|---|---|
| Batch size | `batching.maxFiles` | 5 | Larger batches = fewer API round-trips |
| Max outstanding | `processing.maxOutstandingTasks` | 150 | Controls concurrency to NeMo-Retriever |
| PDF split | `nemoRetriever.pdfSplitPageCount` | 1 | Higher = fewer sub-jobs but one bad page fails more pages |
| Status poll interval | `processing.statusPollInterval` | 5 | How often Nutanix AIDP-Connector polls for completed jobs |
| Status check workers | `threading.statusCheckWorkers` | 10 | Parallel status polling threads |

> **Warning**: Submitting more than ~100 concurrent jobs to a single NeMo-Retriever instance
> can cause it to become unresponsive (V2 API timeouts). See
> [NeMo-Retriever Overload Prevention](#nemo-retriever-overload-prevention).

### 6. completion_handle (sub-stages)

**What it measures**: Processing a completed NeMo-Retriever result — downloading parquets, merging, enriching with metadata, and preparing for Milvus.

Sub-stages tracked in `PIPELINE_LATENCY stage=completion_prepare`:

| Sub-stage | Description |
|---|---|
| `parquet_discovery_sec` | Listing parquet files from S3 (Objects bucket) |
| `download_parquet_sec` | Downloading parquet files from S3 |
| `merge_enrich_sec` | Merging DataFrames, enriching with S3 tags, page-number fix |
| `upload_milvus_sec` | Uploading combined parquet to Milvus S3 bucket |
| `cleanup_sec` | Removing intermediate parquet files |

**Typical latency**: 5–75 seconds total

**If slow**:
- Many parquet files per job (large PDFs with `pdfSplitPageCount=1` produce one parquet per page)
- S3 latency for downloading/uploading parquets
- Large DataFrame merge operations

| Tuning Parameter | Helm Key | Default | Effect |
|---|---|---|---|
| Completion workers | `threading.completionWorkers` | 5 | Parallel completion processing |
| Cleanup parquets | `storage.milvus.cleanupParquetFiles` | true | Disable to skip cleanup (saves time, uses more storage) |

### 7. milvus_batch_insert (sub-stages)

**What it measures**: Batched Milvus bulk insert — accumulates multiple completed files before issuing a single `do_bulk_insert`.

Sub-stages tracked in `PIPELINE_LATENCY stage=milvus_batch_insert`:

| Sub-stage | Description |
|---|---|
| `milvus_batch_prepare` | Preparing file list for bulk insert |
| `milvus_batch_bulk_insert` | Calling Milvus `do_bulk_insert` API |
| `milvus_batch_wait` | Waiting for Milvus to complete the import |
| `milvus_batch_total` | Total batch insert time |

**Typical latency**: 10–40 seconds per batch

**If slow**:
- Milvus cluster under heavy load
- Large number of rows per batch
- Index building after insert (GPU_CAGRA rebuilds)

| Tuning Parameter | Helm Key | Default | Effect |
|---|---|---|---|
| Batch insert max files | `storage.milvus.batchInsertMaxFiles` | 10 | More files = fewer Milvus API calls |
| Batch insert max wait | `storage.milvus.batchInsertMaxWaitSeconds` | 15 | Max wait before flushing partial batch |
| Max bulk insert files | `storage.milvus.maxBulkInsertFiles` | 128 | Max parquets per single `do_bulk_insert` |
| Index type | `storage.milvus.indexType` | GPU_CAGRA | GPU_CAGRA is fast; HNSW for CPU-only |
| Consistency level | `storage.milvus.consistencyLevel` | Bounded | Strong = slower inserts, Bounded = faster |

---

## Tuning Knobs Ranked by Impact

| Rank | Parameter | Helm Key | Default | Why |
|------|-----------|----------|---------|-----|
| 1 | Batch size | `batching.maxFiles` | 5 | Directly controls NeMo-Retriever API call count. Larger = fewer calls, better GPU utilization |
| 2 | Max outstanding tasks | `processing.maxOutstandingTasks` | 150 | Controls concurrency. Too high overloads NeMo-Retriever; too low leaves GPU idle |
| 3 | Download workers | `threading.downloadWorkers` | 5 | Keeps the pipeline fed. Increase if batch_queue is often empty |
| 4 | Completion workers | `threading.completionWorkers` | 5 | Parallel post-processing. Increase if completion_queue backs up |
| 5 | Milvus batch files | `storage.milvus.batchInsertMaxFiles` | 10 | Amortises Milvus overhead. 10 is a good balance |
| 6 | PDF split page count | `nemoRetriever.pdfSplitPageCount` | 1 | 1 = max isolation (one bad page affects only that page); higher = fewer sub-jobs |
| 7 | Status check workers | `threading.statusCheckWorkers` | 10 | Parallel polling. Rarely the bottleneck unless NeMo-Retriever API is slow |
| 8 | Submission workers | `threading.submissionWorkers` | 10 | Parallel REST submissions. Rarely limiting |

---

## Common Bottleneck Patterns

### All jobs stuck in `pending`, `processing` = 0

**Symptom**: `tracker_stats.pending` is high, `processing` stays at 0.

**Cause**: NeMo-Retriever V2 API is unresponsive — typically because it is overwhelmed with too many concurrent jobs.

**Fix**:
1. Clear NeMo-Retriever Redis state (`job_state:*`, `parent:*` keys)
2. Restart the NeMo-Retriever pod
3. Restart Nutanix AIDP-Connector pods
4. Reduce `processing.maxOutstandingTasks` (try 50–80)

### `batch_queue` keeps growing

**Symptom**: `batch_queue` > 30 and climbing.

**Cause**: Submission or NeMo-Retriever can't keep up with downloads.

**Fix**:
- NeMo-Retriever is the bottleneck — increase `batching.maxFiles` to send bigger batches
- Reduce `threading.downloadWorkers` to slow the intake

### High `completion_handle` latency

**Symptom**: `completion_handle` > 60 seconds consistently.

**Cause**: Too many parquet files per job (common with `pdfSplitPageCount=1` and large PDFs).

**Fix**:
- Increase `threading.completionWorkers`
- Increase `nemoRetriever.pdfSplitPageCount` (fewer, larger sub-jobs = fewer parquets)
- Ensure S3 (Objects) endpoint is fast

### High `milvus_batch_insert` latency

**Symptom**: `milvus_batch_total` > 40 seconds.

**Cause**: Milvus cluster is slow or overloaded.

**Fix**:
- Check Milvus cluster health and resource usage
- Use `consistencyLevel: Bounded` instead of `Strong`
- Use GPU index/search (`gpuIndex: true`, `gpuSearch: true`)

---

## NeMo-Retriever Overload Prevention

NeMo-Retriever uses a single-threaded Ray actor for embedding storage. When flooded with
concurrent jobs (each PDF split into N sub-jobs), the internal queue grows unboundedly:

- REST API response time degrades from < 100 ms to 4+ seconds
- Individual parent jobs never complete because their sub-tasks are scattered across a massive backlog
- Eventually the API stops responding entirely

**Recommendations**:

| Setting | Conservative | Moderate | Aggressive |
|---------|-------------|----------|------------|
| `batching.maxFiles` | 3 | 5 | 10–15 |
| `processing.maxOutstandingTasks` | 30 | 80 | 150 |
| `nemoRetriever.pdfSplitPageCount` | 1 | 4 | 16 |

Start conservative and increase while monitoring `active_jobs` and NeMo-Retriever API response times.

---

## Observed Baselines

From a 767-document run on a single NeMo-Retriever GPU instance:

### End-to-End

| Metric | Value |
|--------|-------|
| Wall-clock | ~53 min |
| Throughput | ~14.4 docs/min |
| Average per-document | ~4.2 s (with parallelism) |

### Per-Stage Latency

| Stage | Mean | P95 |
|-------|------|-----|
| `nemo_retriever` (batch) | ~260 s | ~459 s |
| `completion_handle` | ~33 s | ~75 s |
| `submission` | ~4 s | ~6 s |
| `download` | < 1 s | ~2 s |
| `message_queue` | < 1 s | < 1 s |

NeMo-Retriever dominates ~80% of wall-clock time. Tuning `batching.maxFiles` and `processing.maxOutstandingTasks` has the largest impact on throughput.
