# Nutanix AIDP-Connector Helm Chart

This Helm chart deploys the Nutanix AIDP-Connector to a Kubernetes cluster. The Nutanix AIDP-Connector is a Kafka-based document processing pipeline with NeMo-Retriever integration.

## Features

- **Real-time Processing**: Consumes S3 event notifications from Kafka and processes documents in real-time
- **Baseline Processing**: Process existing objects in S3 buckets that predate Kafka notifications
- **NeMo-Retriever Integration**: Leverages NVIDIA NeMo-Retriever for document extraction (text, tables, charts, images)
- **Vector Storage**: Stores embeddings in Milvus for semantic search
- **Flow Control**: Configurable throttling to prevent resource exhaustion
- **Metrics & Monitoring**: OpenTelemetry integration with Prometheus metrics export
- **Crash Recovery**: Persistent state management for baseline processing resume
- **Hot Reload**: Configuration changes without pod restart

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- Access to a Docker registry containing the `nutanix-aidp-connector` image
- AWS credentials for S3 access
- Kafka cluster access
- Milvus cluster access (optional)

## Installation

### Quick Start

**Step 1: Set namespace (optional, defaults to `nutanix-aidp-connector`)**

```bash
# Set namespace via environment variable (optional)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"  # Default: nutanix-aidp-connector

# Create the namespace
kubectl create namespace ${NAMESPACE}
```

**Step 2: Set environment variables with your configuration:**

```bash
export S3_ACCESS_KEY="PBkDfJshR5jG_XO7hgzLRhRxo2_Ck6V_"
export S3_SECRET_KEY="exRjkTtj22W9x9EgaBXJqeJqw4l_UWgC"
export KAFKA_BOOTSTRAP_SERVERS="10.122.17.92:9094"
export NEMO_RETRIEVER_HOST="10.122.17.88"
export NEMO_RETRIEVER_PORT="7670"
export MILVUS_HOST="10.122.17.86"
export MILVUS_PORT="19530"
export OBJECTS_ENDPOINT="objects.dragon.cloudnative.nvdlab.net:443"
export OBJECTS_ACCESS_KEY="your-objects-access-key"
export OBJECTS_SECRET_KEY="your-objects-secret-key"
```

**Step 3: Verify environment variables are set before running Helm commands:**

```bash
# Set namespace (optional, defaults to nutanix-aidp-connector)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

# Verify all required variables are set
echo "NAMESPACE: ${NAMESPACE}"
echo "S3_ACCESS_KEY: ${S3_ACCESS_KEY:-(NOT SET)}"
echo "S3_SECRET_KEY: ${S3_SECRET_KEY:-(NOT SET)}"
echo "KAFKA_BOOTSTRAP_SERVERS: ${KAFKA_BOOTSTRAP_SERVERS:-(NOT SET)}"
echo "S3_ENDPOINT_URL: ${S3_ENDPOINT_URL:-(NOT SET)}"
echo "NEMO_RETRIEVER_HOST: ${NEMO_RETRIEVER_HOST:-(NOT SET)}"
echo "NEMO_RETRIEVER_PORT: ${NEMO_RETRIEVER_PORT:-(NOT SET)}"
echo "MILVUS_HOST: ${MILVUS_HOST:-(NOT SET)}"
echo "MILVUS_PORT: ${MILVUS_PORT:-(NOT SET)}"
echo "OBJECTS_ENDPOINT: ${OBJECTS_ENDPOINT:-(NOT SET)}"
echo "OBJECTS_ACCESS_KEY: ${OBJECTS_ACCESS_KEY:-(NOT SET)}"
echo "OBJECTS_SECRET_KEY: ${OBJECTS_SECRET_KEY:-(NOT SET)}"
```

**Step 4: Install or upgrade using environment variables:**

**First time installation:**
```bash
# Set namespace (optional, defaults to nutanix-aidp-connector)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --namespace ${NAMESPACE} \
  --create-namespace \
  --set global.namespace=${NAMESPACE} \
  --set kafka.bootstrapServers="${KAFKA_BOOTSTRAP_SERVERS}" \
  --set s3.endpointUrl="${S3_ENDPOINT_URL}" \
  --set s3.accessKey="${S3_ACCESS_KEY}" \
  --set s3.secretKey="${S3_SECRET_KEY}" \
  --set nemoRetriever.host="${NEMO_RETRIEVER_HOST}" \
  --set nemoRetriever.port="${NEMO_RETRIEVER_PORT}" \
  --set storage.milvus.host="${MILVUS_HOST}" \
  --set storage.milvus.port="${MILVUS_PORT}" \
  --set storage.objects.endpointUrl="${OBJECTS_ENDPOINT}" \
  --set storage.objects.accessKey="${OBJECTS_ACCESS_KEY}" \
  --set storage.objects.secretKey="${OBJECTS_SECRET_KEY}"

**If release already exists, use upgrade:**
```bash
# Set namespace (optional, defaults to nutanix-aidp-connector)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

helm upgrade nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --namespace ${NAMESPACE} \
  --set global.namespace=${NAMESPACE} \
  --set kafka.bootstrapServers="${KAFKA_BOOTSTRAP_SERVERS}" \
  --set s3.endpointUrl="${S3_ENDPOINT_URL}" \
  --set nemoRetriever.host="${NEMO_RETRIEVER_HOST}" \
  --set nemoRetriever.port="${NEMO_RETRIEVER_PORT}" \
  --set storage.milvus.host="${MILVUS_HOST}" \
  --set storage.milvus.port="${MILVUS_PORT}" \
  --set storage.objects.endpointUrl="${OBJECTS_ENDPOINT}" \
  --set storage.objects.accessKey="${OBJECTS_ACCESS_KEY}" \
  --set storage.objects.secretKey="${OBJECTS_SECRET_KEY}" \
  --set s3.accessKey="${S3_ACCESS_KEY}" \
  --set s3.secretKey="${S3_SECRET_KEY}"

**Note**: If an environment variable is not set, using `--set` with an empty variable will override the default value in `values.yaml` with an empty string. Always ensure environment variables are set before running Helm commands.

### Using a Values File

1. Copy the default values file:
```bash
cp helm/nutanix-aidp-connector/values.yaml my-values.yaml
```

2. Edit `my-values.yaml` with your configuration

3. Install the chart:
```bash
# Set namespace (optional, defaults to nutanix-aidp-connector)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --namespace ${NAMESPACE} \
  --create-namespace \
  --set global.namespace=${NAMESPACE} \
  -f my-values.yaml
```

### Upgrading

```bash
# Set namespace (optional, defaults to nutanix-aidp-connector)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

helm upgrade nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --namespace ${NAMESPACE} \
  --set global.namespace=${NAMESPACE} \
  -f my-values.yaml
```

### Uninstalling

```bash
# Set namespace (optional, defaults to nutanix-aidp-connector)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

helm uninstall nutanix-aidp-connector --namespace ${NAMESPACE}
```

## Configuration

### Configuration Files

The Helm chart uses `values.yaml` for configuration:

1. **`values.yaml`**
   - Contains Helm values for all pipeline settings
   - Configuration is rendered into ConfigMap via templates

### Configuration Parameters

The following table lists the configurable parameters and their default values:

### Image Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `image.registry` | Docker registry | `veerkumarnutanix` |
| `image.repository` | Image repository | `nutanix-aidp-connector` |
| `image.tag` | Image tag | `v1.0.1` |
| `image.pullPolicy` | Image pull policy | `Always` |

### Deployment Configuration (StatefulSet)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `deployment.replicas` | Number of replicas (see [Multi-Replica](#multi-replica-support)) | `1` |
| `deployment.name` | StatefulSet/container name | `nutanix-aidp-connector` |
| `deployment.podManagementPolicy` | Pod startup order (`Parallel` or `OrderedReady`) | `Parallel` |

### Service Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `service.type` | Service type | `ClusterIP` |
| `service.configApiPort` | Configuration API port | `5001` |
| `service.metricsPort` | Metrics port (Prometheus) | `8000` |

### Resource Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `resources.requests.memory` | Memory request | `2Gi` |
| `resources.requests.cpu` | CPU request | `1000m` |
| `resources.limits.memory` | Memory limit | `16Gi` |
| `resources.limits.cpu` | CPU limit | `2000m` |

### Kafka Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `kafka.bootstrapServers` | Kafka bootstrap servers | `""` (Required) |
| `kafka.topic` | Kafka topic | `OSSEvents` |
| `kafka.groupId` | Consumer group ID | `nutanix-aidp-connector-group` |
| `kafka.autoOffsetReset` | Offset reset for new consumer group: `earliest` (replay history) or `latest` (new messages only) | `latest` |
| `kafka.minTopicPartitions` | Expected minimum partitions on topic (warning logged if lower) | `1` |
| `kafka.autoCommit` | Auto commit | `false` |

### S3 Configuration (for reading objects after Kafka notifications)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `s3.region` | AWS region | `us-east-1` |
| `s3.endpointUrl` | S3 endpoint URL | `""` (Required) |
| `s3.verifySsl` | Verify SSL | `false` |
| `s3.accessKey` | S3 access key (for reading objects) | `""` (Required) |
| `s3.secretKey` | S3 secret key (for reading objects) | `""` (Required) |

### Threading Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `threading.downloadWorkers` | Number of parallel download workers | `5` |
| `threading.submissionWorkers` | Number of job submission workers | `10` |
| `threading.statusCheckWorkers` | Number of status check workers | `10` |
| `threading.completionWorkers` | Number of parallel completion/Milvus workers | `5` |

### Batching Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `batching.maxFiles` | Maximum files per batch | `5` |
| `batching.maxBytes` | Maximum bytes per batch | `2000000000` (2GB) |
| `batching.maxWaitSeconds` | Maximum wait time before flushing batch | `30` |

### NeMo-Retriever Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nemoRetriever.host` | NeMo-Retriever host | `nv-ingest.nv-ingest` |
| `nemoRetriever.port` | NeMo-Retriever port | `7670` |
| `nemoRetriever.taskQueue` | Task queue name | `morpheus_task_queue` |
| `nemoRetriever.timeout` | Job timeout (seconds) | `300` |
| `nemoRetriever.textDepth` | Text extraction depth | `page` |
| `nemoRetriever.apiVersion` | NeMo-Retriever API version | `v2` |
| `nemoRetriever.pdfSplitPageCount` | V2 only: pages per subjob (1 = isolate each page) | `1` |

#### Extraction Parameters (configured in values.yaml)

The following extraction parameters are configured in `values.yaml`:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nemoRetriever.extract.extractText` | Extract text content | `true` |
| `nemoRetriever.extract.extractImages` | Extract images | `false` |
| `nemoRetriever.extract.extractTables` | Extract tables | `false` |
| `nemoRetriever.extract.extractCharts` | Extract charts | `false` |
| `nemoRetriever.extract.extractMetadata` | Extract metadata | `false` |
| `nemoRetriever.extract.ocrEnabled` | Enable OCR | `false` |
| `nemoRetriever.extract.ocrLanguage` | OCR language | `eng` |
| `nemoRetriever.extract.extractTablesMethod` | Table extraction method | `yolox` |
| `nemoRetriever.extract.tableOutputFormat` | Table output format | `pseudo_markdown` |

#### Split Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nemoRetriever.split.enabled` | Enable split task | `true` |
| `nemoRetriever.split.splitByPage` | Split by page boundaries | `true` |
| `nemoRetriever.split.splitBySize` | Split by character length | `false` |
| `nemoRetriever.split.maxCharacterLength` | Max characters per chunk (if splitBySize) | `1024` |
| `nemoRetriever.split.sentenceWindowSize` | Sentence window context size | `0` |

#### Embed Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nemoRetriever.embed.enabled` | Enable embed task | `true` |
| `nemoRetriever.embed.dimensions` | Embedding vector dimensions | `2048` |

#### Supported Document Types

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nemoRetriever.supportedExtensions` | File extensions that are processed | `[pdf, docx, doc, txt, md, rtf, pptx, ppt]` |

### Storage Configuration

#### Milvus Configuration (for vector storage)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `storage.milvus.enabled` | Enable Milvus storage | `true` |
| `storage.milvus.host` | Milvus host | `milvus-vectordb.milvus` |
| `storage.milvus.port` | Milvus port | `19530` |
| `storage.milvus.collectionName` | Collection name | `docs_collection` |
| `storage.milvus.bucket` | Milvus bucket for bulk insert | `milvus-store` |
| `storage.milvus.indexType` | Index type | `GPU_CAGRA` |
| `storage.milvus.metricType` | Distance metric | `L2` |
| `storage.milvus.nlist` | Number of clusters for index | `1024` |
| `storage.milvus.nprobe` | Number of clusters to search | `10` |
| `storage.milvus.autoId` | Auto-generate IDs | `true` |
| `storage.milvus.consistencyLevel` | Consistency level | `Bounded` |
| `storage.milvus.autoCreateCollection` | Auto-create collection on startup | `true` |
| `storage.milvus.sparse` | Enable sparse vector field and BM25 for hybrid search | `false` |
| `storage.milvus.gpuIndex` | Use GPU_CAGRA index for dense vectors | `true` |
| `storage.milvus.gpuSearch` | Enable GPU-accelerated search | `true` |
| `storage.milvus.localIndex` | Use local index (FLAT) instead of server-side index | `false` |
| `storage.milvus.enableFilteredCollections` | Enable creation and routing to filtered collections | `false` |
| `storage.milvus.filteredTextCollectionName` | Collection name for filtered text content | `connector_text_collection1` |
| `storage.milvus.filteredTablesCollectionName` | Collection name for filtered table content | `connector_table_collection1` |
| `storage.milvus.filteredChartsCollectionName` | Collection name for filtered chart content | `connector_chart_collection1` |
| `storage.milvus.cleanupParquetFiles` | Clean up parquet files after successful bulk insert | `true` |
| `storage.milvus.maxBulkInsertFiles` | Max parquet files per `do_bulk_insert` request (0 = no limit) | `128` |
| `storage.milvus.batchInsertMaxFiles` | Source files to accumulate before combined Milvus bulk insert (0 = disable) | `10` |
| `storage.milvus.batchInsertMaxWaitSeconds` | Max seconds to wait before flushing an incomplete batch | `15` |

#### Objects Configuration (S3-compatible service for Milvus/NeMo-Retriever storage)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `storage.objects.enabled` | Enable Objects storage | `true` |
| `storage.objects.endpointUrl` | Objects endpoint URL (format: host:port) | `""` |
| `storage.objects.accessKey` | Objects access key | `""` |
| `storage.objects.secretKey` | Objects secret key | `""` |
| `storage.objects.bucket` | Objects bucket name | `nutanix-aidp-connector-staging` |
| `storage.objects.region` | Objects region | `us-east-1` |

**Note**: `storage.objects` is separate from `s3:` configuration. The `s3:` section is used for reading objects after Kafka notifications, while `storage.objects` is used for Milvus/NeMo-Retriever storage operations.

### Processing Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `processing.maxRetries` | Maximum retry attempts for failed jobs | `3` |
| `processing.retryBaseDelay` | Base delay between retries (seconds) | `1` |
| `processing.retryMaxDelay` | Maximum delay between retries (seconds) | `60` |
| `processing.statusPollInterval` | Interval to poll job status (seconds) | `5` |
| `processing.circuitBreakerThreshold` | Failures before circuit breaker opens | `5` |
| `processing.circuitBreakerTimeout` | Circuit breaker timeout (seconds) | `60` |
| `processing.enableSequenceExecution` | Wait for job completion before next submission | `false` |
| `processing.maxOutstandingTasks` | Max concurrent jobs with NeMo-Retriever server | `150` |

#### Flow Control

The pipeline implements flow control to prevent storage overflow and manage resource usage:

- **`maxOutstandingTasks`**: Limits the number of concurrent jobs submitted to NeMo-Retriever
- **Download Throttling**: Downloads are limited to `3x maxOutstandingTasks` files
- **Batch Relationship**: If `maxOutstandingTasks=40` and batch size=5, max 8 batches can be in-flight (40÷5=8)

```
┌─────────────────────────────────────────────────────────────┐
│              Flow Control Example                           │
├─────────────────────────────────────────────────────────────┤
│ maxOutstandingTasks: 150                                    │
│ Downloaded files limit: 150 × 3 = 450 files                │
│ Batch size: 5 files                                         │
│ Max batches in-flight: 150 ÷ 5 = 30 batches                │
└─────────────────────────────────────────────────────────────┘
```

### Telemetry Configuration (OpenTelemetry/Prometheus)

The pipeline exports metrics via OpenTelemetry for Prometheus scraping.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `telemetry.enabled` | Enable telemetry/metrics export | `true` |
| `telemetry.serviceName` | Service name for metrics | `nutanix-aidp-connector` |
| `telemetry.serviceVersion` | Service version label | `1.0.0` |
| `telemetry.prometheusPort` | Port for Prometheus metrics endpoint | `9090` |
| `telemetry.prometheusEndpoint` | Metrics endpoint path | `/metrics` |
| `telemetry.metricExportIntervalMs` | Metric export interval (ms) | `5000` |
| `telemetry.scrapeInterval` | Prometheus scrape interval | `30s` |
| `telemetry.scrapeTimeout` | Prometheus scrape timeout | `10s` |
| `telemetry.serviceMonitorNamespace` | Namespace for ServiceMonitor (must match Prometheus) | `aidp` |

#### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `nutanix-aidp-connector_jobs_submitted_total` | Counter | Total jobs submitted to NeMo-Retriever |
| `nutanix-aidp-connector_jobs_completed_total` | Counter | Total jobs completed successfully |
| `nutanix-aidp-connector_jobs_failed_total` | Counter | Total jobs that failed |
| `nutanix-aidp-connector_files_processed_total` | Counter | Total files processed |
| `nutanix-aidp-connector_files_downloaded_total` | Counter | Total files downloaded from S3 |
| `nutanix-aidp-connector_batches_created_total` | Counter | Total batches created |
| `nutanix-aidp-connector_active_jobs` | Gauge | Current number of active jobs |
| `nutanix-aidp-connector_job_duration_seconds` | Histogram | Job processing duration |
| `nutanix-aidp-connector_submission_throttled_total` | Counter | Times submission was throttled |
| `nutanix-aidp-connector_download_throttled_total` | Counter | Times download was throttled |
| `nutanix-aidp-connector_pipeline_stage_duration_seconds` | Histogram | Per-stage pipeline latency (label: `stage`) |
| `nutanix-aidp-connector_job_status_codes_total` | Counter | Job status checks by HTTP status code (200, 202, 404, 410, 503) |
| `nutanix-aidp-connector_job_errors_by_status_code_total` | Counter | Job errors by HTTP status code (4xx, 5xx only) |

**Job Status Code Metrics Labels:**
- `status_code`: HTTP status code (200, 202, 404, 410, 503)
- `error_type`: Error classification:
  - `none`: Success (200, 202)
  - `validation_failure`: Job validation failed (404 after 10+ checks)
  - `processing_failure`: Job processing failed (503)
  - `expired`: Job result expired/retrieved (404/410)
  - `not_found`: Job not found (404, retrying)

#### Prometheus Integration

The chart creates a `ServiceMonitor` resource for Prometheus Operator. Ensure:

1. Prometheus Operator is installed in the cluster
2. `telemetry.serviceMonitorNamespace` matches your Prometheus namespace
3. Prometheus is configured to watch the ServiceMonitor namespace

```bash
# Check if ServiceMonitor is created
# The namespace comes from telemetry.serviceMonitorNamespace in values.yaml
# The name includes Release.Namespace (e.g. nutanix-aidp-connector-nutanix-aidp-connector2-monitor) for multi-release coexistence
kubectl get servicemonitor -n <telemetry.serviceMonitorNamespace> | grep nutanix-aidp-connector

# Verify Prometheus targets
# Navigate to Prometheus UI → Status → Targets
```

> **Note**: The ServiceMonitor is created in the namespace specified by `telemetry.serviceMonitorNamespace` (default: `aidp`). Its name includes `Release.Namespace` (e.g. `nutanix-aidp-connector-nutanix-aidp-connector2-monitor`) so multiple Nutanix AIDP-Connector releases in different namespaces can coexist without naming conflicts.

### Baseline Processing Configuration

Baseline processing allows you to process existing objects in S3 buckets that predate Kafka notifications.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `baseline.enabled` | Enable baseline processing | `true` |
| `baseline.stateDir` | Directory for baseline state files | `/var/lib/nutanix-aidp-connector/baseline/` |
| `baseline.listBatchSize` | Objects per S3 list API call (max 1000) | `1000` |
| `baseline.maxConcurrentListings` | Number of buckets to process concurrently | `1` |
| `baseline.pauseKafkaDuringBaseline` | Pause Kafka during baseline processing | `true` |
| `baseline.autoDisableCompletedBuckets` | Mark buckets as disabled after completion | `true` |
| `baseline.buckets` | List of buckets to process | `[]` |

#### Bucket Configuration

```yaml
baseline:
  enabled: true
  buckets:
    - bucketName: "my-documents"
      enabled: true
      prefix: ""  # Process all objects
    - bucketName: "my-images"
      enabled: true
      prefix: "photos/"  # Only process objects with this prefix
    - bucketName: "archived-bucket"
      enabled: false  # Skip this bucket
```

#### Baseline Behavior

1. **Synchronous Batch Processing**: Objects are processed in batches. The pipeline lists a batch, processes all objects, then lists the next batch.
2. **Crash Recovery**: State is persisted to disk. On restart, processing resumes from the last completed batch.
3. **Kafka Pausing**: When `pauseKafkaDuringBaseline: true`, Kafka consumption pauses during baseline and resumes after completion.
4. **Completion Tracking**: Each bucket's progress is saved in `stateDir` as JSON files.

#### Checking Baseline Status

```bash
# Check baseline state files (only on the leader pod, ordinal 0)
kubectl exec -n ${NAMESPACE} nutanix-aidp-connector-0 -- \
  ls -la /var/lib/nutanix-aidp-connector/baseline/

# View specific bucket state
kubectl exec -n ${NAMESPACE} nutanix-aidp-connector-0 -- \
  cat /var/lib/nutanix-aidp-connector/baseline/baseline_state_<bucket-name>_*.json
```

### Persistence Configuration

PVCs are managed via StatefulSet `volumeClaimTemplates`, so each pod gets its own PVC automatically (e.g. `logs-nutanix-aidp-connector-0`, `baseline-state-nutanix-aidp-connector-0`).

| Parameter | Description | Default |
|-----------|-------------|---------|
| `persistence.logs.enabled` | Enable per-pod logs PVC | `true` |
| `persistence.logs.size` | Logs PVC size | `5Gi` |
| `persistence.logs.storageClass` | Storage class (empty for default) | `""` |
| `persistence.baselineState.enabled` | Enable per-pod baseline state PVC | `true` |
| `persistence.baselineState.size` | Baseline state PVC size | `1Gi` |
| `persistence.baselineState.storageClass` | Storage class (empty for default) | `""` |

### AWS Credentials

**Important**: AWS credentials should be provided via `--set` flags or a values file. Never commit credentials to version control.

**Note**: Provide credentials as plain text - the Helm chart will automatically base64 encode them.

```bash
helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --namespace nutanix-aidp-connector \
  --create-namespace \
```

Or use a secrets file (not tracked in git):

```yaml
# secrets.yaml (add to .gitignore)
aws:
  accessKeyId: "your-access-key"
  secretAccessKey: "your-secret-key"
```

Then install:
```bash
helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --namespace nutanix-aidp-connector \
  --create-namespace \
  -f secrets.yaml
```

## Examples

### Basic Installation

```bash
# Set namespace (optional, defaults to nutanix-aidp-connector)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --namespace ${NAMESPACE} \
  --create-namespace \
  --set global.namespace=${NAMESPACE} \
```

### Custom Namespace

If you want to use a different namespace, set the `NAMESPACE` environment variable:

```bash
# Set your custom namespace
export NAMESPACE="your-namespace"

# Create the namespace
kubectl create namespace ${NAMESPACE}

# Install with custom namespace
helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --namespace ${NAMESPACE} \
  --create-namespace \
  --set global.namespace=${NAMESPACE} \
```

**Note**: All `kubectl` commands should use `-n ${NAMESPACE}` or `--namespace ${NAMESPACE}` to target the correct namespace.

### NodePort Service

```bash
helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --set service.type=NodePort \
  --set service.nodePort=30001
```

### Custom Resources

```bash
helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --set resources.requests.memory=4Gi \
  --set resources.requests.cpu=2000m \
  --set resources.limits.memory=8Gi \
  --set resources.limits.cpu=4000m
```

### Multi-Replica Support

Nutanix AIDP-Connector is deployed as a **StatefulSet** and supports multiple replicas. Kafka's native consumer group protocol assigns unique partitions to each pod automatically — no manual partition mapping is needed.

**Prerequisites:**
- The Kafka topic (`OSSEvents`) must have **at least as many partitions** as the replica count. If the topic has fewer partitions than pods, some pods will sit idle and log a warning.
- Baseline processing runs **only on pod ordinal 0** (the leader). Worker pods (ordinal >= 1) skip baseline and only handle real-time Kafka events.

```bash
# Single replica (default)
helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --set deployment.replicas=1

# Multi-replica (ensure topic has >= 3 partitions)
helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --set deployment.replicas=3
```

**Migration from Deployment to StatefulSet:**
Helm cannot change a resource from `Deployment` to `StatefulSet` in-place. If upgrading from an older Nutanix AIDP-Connector release that used a Deployment:
```bash
helm uninstall nutanix-aidp-connector -n ${NAMESPACE}
# Clean up orphan PVCs from the old Deployment (e.g. nutanix-aidp-connector-logs-pvc, nutanix-aidp-connector-baseline-state-pvc)
kubectl delete pvc nutanix-aidp-connector-logs-pvc nutanix-aidp-connector-baseline-state-pvc -n ${NAMESPACE} 2>/dev/null
helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector -n ${NAMESPACE} --create-namespace -f my-values.yaml
```

## Troubleshooting

### ConfigMap Has Empty Values Despite Environment Variables Being Set

**Problem**: ConfigMap shows empty values (e.g., `bootstrap_servers: ""`) even though you set environment variables.

**Root Cause**: When you use `--set kafka.bootstrapServers="${KAFKA_BOOTSTRAP_SERVERS}"` and the environment variable is not set in that shell session, it becomes `--set kafka.bootstrapServers=""`, which overrides the default value in `values.yaml` with an empty string.

**Solution**:

1. **Always verify environment variables are set before running Helm:**
   ```bash
   echo "KAFKA_BOOTSTRAP_SERVERS: ${KAFKA_BOOTSTRAP_SERVERS:-(NOT SET)}"
   ```

2. **Set environment variables in the same shell session:**
   ```bash
   export KAFKA_BOOTSTRAP_SERVERS="your-kafka-host:port"
   # Then immediately run helm (in the same shell)
   helm upgrade nutanix-aidp-connector ./helm/nutanix-aidp-connector \
     --set kafka.bootstrapServers="${KAFKA_BOOTSTRAP_SERVERS}" ...
   ```

3. **Use a values file instead** (recommended for production):
   ```bash
   # Create my-values.yaml with all values
   # Then use: helm upgrade -f my-values.yaml
   ```

4. **The template now has a default fallback**, so if you forget to set it, it will use the default from `values.yaml` instead of empty string.

**Check current values:**
```bash
# Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

helm get values nutanix-aidp-connector -n ${NAMESPACE}
kubectl get configmap nutanix-aidp-connector-config -n ${NAMESPACE} -o jsonpath='{.data.config\.yaml}' | grep bootstrap_servers
```

### Installation Fails: Release Name Already in Use

If you get this error:

```
Error: INSTALLATION FAILED: cannot re-use a name that is still in use
```

This means a Helm release with the same name already exists. You have two options:

**Option 1: Use upgrade instead of install (Recommended)**

```bash
# Check existing releases
# Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

helm list -n ${NAMESPACE}

# Set namespace (optional, defaults to nutanix-aidp-connector)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

# Use upgrade instead
helm upgrade nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --namespace ${NAMESPACE} \
  --set global.namespace=${NAMESPACE} \
  --set kafka.bootstrapServers="${KAFKA_BOOTSTRAP_SERVERS}" \
  --set s3.endpointUrl="${S3_ENDPOINT_URL}" \
  --set nemoRetriever.host="${NEMO_RETRIEVER_HOST}" \
  --set nemoRetriever.port="${NEMO_RETRIEVER_PORT}" \
  --set storage.milvus.host="${MILVUS_HOST}" \
  --set storage.milvus.port="${MILVUS_PORT}" \
  --set storage.objects.endpointUrl="${OBJECTS_ENDPOINT}" \
  --set storage.objects.accessKey="${OBJECTS_ACCESS_KEY}" \
  --set storage.objects.secretKey="${OBJECTS_SECRET_KEY}" \
  --set s3.accessKey="${S3_ACCESS_KEY}" \
  --set s3.secretKey="${S3_SECRET_KEY}"

**Option 2: Uninstall first, then install**

```bash
# Uninstall existing release
# Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

helm uninstall nutanix-aidp-connector -n ${NAMESPACE}

# Then install fresh
helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --set kafka.bootstrapServers="${KAFKA_BOOTSTRAP_SERVERS}" \
  --set s3.endpointUrl="${S3_ENDPOINT_URL}" \
  --set nemoRetriever.host="${NEMO_RETRIEVER_HOST}" \
  --set nemoRetriever.port="${NEMO_RETRIEVER_PORT}" \
  --set storage.milvus.host="${MILVUS_HOST}" \
  --set storage.milvus.port="${MILVUS_PORT}" \
  --set storage.objects.endpointUrl="${OBJECTS_ENDPOINT}" \
  --set storage.objects.accessKey="${OBJECTS_ACCESS_KEY}" \
  --set storage.objects.secretKey="${OBJECTS_SECRET_KEY}" \
  --set s3.accessKey="${S3_ACCESS_KEY}" \
  --set s3.secretKey="${S3_SECRET_KEY}"

**Check release status:**

```bash
# Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

helm status nutanix-aidp-connector -n ${NAMESPACE}
# Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

helm list -n ${NAMESPACE}
```

### Installation Fails: Resources Already Exist

If you get an error about resource ownership metadata:

```
Error: Secret/ConfigMap/Deployment/Service/PVC "nutanix-aidp-connector-*" exists and cannot be imported
```

This happens when resources were created manually (not by Helm). Helm requires ownership metadata to manage resources.

#### Clean Up Existing Resources

**Option 1: Use the cleanup script (Recommended)**

```bash
# Set KUBECONFIG to your kubeconfig file location
./helm/nutanix-aidp-connector/cleanup-existing-resources.sh
```

**Option 2: Manual cleanup**

Delete all existing resources before installing with Helm:

```bash
# Set KUBECONFIG to your kubeconfig file location
NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

# Check what exists
echo "Checking for existing resources..."
kubectl get secret,configmap,statefulset,service,pvc -n $NAMESPACE | grep nutanix-aidp-connector || echo "No existing resources found"

# Delete Secret
kubectl delete secret nutanix-aidp-connector-secrets -n $NAMESPACE 2>/dev/null || echo "Secret not found"

# Delete ConfigMap
kubectl delete configmap nutanix-aidp-connector-config -n $NAMESPACE 2>/dev/null || echo "ConfigMap not found"

# Delete StatefulSet
kubectl delete statefulset nutanix-aidp-connector -n $NAMESPACE 2>/dev/null || echo "StatefulSet not found"

# Delete Service
kubectl delete service nutanix-aidp-connector -n $NAMESPACE 2>/dev/null || echo "Service not found"

# Delete PVC (optional - only if you want to start fresh)
# WARNING: This will delete persistent data!
# kubectl delete pvc nutanix-aidp-connector-logs-pvc -n $NAMESPACE 2>/dev/null || echo "PVC not found"

echo "Cleanup complete. You can now install with Helm."
```

#### Quick Cleanup Script

For a complete cleanup (including PVC - **WARNING: deletes data**):

```bash
# Set KUBECONFIG to your kubeconfig file location
NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

# Delete all resources
./helm/nutanix-aidp-connector/cleanup-existing-resources.sh

# Create namespace if it doesn't exist
kubectl create namespace nutanix-aidp-connector --dry-run=client -o yaml | kubectl apply -f -

# Install with Helm
helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --namespace nutanix-aidp-connector \
  --create-namespace \
  --set kafka.bootstrapServers="${KAFKA_BOOTSTRAP_SERVERS}" \
  --set s3.endpointUrl="${S3_ENDPOINT_URL}" \
  --set nemoRetriever.host="${NEMO_RETRIEVER_HOST}" \
  --set nemoRetriever.port="${NEMO_RETRIEVER_PORT}" \
  --set storage.milvus.host="${MILVUS_HOST}" \
  --set storage.milvus.port="${MILVUS_PORT}"

**Note**: If you want to preserve log data, skip the PVC deletion step.

### ImagePullBackOff Error

If you see `ImagePullBackOff` error, it could be due to:

1. **Image doesn't exist in registry** - Need to build and push the image
2. **Private registry requires authentication** - Need to configure imagePullSecrets
3. **Wrong registry/image name** - Check image configuration

#### Solution 1: Build and Push the Image

If the image doesn't exist in the registry, you need to build and push it first.

**Option 1: Use the containerized build script (Recommended)**

```bash
# From project root — builds inside a container for reproducibility
./scripts/build-containerized.sh --version v1.0.0

# Clean rebuild (only invalidates pip layer, not system packages)
./scripts/build-containerized.sh --reset --version v1.0.0
```

**Option 2: Use the legacy build-and-push script**

```bash
# From helm chart directory (wraps scripts/build-and-push-image.sh)
cd helm/nutanix-aidp-connector
./build-and-push-image.sh --version v1.0.0
```

**Option 3: Manual build and push**

```bash
# Build the image
docker compose build

# Tag for registry (check your version)
REGISTRY="veerkumarnutanix"
IMAGE_NAME="nutanix-aidp-connector"
VERSION="v1.0.0"

# Get the docker-compose image name
PROJECT_DIR=$(basename "$(pwd)")
SOURCE_IMAGE="${PROJECT_DIR}_nutanix-aidp-connector:latest"

# Tag for registry
docker tag ${SOURCE_IMAGE} ${REGISTRY}/${IMAGE_NAME}:${VERSION}

# Login to Docker Hub (if needed)
docker login

# Push to registry
docker push ${REGISTRY}/${IMAGE_NAME}:${VERSION}
```

**Option 4: Update Helm to use existing image version**

If you already have an image with a different version (e.g., `v1.0.0`), update Helm values:

```bash
helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --set image.tag="v1.0.0" \
  --set kafka.bootstrapServers="${KAFKA_BOOTSTRAP_SERVERS}" \
  --set s3.endpointUrl="${S3_ENDPOINT_URL}" \
  --set nemoRetriever.host="${NEMO_RETRIEVER_HOST}" \
  --set nemoRetriever.port="${NEMO_RETRIEVER_PORT}" \
  --set storage.milvus.host="${MILVUS_HOST}" \
  --set storage.milvus.port="${MILVUS_PORT}" \
  --set storage.objects.endpointUrl="${OBJECTS_ENDPOINT}" \
  --set storage.objects.accessKey="${OBJECTS_ACCESS_KEY}" \
  --set storage.objects.secretKey="${OBJECTS_SECRET_KEY}" \
  --set s3.accessKey="${S3_ACCESS_KEY}" \
  --set s3.secretKey="${S3_SECRET_KEY}"

#### Solution 2: Configure ImagePullSecrets for Private Registry

If your Docker image is in a private registry (like Docker Hub private repository), you need to create a Kubernetes secret with your Docker credentials and configure the Helm chart to use it.

**Step 1: Create Docker Registry Secret**

```bash
# Set your Docker Hub credentials
export DOCKER_USERNAME="your-dockerhub-username"
export DOCKER_PASSWORD="your-dockerhub-password"
export DOCKER_EMAIL="your-email@example.com"

# Create the secret in the namespace
kubectl create secret docker-registry regcred \
  --docker-server=docker.io \
  --docker-username="${DOCKER_USERNAME}" \
  --docker-password="${DOCKER_PASSWORD}" \
  --docker-email="${DOCKER_EMAIL}" \
  -n nutanix-aidp-connector
```

**Alternative: Using Docker Hub Access Token (Recommended)**

If you prefer using a Docker Hub access token instead of password:

```bash
# Create secret with access token
# Set namespace (optional, defaults to nutanix-aidp-connector)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

kubectl create secret docker-registry regcred \
  --docker-server=docker.io \
  --docker-username="${DOCKER_USERNAME}" \
  --docker-password="${DOCKER_TOKEN}" \
  --docker-email="${DOCKER_EMAIL}" \
  -n ${NAMESPACE}
```

**Step 2: Verify the Secret**

```bash
# Check if secret was created
# Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

kubectl get secret regcred -n ${NAMESPACE}

# View secret details (base64 encoded)
# Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

kubectl get secret regcred -n ${NAMESPACE} -o yaml
```

**Step 3: Install/Upgrade Helm Chart with imagePullSecrets**

**Option A: Using --set flag**

```bash
# Set namespace (optional, defaults to nutanix-aidp-connector)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --namespace ${NAMESPACE} \
  --create-namespace \
  --set global.namespace=${NAMESPACE} \
  --set imagePullSecrets[0]=regcred \
  --set kafka.bootstrapServers="${KAFKA_BOOTSTRAP_SERVERS}" \
  --set s3.endpointUrl="${S3_ENDPOINT_URL}" \
  --set nemoRetriever.host="${NEMO_RETRIEVER_HOST}" \
  --set nemoRetriever.port="${NEMO_RETRIEVER_PORT}" \
  --set storage.milvus.host="${MILVUS_HOST}" \
  --set storage.milvus.port="${MILVUS_PORT}" \
  --set storage.objects.endpointUrl="${OBJECTS_ENDPOINT}" \
  --set storage.objects.accessKey="${OBJECTS_ACCESS_KEY}" \
  --set storage.objects.secretKey="${OBJECTS_SECRET_KEY}" \
  --set s3.accessKey="${S3_ACCESS_KEY}" \
  --set s3.secretKey="${S3_SECRET_KEY}"

**Option B: Update values.yaml**

Edit `helm/nutanix-aidp-connector/values.yaml`:

```yaml
# Image pull secrets for private registries
imagePullSecrets:
  - regcred
```

Then install/upgrade:

```bash
# Set namespace (optional, defaults to nutanix-aidp-connector)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --namespace ${NAMESPACE} \
  --create-namespace \
  --set global.namespace=${NAMESPACE} \
  -f helm/nutanix-aidp-connector/values.yaml
```

**Step 4: Verify Pod Can Pull Image**

```bash
# Check pod status
# Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

kubectl get pods -n ${NAMESPACE}

# Describe pod to see if image pull succeeded
# Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

kubectl describe pod -n ${NAMESPACE} -l app=nutanix-aidp-connector

# Check events
# Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

kubectl get events -n ${NAMESPACE} --sort-by='.lastTimestamp' | grep nutanix-aidp-connector
```

**Troubleshooting:**

If you still see ImagePullBackOff after adding imagePullSecrets:

1. **Verify secret exists:**
   ```bash
   # Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

kubectl get secret regcred -n ${NAMESPACE}
   ```

2. **Check secret is in the same namespace:**
   ```bash
   # Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

kubectl get secrets -n ${NAMESPACE} | grep regcred
   ```

3. **Verify credentials are correct:**
   ```bash
   # Test Docker login locally
   docker login -u "${DOCKER_USERNAME}" -p "${DOCKER_PASSWORD}"
   docker pull veerkumarnutanix/nutanix-aidp-connector:v1.0.0
   ```

4. **Check deployment has imagePullSecrets:**
   ```bash
   # Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

kubectl get statefulset nutanix-aidp-connector -n ${NAMESPACE} -o yaml | grep -A 3 imagePullSecrets
   ```

5. **Delete and recreate pod to pick up new secret:**
   ```bash
   # Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

kubectl delete pod -n ${NAMESPACE} -l app=nutanix-aidp-connector
   ```

**Quick Fix: Push Image to Docker Hub**

If the image exists locally but not in Docker Hub:

```bash
# Login to Docker Hub
docker login

# Push the image
docker push veerkumarnutanix/nutanix-aidp-connector:v1.0.0
```

**Alternative: Use Local Image (for testing)**

If you can't push to Docker Hub, you can load the image directly to cluster nodes:

```bash
# Save image to tar
docker save veerkumarnutanix/nutanix-aidp-connector:v1.0.0 -o nutanix-aidp-connector-v1.0.0.tar

# Load on each cluster node (requires node access)
# On each node:
docker load -i nutanix-aidp-connector-v1.0.0.tar

# Then set imagePullPolicy to Never or IfNotPresent
helm upgrade nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --set image.pullPolicy="IfNotPresent" \
  --reuse-values
```

**Check available images:**

```bash
# Check local images
docker images | grep nutanix-aidp-connector

# Check if image exists in registry (requires docker login)
docker pull veerkumarnutanix/nutanix-aidp-connector:v1.0.0
```

### OOMKilled (Exit Code 137)

**Problem**: Pod is killed with exit code 137 due to exceeding its memory limit.

**Root Cause**: Large PDFs generate hundreds of parquet files during extraction (e.g., a 620-page PDF with `pdfSplitPageCount: 1` produces 620+ parquets). The completion handler downloads and merges all parquets in memory. When multiple large files complete simultaneously under high concurrency, memory spikes past the container limit. This can happen during both baseline ingestion and real-time Kafka processing.

**Fix**:

1. **Increase memory limit**:
   ```yaml
   resources:
     limits:
       memory: "16Gi"  # increase from 8Gi
   ```

2. **Reduce concurrency** to lower peak memory:
   ```yaml
   processing:
     maxOutstandingTasks: 20  # reduce from 50
   ```

3. **Increase `pdfSplitPageCount`** to reduce per-file parquet count:
   ```yaml
   nemoRetriever:
     pdfSplitPageCount: 10  # group 10 pages per subjob instead of 1
   ```

**Note**: During baseline processing, an OOM restart will re-process files from the last checkpoint. If `forceRerun: true` is set, all files are re-processed from scratch — set it to `false` after a successful baseline run.

**Diagnosis**:
```bash
kubectl describe pod nutanix-aidp-connector-0 -n ${NAMESPACE} | grep -A5 "Last State"
kubectl logs nutanix-aidp-connector-0 -n ${NAMESPACE} --previous --tail=50
```

### Check Pod Status

```bash
kubectl get pods -n ${NAMESPACE} -l app=nutanix-aidp-connector
```

### View Logs

```bash
kubectl logs -n ${NAMESPACE} -l app=nutanix-aidp-connector -f
```

### Check Pipeline Statistics

```bash
# View recent pipeline stats
kubectl logs -n ${NAMESPACE} -l app=nutanix-aidp-connector --tail=50 | grep "Pipeline statistics"
```

### Check Baseline Status

```bash
# List baseline state files (leader pod only)
kubectl exec -n ${NAMESPACE} nutanix-aidp-connector-0 -- \
  ls -la /var/lib/nutanix-aidp-connector/baseline/

# View baseline state for a bucket
kubectl exec -n ${NAMESPACE} nutanix-aidp-connector-0 -- \
  cat /var/lib/nutanix-aidp-connector/baseline/baseline_state_*.json | jq .
```

### Check Metrics Endpoint

```bash
# Port-forward and check metrics
kubectl port-forward -n ${NAMESPACE} svc/nutanix-aidp-connector 9090:9090 &
curl -s http://localhost:9090/metrics | grep nutanix-aidp-connector_
```

### Check ConfigMap

```bash
# View full ConfigMap
kubectl get configmap nutanix-aidp-connector-config -o yaml

# Check file types count
kubectl get configmap nutanix-aidp-connector-config -o jsonpath='{.data.config\.yaml}' | grep -c "nemo_retriever:"
```

### Check Secret

```bash
kubectl get secret nutanix-aidp-connector-secrets -o yaml
```

### Verify Service

```bash
kubectl get svc nutanix-aidp-connector

# Test health endpoint (if NodePort or port-forwarded)
curl http://<service-ip>:<port>/health
```

### Verify Configuration

```bash
# Check config status
curl http://<service-ip>:<port>/config/status | jq .

# Check file types
curl http://<service-ip>:<port>/config/file-types | jq .
```

## Updating Configuration

### Hot Reloading

The pipeline supports hot-reloading of configuration without pod restart. After updating the ConfigMap:

```bash
# Update ConfigMap
kubectl apply -f <updated-configmap>

# Or edit directly
# Set namespace (if not already set)
export NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

kubectl edit configmap nutanix-aidp-connector-config -n ${NAMESPACE}

# Trigger reload (optional - auto-reloads within 30 seconds)
curl -X POST http://<service-ip>:<port>/config/reload
```

The pipeline will automatically reload the configuration within 30 seconds.

### Updating via Helm

To update the configuration file and redeploy:

```bash
# 1. Update values.yaml or use --set flags
# 2. Upgrade the release
helm upgrade nutanix-aidp-connector ./helm/nutanix-aidp-connector -f my-values.yaml
```

Configuration changes are managed via Helm values and `helm upgrade` commands. See the [Updating Configuration](#updating-configuration) section above.

## Chart Structure

```
helm/nutanix-aidp-connector/
├── Chart.yaml                    # Chart metadata
├── values.yaml                   # Default values
├── build-and-push-image.sh       # Legacy build helper (wraps scripts/build-and-push-image.sh)
├── cleanup-existing-resources.sh # Helper script to clean up orphaned resources
├── files/
├── templates/
│   ├── _helpers.tpl              # Template helpers
│   ├── configmap.yaml            # ConfigMap template (renders config.yaml for hot-reload)
│   ├── secret.yaml               # Secret template (S3 + Objects credentials)
│   ├── pvc.yaml                  # (reserved — PVCs now managed by StatefulSet volumeClaimTemplates)
│   ├── statefulset.yaml          # StatefulSet template (multi-replica, leader-only baseline)
│   ├── headless-service.yaml     # Headless Service for StatefulSet stable DNS
│   ├── service.yaml              # Service template (config API + metrics ports)
│   ├── servicemonitor.yaml       # Prometheus ServiceMonitor (name includes Release.Namespace)
│   └── pre-install-validation-hook.yaml  # Pre-install validation
└── README.md                     # This file
```

> **Note**: The recommended build script is `scripts/build-containerized.sh` (from project root). The `build-and-push-image.sh` in this directory is a legacy wrapper.

## Monitoring

### Accessing Metrics

The pipeline exposes Prometheus metrics at `/metrics` on the configured port (default: 9090).

```bash
# Port-forward to access metrics locally
kubectl port-forward -n ${NAMESPACE} svc/nutanix-aidp-connector 9090:9090

# View metrics
curl http://localhost:9090/metrics
```

### Grafana Dashboard

If Grafana is deployed with Prometheus, you can create dashboards using the exported metrics:

1. Navigate to Grafana UI
2. Add Prometheus as a data source (if not already configured)
3. Create a new dashboard with panels for:
   - `nutanix-aidp-connector_active_jobs` - Current active jobs gauge
   - `rate(nutanix-aidp-connector_jobs_submitted_total[5m])` - Job submission rate
   - `rate(nutanix-aidp-connector_files_processed_total[5m])` - File processing rate
   - `histogram_quantile(0.95, nutanix-aidp-connector_job_duration_seconds_bucket)` - P95 job duration

### Prometheus Query Examples

#### Job Status Code Monitoring

**Total errors by HTTP status code:**
```promql
sum(rate(nutanix-aidp-connector_job_errors_by_status_code_total[5m])) by (status_code)
```

**Validation failures (jobs rejected during validation):**
```promql
sum(rate(nutanix-aidp-connector_job_status_codes_total{status_code="404", error_type="validation_failure"}[5m]))
```

**Processing failures (jobs that failed during execution):**
```promql
sum(rate(nutanix-aidp-connector_job_status_codes_total{status_code="503", error_type="processing_failure"}[5m]))
```

**Success rate (HTTP 200):**
```promql
sum(rate(nutanix-aidp-connector_job_status_codes_total{status_code="200"}[5m])) / 
sum(rate(nutanix-aidp-connector_job_status_codes_total[5m]))
```

**Error rate by error type:**
```promql
sum(rate(nutanix-aidp-connector_job_status_codes_total{status_code!="200", status_code!="202"}[5m])) by (error_type)
```

**HTTP 404 breakdown (validation vs expired):**
```promql
sum(rate(nutanix-aidp-connector_job_status_codes_total{status_code="404"}[5m])) by (error_type)
```

**HTTP 410 (expired results) count:**
```promql
sum(rate(nutanix-aidp-connector_job_status_codes_total{status_code="410"}[5m]))
```

**All status codes over time:**
```promql
sum(rate(nutanix-aidp-connector_job_status_codes_total[5m])) by (status_code)
```

#### Alert Examples

**Alert on high validation failure rate:**
```promql
rate(nutanix-aidp-connector_job_status_codes_total{status_code="404", error_type="validation_failure"}[5m]) > 0.1
```

**Alert on processing failures:**
```promql
rate(nutanix-aidp-connector_job_status_codes_total{status_code="503", error_type="processing_failure"}[5m]) > 0.05
```

**Alert on low success rate:**
```promql
(
  sum(rate(nutanix-aidp-connector_job_status_codes_total{status_code="200"}[5m])) / 
  sum(rate(nutanix-aidp-connector_job_status_codes_total[5m]))
) < 0.9
```

### Prometheus Targets

Verify the pipeline is being scraped by Prometheus:

```bash
# Check ServiceMonitor (namespace from telemetry.serviceMonitorNamespace in values.yaml)
kubectl get servicemonitor -n <telemetry.serviceMonitorNamespace> | grep nutanix-aidp-connector

# Check Prometheus targets (via Prometheus UI)
# Navigate to: Status → Targets → Look for nutanix-aidp-connector
```

## Additional Resources

- **Architecture**: See [ARCHITECTURE.md](../../ARCHITECTURE.md) for system architecture details
- **Main README**: See [README.md](../../README.md) for project overview

## Support

For issues and questions, please contact: veer.kumar@nutanix.com
