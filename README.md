<!--
Copyright (c) 2025 Nutanix Inc. All rights reserved.

Author: veer.kumar@nutanix.com
-->

# Nutanix AIDP-Connector

A fault-tolerant Kubernetes service that processes S3-compatible object storage (Nutanix Objects) via Kafka notifications, submits them to NeMo-Retriever for GPU-accelerated parsing and embedding, and stores vectors in Milvus.

## Features

- **Real-time & baseline processing** — Kafka-driven for new/delete objects; paginated S3 listing for existing data
- **NeMo-Retriever integration** — extract, split, embed, and store with per-file-type task configs
- **Milvus bulk insert** — batched inserts with dynamic fields, S3 tag/metadata enrichment
- **Multi-level flow control** — download, submission, and baseline throttling
- **Hot-reload configuration** — update `config.yaml` without pod restart (30 s detection)
- **Crash recovery** — Kafka offset-based for real-time; PVC-persisted state for baseline
- **Observability** — Prometheus histograms/counters via OpenTelemetry; REST health & config API

## Prerequisites

- Kubernetes 1.19+ and Helm 3.0+
- S3-compatible object storage (Nutanix Objects)
- Apache Kafka 3.x+ with S3 event notifications enabled
- NeMo-Retriever 26.1.2+ (GPU) — V1 and V2 APIs supported;
- Milvus 2.4+ vector database

## Quick Start

```bash
export NAMESPACE="nutanix-aidp-connector"
export AWS_ACCESS_KEY_ID="<access-key>"
export AWS_SECRET_ACCESS_KEY="<secret-key>"
export KAFKA_BOOTSTRAP_SERVERS="<kafka-host:port>"
export S3_ENDPOINT_URL="<s3-endpoint>"
export NEMO_RETRIEVER_HOST="<nemo-retriever-host>"
export NEMO_RETRIEVER_PORT="7670"
export MILVUS_HOST="<milvus-host>"
export MILVUS_PORT="19530"
export OBJECTS_ENDPOINT="<objects-endpoint:443>"
export OBJECTS_ACCESS_KEY="<objects-access-key>"
export OBJECTS_SECRET_KEY="<objects-secret-key>"

kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \
  --namespace ${NAMESPACE} \
  --create-namespace \
  --set global.namespace=${NAMESPACE} \
  --set kafka.bootstrapServers="${KAFKA_BOOTSTRAP_SERVERS}" \
  --set s3.endpointUrl="${S3_ENDPOINT_URL}" \
  --set s3.accessKey="${AWS_ACCESS_KEY_ID}" \
  --set s3.secretKey="${AWS_SECRET_ACCESS_KEY}" \
  --set nemoRetriever.host="${NEMO_RETRIEVER_HOST}" \
  --set nemoRetriever.port="${NEMO_RETRIEVER_PORT}" \
  --set storage.milvus.host="${MILVUS_HOST}" \
  --set storage.milvus.port="${MILVUS_PORT}" \
  --set storage.objects.endpointUrl="${OBJECTS_ENDPOINT}" \
  --set storage.objects.accessKey="${OBJECTS_ACCESS_KEY}" \
  --set storage.objects.secretKey="${OBJECTS_SECRET_KEY}"
```

See [helm/nutanix-aidp-connector/README.md](helm/nutanix-aidp-connector/README.md) for upgrade, uninstall, values reference, and troubleshooting.

## Building

```bash
# Production image (containerized build — recommended)
./scripts/build-containerized.sh --version v1.0.0

# Clean rebuild (invalidates pip cache only, not system packages)
./scripts/build-containerized.sh --reset --version v1.0.0

# Direct Docker build
docker build -t nutanix-aidp-connector:latest -f Dockerfile .
```

The Dockerfile uses a 3-stage build (builder-base → builder → runtime) so `--reset` only invalidates the pip layer.

## Architecture Overview

```
Nutanix Objects ──► Kafka ──► Nutanix AIDP-Connector Pod ──► NeMo-Retriever (GPU) ──► Milvus
                                │
                    ┌───────────┴────────────┐
                    │  Kafka Consumer         │
                    │  Baseline Orchestrator  │
                    │         ▼               │
                    │  Download → Batch →     │
                    │  Submit → Track →       │
                    │  Complete → Milvus      │
                    │                         │
                    │  Config API  (5001)     │
                    │  Prometheus  (9090)     │
                    └─────────────────────────┘
```

| Processing Mode | Description |
|-----------------|-------------|
| **Real-time** | Kafka consumer processes S3 event notifications as they arrive |
| **Baseline** | Lists existing S3 objects in configured buckets with crash-safe state |

See [ARCHITECTURE.md](ARCHITECTURE.md) for component details, threading model, flow control, crash recovery, and design decisions.

## Project Structure

```
nutanix_aidp_connector/
├── main.py                         # Entry point & orchestrator
├── config/                         # Pydantic settings, hot-reload managers
├── kafka_consumer/                 # Kafka consumer with auto-offset
├── s3_downloader/                  # S3 downloads with retry
├── batch_manager/                  # Time/size/count batch window
├── nemo_retriever/                      # Job submitter, status tracker, task defs
├── completion/                     # Completion handler, Milvus batch inserter
├── baseline/                       # Orchestrator, object lister, state manager
├── delete_handler.py               # Batched Milvus deletes on S3 remove events
├── storage/                        # Milvus collection management
├── api/                            # Flask config API + Prometheus endpoint
└── utils/                          # Logging, telemetry

helm/nutanix-aidp-connector/                # Helm chart (values, templates, README)
scripts/                            # Build scripts, aidp_cmd_tool, run_recall
tests/                              # Unit & integration tests
```

## Documentation

| Document | Contents |
|----------|----------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Deep-dive: components, threading, flow control, crash recovery, metrics, API reference, configuration, troubleshooting |
| [docs/PERFORMANCE_TUNING.md](docs/PERFORMANCE_TUNING.md) | Pipeline latency monitoring, per-stage tuning knobs, bottleneck diagnosis, NeMo-Retriever overload prevention |
| [helm/nutanix-aidp-connector/README.md](helm/nutanix-aidp-connector/README.md) | Helm chart: installation, upgrade, values reference, Prometheus queries, Kubernetes access |
