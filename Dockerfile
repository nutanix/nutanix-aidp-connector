# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com

# Multi-stage Dockerfile for Nutanix AIDP-Connector
# BASE_IMAGE can be overridden to avoid Docker Hub pulls (e.g. --build-arg BASE_IMAGE=nutanix-aidp-connector-builder:latest)
ARG BASE_IMAGE=python:3.12-slim

# Stage 1: Base with system deps and requirements only (never invalidated by --reset)
FROM ${BASE_IMAGE} AS builder-base
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .

# Stage 2: Pip install only (--reset only invalidates this stage via PIP_CACHEBUST; apt-get stays in builder-base)
FROM builder-base AS builder
ARG PIP_CACHEBUST=
RUN echo "pip cachebust: ${PIP_CACHEBUST}" && pip install --upgrade pip && \
    pip install --no-cache-dir --target=/install -r requirements.txt

# Stage 3: Runtime stage (minimal production image)
FROM ${BASE_IMAGE}

# Set working directory
WORKDIR /app

# Install only runtime system dependencies (no build tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy Python dependencies from builder stage to site-packages
COPY --from=builder /install /usr/local/lib/python3.12/site-packages/

# Copy nutanix_aidp_connector code
COPY nutanix_aidp_connector/ /app/nutanix_aidp_connector/

# Copy utility scripts (for interactive use inside the pod)
COPY scripts/ /app/scripts/

# Create necessary directories
RUN mkdir -p /var/lib/nutanix-aidp-connector /var/log/nutanix-aidp-connector /tmp/nutanix-aidp-connector-downloads

# Create non-root user for security
RUN groupadd -r pipelineuser && useradd -r -g pipelineuser pipelineuser
RUN chown -R pipelineuser:pipelineuser /app /var/lib/nutanix-aidp-connector /var/log/nutanix-aidp-connector /tmp/nutanix-aidp-connector-downloads
USER pipelineuser

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/app/nutanix_aidp_connector"
ENV PIPELINE_CONFIG="/app/nutanix_aidp_connector/config/config.yaml"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Run Nutanix AIDP-Connector
WORKDIR /app/nutanix_aidp_connector
CMD ["python", "-m", "main"]

