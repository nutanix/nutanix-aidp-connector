#!/bin/bash
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com
#
# Build and Push Docker Image Script (Traditional)
# This script builds Docker images locally and pushes them to a registry
# Can be used standalone or called from other scripts
#
# NOTE: For containerized builds (recommended), use scripts/build-containerized.sh instead
# Containerized builds ensure consistency and don't require local Python/build tools

set -e

# Default values
REGISTRY="${REGISTRY:-veerkumarnutanix}"
IMAGE_NAME="${IMAGE_NAME:-nutanix-aidp-connector}"
VERSION="${VERSION:-v1.0.0}"
RESET="${RESET:-false}"
PUSH="${PUSH:-true}"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --registry)
            REGISTRY="$2"
            shift 2
            ;;
        --image-name)
            IMAGE_NAME="$2"
            shift 2
            ;;
        --version)
            VERSION="$2"
            shift 2
            ;;
        --reset)
            RESET=true
            shift
            ;;
        --no-push)
            PUSH=false
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --registry REGISTRY     Docker registry (default: veerkumarnutanix)"
            echo "  --image-name NAME       Image name (default: nutanix-aidp-connector)"
            echo "  --version VERSION       Image version/tag (default: v1.0.0)"
            echo "  --reset                 Force pip install to re-run (keeps base image and system deps cached)"
            echo "  --no-push               Build only; do not push to registry"
            echo "  --help                  Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  REGISTRY                Docker registry"
            echo "  IMAGE_NAME              Image name"
            echo "  VERSION                 Image version/tag"
            echo "  RESET                   Set to 'true' to re-run pip install only (no full --no-cache)"
            echo ""
            echo "Examples:"
            echo "  $0 --version v3.0.0"
            echo "  $0 --reset --version v2.0.0"
            echo "  REGISTRY=myregistry IMAGE_NAME=myapp VERSION=v1.0.0 $0"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Get script directory and project root (one level up from scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Docker image name for local build
LOCAL_IMAGE="${IMAGE_NAME}:latest"

echo "=========================================="
echo "Build and Push Docker Image"
echo "=========================================="
echo "Registry:    $REGISTRY"
echo "Image Name:  $IMAGE_NAME"
echo "Version:     $VERSION"
echo "Reset Mode:  $RESET"
echo "Project Root: $PROJECT_ROOT"
echo "=========================================="
echo ""

# Change to project root for docker build
cd "$PROJECT_ROOT"

# Handle --reset flag: only force pip install to re-run (invalidate from pip layer); keep base image and system deps cached
if [ "$RESET" = true ]; then
    echo "RESET mode: Forcing pip install to re-run (base image and apt-get remain cached)..."
    docker build --build-arg BASE_IMAGE=python:3.12-slim --build-arg PIP_CACHEBUST="$(date +%s)" -t ${LOCAL_IMAGE} -f Dockerfile .
    SOURCE_IMAGE="${LOCAL_IMAGE}"
else
    echo "Building image using docker build (with cache)..."
    docker build -t ${LOCAL_IMAGE} -f Dockerfile .
    SOURCE_IMAGE="${LOCAL_IMAGE}"
fi

# Tag for registry
echo ""
REGISTRY_IMAGE="${REGISTRY}/${IMAGE_NAME}:${VERSION}"
echo "Tagging image for registry..."
docker tag "${SOURCE_IMAGE}" "${REGISTRY_IMAGE}"
echo "Tagged: ${SOURCE_IMAGE} -> ${REGISTRY_IMAGE}"

# Push to registry (unless --no-push)
if [ "$PUSH" = true ]; then
    echo ""
    echo "Pushing image to registry..."
    echo "Note: You may need to authenticate first: docker login"
    if docker push "${REGISTRY_IMAGE}"; then
        echo ""
        echo "=========================================="
        echo "✓ Image successfully pushed!"
        echo "=========================================="
        echo "Image: ${REGISTRY_IMAGE}"
        echo ""
        echo "You can now use this image in Kubernetes/Helm:"
        echo "  image: ${REGISTRY_IMAGE}"
        echo ""
    else
        echo ""
        echo "=========================================="
        echo "✗ Failed to push image"
        echo "=========================================="
        echo "Image was built and tagged locally: ${REGISTRY_IMAGE}"
        echo "To push manually:"
        echo "  1. docker login"
        echo "  2. docker push ${REGISTRY_IMAGE}"
        exit 1
    fi
else
    echo ""
    echo "=========================================="
    echo "✓ Build complete (push skipped with --no-push)"
    echo "=========================================="
    echo "Image: ${REGISTRY_IMAGE}"
    echo "To push later: docker push ${REGISTRY_IMAGE}"
    echo ""
fi

