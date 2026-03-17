#!/bin/bash
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com
#
# Containerized Build Script for Nutanix AIDP Connector
# This script builds Docker images inside a container to ensure consistency
# and avoid polluting the local environment

set -e

# Default values
# Note: These can be overridden by environment variables
REGISTRY="${REGISTRY:-veerkumarnutanix}"
IMAGE_NAME="${IMAGE_NAME:-nutanix-aidp-connector}"
VERSION="${VERSION:-v1.0.0}"
RESET="${RESET:-false}"
BUILD_TYPE="${BUILD_TYPE:-production}"
NO_PUSH="${NO_PUSH:-false}"

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
        --build-type)
            BUILD_TYPE="$2"
            shift 2
            ;;
        --no-push)
            NO_PUSH=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --registry REGISTRY     Docker registry (default: veerkumarnutanix)"
            echo "  --image-name NAME      Image name (default: nutanix-aidp-connector)"
            echo "  --version VERSION      Image version/tag (default: v1.0.0)"
            echo "  --reset                Clean build from scratch (no cache)"
            echo "  --build-type TYPE      Build type: production|build (default: production)"
            echo "  --no-push              Skip pushing image to registry"
            echo "  --help                 Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0 --version v3.0.0"
            echo "  $0 --reset --version v2.0.0"
            echo "  $0 --no-push           # Build only, don't push"
            echo "  $0 --build-type build  # Build development/test container"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Determine Dockerfile based on build type
if [ "$BUILD_TYPE" = "build" ]; then
    DOCKERFILE="Dockerfile.build"
    CONTAINER_IMAGE="${IMAGE_NAME}-builder:latest"
else
    DOCKERFILE="Dockerfile"
    CONTAINER_IMAGE="${IMAGE_NAME}:latest"
fi

# Docker image name for local build
LOCAL_IMAGE="${IMAGE_NAME}:latest"
REGISTRY_IMAGE="${REGISTRY}/${IMAGE_NAME}:${VERSION}"

echo "=========================================="
echo "Containerized Build for Nutanix AIDP Connector"
echo "=========================================="
echo "Registry:    $REGISTRY"
echo "Image Name:  $IMAGE_NAME"
echo "Version:     $VERSION"
echo "Build Type:  $BUILD_TYPE"
echo "Reset Mode:  $RESET"
echo "Dockerfile:  $DOCKERFILE"
echo "Project Root: $PROJECT_ROOT"
echo "=========================================="
echo ""

# Check if Docker is running and accessible
echo "Checking Docker availability..."
if ! command -v docker > /dev/null 2>&1; then
    echo "Error: Docker command not found. Please install Docker."
    exit 1
fi

# Simple Docker check - verify we can run docker ps
# Show actual error if it fails
DOCKER_CHECK=$(docker ps 2>&1)
if [ $? -ne 0 ]; then
    echo "Error: Cannot connect to Docker daemon."
    echo ""
    echo "Docker error: $DOCKER_CHECK"
    echo ""
    echo "Possible solutions:"
    echo "  1. Docker daemon not running: sudo systemctl start docker"
    echo "  2. Permission denied: sudo usermod -aG docker $USER (then logout/login)"
    echo "  3. Use sudo: sudo $0 $@"
    exit 1
fi

echo "✓ Docker is available"

# Change to project root
cd "$PROJECT_ROOT"

# Build the builder container if it doesn't exist or if reset is requested
BUILDER_IMAGE="nutanix-aidp-connector-builder:latest"
if ! docker images --format "{{.Repository}}:{{.Tag}}" | grep -q "^nutanix-aidp-connector-builder:latest$"; then
    echo "Building builder container..."
    docker build -t ${BUILDER_IMAGE} -f Dockerfile.build .
    if [ $? -ne 0 ]; then
        echo "Error: Failed to build builder container"
        exit 1
    fi
    echo "✓ Builder container built successfully"
else
    echo "✓ Using existing builder container"
fi

# Prepare build command — reuse cached builder as base to avoid Docker Hub rate limits
BUILD_CMD="docker build --build-arg BASE_IMAGE=${BUILDER_IMAGE}"
if [ "$RESET" = true ]; then
    BUILD_CMD="$BUILD_CMD --build-arg PIP_CACHEBUST=$(date +%s)"
fi
BUILD_CMD="$BUILD_CMD -t ${LOCAL_IMAGE} -f ${DOCKERFILE} ."

# Run build inside container using docker-in-docker
# Note: DOCKER_API_VERSION ensures compatibility between container's Docker CLI and host's Docker daemon
echo "Building ${BUILD_TYPE} image inside container..."
docker run --rm \
    -v "$PROJECT_ROOT:/workspace/project" \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -w /workspace/project \
    -e DOCKER_BUILDKIT=0 \
    -e DOCKER_API_VERSION=1.43 \
    ${BUILDER_IMAGE} \
    sh -c "$BUILD_CMD"

if [ $? -ne 0 ]; then
    echo "Error: Build failed inside container"
    exit 1
fi
echo "✓ Build completed successfully"

# Tag for registry
echo ""
echo "Tagging image for registry..."
docker tag ${LOCAL_IMAGE} ${REGISTRY_IMAGE}
echo "Tagged: ${LOCAL_IMAGE} -> ${REGISTRY_IMAGE}"

# Push to registry (unless --no-push is specified)
if [ "$NO_PUSH" = true ]; then
    echo ""
    echo "=========================================="
    echo "✓ Image built successfully (push skipped)"
    echo "=========================================="
    echo "Local image: ${LOCAL_IMAGE}"
    echo "Tagged as: ${REGISTRY_IMAGE}"
    echo ""
    echo "To push manually:"
    echo "  docker login"
    echo "  docker push ${REGISTRY_IMAGE}"
    exit 0
fi

echo ""
echo "Pushing image to registry..."
echo "Note: You may need to authenticate first: docker login"
if docker push ${REGISTRY_IMAGE}; then
    echo ""
    echo "=========================================="
    echo "✓ Image successfully built and pushed!"
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
