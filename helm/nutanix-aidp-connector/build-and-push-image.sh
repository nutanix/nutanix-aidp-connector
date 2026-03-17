#!/bin/bash
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com
#
# Build and Push Docker Image for Helm Chart
# This script calls the main build script from the project root

# Get the project root (two levels up from helm/nutanix-aidp-connector/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_SCRIPT="${PROJECT_ROOT}/scripts/build-and-push-image.sh"

# Check if build script exists
if [ ! -f "$BUILD_SCRIPT" ]; then
    echo "Error: Build script not found at $BUILD_SCRIPT"
    echo "Make sure you're running this from the helm chart directory"
    exit 1
fi

# Default values (can be overridden)
REGISTRY="${REGISTRY:-veerkumarnutanix}"
IMAGE_NAME="${IMAGE_NAME:-nutanix-aidp-connector}"
VERSION="${VERSION:-v3.0.0}"  # Default to v3.0.0 to match Helm values
RESET="${RESET:-false}"

# Parse arguments and pass them to the main script
"$BUILD_SCRIPT" "$@"

