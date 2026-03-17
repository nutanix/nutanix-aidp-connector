#!/bin/bash
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Quick script to test hot-reloading embed model configuration
# Usage: ./test_hot_reload.sh [namespace] [model_name]

set -e

NAMESPACE="${1:-nutanix-aidp-connector4}"
NEW_MODEL="${2:-embed}"
CONFIGMAP_NAME="nutanix-aidp-connector-config"

echo "=========================================="
echo "Testing Hot-Reload: Embed Model"
echo "=========================================="
echo "Namespace: ${NAMESPACE}"
echo "New Model: ${NEW_MODEL}"
echo ""

# Check if namespace exists
if ! kubectl get namespace ${NAMESPACE} &>/dev/null; then
    echo "❌ Error: Namespace '${NAMESPACE}' not found"
    exit 1
fi

# Check if ConfigMap exists
if ! kubectl get configmap ${CONFIGMAP_NAME} -n ${NAMESPACE} &>/dev/null; then
    echo "❌ Error: ConfigMap '${CONFIGMAP_NAME}' not found in namespace '${NAMESPACE}'"
    exit 1
fi

# Get pod name
POD_NAME=$(kubectl get pods -n ${NAMESPACE} -l app=nutanix-aidp-connector -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -z "${POD_NAME}" ]; then
    echo "❌ Error: No nutanix-aidp-connector pod found in namespace '${NAMESPACE}'"
    exit 1
fi

echo "✅ Found pod: ${POD_NAME}"
echo ""

# Step 1: Get current model
echo "Step 1: Checking current model..."
echo "-----------------------------------"
CURRENT_MODEL=$(kubectl get configmap ${CONFIGMAP_NAME} -n ${NAMESPACE} -o jsonpath='{.data.config\.yaml}' | grep -A 2 "embed:" | grep "dimensions:" | awk '{print $2}' | tr -d '"')
echo "Current model: ${CURRENT_MODEL}"
echo ""

# Step 2: Extract ConfigMap
echo "Step 2: Extracting ConfigMap..."
echo "-----------------------------------"
TEMP_FILE="/tmp/config_${NAMESPACE}.yaml"
kubectl get configmap ${CONFIGMAP_NAME} -n ${NAMESPACE} -o jsonpath='{.data.config\.yaml}' > ${TEMP_FILE}
echo "✅ Extracted to: ${TEMP_FILE}"
echo ""

# Step 3: Update model
echo "Step 3: Updating model to '${NEW_MODEL}'..."
echo "-----------------------------------"
if command -v yq &> /dev/null; then
    # Use yq if available
    yq eval ".embed_tasks.document_config.model = \"${NEW_MODEL}\"" -i ${TEMP_FILE}
    echo "✅ Updated using yq"
else
    # Use sed as fallback
    sed -i "s/model: \"${CURRENT_MODEL}\"/model: \"${NEW_MODEL}\"/g" ${TEMP_FILE}
    echo "✅ Updated using sed"
fi
echo ""

# Step 4: Apply updated ConfigMap
echo "Step 4: Applying updated ConfigMap..."
echo "-----------------------------------"
kubectl create configmap ${CONFIGMAP_NAME} \
  --from-file=config.yaml=${TEMP_FILE} \
  --dry-run=client -o yaml | kubectl apply -n ${NAMESPACE} -f -
echo "✅ ConfigMap updated"
echo ""

# Step 5: Wait a moment for file system sync
echo "Step 5: Waiting for file system sync..."
echo "-----------------------------------"
sleep 2
echo ""

# Step 6: Trigger manual reload
echo "Step 6: Triggering manual reload..."
echo "-----------------------------------"
# Kill any existing port-forward
pkill -f "port-forward.*${POD_NAME}.*5001" 2>/dev/null || true
sleep 1

# Start port-forward in background
kubectl port-forward pod/${POD_NAME} 5001:5001 -n ${NAMESPACE} > /tmp/port-forward.log 2>&1 &
PF_PID=$!
echo "✅ Port-forward started (PID: ${PF_PID})"
sleep 3

# Trigger reload
RELOAD_RESPONSE=$(curl -s -X POST http://localhost:5001/config/reload)
echo "Reload response: ${RELOAD_RESPONSE}"
echo ""

# Step 7: Verify the change
echo "Step 7: Verifying the change..."
echo "-----------------------------------"
sleep 2
VERIFY_RESPONSE=$(curl -s http://localhost:5001/config/embed/pdf 2>/dev/null || echo "{}")
if echo "${VERIFY_RESPONSE}" | grep -q "${NEW_MODEL}"; then
    echo "✅ Model successfully updated to '${NEW_MODEL}'"
else
    echo "⚠️  Could not verify via API. Checking logs..."
fi
echo ""

# Step 8: Check logs
echo "Step 8: Checking logs for reload confirmation..."
echo "-----------------------------------"
kubectl logs ${POD_NAME} -n ${NAMESPACE} --tail=30 | grep -i "reload\|configuration.*changed\|model.*embed" || echo "No reload messages found in recent logs"
echo ""

# Cleanup
echo "Cleaning up..."
kill ${PF_PID} 2>/dev/null || true
rm -f ${TEMP_FILE}
echo "✅ Cleanup complete"
echo ""

echo "=========================================="
echo "Hot-Reload Test Complete!"
echo "=========================================="
echo ""
echo "To verify manually:"
echo "  1. kubectl logs ${POD_NAME} -n ${NAMESPACE} | grep -i reload"
echo "  2. kubectl port-forward pod/${POD_NAME} 5001:5001 -n ${NAMESPACE}"
echo "  3. curl http://localhost:5001/config/embed/pdf | jq"
echo ""


