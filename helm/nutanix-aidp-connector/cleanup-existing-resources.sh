#!/bin/bash
# Copyright (c) 2025 Nutanix Inc. All rights reserved.
#
# Author: veer.kumar@nutanix.com
#
# Cleanup script to remove existing Nutanix AIDP-Connector resources before Helm installation
# This is needed when resources were created manually (not by Helm)

set -e

export KUBECONFIG="${KUBECONFIG:-k8s/kubeconfig.yaml}"
NAMESPACE="${NAMESPACE:-nutanix-aidp-connector}"

echo "=========================================="
echo "Cleaning up existing Nutanix AIDP-Connector resources"
echo "Namespace: $NAMESPACE"
echo "=========================================="
echo ""

# Check what exists
echo "Checking for existing resources..."
EXISTING=$(kubectl get secret,configmap,statefulset,deployment,service,pvc,servicemonitor -n $NAMESPACE 2>/dev/null | grep nutanix-aidp-connector || true)

if [ -z "$EXISTING" ]; then
    echo "✓ No existing resources found. Ready for Helm installation."
    exit 0
fi

echo "Found existing resources:"
echo "$EXISTING"
echo ""

# Confirm deletion
read -p "Do you want to delete these resources? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted. Resources not deleted."
    exit 1
fi

echo ""
echo "Deleting resources..."

# Delete Secret
echo -n "  Deleting Secret... "
kubectl delete secret nutanix-aidp-connector-secrets -n $NAMESPACE 2>/dev/null && echo "✓" || echo "  (not found)"

# Delete ConfigMap
echo -n "  Deleting ConfigMap... "
kubectl delete configmap nutanix-aidp-connector-config -n $NAMESPACE 2>/dev/null && echo "✓" || echo "  (not found)"

# Delete StatefulSet (current) or Deployment (legacy)
echo -n "  Deleting StatefulSet... "
kubectl delete statefulset nutanix-aidp-connector -n $NAMESPACE 2>/dev/null && echo "✓" || echo "  (not found)"
echo -n "  Deleting Deployment (legacy)... "
kubectl delete deployment nutanix-aidp-connector -n $NAMESPACE 2>/dev/null && echo "✓" || echo "  (not found)"

# Delete Services (main + headless)
echo -n "  Deleting Service... "
kubectl delete service nutanix-aidp-connector -n $NAMESPACE 2>/dev/null && echo "✓" || echo "  (not found)"
echo -n "  Deleting Headless Service... "
kubectl delete service nutanix-aidp-connector-headless -n $NAMESPACE 2>/dev/null && echo "✓" || echo "  (not found)"

# Delete PVCs (optional — StatefulSet volumeClaimTemplates create per-pod PVCs)
# WARNING: This will delete persistent data (baseline state, logs)!
if [ "${DELETE_PVC:-false}" = "true" ]; then
    echo "  Deleting PVCs..."
    for pvc in $(kubectl get pvc -n $NAMESPACE -o name 2>/dev/null | grep nutanix-aidp-connector); do
        echo -n "    $pvc... "
        kubectl delete $pvc -n $NAMESPACE 2>/dev/null && echo "✓" || echo "  (not found)"
    done
else
    echo "  Skipping PVC deletion (set DELETE_PVC=true to delete)"
fi

echo ""
echo "=========================================="
echo "✓ Cleanup complete!"
echo "=========================================="
echo ""
echo "You can now install with Helm:"
echo ""
echo "  helm install nutanix-aidp-connector ./helm/nutanix-aidp-connector \\"
echo "    --set kafka.bootstrapServers=\"\${KAFKA_BOOTSTRAP_SERVERS}\" \\"
echo "    --set nemoRetriever.host=\"\${NEMO_RETRIEVER_HOST}\" \\"
echo "    --set milvus.host=\"\${MILVUS_HOST}\""
echo ""

