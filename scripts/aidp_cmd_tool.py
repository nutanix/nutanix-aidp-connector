#!/usr/bin/env python3
"""
Batch Size Experiment Script for Nutanix AIDP Connector Pipeline

This script runs experiments with different batch sizes to measure performance.
It automates the deployment, file upload, and monitoring process.

Usage:
    python scripts/aidp_cmd_tool.py

The script will prompt you to choose:
    1. Upload objects to bucket only (with optional S3 tags)
    2. Run full experiment (uninstall, delete bucket, install, upload, wait, monitor)
    3. List objects in bucket (with optional tag filter)
    4. List Milvus records (with optional tag filter)

Environment Variables Required:
    NAMESPACE, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, KAFKA_BOOTSTRAP_SERVERS,
    S3_ENDPOINT_URL, NEMO_RETRIEVER_HOST, NEMO_RETRIEVER_PORT, MILVUS_HOST, MILVUS_PORT,
    OBJECTS_ENDPOINT, OBJECTS_ACCESS_KEY, OBJECTS_SECRET_KEY

Environment Variables Optional:
    LOCAL_FILES_DIR - Local directory containing files to upload (default: /Users/veer.kumar/Downloads/downloaded_files/pdf-uploads)
    KUBECONFIG - Path to kubeconfig file (default: scripts/kubeconfig_new.conf)

Author: veer.kumar@nutanix.com
"""

import os
import sys
import time
import subprocess
import json
import logging
import random
import re
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote as urlquote

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'aidp_cmd_tool_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

def _load_pod_config():
    """Try to load the Nutanix AIDP Connector config.yaml from well-known pod paths.

    Returns a dict with keys matching ENV_CONFIG, or {} if not inside a pod.
    """
    import yaml
    candidate_paths = [
        os.getenv('PIPELINE_CONFIG', ''),
        '/app/nutanix_aidp_connector/config-data/config.yaml',
        '/app/nutanix_aidp_connector/config/config.yaml',
    ]
    for cfg_path in candidate_paths:
        if cfg_path and os.path.isfile(cfg_path):
            try:
                with open(cfg_path) as f:
                    cfg = yaml.safe_load(f) or {}
                s3_cfg = cfg.get('s3', {})
                nvi_cfg = cfg.get('nemo_retriever', {})
                stor_cfg = cfg.get('storage', {})
                mil_cfg = stor_cfg.get('milvus', {})
                obj_cfg = stor_cfg.get('objects', {})
                return {
                    'S3_ENDPOINT_URL': s3_cfg.get('endpoint_url'),
                    'KAFKA_BOOTSTRAP_SERVERS': cfg.get('kafka', {}).get('bootstrap_servers'),
                    'NEMO_RETRIEVER_HOST': nvi_cfg.get('host'),
                    'NEMO_RETRIEVER_PORT': str(nvi_cfg.get('port', '')),
                    'MILVUS_HOST': mil_cfg.get('host'),
                    'MILVUS_PORT': str(mil_cfg.get('port', '')),
                    'MILVUS_COLLECTION': mil_cfg.get('collection_name'),
                    'OBJECTS_ENDPOINT': obj_cfg.get('endpoint_url'),
                    'SOURCE_BUCKET': cfg.get('baseline', {}).get('buckets', [{}])[0].get('bucket_name'),
                }
            except Exception:
                pass
    return {}


def _env(*names, default=''):
    """Return the first non-empty value from the listed env var names."""
    for name in names:
        val = os.getenv(name, '')
        if val:
            return val
    return default


_pod_cfg = _load_pod_config()

# Environment variables — checks multiple env var names for compatibility
# when running outside the cluster vs inside the pod.
ENV_CONFIG = {
    'NAMESPACE': os.getenv('NAMESPACE', 'nutanix-aidp-connector'),
    'AWS_ACCESS_KEY_ID': _env('AWS_ACCESS_KEY_ID', 'S3_ACCESS_KEY', 'OBJECTS_ACCESS_KEY',
                              default='zrNtUAV2tiIcWSHNsyRB2KjKyHsUe4JO'),
    'AWS_SECRET_ACCESS_KEY': _env('AWS_SECRET_ACCESS_KEY', 'S3_SECRET_KEY', 'OBJECTS_SECRET_KEY',
                                  default='_PQnz8h85La0OM_p6iKF5ox8aAnJJJ4g'),
    'KAFKA_BOOTSTRAP_SERVERS': _env('KAFKA_BOOTSTRAP_SERVERS',
                                    default=_pod_cfg.get('KAFKA_BOOTSTRAP_SERVERS',
                                                         'my-cluster-kafka-external-bootstrap.kafka:9094')),
    'S3_ENDPOINT_URL': _env('S3_ENDPOINT_URL',
                            default=_pod_cfg.get('S3_ENDPOINT_URL',
                                                 'https://objects.dragon.cloudnative.nvdlab.net')),
    'NEMO_RETRIEVER_HOST': _env('NEMO_RETRIEVER_HOST',
                           default=_pod_cfg.get('NEMO_RETRIEVER_HOST', 'nemo-retriever.nemo-retriever')),
    'NEMO_RETRIEVER_PORT': _env('NEMO_RETRIEVER_PORT',
                           default=_pod_cfg.get('NEMO_RETRIEVER_PORT', '7670')),
    'MILVUS_HOST': _env('MILVUS_HOST',
                        default=_pod_cfg.get('MILVUS_HOST', 'milvus-vectordb.milvus')),
    'MILVUS_PORT': _env('MILVUS_PORT',
                        default=_pod_cfg.get('MILVUS_PORT', '19530')),
}

# Derived environment variables
ENV_CONFIG['OBJECTS_ENDPOINT'] = _env('OBJECTS_ENDPOINT',
                                      default=_pod_cfg.get('OBJECTS_ENDPOINT', ENV_CONFIG['S3_ENDPOINT_URL']))
ENV_CONFIG['OBJECTS_ACCESS_KEY'] = _env('OBJECTS_ACCESS_KEY', 'S3_ACCESS_KEY',
                                        default=ENV_CONFIG['AWS_ACCESS_KEY_ID'])
ENV_CONFIG['OBJECTS_SECRET_KEY'] = _env('OBJECTS_SECRET_KEY', 'S3_SECRET_KEY',
                                        default=ENV_CONFIG['AWS_SECRET_ACCESS_KEY'])

# Experiment configuration
MAX_OUTSTANDING_TASKS_LIST = [100, 400]  # Values to iterate over (batch_size will be set to each value)
SOURCE_BUCKET = _pod_cfg.get('SOURCE_BUCKET') or os.getenv('SOURCE_BUCKET', 'aidp-docs')
LOCAL_FILES_DIR = os.getenv('LOCAL_FILES_DIR', '/home/veer.kumar/downloaded_files/pdf-uploads/')
KUBECONFIG_PATH = os.getenv('KUBECONFIG', 'scripts/kubeconfig_new.conf')
HELM_CHART_PATH = './helm/nutanix-aidp-connector'
BASE_COLLECTION_NAME = _pod_cfg.get('MILVUS_COLLECTION') or os.getenv('MILVUS_COLLECTION', 'nutanix_aidp_connector_objects_nemo_retriever_batch')

# Timing configuration
UNINSTALL_WAIT_SECONDS = 60
POD_READY_TIMEOUT_SECONDS = 400  # 5 minutes
INITIAL_WAIT_SECONDS = 240  # 240/60 = 4 minutes - wait before monitoring starts
MONITORING_DURATION_SECONDS = 4800  # 4800/60 = 80 minutes - monitoring window after initial wait
JOB_CHECK_INTERVAL_SECONDS = 60  # Check every 1 minutes

# ============================================================================
# Helper Functions
# ============================================================================

def run_command(cmd, check=True, capture_output=True, timeout=300):
    """Run a shell command and return the result."""
    logger.debug(f"Running command: {cmd}")
    try:
        # Use stdout/stderr PIPE for Python 3.6 compatibility (capture_output added in 3.7)
        # Use universal_newlines for Python 3.6 compatibility (text added in 3.7)
        # Use dict() constructor for Python 3.6 compatibility (dictionary unpacking requires 3.5+)
        env = dict(os.environ)
        env['KUBECONFIG'] = KUBECONFIG_PATH
        
        kwargs = {
            'shell': True,
            'check': check,
            'universal_newlines': True,
            'timeout': timeout,
            'env': env
        }
        if capture_output:
            kwargs['stdout'] = subprocess.PIPE
            kwargs['stderr'] = subprocess.PIPE
        
        result = subprocess.run(cmd, **kwargs)
        return result
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {cmd}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        if check:
            raise
        return e
    except subprocess.TimeoutExpired as e:
        logger.error(f"Command timed out: {cmd}")
        raise


def get_kubectl_cmd():
    """Get kubectl command with kubeconfig."""
    return f"kubectl --kubeconfig={KUBECONFIG_PATH}"


def get_helm_cmd():
    """Get helm command with kubeconfig."""
    return f"helm --kubeconfig={KUBECONFIG_PATH}"


def release_milvus_collection(collection_name):
    """Release a Milvus collection from memory."""
    try:
        from pymilvus import connections, Collection
        
        logger.info(f"Releasing Milvus collection: {collection_name}")
        
        # Connect to Milvus
        connections.connect(
            alias='experiment',
            host=ENV_CONFIG['MILVUS_HOST'],
            port=int(ENV_CONFIG['MILVUS_PORT'])
        )
        
        try:
            collection = Collection(collection_name, using='experiment')
            collection.release()
            logger.info(f"Successfully released collection: {collection_name}")
        except Exception as e:
            # Collection might not exist or already released
            logger.warning(f"Could not release collection {collection_name}: {e}")
        finally:
            connections.disconnect('experiment')
            
    except ImportError:
        logger.warning("pymilvus not available, skipping collection release")
    except Exception as e:
        logger.error(f"Error releasing Milvus collection: {e}")


# ============================================================================
# S3 Tag Helpers
# ============================================================================

# Regex for characters allowed in S3 tag keys/values:
# Unicode letters, digits, whitespace, and: + - = . _ : / @
_S3_TAG_DISALLOWED_RE = re.compile(r'[^\w\s+\-=._:/@]', re.UNICODE)


def sanitize_s3_tag_value(raw_value, max_len=256):
    """Sanitize a string so it is a valid S3 object-tag value.

    AWS rules:
      - Max 256 UTF-8 characters
      - Allowed: Unicode letters, digits, whitespace, + - = . _ : / @
    """
    value = raw_value.strip()
    value = _S3_TAG_DISALLOWED_RE.sub('_', value)
    return value[:max_len]


def sanitize_s3_tag_key(raw_key, max_len=128):
    """Sanitize a string so it is a valid S3 object-tag key.

    AWS rules:
      - Max 128 UTF-8 characters
      - Same character set as values
      - Must not start with 'aws:'
    """
    key = raw_key.strip()
    key = _S3_TAG_DISALLOWED_RE.sub('_', key)
    key = key[:max_len]
    if key.lower().startswith('aws:'):
        key = '_' + key
    return key


def parse_and_sanitize_tags(raw_input):
    """Parse comma-separated tags from user input.

    Supports two formats:
      - Simple values:  "finance, science"  -> [{'Key': 'category', 'Value': 'finance'}, ...]
      - Key=value:      "dept=finance, dept=science" -> [{'Key': 'dept', 'Value': 'finance'}, ...]

    Returns a list of {'Key': str, 'Value': str} dicts (one per tag the user
    entered).  Each object will later get exactly ONE of these assigned at
    random.
    """
    if not raw_input or not raw_input.strip():
        return []

    tags = []
    for part in raw_input.split(','):
        part = part.strip()
        if not part:
            continue
        if '=' in part:
            key_raw, _, val_raw = part.partition('=')
            key = sanitize_s3_tag_key(key_raw)
            value = sanitize_s3_tag_value(val_raw)
        else:
            key = 'category'
            value = sanitize_s3_tag_value(part)

        if key and value:
            tags.append({'Key': key, 'Value': value})

    return tags


def prompt_for_tags():
    """Prompt the user for tags to associate with uploaded objects."""
    print("\nEnter tags to associate with uploaded objects.")
    print("  - Comma-separated values, e.g.: finance, science, engineering")
    print("  - Or key=value pairs, e.g.: dept=finance, dept=science")
    print("  - Press Enter to skip (no tags)")
    try:
        raw = input("\nTags: ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return []

    tags = parse_and_sanitize_tags(raw)
    if tags:
        print(f"Parsed {len(tags)} tag(s):")
        for t in tags:
            print(f"  {t['Key']}={t['Value']}")
    else:
        print("No tags will be applied.")
    return tags


def build_tagging_header(tag_dict):
    """Build the URL-encoded Tagging header string for S3 PutObject.

    tag_dict: single {'Key': ..., 'Value': ...} dict
    Returns:  'key=value' (URL-encoded)
    """
    return f"{urlquote(tag_dict['Key'])}={urlquote(tag_dict['Value'])}"


# ============================================================================
# S3 Operations
# ============================================================================

def get_s3_client():
    """Create and return an S3 client."""
    import boto3
    from botocore.config import Config
    import urllib3
    
    # Disable SSL warnings for self-signed certs
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    session = boto3.Session(
        aws_access_key_id=ENV_CONFIG['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=ENV_CONFIG['AWS_SECRET_ACCESS_KEY']
    )
    
    s3_client = session.client(
        's3',
        endpoint_url=ENV_CONFIG['S3_ENDPOINT_URL'],
        verify=False,
        config=Config(signature_version='s3v4')
    )
    
    return s3_client


def delete_all_objects_in_bucket(bucket_name):
    """Delete all objects in the specified S3 bucket."""
    logger.info(f"Deleting all objects in bucket: {bucket_name}")
    
    s3_client = get_s3_client()
    
    try:
        # List all objects
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        
        total_deleted = 0
        for page in pages:
            if 'Contents' not in page:
                continue
            
            objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
            
            if objects_to_delete:
                response = s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': objects_to_delete}
                )
                deleted_count = len(response.get('Deleted', []))
                total_deleted += deleted_count
                logger.info(f"Deleted {deleted_count} objects from {bucket_name}")
        
        logger.info(f"Total objects deleted from {bucket_name}: {total_deleted}")
        return total_deleted
        
    except Exception as e:
        logger.error(f"Error deleting objects from {bucket_name}: {e}")
        raise


def upload_files_to_bucket(local_dir, bucket_name, prefix='pdf-uploads', tags=None):
    """Upload all files from local directory to S3 bucket.

    Parameters
    ----------
    tags : list[dict] | None
        List of {'Key': ..., 'Value': ...} dicts.  If provided, each object
        is assigned exactly one tag chosen uniformly at random.
    """
    logger.info(f"Uploading files from {local_dir} to {bucket_name}/{prefix}")
    if tags:
        logger.info(f"Will randomly assign one of {len(tags)} tag(s) to each object")
    
    s3_client = get_s3_client()
    local_path = Path(local_dir)
    
    if not local_path.exists():
        logger.error(f"Local directory does not exist: {local_dir}")
        raise FileNotFoundError(f"Directory not found: {local_dir}")
    
    uploaded_count = 0
    total_size = 0
    tag_counts = {f"{t['Key']}={t['Value']}": 0 for t in tags} if tags else {}
    
    for file_path in local_path.glob('**/*'):
        if file_path.is_file():
            relative_path = file_path.relative_to(local_path)
            s3_key = f"{prefix}/{relative_path}" if prefix else str(relative_path)
            
            try:
                file_size = file_path.stat().st_size
                extra_args = {}
                if tags:
                    chosen_tag = random.choice(tags)
                    extra_args['Tagging'] = build_tagging_header(chosen_tag)
                    tag_label = f"{chosen_tag['Key']}={chosen_tag['Value']}"
                    tag_counts[tag_label] = tag_counts.get(tag_label, 0) + 1

                s3_client.upload_file(
                    str(file_path), bucket_name, s3_key,
                    ExtraArgs=extra_args if extra_args else None,
                )
                uploaded_count += 1
                total_size += file_size
                
                if uploaded_count % 10 == 0:
                    logger.info(f"Uploaded {uploaded_count} files ({total_size / 1024 / 1024:.2f} MB)...")
                    
            except Exception as e:
                logger.error(f"Error uploading {file_path}: {e}")
                raise
    
    logger.info(f"Total files uploaded: {uploaded_count} ({total_size / 1024 / 1024:.2f} MB)")
    if tag_counts:
        logger.info("Tag distribution:")
        for tag_label, count in sorted(tag_counts.items()):
            logger.info(f"  {tag_label}: {count} objects")
    return uploaded_count


# ============================================================================
# Kubernetes/Helm Operations
# ============================================================================

def helm_uninstall(release_name='nutanix-aidp-connector'):
    """Uninstall the Helm release."""
    namespace = ENV_CONFIG['NAMESPACE']
    logger.info(f"Uninstalling Helm release '{release_name}' from namespace '{namespace}'")
    
    cmd = f"{get_helm_cmd()} uninstall {release_name} -n {namespace}"
    result = run_command(cmd, check=False)
    
    if result.returncode == 0:
        logger.info(f"Successfully uninstalled {release_name}")
    else:
        logger.warning(f"Helm uninstall returned non-zero: {result.stderr}")
    
    return result.returncode == 0


def helm_install(max_outstanding_tasks):
    """Install the Helm chart with batch size set to maxOutstandingTasks."""
    namespace = ENV_CONFIG['NAMESPACE']
    batch_size = max_outstanding_tasks  # Set batch size to maxOutstandingTasks
    collection_name = f"{BASE_COLLECTION_NAME}_{batch_size}"
    
    logger.info(f"Installing Helm release with maxOutstandingTasks={max_outstanding_tasks}, batch_size={batch_size}, collection={collection_name}")
    
    cmd = f"""{get_helm_cmd()} install nutanix-aidp-connector {HELM_CHART_PATH} \\
        --namespace "{namespace}" \\
        --set global.namespace="{namespace}" \\
        --set aws.accessKeyId="{ENV_CONFIG['AWS_ACCESS_KEY_ID']}" \\
        --set aws.secretAccessKey="{ENV_CONFIG['AWS_SECRET_ACCESS_KEY']}" \\
        --set kafka.bootstrapServers="{ENV_CONFIG['KAFKA_BOOTSTRAP_SERVERS']}" \\
        --set s3.endpointUrl="{ENV_CONFIG['S3_ENDPOINT_URL']}" \\
        --set nemoRetriever.host="{ENV_CONFIG['NEMO_RETRIEVER_HOST']}" \\
        --set nemoRetriever.port="{ENV_CONFIG['NEMO_RETRIEVER_PORT']}" \\
        --set storage.milvus.host="{ENV_CONFIG['MILVUS_HOST']}" \\
        --set storage.milvus.port="{ENV_CONFIG['MILVUS_PORT']}" \\
        --set storage.milvus.collectionName="{collection_name}" \\
        --set storage.objects.endpointUrl="{ENV_CONFIG['OBJECTS_ENDPOINT']}" \\
        --set storage.objects.accessKey="{ENV_CONFIG['OBJECTS_ACCESS_KEY']}" \\
        --set storage.objects.secretKey="{ENV_CONFIG['OBJECTS_SECRET_KEY']}" \\
        --set s3.accessKey="{ENV_CONFIG['AWS_ACCESS_KEY_ID']}" \\
        --set s3.secretKey="{ENV_CONFIG['AWS_SECRET_ACCESS_KEY']}" \\
        --set batching.maxFiles="{batch_size}" """
    
    result = run_command(cmd, timeout=120)
    
    if result.returncode == 0:
        logger.info(f"Successfully installed Helm release with batch_size={batch_size}")
    else:
        logger.error(f"Helm install failed: {result.stderr}")
    
    return result.returncode == 0


def wait_for_pod_ready(timeout_seconds=POD_READY_TIMEOUT_SECONDS):
    """Wait for the Nutanix AIDP Connector pod to be in Running state."""
    namespace = ENV_CONFIG['NAMESPACE']
    logger.info(f"Waiting for pod to be ready in namespace '{namespace}' (timeout: {timeout_seconds}s)")
    
    start_time = time.time()
    
    while time.time() - start_time < timeout_seconds:
        cmd = f"{get_kubectl_cmd()} get pods -n {namespace} -l app=nutanix-aidp-connector -o json"
        result = run_command(cmd, check=False)
        
        if result.returncode == 0:
            try:
                pods_data = json.loads(result.stdout)
                pods = pods_data.get('items', [])
                
                for pod in pods:
                    pod_name = pod.get('metadata', {}).get('name', 'unknown')
                    phase = pod.get('status', {}).get('phase', 'Unknown')
                    
                    # Check container statuses
                    container_statuses = pod.get('status', {}).get('containerStatuses', [])
                    all_ready = all(cs.get('ready', False) for cs in container_statuses)
                    
                    logger.debug(f"Pod {pod_name}: phase={phase}, all_ready={all_ready}")
                    
                    if phase == 'Running' and all_ready:
                        logger.info(f"Pod {pod_name} is ready!")
                        return True, pod_name
                        
            except json.JSONDecodeError:
                logger.warning("Failed to parse pod status JSON")
        
        elapsed = int(time.time() - start_time)
        logger.info(f"Waiting for pod... ({elapsed}s / {timeout_seconds}s)")
        time.sleep(10)
    
    logger.error(f"Timeout waiting for pod to be ready after {timeout_seconds}s")
    return False, None


def get_pod_logs(pod_name, tail_lines=100):
    """Get recent logs from the specified pod."""
    namespace = ENV_CONFIG['NAMESPACE']
    cmd = f"{get_kubectl_cmd()} logs {pod_name} -n {namespace} --tail={tail_lines}"
    result = run_command(cmd, check=False)
    return result.stdout if result.returncode == 0 else ""


def check_active_jobs(pod_name):
    """Check if there are active jobs being processed."""
    logs = get_pod_logs(pod_name, tail_lines=50)
    
    # Look for indicators of active processing
    active_indicators = [
        '"active_jobs": 0',
        '"active_files": 0',
        '"processing": 0',
        '"pending": 0'
    ]
    
    # Check if all indicators show zero (no active jobs)
    all_idle = all(indicator in logs for indicator in active_indicators)
    
    # Also check for recent processing activity
    processing_keywords = ['Submitted batch', 'Processing completion', 'Bulk insert task']
    recent_activity = any(keyword in logs for keyword in processing_keywords)
    
    if all_idle and not recent_activity:
        logger.info("No active jobs detected - pipeline is idle")
        return False
    else:
        logger.info("Active jobs detected or recent activity found")
        return True


# ============================================================================
# List / Query Operations
# ============================================================================

def list_objects_with_tags(bucket_name, filter_tags=None):
    """List objects in an S3 bucket, optionally filtering by tags.

    S3 ListObjectsV2 does not support server-side tag filtering, so we list
    all objects and fetch each object's tags client-side.

    Parameters
    ----------
    filter_tags : list[dict] | None
        If provided, only show objects whose tags contain at least one of
        the given {'Key': ..., 'Value': ...} entries.
    """
    s3_client = get_s3_client()
    paginator = s3_client.get_paginator('list_objects_v2')

    filter_set = set()
    if filter_tags:
        for t in filter_tags:
            filter_set.add((t['Key'], t['Value']))

    total = 0
    matched = 0
    results = []

    logger.info(f"Listing objects in bucket '{bucket_name}' ...")
    if filter_set:
        logger.info(f"Filtering by tags: {[f'{k}={v}' for k, v in filter_set]}")
    else:
        logger.info("No tag filter — showing all objects")

    for page in paginator.paginate(Bucket=bucket_name):
        if 'Contents' not in page:
            continue
        for obj in page['Contents']:
            total += 1
            key = obj['Key']
            size = obj['Size']
            last_modified = obj.get('LastModified', '')

            obj_tags = {}
            try:
                tag_resp = s3_client.get_object_tagging(Bucket=bucket_name, Key=key)
                for tag in tag_resp.get('TagSet', []):
                    obj_tags[tag['Key']] = tag['Value']
            except Exception:
                pass

            if filter_set:
                if not any((k, v) in filter_set for k, v in obj_tags.items()):
                    continue

            matched += 1
            results.append({
                'key': key,
                'size': size,
                'last_modified': str(last_modified),
                'tags': obj_tags,
            })

            if total % 50 == 0:
                logger.info(f"  Scanned {total} objects so far, {matched} matched ...")

    # Print results
    print(f"\n{'='*80}")
    print(f"BUCKET: {bucket_name}  |  Total scanned: {total}  |  Matched: {matched}")
    print(f"{'='*80}")
    print(f"{'Key':<60} {'Size':>10} {'Tags'}")
    print(f"{'-'*60} {'-'*10} {'-'*30}")
    for r in results:
        tag_str = ', '.join(f"{k}={v}" for k, v in r['tags'].items()) if r['tags'] else '(none)'
        size_str = f"{r['size']:,}"
        key_display = r['key'] if len(r['key']) <= 58 else '...' + r['key'][-55:]
        print(f"{key_display:<60} {size_str:>10} {tag_str}")

    print(f"\nTotal: {matched} object(s)")
    return results


def list_milvus_records(collection_name, filter_tags=None, limit=50):
    """Query Milvus records, optionally filtering by S3 tags stored in content_metadata.

    Tags are stored in content_metadata as 's3_tag_{key}' by the Nutanix AIDP Connector pipeline.

    Parameters
    ----------
    filter_tags : list[dict] | None
        Tags to filter by.  Each dict has 'Key' and 'Value'.
        If empty/None, all records are returned (up to *limit*).
    limit : int
        Maximum number of records to return.
    """
    try:
        from pymilvus import MilvusClient
    except ImportError:
        logger.error("pymilvus is not installed. Run: pip install pymilvus")
        return []

    milvus_uri = f"http://{ENV_CONFIG['MILVUS_HOST']}:{ENV_CONFIG['MILVUS_PORT']}"
    client = MilvusClient(milvus_uri)

    # Build filter expression
    filter_expr = ""
    if filter_tags:
        clauses = []
        for t in filter_tags:
            milvus_key = f"s3_tag_{t['Key']}"
            escaped_val = t['Value'].replace('"', '\\"')
            clauses.append(f'content_metadata["{milvus_key}"] == "{escaped_val}"')
        filter_expr = " or ".join(clauses)
        logger.info(f"Milvus filter: {filter_expr}")
    else:
        filter_expr = "pk >= 0"
        logger.info("No tag filter — listing all records")

    logger.info(f"Querying collection '{collection_name}' (limit={limit}) ...")

    try:
        results = client.query(
            collection_name=collection_name,
            filter=filter_expr,
            output_fields=["pk", "vector", "source", "content_metadata", "text"],
            limit=limit,
        )
    except Exception as e:
        logger.error(f"Milvus query failed: {e}")
        return []

    VECTOR_DISPLAY_LEN = 5

    print(f"\n{'='*100}")
    print(f"COLLECTION: {collection_name}  |  Records returned: {len(results)}")
    if filter_tags:
        print(f"FILTER: {filter_expr}")
    print(f"{'='*100}")

    for i, rec in enumerate(results):
        print(f"\n--- Record {i+1} ---")

        for field in ["pk", "vector", "source", "content_metadata", "text"]:
            val = rec.get(field)
            if val is None:
                print(f"  {field}: null")
                continue

            if field == "vector":
                if isinstance(val, list) and len(val) > VECTOR_DISPLAY_LEN:
                    truncated = val[:VECTOR_DISPLAY_LEN]
                    print(f"  {field}: [{', '.join(f'{v:.6f}' for v in truncated)}, ... ] (dim={len(val)})")
                else:
                    print(f"  {field}: {val}")
            elif field in ("source", "content_metadata"):
                if isinstance(val, str):
                    try:
                        val = json.loads(val)
                    except Exception:
                        pass
                print(f"  {field}:")
                if isinstance(val, dict):
                    for k, v in val.items():
                        print(f"    {k}: {v}")
                else:
                    print(f"    {val}")
            elif field == "text":
                text_str = str(val)
                if len(text_str) > 200:
                    text_str = text_str[:200] + "..."
                print(f"  {field}: {text_str}")
            else:
                print(f"  {field}: {val}")

    print(f"\nTotal: {len(results)} record(s)")
    return results


# ============================================================================
# Experiment Functions
# ============================================================================

def run_experiment_for_batch_size(max_outstanding_tasks, experiment_results, tags=None):
    """Run the complete experiment with batch size set to maxOutstandingTasks."""
    batch_size = max_outstanding_tasks  # Set batch size to maxOutstandingTasks
    logger.info("=" * 80)
    logger.info(f"STARTING EXPERIMENT: maxOutstandingTasks={max_outstanding_tasks}, batch_size={batch_size}")
    logger.info("=" * 80)
    
    experiment_start = datetime.now()
    collection_name = f"{BASE_COLLECTION_NAME}_{batch_size}"
    result = {
        'max_outstanding_tasks': max_outstanding_tasks,
        'batch_size': batch_size,
        'collection_name': collection_name,
        'start_time': experiment_start.isoformat(),
        'status': 'unknown',
        'files_uploaded': 0,
        'duration_seconds': 0,
        'errors': []
    }
    
    try:
        # Step 1: Uninstall existing release
        logger.info("Step 1: Uninstalling existing Helm release...")
        helm_uninstall()
        
        logger.info(f"Waiting {UNINSTALL_WAIT_SECONDS}s after uninstall...")
        time.sleep(UNINSTALL_WAIT_SECONDS)
        
        # Step 2: Delete all objects in bucket
        logger.info("Step 2: Deleting all objects in source bucket...")
        delete_all_objects_in_bucket(SOURCE_BUCKET)
        
        # Step 3 & 4: Install with batch size set to maxOutstandingTasks
        logger.info(f"Step 3 & 4: Installing Helm chart with maxOutstandingTasks={max_outstanding_tasks}, batch_size={batch_size}...")
        if not helm_install(max_outstanding_tasks):
            result['status'] = 'install_failed'
            result['errors'].append('Helm install failed')
            return result
        
        # Step 5: Wait for pod to be ready
        logger.info("Step 5: Waiting for pod to be ready...")
        pod_ready, pod_name = wait_for_pod_ready()
        
        if not pod_ready:
            result['status'] = 'pod_not_ready'
            result['errors'].append('Pod did not become ready in time')
            return result
        
        result['pod_name'] = pod_name
        
        # Step 6: Upload files to S3
        logger.info("Step 6: Uploading files to S3...")
        try:
            files_uploaded = upload_files_to_bucket(LOCAL_FILES_DIR, SOURCE_BUCKET, tags=tags)
            result['files_uploaded'] = files_uploaded
        except Exception as e:
            result['status'] = 'upload_failed'
            result['errors'].append(f'File upload failed: {str(e)}')
            return result
        
        # Step 7: Initial wait period (no monitoring)
        logger.info(f"Step 7a: Waiting {INITIAL_WAIT_SECONDS/3600:.1f} hours before starting monitoring...")
        initial_wait_end = datetime.now() + timedelta(seconds=INITIAL_WAIT_SECONDS)
        
        while datetime.now() < initial_wait_end:
            remaining = (initial_wait_end - datetime.now()).total_seconds()
            logger.info(f"Initial wait - Time remaining: {remaining/60:.1f} minutes")
            time.sleep(300)  # Log every 5 minutes during initial wait
        
        # Step 8: Monitoring period
        logger.info(f"Step 7b: Starting {MONITORING_DURATION_SECONDS/60:.0f} minute monitoring period...")
        monitor_end_time = datetime.now() + timedelta(seconds=MONITORING_DURATION_SECONDS)
        
        last_activity_time = datetime.now()
        consecutive_idle_checks = 0
        required_idle_checks = 5  # Require 5 consecutive idle checks (5 minutes)
        
        while datetime.now() < monitor_end_time:
            time.sleep(JOB_CHECK_INTERVAL_SECONDS)
            
            has_active_jobs = check_active_jobs(pod_name)
            
            if has_active_jobs:
                last_activity_time = datetime.now()
                consecutive_idle_checks = 0
            else:
                consecutive_idle_checks += 1
                idle_duration = (datetime.now() - last_activity_time).total_seconds()
                logger.info(f"Pipeline idle for {idle_duration:.0f}s ({consecutive_idle_checks} consecutive checks)")
                
                if consecutive_idle_checks >= required_idle_checks:
                    logger.info(f"Pipeline has been idle for {required_idle_checks} consecutive checks - processing complete")
                    break
            
            remaining = (monitor_end_time - datetime.now()).total_seconds()
            logger.info(f"Monitoring - Time remaining: {remaining/60:.1f} minutes")
        
        result['status'] = 'completed'
        
    except Exception as e:
        logger.error(f"Experiment failed with error: {e}")
        result['status'] = 'error'
        result['errors'].append(str(e))
    
    experiment_end = datetime.now()
    result['end_time'] = experiment_end.isoformat()
    result['duration_seconds'] = (experiment_end - experiment_start).total_seconds()
    
    logger.info(f"Experiment completed: maxOutstandingTasks={max_outstanding_tasks}, batch_size={batch_size}, status={result['status']}, duration={result['duration_seconds']/60:.1f} minutes")
    
    # Release Milvus collection after experiment
    logger.info(f"Releasing Milvus collection: {collection_name}")
    release_milvus_collection(collection_name)
    
    experiment_results.append(result)
    return result


def save_results(results, filename='experiment_results.json'):
    """Save experiment results to a JSON file."""
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2)
    logger.info(f"Results saved to {filename}")


def print_summary(results):
    """Print a summary of all experiment results."""
    logger.info("\n" + "=" * 80)
    logger.info("EXPERIMENT SUMMARY")
    logger.info("=" * 80)
    
    for result in results:
        logger.info(f"\nMax Outstanding Tasks: {result.get('max_outstanding_tasks', result.get('batch_size', 'N/A'))}")
        logger.info(f"  Batch Size: {result['batch_size']}")
        logger.info(f"  Collection: {result['collection_name']}")
        logger.info(f"  Status: {result['status']}")
        logger.info(f"  Files Uploaded: {result['files_uploaded']}")
        logger.info(f"  Duration: {result['duration_seconds']/60:.1f} minutes")
        if result['errors']:
            logger.info(f"  Errors: {result['errors']}")
    
    logger.info("\n" + "=" * 80)


# ============================================================================
# User Interaction
# ============================================================================

def get_user_choice():
    """Get user choice for what to do."""
    print("\n" + "=" * 80)
    print("AIDP CONNECTOR BATCH SIZE EXPERIMENT - MENU")
    print("=" * 80)
    print("1. Upload objects to bucket only")
    print("2. Run full experiment (uninstall, delete bucket, install, upload, wait, monitor)")
    print("3. List objects in bucket (with optional tag filter)")
    print("4. List Milvus records (with optional tag filter)")
    print("=" * 80)
    
    valid = {'1', '2', '3', '4'}
    while True:
        try:
            choice = input(f"\nEnter your choice ({'/'.join(sorted(valid))}): ").strip()
            if choice in valid:
                return int(choice)
            else:
                print(f"Invalid choice. Please enter one of: {', '.join(sorted(valid))}")
        except (EOFError, KeyboardInterrupt):
            print("\n\nExiting...")
            sys.exit(0)
        except Exception as e:
            print(f"Error reading input: {e}. Please try again.")


def upload_only():
    """Just upload files to the bucket without running the full experiment."""
    logger.info("=" * 80)
    logger.info("UPLOAD ONLY MODE")
    logger.info("=" * 80)
    logger.info(f"Source bucket: {SOURCE_BUCKET}")
    logger.info(f"Local files directory: {LOCAL_FILES_DIR}")
    logger.info("=" * 80)
    
    # Validate prerequisites
    logger.info("\nValidating prerequisites...")
    
    # Check if boto3 is available
    try:
        import boto3
        logger.info("✓ boto3 is available")
    except ImportError:
        logger.error("✗ boto3 is not installed. Run: pip install boto3")
        sys.exit(1)
    
    # Check if local files directory exists
    if not Path(LOCAL_FILES_DIR).exists():
        logger.error(f"✗ Local files directory not found: {LOCAL_FILES_DIR}")
        logger.error("  Please update LOCAL_FILES_DIR in the script or create the directory")
        sys.exit(1)
    
    file_count = len([f for f in Path(LOCAL_FILES_DIR).glob('**/*') if f.is_file()])
    logger.info(f"✓ Local files directory exists ({file_count} files)")
    
    # Prompt for tags
    tags = prompt_for_tags()
    
    # Upload files
    logger.info("\nStarting file upload...")
    try:
        files_uploaded = upload_files_to_bucket(LOCAL_FILES_DIR, SOURCE_BUCKET, tags=tags)
        logger.info("=" * 80)
        logger.info(f"✓ Upload complete! Uploaded {files_uploaded} files to {SOURCE_BUCKET}")
        logger.info("=" * 80)
        return 0
    except Exception as e:
        logger.error(f"✗ Upload failed: {e}")
        return 1


def list_objects_menu():
    """Interactive menu to list objects in the bucket with optional tag filter."""
    try:
        import boto3
    except ImportError:
        logger.error("boto3 is not installed. Run: pip install boto3")
        return 1

    print("\nList objects in bucket with optional tag filter.")
    print(f"Bucket: {SOURCE_BUCKET}")
    print("\nEnter tag(s) to filter by (same format as upload).")
    print("  e.g.: finance, science   OR   dept=finance")
    print("  Press Enter to list all objects.")
    try:
        raw = input("\nFilter tags: ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return 0

    filter_tags = parse_and_sanitize_tags(raw)
    list_objects_with_tags(SOURCE_BUCKET, filter_tags=filter_tags if filter_tags else None)
    return 0


def list_milvus_menu():
    """Interactive menu to list Milvus records with optional tag filter."""
    try:
        from pymilvus import MilvusClient
    except ImportError:
        logger.error("pymilvus is not installed. Run: pip install pymilvus")
        return 1

    milvus_uri = f"http://{ENV_CONFIG['MILVUS_HOST']}:{ENV_CONFIG['MILVUS_PORT']}"
    client = MilvusClient(milvus_uri)
    collections = client.list_collections()
    print(f"\nAvailable Milvus collections: {collections}")

    try:
        collection_name = input(f"\nCollection name [{BASE_COLLECTION_NAME}]: ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return 0
    if not collection_name:
        collection_name = BASE_COLLECTION_NAME

    print("\nEnter tag(s) to filter by (same format as upload).")
    print("  e.g.: finance   OR   dept=finance")
    print("  Press Enter to list all records.")
    try:
        raw = input("\nFilter tags: ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return 0

    filter_tags = parse_and_sanitize_tags(raw)

    try:
        limit_str = input("Max records to return [50]: ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return 0
    limit = int(limit_str) if limit_str.isdigit() else 50

    list_milvus_records(
        collection_name,
        filter_tags=filter_tags if filter_tags else None,
        limit=limit,
    )
    return 0


# ============================================================================
# Main
# ============================================================================

def main():
    """Main entry point for the experiment script."""
    # Get user choice
    choice = get_user_choice()
    
    if choice == 1:
        return upload_only()
    
    elif choice == 3:
        return list_objects_menu()
    
    elif choice == 4:
        return list_milvus_menu()
    
    elif choice == 2:
        # Run full experiment
        logger.info("=" * 80)
        logger.info("AIDP CONNECTOR BATCH SIZE EXPERIMENT")
        logger.info("=" * 80)
        logger.info(f"Max Outstanding Tasks to test: {MAX_OUTSTANDING_TASKS_LIST}")
        logger.info(f"Note: Batch size will be set to maxOutstandingTasks for each iteration")
        logger.info(f"Source bucket: {SOURCE_BUCKET}")
        logger.info(f"Local files directory: {LOCAL_FILES_DIR}")
        logger.info(f"Namespace: {ENV_CONFIG['NAMESPACE']}")
        logger.info(f"Initial wait: {INITIAL_WAIT_SECONDS/3600:.1f} hours, then monitor for {MONITORING_DURATION_SECONDS/60:.0f} minutes")
        logger.info("=" * 80)
        
        # Validate prerequisites
        logger.info("\nValidating prerequisites...")
        
        # Check if boto3 is available
        try:
            import boto3
            logger.info("✓ boto3 is available")
        except ImportError:
            logger.error("✗ boto3 is not installed. Run: pip install boto3")
            sys.exit(1)
        
        # Check if kubectl is accessible
        result = run_command(f"{get_kubectl_cmd()} version --client", check=False)
        if result.returncode != 0:
            logger.error(f"✗ kubectl not accessible. Check KUBECONFIG: {KUBECONFIG_PATH}")
            sys.exit(1)
        logger.info("✓ kubectl is accessible")
        
        # Check if helm is accessible
        result = run_command(f"{get_helm_cmd()} version", check=False)
        if result.returncode != 0:
            logger.error("✗ helm not accessible")
            sys.exit(1)
        logger.info("✓ helm is accessible")
        
        # Check if local files directory exists
        if not Path(LOCAL_FILES_DIR).exists():
            logger.error(f"✗ Local files directory not found: {LOCAL_FILES_DIR}")
            logger.error("  Please update LOCAL_FILES_DIR in the script or create the directory")
            sys.exit(1)
        
        file_count = len([f for f in Path(LOCAL_FILES_DIR).glob('**/*') if f.is_file()])
        logger.info(f"✓ Local files directory exists ({file_count} files)")
        
        # Check if pymilvus is available (needed for releasing collections)
        try:
            import pymilvus
            logger.info("✓ pymilvus is available")
        except ImportError:
            logger.warning("⚠ pymilvus is not installed. Collection release will be skipped. Run: pip install pymilvus")
        
        # Prompt for tags (applied to all experiment iterations)
        tags = prompt_for_tags()

        # Run experiments - iterate over maxOutstandingTasks values
        experiment_results = []
        
        for max_outstanding_tasks in MAX_OUTSTANDING_TASKS_LIST:
            run_experiment_for_batch_size(max_outstanding_tasks, experiment_results, tags=tags)
            
            # Save intermediate results
            save_results(experiment_results)
        
        # Print final summary
        print_summary(experiment_results)
        
        # Save final results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_results(experiment_results, f'experiment_results_{timestamp}.json')
        
        logger.info("\nExperiment complete!")
        return 0


if __name__ == '__main__':
    sys.exit(main())
