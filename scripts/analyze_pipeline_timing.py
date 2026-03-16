#!/usr/bin/env python3
"""
Analyze Nutanix AIDP-Connector pipeline timing from pod logs

This script extracts timing information for different pipeline stages:
1. Download time
2. NeMo-Retriever processing time (from submission to completion)
3. Milvus bulk upload time

Usage:
    python scripts/analyze_pipeline_timing.py
"""

import os
import sys
import subprocess
import json
import re
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

def parse_timestamp(timestamp_str: str) -> datetime:
    """Parse ISO timestamp string to datetime"""
    try:
        # Remove 'Z' and parse
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1] + '+00:00'
        return datetime.fromisoformat(timestamp_str)
    except Exception as e:
        print(f"Error parsing timestamp {timestamp_str}: {e}")
        return None

def get_pod_logs(pod_name: str, namespace: str = "nutanix-aidp-connector", kubeconfig: str = None) -> List[str]:
    """Get logs from pod"""
    cmd = ["kubectl", "logs", "-n", namespace, pod_name, "--tail=10000"]
    if kubeconfig:
        cmd.extend(["--kubeconfig", kubeconfig])
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.split('\n')
    except subprocess.CalledProcessError as e:
        print(f"Error getting logs: {e}")
        return []

def extract_file_identifier(log_line: str) -> Optional[str]:
    """Extract file identifier from log line"""
    # Try to extract batch_id or file name from various log patterns
    patterns = [
        r'batch_id:\s*([a-f0-9-]+)',
        r'file:\s*([a-f0-9-]+)',
        r'pdf-uploads_(\d+\.pdf)',
        r'embeddings.*?(\d+\.pdf)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, log_line)
        if match:
            return match.group(1)
    return None

def analyze_logs(log_lines: List[str]) -> Dict:
    """Analyze logs to extract timing information"""
    
    # Track events by batch_id and file_id
    events = defaultdict(list)
    
    # Parse all log lines
    for line in log_lines:
        if not line.strip():
            continue
        
        try:
            # Try to parse as JSON
            log_entry = json.loads(line)
            timestamp_str = log_entry.get('timestamp', '')
            message = log_entry.get('message', '')
            
            if not timestamp_str:
                continue
            
            timestamp = parse_timestamp(timestamp_str)
            if not timestamp:
                continue
            
            # Extract batch_id or file identifier
            batch_id = None
            file_id = None
            
            # Look for batch_id in message or context
            batch_match = re.search(r'batch_id:\s*([a-f0-9-]+)', message)
            if batch_match:
                batch_id = batch_match.group(1)
            
            # Also check for batch_id in "Submitted batch" message
            submitted_batch_match = re.search(r'Submitted batch\s+([a-f0-9-]+)', message)
            if submitted_batch_match:
                batch_id = submitted_batch_match.group(1)
            
            # Look for file_id
            file_match = re.search(r'file:\s*([a-f0-9-]+)', message)
            if file_match:
                file_id = file_match.group(1)
            
            # Look for file name in embeddings path or download messages
            file_name_match = re.search(r'pdf-uploads[\/_](\d+\.pdf)', message)
            if file_name_match:
                file_id = file_name_match.group(1)
            
            # Look for job_id
            job_match = re.search(r'job\s+(\d+)', message)
            if job_match:
                job_id = job_match.group(1)
            
            # Extract batch_id from context if available
            context = log_entry.get('context', {})
            if not batch_id and 'batch_id' in context:
                batch_id = context.get('batch_id')
            
            # Use batch_id as primary identifier, fallback to file_id or job_id
            identifier = batch_id or file_id or job_id
            
            # Categorize events
            event_type = None
            if 'Queued file for download' in message:
                event_type = 'queued'
            elif 'Successfully downloaded' in message:
                event_type = 'downloaded'
            elif 'Submitted batch' in message:
                event_type = 'batch_submitted'
            elif 'Processing completion for job' in message:
                event_type = 'nemo_retriever_completed'
            elif 'Triggering bulk insert for file' in message:
                event_type = 'bulk_insert_started'
            elif 'Bulk insert task' in message and 'completed successfully' in message:
                event_type = 'bulk_insert_task_completed'
            elif 'Bulk insert for file completed' in message:
                event_type = 'bulk_insert_file_completed'
            
            if event_type:
                # Use identifier if available, otherwise use a generic key
                key = identifier if identifier else 'unknown'
                events[key].append({
                    'timestamp': timestamp,
                    'event_type': event_type,
                    'message': message,
                    'batch_id': batch_id,
                    'file_id': file_id,
                    'job_id': job_id
                })
        except json.JSONDecodeError:
            # Not JSON, skip
            continue
        except Exception as e:
            # Skip malformed lines
            continue
    
    return events

def calculate_timings(events: Dict) -> Dict:
    """Calculate timing statistics from events"""
    
    timings = {
        'download_times': [],
        'nemo_retriever_times': [],
        'bulk_insert_times': [],
        'total_pipeline_times': [],
        'file_stats': []
    }
    
    # Group events by batch_id or file_id
    for identifier, event_list in events.items():
        if not event_list:
            continue
        
        # Sort by timestamp
        event_list.sort(key=lambda x: x['timestamp'])
        
        # Find key events - track multiple occurrences for batch-level analysis
        queued_times = []
        downloaded_times = []
        batch_submitted_time = None
        nemo_retriever_completed_times = []
        bulk_insert_started_times = []
        bulk_insert_completed_times = []
        
        for event in event_list:
            if event['event_type'] == 'queued':
                queued_times.append(event['timestamp'])
            elif event['event_type'] == 'downloaded':
                downloaded_times.append(event['timestamp'])
            elif event['event_type'] == 'batch_submitted':
                batch_submitted_time = event['timestamp']
            elif event['event_type'] == 'nemo_retriever_completed':
                nemo_retriever_completed_times.append(event['timestamp'])
            elif event['event_type'] == 'bulk_insert_started':
                bulk_insert_started_times.append(event['timestamp'])
            elif event['event_type'] == 'bulk_insert_file_completed':
                bulk_insert_completed_times.append(event['timestamp'])
        
        # Use first/last times for calculations
        queued_time = queued_times[0] if queued_times else None
        downloaded_time = downloaded_times[-1] if downloaded_times else None
        nemo_retriever_completed_time = nemo_retriever_completed_times[-1] if nemo_retriever_completed_times else None
        bulk_insert_started_time = bulk_insert_started_times[0] if bulk_insert_started_times else None
        bulk_insert_completed_time = bulk_insert_completed_times[-1] if bulk_insert_completed_times else None
        
        # Calculate durations
        file_stat = {'identifier': identifier}
        
        # Download time
        if queued_time and downloaded_time:
            download_duration = (downloaded_time - queued_time).total_seconds()
            timings['download_times'].append(download_duration)
            file_stat['download_seconds'] = download_duration
        
        # NeMo-Retriever processing time (from batch submission to completion)
        if batch_submitted_time and nemo_retriever_completed_time:
            nemo_retriever_duration = (nemo_retriever_completed_time - batch_submitted_time).total_seconds()
            timings['nemo_retriever_times'].append(nemo_retriever_duration)
            file_stat['nemo_retriever_seconds'] = nemo_retriever_duration
        
        # Bulk insert time (from start to completion)
        if bulk_insert_started_time and bulk_insert_completed_time:
            bulk_insert_duration = (bulk_insert_completed_time - bulk_insert_started_time).total_seconds()
            timings['bulk_insert_times'].append(bulk_insert_duration)
            file_stat['bulk_insert_seconds'] = bulk_insert_duration
        
        # Total pipeline time (from queued to bulk insert completed)
        if queued_time and bulk_insert_completed_time:
            total_duration = (bulk_insert_completed_time - queued_time).total_seconds()
            timings['total_pipeline_times'].append(total_duration)
            file_stat['total_seconds'] = total_duration
        
        if file_stat.get('download_seconds') or file_stat.get('nemo_retriever_seconds') or file_stat.get('bulk_insert_seconds'):
            timings['file_stats'].append(file_stat)
    
    return timings

def print_statistics(timings: Dict):
    """Print timing statistics"""
    
    print("\n" + "="*80)
    print("PIPELINE TIMING ANALYSIS")
    print("="*80 + "\n")
    
    # Download statistics
    if timings['download_times']:
        download_times = timings['download_times']
        print(f"DOWNLOAD TIME (from queued to downloaded):")
        print(f"  Count: {len(download_times)}")
        print(f"  Average: {sum(download_times)/len(download_times):.2f} seconds")
        print(f"  Min: {min(download_times):.2f} seconds")
        print(f"  Max: {max(download_times):.2f} seconds")
        print(f"  Median: {sorted(download_times)[len(download_times)//2]:.2f} seconds")
    else:
        print("DOWNLOAD TIME: No data found")
    
    print()
    
    # NeMo-Retriever processing statistics
    if timings['nemo_retriever_times']:
        nemo_retriever_times = timings['nemo_retriever_times']
        print(f"NEMO-RETRIEVER PROCESSING TIME (from batch submitted to completion):")
        print(f"  Count: {len(nemo_retriever_times)}")
        print(f"  Average: {sum(nemo_retriever_times)/len(nemo_retriever_times):.2f} seconds")
        print(f"  Min: {min(nemo_retriever_times):.2f} seconds")
        print(f"  Max: {max(nemo_retriever_times):.2f} seconds")
        print(f"  Median: {sorted(nemo_retriever_times)[len(nemo_retriever_times)//2]:.2f} seconds")
    else:
        print("NEMO-RETRIEVER PROCESSING TIME: No data found")
    
    print()
    
    # Bulk insert statistics
    if timings['bulk_insert_times']:
        bulk_insert_times = timings['bulk_insert_times']
        print(f"MILVUS BULK UPLOAD TIME (from trigger to completion):")
        print(f"  Count: {len(bulk_insert_times)}")
        print(f"  Average: {sum(bulk_insert_times)/len(bulk_insert_times):.2f} seconds")
        print(f"  Min: {min(bulk_insert_times):.2f} seconds")
        print(f"  Max: {max(bulk_insert_times):.2f} seconds")
        print(f"  Median: {sorted(bulk_insert_times)[len(bulk_insert_times)//2]:.2f} seconds")
    else:
        print("MILVUS BULK UPLOAD TIME: No data found")
    
    print()
    
    # Total pipeline time
    if timings['total_pipeline_times']:
        total_times = timings['total_pipeline_times']
        print(f"TOTAL PIPELINE TIME (from queued to bulk insert completed):")
        print(f"  Count: {len(total_times)}")
        print(f"  Average: {sum(total_times)/len(total_times):.2f} seconds ({sum(total_times)/len(total_times)/60:.2f} minutes)")
        print(f"  Min: {min(total_times):.2f} seconds ({min(total_times)/60:.2f} minutes)")
        print(f"  Max: {max(total_times):.2f} seconds ({max(total_times)/60:.2f} minutes)")
        print(f"  Median: {sorted(total_times)[len(total_times)//2]:.2f} seconds ({sorted(total_times)[len(total_times)//2]/60:.2f} minutes)")
    else:
        print("TOTAL PIPELINE TIME: No data found")
    
    print()
    
    # Percentage breakdown
    if timings['total_pipeline_times'] and timings['download_times'] and timings['nemo_retriever_times'] and timings['bulk_insert_times']:
        avg_total = sum(timings['total_pipeline_times'])/len(timings['total_pipeline_times'])
        avg_download = sum(timings['download_times'])/len(timings['download_times'])
        avg_nemo_retriever = sum(timings['nemo_retriever_times'])/len(timings['nemo_retriever_times'])
        avg_bulk_insert = sum(timings['bulk_insert_times'])/len(timings['bulk_insert_times'])
        
        print("TIME BREAKDOWN (as percentage of total pipeline time):")
        print(f"  Download: {avg_download/avg_total*100:.1f}%")
        print(f"  NeMo-Retriever Processing: {avg_nemo_retriever/avg_total*100:.1f}%")
        print(f"  Milvus Upload: {avg_bulk_insert/avg_total*100:.1f}%")
        print(f"  Other/Overhead: {(avg_total - avg_download - avg_nemo_retriever - avg_bulk_insert)/avg_total*100:.1f}%")
    
    print("\n" + "="*80)

def main():
    kubeconfig = "/home/veer.kumar/workspace/objects-nutanix-aidp-connectors/scripts/kubeconfig_new.conf"
    namespace = "nutanix-aidp-connector"
    
    # Get pod name
    cmd = ["kubectl", "get", "pods", "-n", namespace, "-l", "app=nutanix-aidp-connector", "-o", "jsonpath={.items[0].metadata.name}"]
    if os.path.exists(kubeconfig):
        cmd.extend(["--kubeconfig", kubeconfig])
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        pod_name = result.stdout.strip()
        if not pod_name:
            print("No Nutanix AIDP-Connector pod found")
            return
        print(f"Analyzing logs from pod: {pod_name}")
    except subprocess.CalledProcessError as e:
        print(f"Error getting pod name: {e}")
        return
    
    # Get logs
    print("Fetching logs...")
    log_lines = get_pod_logs(pod_name, namespace, kubeconfig)
    print(f"Retrieved {len(log_lines)} log lines")
    
    # Analyze logs
    print("Analyzing logs...")
    events = analyze_logs(log_lines)
    print(f"Found events for {len(events)} identifiers")
    
    # Debug: show sample events
    if events:
        sample_id = list(events.keys())[0]
        print(f"\nSample events for identifier '{sample_id}':")
        for event in events[sample_id][:10]:
            print(f"  {event['event_type']}: {event['timestamp']} - {event['message'][:80]}")
        print(f"\nTotal events for this identifier: {len(events[sample_id])}")
    
    # Calculate timings
    timings = calculate_timings(events)
    
    # Print statistics
    print_statistics(timings)
    
    # Print sample file stats
    if timings['file_stats']:
        print("\nSAMPLE FILE STATISTICS (first 10 files):")
        print("-"*80)
        for i, stat in enumerate(timings['file_stats'][:10]):
            print(f"\nFile {i+1} ({stat.get('identifier', 'unknown')}):")
            if 'download_seconds' in stat:
                print(f"  Download: {stat['download_seconds']:.2f} seconds")
            if 'nemo_retriever_seconds' in stat:
                print(f"  NeMo-Retriever: {stat['nemo_retriever_seconds']:.2f} seconds")
            if 'bulk_insert_seconds' in stat:
                print(f"  Bulk Insert: {stat['bulk_insert_seconds']:.2f} seconds")
            if 'total_seconds' in stat:
                print(f"  Total: {stat['total_seconds']:.2f} seconds ({stat['total_seconds']/60:.2f} minutes)")

if __name__ == "__main__":
    main()
