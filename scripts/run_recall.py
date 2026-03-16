import pandas as pd
import numpy as np
import requests
import json
import os
import re
import sys
from collections import defaultdict
from pymilvus import MilvusClient

MILVUS_URI = "http://milvus-vectordb.milvus:19530"
EMBEDDING_ENDPOINT = "http://llama-32-nv-embedqa-1b-v2.nim-service:8000/v1/embeddings"
MODEL_NAME = "nvidia/llama-3.2-nv-embedqa-1b-v2"
TOP_K = 10

PDF_STEM_RE = re.compile(r'(\d+)\.pdf')


def extract_pdf_stem(source_id):
    """Extract PDF numeric stem from various source_id formats:
    - 'data/bo767/1006540.pdf'         -> '1006540'
    - 'aidp-docs/pdf-uploads/1006540.pdf/null' -> '1006540'
    - '1006540.pdf#page_3'             -> '1006540'
    """
    m = PDF_STEM_RE.search(source_id)
    if m:
        return m.group(1)
    return os.path.basename(source_id).split(".")[0]


def get_embeddings(texts, batch_size=64):
    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i+batch_size]
        resp = requests.post(
            EMBEDDING_ENDPOINT,
            json={"input": batch, "model": MODEL_NAME, "input_type": "query"},
            timeout=60
        )
        resp.raise_for_status()
        data = resp.json()
        sorted_data = sorted(data["data"], key=lambda x: x["index"])
        all_embeddings.extend([d["embedding"] for d in sorted_data])
        print(f"  Embedded batch {i//batch_size + 1} ({len(all_embeddings)}/{len(texts)})", flush=True)
    return all_embeddings


def search_collection(client, collection_name, embeddings, top_k=10):
    all_results = []
    batch_size = 100
    for i in range(0, len(embeddings), batch_size):
        batch = embeddings[i:i+batch_size]
        results = client.search(
            collection_name=collection_name,
            data=batch,
            anns_field="vector",
            limit=top_k,
            output_fields=["text", "source", "content_metadata"],
            consistency_level="Bounded",
        )
        all_results.extend(results)
        print(f"  Searched batch {i//batch_size + 1} ({len(all_results)}/{len(embeddings)})", flush=True)
    return all_results


def parse_result(result):
    """Parse a single search result, returning (pdf_stem, page_number)."""
    source = result["entity"]["source"]
    if isinstance(source, str):
        source = json.loads(source)
    cm = result["entity"]["content_metadata"]
    if isinstance(cm, str):
        cm = json.loads(cm)

    source_id = source.get("source_id", source.get("source_name", ""))
    pdf_stem = extract_pdf_stem(source_id)
    page_num = cm.get("page_number", 0)
    return pdf_stem, page_num


def get_recall_scores(query_df, all_answers, label="", page_offset=0):
    hits = defaultdict(list)
    for i in range(len(query_df)):
        expected_pdf_page = query_df["pdf_page"].iloc[i]
        retrieved_answers = all_answers[i]
        retrieved_pdf_pages = []
        for result in retrieved_answers:
            pdf, page = parse_result(result)
            adjusted_page = page + page_offset
            retrieved_pdf_pages.append(f"{pdf}_{adjusted_page}")

        for k in [1, 3, 5, 10]:
            hits[k].append(expected_pdf_page in retrieved_pdf_pages[:k])

    offset_str = f" (page_offset={page_offset})" if page_offset else ""
    print(f"\n{'='*50}", flush=True)
    print(f"Collection: {label}{offset_str}", flush=True)
    print(f"{'='*50}", flush=True)
    for k in sorted(hits.keys()):
        print(f"  - Recall @{k}: {np.mean(hits[k]):.3f}", flush=True)
    return hits


def main():
    df_query = pd.read_csv("/tmp/text_query_answer_gt_page.csv")
    df_query["pdf_page"] = (
        df_query["pdf"].str.replace(".pdf", "", regex=False).str.split(".").str[0]
        + "_"
        + df_query["gt_page"].astype(str)
    )
    print(f"Loaded {len(df_query)} queries", flush=True)
    print(f"Sample pdf_page values: {df_query['pdf_page'].head(5).tolist()}", flush=True)
    print(f"Ground truth page indexing: gt_page column values = {df_query['gt_page'].head(5).tolist()}", flush=True)

    print("\nGenerating embeddings for queries...", flush=True)
    queries = df_query["query"].tolist()
    embeddings = get_embeddings(queries)
    print(f"Generated {len(embeddings)} embeddings, dim={len(embeddings[0])}", flush=True)

    client = MilvusClient(MILVUS_URI)

    for coll in ["ntnx_docs_collection1", "single_file_text"]:
        try:
            stats = client.get_collection_stats(coll)
            print(f"\nCollection '{coll}': {stats['row_count']} rows", flush=True)
        except Exception as e:
            print(f"\nCollection '{coll}': ERROR - {e}", flush=True)

    # --- single_file_text (0-indexed pages) ---
    print("\nSearching single_file_text...", flush=True)
    results_sft = search_collection(client, "single_file_text", embeddings, TOP_K)
    get_recall_scores(df_query, results_sft, "single_file_text", page_offset=0)

    # --- ntnx_docs_collection1 ---
    print("\nSearching ntnx_docs_collection1...", flush=True)
    results_ntnx = search_collection(client, "ntnx_docs_collection1", embeddings, TOP_K)

    # Check what page indexing the collection uses
    sample_pages = set()
    for r in results_ntnx[0][:5]:
        _, page = parse_result(r)
        sample_pages.add(page)
    min_page = min(sample_pages) if sample_pages else -1
    print(f"\n  Sample page numbers from ntnx_docs: {sorted(sample_pages)} (min={min_page})", flush=True)

    # Try both 0-offset (if already 0-indexed) and -1 offset (if still 1-indexed)
    get_recall_scores(df_query, results_ntnx, "ntnx_docs_collection1", page_offset=0)
    get_recall_scores(df_query, results_ntnx, "ntnx_docs_collection1 (adjusted -1)", page_offset=-1)

    # Sample comparisons
    print(f"\n\n{'='*60}", flush=True)
    print("SAMPLE COMPARISONS (first 5 queries)", flush=True)
    print(f"{'='*60}", flush=True)
    for i in range(min(5, len(df_query))):
        print(f"\nQuery {i}: '{queries[i][:80]}...'", flush=True)
        print(f"Expected: {df_query['pdf_page'].iloc[i]}", flush=True)

        for coll_name, results in [("ntnx_docs", results_ntnx), ("single_file", results_sft)]:
            retrieved = []
            for r in results[i][:5]:
                pdf, page = parse_result(r)
                retrieved.append(f"{pdf}_{page}")
            print(f"  {coll_name} top-5: {retrieved}", flush=True)

    # Detailed page number analysis for a known multi-page doc
    print(f"\n\n{'='*60}", flush=True)
    print("PAGE NUMBER ANALYSIS for 1102434.pdf", flush=True)
    print(f"{'='*60}", flush=True)
    for coll in ["ntnx_docs_collection1", "single_file_text"]:
        print(f"\n--- {coll} ---", flush=True)
        try:
            results = client.query(
                collection_name=coll,
                filter='source["source_name"] like "%1102434%"',
                output_fields=["source", "content_metadata"],
                limit=50
            )
            pages = []
            for r in results:
                source = r["source"]
                if isinstance(source, str):
                    source = json.loads(source)
                cm = r["content_metadata"]
                if isinstance(cm, str):
                    cm = json.loads(cm)
                pages.append(cm.get("page_number", "?"))
            pages.sort()
            print(f"  Found {len(results)} rows, page numbers: {pages}", flush=True)
            if pages:
                print(f"  Min page: {min(pages)}, Max page: {max(pages)}", flush=True)
        except Exception as e:
            print(f"  Error: {e}", flush=True)


if __name__ == "__main__":
    main()
