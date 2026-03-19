"""
watchdog/core/dbt_advisor.py
----------------------------
Reads dbt manifest.json and cross-checks model configurations.
"""

import json
from pathlib import Path
from .models import Finding

def read_manifest(target_dir: str) -> dict:
    """Read dbt manifest.json from the target directory."""
    manifest_path = Path(target_dir) / "manifest.json"
    if not manifest_path.exists():
        return {}
    
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def check_missing_clustering_config(model_node: dict) -> Finding | None:
    """
    Check if an incremental model has partition_by but lacks cluster_by.
    Clustering reduces MERGE scan bytes significantly.
    """
    config = model_node.get("config", {})
    materialized = config.get("materialized")
    partition_by = config.get("partition_by")
    cluster_by = config.get("cluster_by")

    if materialized == "incremental" and partition_by and not cluster_by:
        return Finding(
            model=model_node.get("name", "unknown"),
            rule="missing_clustering_config",
            severity="info",
            description=(
                "Model is incremental and partitioned, but lacks clustering. "
                "Adding `cluster_by` can reduce MERGE scan bytes by up to 88% by enabling block pruning."
            ),
            snippet="config(materialized='incremental', partition_by=...)"
        )
    return None

def advise(model_name: str, target_dir: str) -> list[Finding]:
    """
    Run config checks against a specific model from the manifest.
    Returns a list of findings (or empty list if none/manifest missing).
    """
    manifest = read_manifest(target_dir)
    if not manifest:
        return []

    findings = []
    
    # Locate the model in the manifest nodes
    nodes = manifest.get("nodes", {})
    model_node = None
    for node_id, node in nodes.items():
        if node.get("resource_type") == "model" and node.get("name") == model_name:
            model_node = node
            break
            
    if model_node:
        f1 = check_missing_clustering_config(model_node)
        if f1:
            findings.append(f1)
            
    return findings
