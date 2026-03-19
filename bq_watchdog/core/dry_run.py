"""
watchdog/core/dry_run.py
------------------------
BigQuery dry run engine.
Free, instant, no slots consumed.
Runs models in parallel to minimise CI wait time.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from .collector import CompiledModel
from .models import DryRunResult


def dry_run_model(
    model:      CompiledModel,
    project_id: str,
    location:   str = "EU",
) -> DryRunResult:
    """
    Run a single BigQuery dry run.

    Key facts about dry runs:
    - Free: no bytes billed, no slots consumed
    - Instant: completes in milliseconds
    - Accurate: respects partition pruning
    - Honest: returns upper bound on bytes processed
    """
    client     = bigquery.Client(project=project_id)
    job_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=False,
    )

    try:
        job = client.query(model.sql, job_config=job_config, location=location)
        return DryRunResult(
            model=model.name,
            bytes_processed=job.total_bytes_processed or 0,
        )
    except Exception as e:
        return DryRunResult(
            model=model.name,
            bytes_processed=0,
            error=str(e),
        )


def dry_run_all(
    models:      list[CompiledModel],
    project_id:  str,
    location:    str = "EU",
    max_workers: int = 5,
) -> list[DryRunResult]:
    """
    Run dry runs for all models in parallel.
    Returns results sorted by cost descending (most expensive first).
    """
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(dry_run_model, m, project_id, location): m
            for m in models
        }
        for future in as_completed(futures):
            results.append(future.result())

    return sorted(results, key=lambda r: r.cost_usd, reverse=True)
