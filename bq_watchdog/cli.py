"""
watchdog/cli.py
---------------
CLI entrypoint for bq-watchdog.
Usage:
    watchdog run --project my-gcp-project
    watchdog run --project my-gcp-project --target ./target --location EU
    watchdog run --project my-gcp-project --post-pr-comment --pr-number 42
"""

import os
import sys
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

import click
from rich.console import Console
from rich.table import Table
from rich import print as rprint

from bq_watchdog.core.collector  import collect_compiled_sql
from bq_watchdog.core.dry_run    import dry_run_all
from bq_watchdog.core.analyser   import analyse
from bq_watchdog.core.dbt_advisor import advise
from bq_watchdog.core.models     import ModelReport, WatchdogResult
from bq_watchdog.agent.suggester import suggest_fixes_for_flagged

console = Console()


@click.group()
def cli():
    """🐕 bq-watchdog — AI-powered BigQuery cost guard for dbt"""
    pass


@cli.command()
@click.option("--project",         required=True,  help="GCP project ID")
@click.option("--target",          default="target", help="dbt target directory")
@click.option("--location",        default="EU",   help="BigQuery location")
@click.option("--warn-threshold",  default=0.50,   help="Cost (USD) to warn")
@click.option("--block-threshold", default=5.00,   help="Cost (USD) to block")
@click.option("--no-ai",           is_flag=True,   help="Skip AI suggestions")
@click.option("--post-pr-comment", is_flag=True,   help="Post comment to GitHub PR")
@click.option("--pr-number",       default=None,   type=int, help="PR number")
@click.option("--price-per-tb",    default=6.25,   help="BigQuery on-demand price per TB (USD)")
@click.option("--schedule",        type=click.Choice(["hourly", "daily", "weekly"]), help="Compute monthly cost based on this schedule")
@click.option("--output",          type=click.Choice(["table", "json", "sarif"]), default="table", help="Output format")
def run(project, target, location, warn_threshold,
        block_threshold, no_ai, post_pr_comment, pr_number, price_per_tb, schedule, output):
    """Run cost analysis on compiled dbt models."""

    console.print("\n[bold]🐕 bq-watchdog[/bold]")
    console.print("─" * 50)

    # 1. Collect compiled SQL
    try:
        models = collect_compiled_sql(target)
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)

    console.print(f"Found [cyan]{len(models)}[/cyan] compiled models\n")

    # 2. Run dry runs
    console.print("Running BigQuery dry runs (free)...")
    dry_run_results = dry_run_all(models, project_id=project, location=location)

    # Map results by model name
    dry_run_map = {r.model: r for r in dry_run_results}
    sql_map     = {m.name: m.sql for m in models}

    # 3. Static analysis & dbt config checks
    finding_map = {}
    for model in models:
        # SQL analysis
        findings = analyse(model.name, model.sql)
        # dbt config analysis
        config_findings = advise(model.name, target)
        
        all_findings = findings + config_findings
        if all_findings:
            finding_map[model.name] = all_findings

    # 4. Build reports
    reports = []
    for model in models:
        dr = dry_run_map.get(model.name)
        if not dr:
            continue

        # Override severity based on custom thresholds
        if dr.cost_usd >= block_threshold:
            dr_severity = "block"
        elif dr.cost_usd >= warn_threshold:
            dr_severity = "warn"
        else:
            dr_severity = "ok"

        reports.append(ModelReport(
            name=model.name,
            sql=model.sql,
            dry_run=dr,
            findings=finding_map.get(model.name, []),
        ))

    # 5. AI suggestions for flagged models
    suggestions = {}
    if not no_ai and any(r.overall_severity != "ok" for r in reports):
        suggestions = suggest_fixes_for_flagged(reports, target_dir=target)
        for report in reports:
            if report.name in suggestions:
                report.suggestion = suggestions[report.name]

    # 6. Print results based on output format
    if output == "json":
        sys.stdout.write(result.model_dump_json(indent=2))
        sys.exit(1 if result.has_blocks else 0)
    elif output == "sarif":
        _print_sarif(result)
        sys.exit(1 if result.has_blocks else 0)
        
    _print_results_table(reports, schedule)

    # 7. Print suggestions
    for report in reports:
        if report.suggestion:
            console.print(f"\n[bold red]❌ {report.name}[/bold red]"
                          if report.overall_severity == "block"
                          else f"\n[bold yellow]⚠️  {report.name}[/bold yellow]")
            console.print("─" * 50)
            console.print(report.suggestion)

    # 8. Build final result
    result = WatchdogResult(
        reports=reports,
        project_id=project,
        run_at=datetime.utcnow().isoformat(),
    )

    # 9. Post PR comment
    if post_pr_comment and pr_number:
        _post_pr_comment(result, pr_number)

    # 10. Exit code — fail CI if there are blocks
    if result.has_blocks:
        console.print(
            f"\n[bold red]❌ {len([r for r in reports if r.overall_severity == 'block'])} "
            f"model(s) blocked. Fix issues above before merging.[/bold red]"
        )
        sys.exit(1)
    else:
        console.print(
            f"\n[bold green]✅ All models passed "
            f"(total: ${result.total_cost_usd:.4f}/run)[/bold green]"
        )
        sys.exit(0)


def _print_results_table(reports: list[ModelReport], schedule: str = None) -> None:
    table = Table(title="BigQuery Cost Estimate")
    table.add_column("Model",    style="cyan",  no_wrap=True)
    table.add_column("Scan",     justify="right")
    table.add_column("Cost/run", justify="right")
    table.add_column("Issues",   justify="center")
    table.add_column("Status",   justify="center")

    if schedule:
        table.add_column("Monthly", justify="right")

    multiplier = {"hourly": 730, "daily": 30, "weekly": 4.33}.get(schedule, 1)

    for r in sorted(reports, key=lambda x: x.dry_run.cost_usd, reverse=True):
        issue_count = len(r.findings)
        status_color = {
            "ok":    "green",
            "warn":  "yellow",
            "block": "red",
            "error": "red",
        }.get(r.overall_severity, "white")
        
        row = [
            r.name,
            f"{r.dry_run.gb:.1f} GB" if not r.dry_run.error else "error",
            f"${r.dry_run.cost_usd:.4f}" if not r.dry_run.error else "—",
            str(issue_count) if issue_count else "—",
            f"[{status_color}]{r.dry_run.icon} {r.overall_severity.upper()}[/]",
        ]
        
        if schedule:
            monthly = r.dry_run.cost_usd * multiplier
            row.insert(3, f"${monthly:.2f}" if not r.dry_run.error else "—")
            
        table.add_row(*row)

    console.print(table)


def _print_sarif(result: WatchdogResult) -> None:
    """Generate and print SARIF format output for GitHub Code Scanning."""
    sarif = {
        "$schema": "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemata/sarif-schema-2.1.0.json",
        "version": "2.1.0",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "bq-watchdog",
                        "informationUri": "https://github.com/carlonuccio/bq-watchdog",
                        "rules": [
                            {"id": "cross_join", "name": "Cross Join", "shortDescription": {"text": "Cartesian product detected"}},
                            {"id": "select_star", "name": "Select Star", "shortDescription": {"text": "SELECT * scans all columns"}},
                            {"id": "limit_without_filter", "name": "Limit w/o Filter", "shortDescription": {"text": "LIMIT without WHERE scans full table"}},
                            {"id": "missing_partition_filter", "name": "Missing Filter", "shortDescription": {"text": "High volume table with no WHERE clause"}},
                            {"id": "self_join", "name": "Self Join", "shortDescription": {"text": "Table joined to itself"}},
                            {"id": "repeated_cte_reference", "name": "Repeated CTE", "shortDescription": {"text": "CTE referenced multiple times"}},
                            {"id": "regex_in_where", "name": "Regex pattern", "shortDescription": {"text": "Expensive regex found"}},
                            {"id": "join_order_large_first", "name": "Join Order", "shortDescription": {"text": "Large table not driving the join"}},
                            {"id": "dynamic_partition_pruning_risk", "name": "Pruning Risk", "shortDescription": {"text": "Dynamic partition pruning risk"}},
                            {"id": "missing_clustering_config", "name": "Missing Clustering", "shortDescription": {"text": "Add clustering for better partition pruning"}},
                        ]
                    }
                },
                "results": []
            }
        ]
    }

    for report in result.reports:
        # Create a result for the dry-run cost if it is warned/blocked
        if report.dry_run.severity in ("warn", "block"):
            sarif["runs"][0]["results"].append({
                "ruleId": "high_cost",
                "level": "error" if report.dry_run.severity == "block" else "warning",
                "message": {
                    "text": f"Estimated cost is ${report.dry_run.cost_usd:.4f}/run ({report.dry_run.gb:.1f} GB scanned)."
                },
                "locations": [{
                    "physicalLocation": {
                        "artifactLocation": {"uri": f"models/{report.name}.sql"}
                    }
                }]
            })
            
        # Create results for AST findings
        for finding in report.findings:
            sarif["runs"][0]["results"].append({
                "ruleId": finding.rule,
                "level": "error" if finding.severity == "block" else ("warning" if finding.severity == "warn" else "note"),
                "message": {
                    "text": finding.description
                },
                "locations": [{
                    "physicalLocation": {
                        "artifactLocation": {"uri": f"models/{report.name}.sql"}
                    }
                }]
            })

    import sys
    import json
    sys.stdout.write(json.dumps(sarif, indent=2))



def _post_pr_comment(result: WatchdogResult, pr_number: int) -> None:
    """Post cost report as a GitHub PR comment."""
    token     = os.getenv("GITHUB_TOKEN")
    repo_name = os.getenv("GITHUB_REPOSITORY")

    if not token or not repo_name:
        console.print("[yellow]⚠️  GITHUB_TOKEN or GITHUB_REPOSITORY not set — skipping PR comment[/yellow]")
        return

    try:
        from github import Github
        from bq_watchdog.output.pr_comment import build_comment, post_comment

        comment_body = build_comment(result)
        post_comment(token, repo_name, pr_number, comment_body)
        console.print(f"[green]✅ Posted PR comment to #{pr_number}[/green]")
    except Exception as e:
        console.print(f"[yellow]⚠️  Failed to post PR comment: {e}[/yellow]")


if __name__ == "__main__":
    cli()
