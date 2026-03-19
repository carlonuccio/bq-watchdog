"""
watchdog/agent/suggester.py
---------------------------
Claude-powered fix suggestions for flagged models.
Called only when a model has warn/block findings.
Returns markdown formatted for a GitHub PR comment.
"""

import anthropic
from bq_watchdog.core.models import Finding, DryRunResult

def _get_client():
    return anthropic.Anthropic()

SYSTEM = """You are a BigQuery cost optimisation expert reviewing a dbt model in a GitHub PR.

The model has been flagged for cost issues. Your job:
1. Explain clearly WHY this query is expensive — cite the specific bytes and cost
2. Show the exact problem in the SQL (quote the relevant lines)
3. Provide a corrected SQL version that fixes the issue
4. Explain what changed and why it reduces cost

Format your response as GitHub-flavoured Markdown.
Use a code block for the corrected SQL.
Be concise — this appears directly in a PR comment.
Never change business logic, only cost optimisation.
If you need to make assumptions (e.g. which column is the partition key), say so.
"""


def suggest_fix(
    model_name: str,
    sql:        str,
    findings:   list[Finding],
    dry_run:    DryRunResult,
) -> str:
    """
    Generate an AI fix suggestion for a flagged model.
    Returns markdown string for the PR comment.
    """
    findings_text = "\n".join([
        f"- [{f.severity.upper()}] `{f.rule}`: {f.description}"
        for f in findings
    ])

    # Truncate very long SQL for the prompt
    sql_for_prompt = sql if len(sql) < 3000 else sql[:3000] + "\n-- [truncated]"

    response = _get_client().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=800,
        system=SYSTEM,
        messages=[{
            "role": "user",
            "content": (
                f"**Model:** `{model_name}`\n"
                f"**Estimated scan:** {dry_run.gb:.1f} GB "
                f"(${dry_run.cost_usd:.4f} per run)\n\n"
                f"**Issues detected:**\n{findings_text}\n\n"
                f"**Compiled SQL:**\n```sql\n{sql_for_prompt}\n```\n\n"
                f"Please explain the problem and provide an optimised version."
            )
        }]
    )

    return response.content[0].text


def suggest_fixes_for_flagged(
    reports: list,   # list[ModelReport]
) -> dict[str, str]:
    """
    Generate suggestions for all flagged models.
    Returns dict of model_name → suggestion markdown.
    """
    suggestions = {}
    flagged = [r for r in reports if r.overall_severity in ("warn", "block")]

    if not flagged:
        return suggestions

    print(f"\n🤖 Generating AI suggestions for {len(flagged)} flagged model(s)...")

    for report in flagged:
        if not report.findings and report.dry_run.severity == "ok":
            continue
        print(f"  Analysing {report.name}...")
        suggestions[report.name] = suggest_fix(
            model_name=report.name,
            sql=report.sql,
            findings=report.findings,
            dry_run=report.dry_run,
        )

    return suggestions