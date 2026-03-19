"""
watchdog/output/pr_comment.py
------------------------------
Build and post GitHub PR comments with cost breakdown.
"""

from github import Github
from bq_watchdog.core.models import WatchdogResult


def build_comment(result: WatchdogResult) -> str:
    """Build markdown PR comment from WatchdogResult."""
    lines = ["## 🐕 bq-watchdog cost report\n"]

    # Cost table
    lines.append("| Model | Scan | Cost/run | Status |")
    lines.append("|-------|------|----------|--------|")

    for r in sorted(result.reports,
                    key=lambda x: x.dry_run.cost_usd, reverse=True):
        if r.dry_run.error:
            row = f"| `{r.name}` | error | — | 💥 ERROR |"
        else:
            row = (
                f"| `{r.name}` "
                f"| {r.dry_run.gb:.1f} GB "
                f"| ${r.dry_run.cost_usd:.4f} "
                f"| {r.dry_run.icon} {r.overall_severity.upper()} |"
            )
        lines.append(row)

    lines.append(f"\n**Total estimated cost per run: ${result.total_cost_usd:.4f}**")

    # Monthly estimate (assumes daily schedule)
    monthly = result.total_cost_usd * 30
    if monthly > 1:
        lines.append(f"_~${monthly:.2f}/month on a daily schedule_")

    # AI suggestions for flagged models
    for r in result.reports:
        if r.suggestion:
            icon  = "❌" if r.overall_severity == "block" else "⚠️"
            lines.append(f"\n---\n### {icon} `{r.name}` — {r.overall_severity.upper()}\n")
            lines.append(r.suggestion)

    # Block notice
    if result.has_blocks:
        lines.append(
            "\n> ❌ **This PR is blocked** — one or more models exceed the "
            "cost threshold. Fix the issues above before merging."
        )

    lines.append(
        "\n<sub>bq-watchdog · dry runs are free and instant · "
        "[GitHub](https://github.com/yourusername/bq-watchdog)</sub>"
    )

    return "\n".join(lines)


def post_comment(
    token:      str,
    repo_name:  str,
    pr_number:  int,
    body:       str,
) -> None:
    """Post (or update) bq-watchdog comment on a PR."""
    g  = Github(token)
    pr = g.get_repo(repo_name).get_pull(pr_number)

    # Delete previous bq-watchdog comment to avoid clutter
    for comment in pr.get_issue_comments():
        if "bq-watchdog" in comment.body:
            comment.delete()

    pr.create_issue_comment(body)
