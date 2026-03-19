"""
watchdog/core/analyser.py
-------------------------
Static SQL analysis using sqlglot AST parsing.
Detects BigQuery cost anti-patterns before they reach production.

Why sqlglot instead of regex:
- Parses SQL into a proper AST — no false positives
- Supports BigQuery dialect natively
- Can distinguish COUNT(*) from SELECT * correctly
- Handles CTEs, subqueries, nested selects
"""

import sqlglot
import sqlglot.expressions as exp
from .models import Finding

# Table name patterns that likely indicate large/partitioned tables
HIGH_RISK_TABLE_PATTERNS = frozenset([
    "events", "logs", "sessions", "pageviews", "clicks",
    "impressions", "raw_", "fact_", "stream_", "tracking",
])


def analyse(model_name: str, sql: str) -> list[Finding]:
    """
    Parse SQL and detect cost anti-patterns.
    Returns list of findings, empty if query looks clean.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect="bigquery")
    except sqlglot.errors.ParseError:
        return []   # unparseable — dry run will report the error

    findings = []
    findings += _check_select_star(model_name, tree)
    findings += _check_missing_partition_filter(model_name, tree)
    findings += _check_limit_without_filter(model_name, tree)
    findings += _check_cross_join(model_name, tree)
    return findings


def _check_select_star(
    model: str, tree: exp.Expression
) -> list[Finding]:
    findings = []
    for node in tree.find_all(exp.Star):
        # COUNT(*) is fine — skip
        if isinstance(node.parent, exp.Count):
            continue
        findings.append(Finding(
            model=model,
            rule="select_star",
            severity="warn",
            description=(
                "SELECT * scans every column in the table. "
                "BigQuery charges per byte — unused columns cost money. "
                "Specify only the columns your model needs."
            ),
            snippet="SELECT *",
        ))
        break   # one finding per model is enough

    return findings


def _check_missing_partition_filter(
    model: str, tree: exp.Expression
) -> list[Finding]:
    findings = []

    for table_node in tree.find_all(exp.Table):
        name = (table_node.name or "").lower()
        if not any(p in name for p in HIGH_RISK_TABLE_PATTERNS):
            continue

        # Find the SELECT that contains this table
        select = table_node.find_ancestor(exp.Select)
        if not select:
            continue

        # If there's no WHERE clause, flag it
        if not select.find(exp.Where):
            findings.append(Finding(
                model=model,
                rule="missing_partition_filter",
                severity="warn",
                description=(
                    f"Table `{name}` looks like a high-volume table "
                    f"with no WHERE filter — this may scan the full table. "
                    f"Add a date/partition filter to reduce bytes scanned."
                ),
                snippet=f"FROM {name} (no WHERE clause)",
            ))

    return findings


def _check_limit_without_filter(
    model: str, tree: exp.Expression
) -> list[Finding]:
    findings = []
    for select in tree.find_all(exp.Select):
        has_limit = bool(select.find(exp.Limit))
        has_where = bool(select.find(exp.Where))
        if has_limit and not has_where:
            findings.append(Finding(
                model=model,
                rule="limit_without_filter",
                severity="warn",
                description=(
                    "LIMIT without WHERE scans the full table before limiting rows. "
                    "This is a common misconception — LIMIT 100 does not reduce cost. "
                    "Add a WHERE clause to actually reduce bytes scanned."
                ),
                snippet="LIMIT without WHERE",
            ))
            break
    return findings


def _check_cross_join(
    model: str, tree: exp.Expression
) -> list[Finding]:
    findings = []
    for join in tree.find_all(exp.Join):
        join_type = str(join.args.get("kind", "")).upper()
        if join_type == "CROSS":
            findings.append(Finding(
                model=model,
                rule="cross_join",
                severity="block",
                description=(
                    "CROSS JOIN multiplies every row in both tables — "
                    "this is almost always unintentional and extremely expensive. "
                    "Verify this is intended and add a comment explaining why."
                ),
                snippet="CROSS JOIN",
            ))
    return findings
