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
    # Accumulate findings
    findings += _check_select_star(model_name, tree)
    findings += _check_cross_join(model_name, tree)
    findings += _check_limit_without_filter(model_name, tree)
    findings += _check_missing_partition_filter(model_name, tree)
    
    # New checks
    findings += _check_self_join(model_name, tree)
    findings += _check_repeated_cte_reference(model_name, tree)
    findings += _check_regex_in_where(model_name, tree)
    findings += _check_join_order_large_first(model_name, tree)
    findings += _check_dynamic_partition_pruning_risk(model_name, tree)
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


def _check_self_join(
    model: str, tree: exp.Expression
) -> list[Finding]:
    findings = []
    
    # Check each SELECT or subquery independently
    for select in tree.find_all(exp.Select):
        # Find all table names in this SELECT's FROM and JOIN clauses
        # We only care about base tables, not aliases
        tables = []
        for table_node in select.find_all(exp.Table):
            if table_node.name:
                tables.append(table_node.name.lower())
        
        # Check for duplicates
        seen = set()
        for t in tables:
            if t in seen:
                findings.append(Finding(
                    model=model,
                    rule="self_join",
                    severity="warn",
                    description=(
                        f"Table `{t}` is joined against itself. "
                        "Self-joins are computationally expensive. "
                        "Consider using window functions instead."
                    ),
                    snippet=f"JOIN {t}",
                ))
            else:
                seen.add(t)
    return findings


def _check_repeated_cte_reference(
    model: str, tree: exp.Expression
) -> list[Finding]:
    findings = []
    
    with_node = tree.find(exp.With)
    if not with_node:
        return findings

    # Get names of all CTEs defined in the WITH clause
    cte_names = [cte.alias.lower() for cte in with_node.expressions if cte.alias]
    
    if not cte_names:
        return findings

    # We will count how many times each CTE name appears as a Table source
    # anywhere in the entire query. 
    cte_ref_counts = {name: 0 for name in cte_names}
    
    for table_node in tree.find_all(exp.Table):
        name = (table_node.name or "").lower()
        if name in cte_ref_counts:
            cte_ref_counts[name] += 1
            
    for cte_name, count in cte_ref_counts.items():
        # A CTE will be referenced at least once (otherwise it's unused), 
        # but if it's > 1, it's evaluated multiple times.
        if count > 1:
             findings.append(Finding(
                model=model,
                rule="repeated_cte_reference",
                severity="warn",
                description=(
                    f"CTE `{cte_name}` is referenced {count} times. "
                    "BigQuery may re-evaluate the CTE on each reference, which can be expensive. "
                    "Consider materializing it as a temporary table if it's computationally heavy."
                ),
                snippet=f"{cte_name}",
            ))

    return findings


def _check_regex_in_where(
    model: str, tree: exp.Expression
) -> list[Finding]:
    findings = []
    for node in tree.find_all(exp.Anonymous, exp.RegexpLike):
        if isinstance(node, exp.Anonymous):
            func_name = node.name.upper()
        else:
            func_name = "REGEXP_LIKE" # Actually REGEXP_CONTAINS mostly, RegexpLike applies here
            
        if func_name.startswith("REGEXP_"):
            if node.find_ancestor(exp.Where) or node.find_ancestor(exp.Select):
                findings.append(Finding(
                    model=model,
                    rule="regex_in_where",
                    severity="warn",
                    description=(
                        f"Found `{func_name}` in query. "
                        "Regular expressions are CPU-intensive. "
                        "Avoid repeatedly transforming data; instead, consider materializing the parsed result in a staging table."
                    ),
                    snippet=f"{func_name}(...)",
                ))
                break
    return findings


def _check_join_order_large_first(
    model: str, tree: exp.Expression
) -> list[Finding]:
    findings = []
    
    for select in tree.find_all(exp.Select):
        if not select.args.get("joins"):
            continue
            
        for join in select.args.get("joins"):
            join_table_node = join.this
            if isinstance(join_table_node, exp.Table):
                # .name on Table gives the base identifier, e.g. "events" from `p.d.events`
                join_table_name = (join_table_node.name or "").lower()
                
                if any(p in join_table_name for p in HIGH_RISK_TABLE_PATTERNS):
                    findings.append(Finding(
                        model=model,
                        rule="join_order_large_first",
                        severity="info",
                        description=(
                            f"Table `{join_table_name}` looks like a large/fact table but is on the right side of a JOIN. "
                            "BigQuery optimizes broadcast joins best when the largest table is placed first (on the left)."
                        ),
                        snippet=f"JOIN ... {join_table_name}",
                    ))
                    
    return findings


def _check_dynamic_partition_pruning_risk(
    model: str, tree: exp.Expression
) -> list[Finding]:
    findings = []
    
    for where in tree.find_all(exp.Where):
        for subquery in where.find_all(exp.Subquery):
            if subquery.find(exp.AggFunc):
                findings.append(Finding(
                    model=model,
                    rule="dynamic_partition_pruning_risk",
                    severity="warn",
                    description=(
                        "Found an aggregate subquery in the WHERE clause. "
                        "BigQuery does not support dynamic partition pruning. "
                        "This will force a full table scan of the source table. "
                        "To fix this in dbt, pre-compute the value using a Jinja `run_query` hook before the model SQL."
                    ),
                    snippet="WHERE ... (SELECT MAX(...) ...)",
                ))
                return findings

    return findings
