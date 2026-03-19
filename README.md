# 🐕 bq-watchdog

> AI-powered BigQuery cost guard for dbt projects.
> Catches expensive queries in CI — before they hit production.

[![PyPI](https://img.shields.io/pypi/v/bq-watchdog)](https://pypi.org/project/bq-watchdog/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Tests](https://github.com/carlonuccio/bq-watchdog/actions/workflows/tests.yml/badge.svg)](https://github.com/carlonuccio/bq-watchdog/actions)

---

## The problem

Someone merges a dbt model that looks fine in dev.
In production it scans 40 TB on every run.
You find out when the GCP bill arrives.

`bq-watchdog` catches it in the PR — before it ever reaches production.

```
PR #47 — add daily_revenue model

🐕 bq-watchdog cost report

| Model             | Scan    | Cost/run | Status     |
|-------------------|---------|----------|------------|
| customer_lifetime | 2.1 TB  | $13.13   | ❌ BLOCK   |
| daily_revenue     | 180 GB  | $1.13    | ⚠️ WARN    |
| stg_orders        | 0.3 GB  | $0.00    | ✅ OK      |
| stg_customers     | 0.1 GB  | $0.00    | ✅ OK      |

❌ customer_lifetime — BLOCK

Problem: SELECT * scans all 47 columns (2.1 TB). On a daily schedule
this costs ~$4,800/month.

Fix:
  SELECT customer_id, event_type, event_date
  FROM `project.dataset.events`
  WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)

This reduces scan from 2.1 TB to ~12 GB — saving ~$13 per run.
```

---

## How it works

1. **dbt compile** — generates `target/compiled/` with final SQL for every model
2. **Dry run** — BigQuery dry run per model (free, instant, no slots used)
3. **Static analysis** — AST-based detection of anti-patterns via `sqlglot`
4. **dbt Config Advisor** — Inspects `manifest.json` for suboptimal configurations (e.g. missing clustering)
5. **AI fix** — Claude explains the problem, suggests config optimizations, and rewrites the SQL
6. **PR comment** — structured cost table + fixes posted automatically

---

## Quickstart

### As a GitHub Action (recommended)

Add this to `.github/workflows/bq_cost_check.yml`:

```yaml
name: BigQuery Cost Check

on:
  pull_request:
    paths:
      - "models/**"

jobs:
  cost-check:
    runs-on: ubuntu-latest
    permissions:
      contents:      read
      id-token:      write
      pull-requests: write

    steps:
      - uses: actions/checkout@v4

      - uses: carlonuccio/bq-watchdog@v1
        with:
          gcp_project:       ${{ vars.GCP_PROJECT_ID }}
          anthropic_api_key: ${{ secrets.ANTHROPIC_API_KEY }}
```

That's it. Every PR that touches `models/**` now gets a cost breakdown.

### As a CLI

```bash
pip install bq-watchdog

# Run against your dbt project
dbt compile
watchdog run --project my-gcp-project

# Compute monthly cost based on schedule and output to SARIF for GitHub Advanced Security
watchdog run --project my-gcp-project \
  --schedule daily \
  --output sarif > bq-watchdog-results.sarif

# Custom thresholds
watchdog run --project my-gcp-project \
  --warn-threshold 0.25 \
  --block-threshold 2.00
```

---

## Anti-patterns detected

| Rule | Severity | Description |
|------|----------|-------------|
| `select_star` | ⚠️ warn | `SELECT *` scans all columns |
| `missing_partition_filter` | ⚠️ warn | High-volume table with no WHERE clause |
| `limit_without_filter` | ⚠️ warn | `LIMIT` without `WHERE` still scans full table |
| `cross_join` | ❌ block | Cartesian product — almost always unintentional |
| `self_join` | ⚠️ warn | Table joined to itself — use window functions instead |
| `repeated_cte_reference` | ⚠️ warn | CTE referenced multiple times — consider materialization |
| `regex_in_where` | ⚠️ warn | Expensive `REGEXP` operations found in filter |
| `join_order_large_first` | ℹ️ info | Fact/large table is on the right side of a JOIN |
| `dynamic_partition_pruning_risk` | ⚠️ warn | Scalar subquery in filter prevents dynamic partition pruning |
| `missing_clustering_config` | ℹ️ info | Incremental partitioned table missing `cluster_by` config |

---

## Configuration

| Input | Default | Description |
|-------|---------|-------------|
| `gcp_project` | required | GCP project ID |
| `bq_location` | `EU` | BigQuery dataset location |
| `warn_threshold` | `0.50` | Cost (USD) per run to warn |
| `block_threshold` | `5.00` | Cost (USD) per run to block PR |
| `anthropic_api_key` | optional | Enables AI fix suggestions |
| `post_pr_comment` | `true` | Post cost breakdown to PR |
| `schedule` | optional | Compute monthly costs (e.g., `hourly`, `daily`, `weekly`) |
| `output` | `table` | Output format (`table`, `json`, `sarif`) |

---

## Local development

```bash
git clone https://github.com/carlonuccio/bq-watchdog
cd bq-watchdog

python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Run tests (no GCP credentials needed)
pytest tests/ -v

# Run against a real project
cp .env.example .env    # add your keys
dbt compile             # in your dbt project
watchdog run --project your-gcp-project --target path/to/target
```

---

## Why dry runs are safe

BigQuery dry runs:
- Are **completely free** — no bytes billed, no slots consumed
- Complete in **milliseconds**
- **Respect partition pruning** — accurate estimates for filtered queries
- Do **not** execute the query or return any data

---

## Comparison

| Tool | Cost detection | AI fixes | Open source | Price |
|------|---------------|---------|-------------|-------|
| **bq-watchdog** | ✅ Pre-merge | ✅ Yes | ✅ MIT | Free |
| Monte Carlo | ✅ Post-run | ✅ Yes | ❌ No | $100k+/yr |
| Bigeye | ✅ Post-run | ❌ No | ❌ No | $$$|
| Manual review | ✅ Sometimes | ❌ No | — | Your time |

---

## Roadmap

- [ ] Dataform support
- [ ] Slot vs on-demand pricing advisor
- [ ] Cost trend dashboard across PRs
- [ ] Pre-commit hook
- [ ] dbt Cloud webhook integration

---

## Contributing

PRs welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

---

## License

MIT — see [LICENSE](LICENSE).

---

Built by [Carlo Nuccio](https://linkedin.com/in/carlonuccio) ·
[LinkedIn post](https://linkedin.com) ·
[dbt Slack discussion](#)
