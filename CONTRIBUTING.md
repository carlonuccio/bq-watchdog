# Contributing to bq-watchdog

Thanks for your interest in contributing!

## Setup

```bash
git clone https://github.com/carlonuccio/bq-watchdog
cd bq-watchdog
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Running tests

The analyser and model tests run with **zero GCP credentials**:

```bash
pytest tests/ -v
```

Tests that require a real BigQuery connection are marked `@pytest.mark.integration`
and are skipped by default:

```bash
pytest tests/ -v -m "not integration"   # default — no credentials needed
pytest tests/ -v -m integration          # requires GCP credentials
```

## Adding a new anti-pattern rule

1. Add a new `_check_*` function in `watchdog/core/analyser.py`
2. Call it from `analyse()` 
3. Add a fixture SQL file in `tests/fixtures/`
4. Add tests in `tests/test_analyser.py`

Example:

```python
# watchdog/core/analyser.py

def _check_my_new_rule(model: str, tree: exp.Expression) -> list[Finding]:
    findings = []
    # ... detect pattern using sqlglot AST
    return findings

def analyse(model_name: str, sql: str) -> list[Finding]:
    ...
    findings += _check_my_new_rule(model_name, tree)   # add here
    return findings
```

## Submitting a PR

- Keep PRs focused — one rule or feature per PR
- All tests must pass
- Add a test for every new rule
- Update README if adding a new rule to the detection table

## Roadmap items open for contribution

- [ ] Dataform SQL support
- [ ] `repeated_subquery` rule — detect CTEs that should be materialised
- [ ] `unpartitioned_table_scan` rule — detect scans on unpartitioned tables > 1 GB
- [ ] Snowflake adapter (same dry run concept, different API)
- [ ] Pre-commit hook integration
