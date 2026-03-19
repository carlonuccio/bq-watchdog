"""
tests/test_analyser.py
----------------------
Tests for the static SQL analyser.
Runs with zero GCP credentials — pure Python AST parsing.

Run with:
    pytest tests/ -v
"""

import pytest
from pathlib import Path
from bq_watchdog.core.analyser import analyse
from bq_watchdog.core.models import DryRunResult, ModelReport, Finding, WatchdogResult

FIXTURES = Path(__file__).parent / "fixtures"


def sql(filename: str) -> str:
    return (FIXTURES / filename).read_text()


# ── SELECT * ──────────────────────────────────────────────────────────────────

class TestSelectStar:
    def test_detects_bare_select_star(self):
        findings = analyse("model", "SELECT * FROM `project.dataset.orders`")
        assert any(f.rule == "select_star" for f in findings)

    def test_detects_from_fixture(self):
        findings = analyse("select_star", sql("select_star.sql"))
        assert any(f.rule == "select_star" for f in findings)

    def test_count_star_not_flagged(self):
        findings = analyse("model", "SELECT COUNT(*) as n FROM `p.d.orders`")
        assert not any(f.rule == "select_star" for f in findings)

    def test_severity_is_warn(self):
        findings = analyse("model", "SELECT * FROM `p.d.orders`")
        star = next(f for f in findings if f.rule == "select_star")
        assert star.severity == "warn"


# ── CROSS JOIN ────────────────────────────────────────────────────────────────

class TestCrossJoin:
    def test_detects_cross_join(self):
        findings = analyse("model", sql("cross_join.sql"))
        assert any(f.rule == "cross_join" for f in findings)

    def test_cross_join_is_block(self):
        findings = analyse("model", sql("cross_join.sql"))
        cross = next(f for f in findings if f.rule == "cross_join")
        assert cross.severity == "block"

    def test_inner_join_not_flagged(self):
        q = """
        SELECT a.id, b.name
        FROM `p.d.orders` a
        JOIN `p.d.customers` b ON a.customer_id = b.id
        WHERE a.order_date >= '2024-01-01'
        """
        findings = analyse("model", q)
        assert not any(f.rule == "cross_join" for f in findings)

    def test_left_join_not_flagged(self):
        q = """
        SELECT a.id, b.name
        FROM `p.d.orders` a
        LEFT JOIN `p.d.customers` b ON a.customer_id = b.id
        """
        findings = analyse("model", q)
        assert not any(f.rule == "cross_join" for f in findings)


# ── LIMIT WITHOUT WHERE ───────────────────────────────────────────────────────

class TestLimitWithoutWhere:
    def test_detects_limit_no_where(self):
        findings = analyse("model", "SELECT id FROM `p.d.orders` LIMIT 100")
        assert any(f.rule == "limit_without_filter" for f in findings)

    def test_limit_with_where_ok(self):
        q = "SELECT id FROM `p.d.orders` WHERE date = '2024-01-01' LIMIT 100"
        findings = analyse("model", q)
        assert not any(f.rule == "limit_without_filter" for f in findings)


# ── MISSING PARTITION FILTER ──────────────────────────────────────────────────

class TestMissingPartitionFilter:
    def test_detects_events_no_filter(self):
        findings = analyse("model", sql("missing_partition.sql"))
        assert any(f.rule == "missing_partition_filter" for f in findings)

    def test_events_with_filter_ok(self):
        q = """
        SELECT user_id FROM `project.dataset.events`
        WHERE event_date = CURRENT_DATE()
        """
        findings = analyse("model", q)
        assert not any(f.rule == "missing_partition_filter" for f in findings)

    def test_normal_table_not_flagged(self):
        q = "SELECT id, name FROM `project.dataset.customers`"
        findings = analyse("model", q)
        assert not any(f.rule == "missing_partition_filter" for f in findings)


# ── CLEAN QUERY ───────────────────────────────────────────────────────────────

class TestCleanQuery:
    def test_clean_fixture_has_no_findings(self):
        findings = analyse("clean_query", sql("clean_query.sql"))
        assert findings == []

    def test_specific_columns_with_filter(self):
        q = """
        SELECT order_id, customer_id, total
        FROM `project.dataset.orders`
        WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        """
        findings = analyse("model", q)
        assert findings == []


# ── EXPENSIVE QUERY (multiple issues) ────────────────────────────────────────

class TestExpensiveQuery:
    def test_expensive_fixture_gets_multiple_findings(self):
        findings = analyse("expensive", sql("expensive_query.sql"))
        rules = {f.rule for f in findings}
        assert len(rules) >= 2

    def test_all_findings_have_required_fields(self):
        findings = analyse("model", "SELECT * FROM `p.d.events` LIMIT 100")
        for f in findings:
            assert f.model
            assert f.rule
            assert f.severity in ("block", "warn", "info")
            assert f.description


# ── MODELS ────────────────────────────────────────────────────────────────────

class TestModels:
    def test_dry_run_cost_one_tb(self):
        dr = DryRunResult(model="test", bytes_processed=1024**4)
        assert abs(dr.cost_usd - 6.25) < 0.01
        assert dr.severity == "block"

    def test_dry_run_free_query(self):
        dr = DryRunResult(model="test", bytes_processed=100 * 1024**2)
        assert dr.cost_usd < 0.01
        assert dr.severity == "ok"

    def test_dry_run_warn_threshold(self):
        dr = DryRunResult(model="test", bytes_processed=85 * 1024**3)
        assert dr.severity == "warn"

    def test_model_report_suggestion_mutable(self):
        """ModelReport.suggestion must be settable after creation."""
        dr     = DryRunResult(model="test", bytes_processed=0)
        report = ModelReport(name="test", sql="SELECT 1", dry_run=dr)
        assert report.suggestion is None
        report.suggestion = "Use a partition filter"
        assert report.suggestion == "Use a partition filter"

    def test_model_report_block_on_finding(self):
        dr      = DryRunResult(model="test", bytes_processed=0)
        finding = Finding(
            model="test", rule="cross_join",
            severity="block", description="cross join"
        )
        report = ModelReport(name="test", sql="", dry_run=dr, findings=[finding])
        assert report.overall_severity == "block"

    def test_watchdog_total_cost(self):
        dr1    = DryRunResult(model="a", bytes_processed=1024**4)
        dr2    = DryRunResult(model="b", bytes_processed=2 * 1024**4)
        result = WatchdogResult(
            reports=[
                ModelReport(name="a", sql="", dry_run=dr1),
                ModelReport(name="b", sql="", dry_run=dr2),
            ],
            project_id="p", run_at="now"
        )
        assert abs(result.total_cost_usd - 18.75) < 0.01

    def test_watchdog_has_blocks(self):
        dr_block = DryRunResult(model="a", bytes_processed=10 * 1024**4)
        dr_ok    = DryRunResult(model="b", bytes_processed=0)
        result   = WatchdogResult(
            reports=[
                ModelReport(name="a", sql="", dry_run=dr_block),
                ModelReport(name="b", sql="", dry_run=dr_ok),
            ],
            project_id="p", run_at="now"
        )
        assert result.has_blocks is True
        assert len(result.flagged) == 1
