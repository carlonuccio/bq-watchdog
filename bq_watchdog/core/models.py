"""
watchdog/core/models.py
-----------------------
Pydantic models for bq-watchdog findings and results.
"""

from pydantic import BaseModel, computed_field, ConfigDict
from typing import Literal, Optional

BQ_PRICE_PER_TB = 6.25   # USD per TB, on-demand pricing 2025


class Finding(BaseModel):
    model:       str
    rule:        str
    severity:    Literal["block", "warn", "info"]
    description: str
    snippet:     str = ""


class DryRunResult(BaseModel):
    model:           str
    bytes_processed: int = 0
    error:           str | None = None

    @computed_field
    @property
    def gb(self) -> float:
        return self.bytes_processed / (1024 ** 3)

    @computed_field
    @property
    def tb(self) -> float:
        return self.bytes_processed / (1024 ** 4)

    @computed_field
    @property
    def cost_usd(self) -> float:
        return self.tb * BQ_PRICE_PER_TB

    @computed_field
    @property
    def severity(self) -> str:
        if self.error:
            return "error"
        if self.cost_usd >= 5.0:
            return "block"
        if self.cost_usd >= 0.50:
            return "warn"
        return "ok"

    @computed_field
    @property
    def icon(self) -> str:
        return {"ok": "✅", "warn": "⚠️", "block": "❌", "error": "💥"}.get(
            self.severity, "⚪"
        )


class ModelReport(BaseModel):
    """Combined dry run + static analysis + AI suggestion for one model."""

    # Allow mutation so cli.py can set .suggestion after creation
    model_config = ConfigDict(frozen=False)

    name:       str
    sql:        str
    dry_run:    DryRunResult
    findings:   list[Finding] = []
    suggestion: Optional[str] = None   # AI-generated fix, populated after init

    @computed_field
    @property
    def overall_severity(self) -> str:
        severities = [self.dry_run.severity]
        for f in self.findings:
            severities.append(f.severity)
        if "block" in severities or "error" in severities:
            return "block"
        if "warn" in severities:
            return "warn"
        return "ok"


class WatchdogResult(BaseModel):
    """Full result for all models in a dbt project."""
    reports:     list[ModelReport]
    project_id:  str
    run_at:      str

    @computed_field
    @property
    def total_cost_usd(self) -> float:
        return sum(r.dry_run.cost_usd for r in self.reports)

    @computed_field
    @property
    def has_blocks(self) -> bool:
        return any(r.overall_severity == "block" for r in self.reports)

    @computed_field
    @property
    def flagged(self) -> list[ModelReport]:
        return [r for r in self.reports if r.overall_severity != "ok"]
