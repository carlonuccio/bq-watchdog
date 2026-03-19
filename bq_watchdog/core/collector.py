"""
watchdog/core/collector.py
--------------------------
Reads compiled SQL files from dbt target/compiled/.
Works with any dbt project — no configuration needed.
"""

from pathlib import Path
from dataclasses import dataclass


@dataclass
class CompiledModel:
    name: str
    path: Path
    sql:  str


# dbt internal directories to skip
SKIP_DIRS = frozenset({"tests", "schema_test", "data_test", "snapshots"})


def collect_compiled_sql(target_dir: str = "target") -> list[CompiledModel]:
    """
    Collect all compiled model SQL files from dbt target/compiled/.
    Skips test files and empty SQL files.

    Args:
        target_dir: Path to dbt target directory (default: "target")

    Returns:
        List of CompiledModel sorted by name

    Raises:
        FileNotFoundError: if target/compiled/ doesn't exist
    """
    compiled_path = Path(target_dir) / "compiled"

    if not compiled_path.exists():
        raise FileNotFoundError(
            f"No compiled SQL found at {compiled_path}.\n"
            f"Run `dbt compile` first, then re-run bq-watchdog."
        )

    models = []
    for sql_file in compiled_path.rglob("*.sql"):
        # Skip dbt test directories
        if any(skip in sql_file.parts for skip in SKIP_DIRS):
            continue

        sql = sql_file.read_text().strip()
        if not sql:
            continue

        models.append(CompiledModel(
            name=sql_file.stem,
            path=sql_file,
            sql=sql,
        ))

    return sorted(models, key=lambda m: m.name)
