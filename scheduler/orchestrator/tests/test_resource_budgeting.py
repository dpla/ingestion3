"""Tests for resource budgeting when running parallel hub ingests."""

import pytest
from scheduler.orchestrator.config import ResourceBudget


class TestResourceBudget:
    """Test ResourceBudget.compute() calculations."""

    def test_single_pipeline_defaults(self):
        """Single pipeline (parallel=1) uses full resources."""
        budget = ResourceBudget.compute(parallel=1)
        assert budget.memory_gb == 8
        assert budget.spark_cores == 4
        assert budget.spark_master == "local[4]"
        assert "-Xmx8g" in budget.sbt_opts

    def test_parallel_2_splits_resources(self):
        """parallel=2 should split resources roughly in half."""
        budget = ResourceBudget.compute(parallel=2, total_memory_gb=16, total_cores=8)
        assert budget.memory_gb == 8  # 16/2 = 8, capped at 8
        assert budget.spark_cores == 4  # 8/2 = 4
        assert budget.spark_master == "local[4]"

    def test_parallel_3_splits_resources(self):
        """parallel=3 on a 12-core 16GB machine."""
        budget = ResourceBudget.compute(parallel=3, total_memory_gb=16, total_cores=12)
        assert budget.memory_gb == 5  # 16/3 = 5
        assert budget.spark_cores == 4  # 12/3 = 4
        assert budget.spark_master == "local[4]"

    def test_parallel_high_ensures_minimums(self):
        """High parallelism still gives at least 1 core and 2g."""
        budget = ResourceBudget.compute(parallel=10, total_memory_gb=16, total_cores=8)
        assert budget.memory_gb >= 2
        assert budget.spark_cores >= 1

    def test_parallel_1_uses_standard_defaults(self):
        """parallel=1 uses the standard 8g/local[4] regardless of args."""
        budget = ResourceBudget.compute(parallel=1, total_memory_gb=64, total_cores=32)
        assert budget.memory_gb == 8
        assert budget.spark_cores == 4

    def test_sbt_opts_format(self):
        """SBT_OPTS should have valid JVM flags."""
        budget = ResourceBudget.compute(parallel=2, total_memory_gb=16, total_cores=8)
        assert budget.sbt_opts.startswith("-Xms")
        assert "-Xmx" in budget.sbt_opts
        assert "-XX:+UseG1GC" in budget.sbt_opts

    def test_spark_master_format(self):
        """Spark master should be local[N] format."""
        budget = ResourceBudget.compute(parallel=3, total_memory_gb=16, total_cores=6)
        assert budget.spark_master.startswith("local[")
        assert budget.spark_master.endswith("]")
        # Extract core count
        cores = int(budget.spark_master.replace("local[", "").replace("]", ""))
        assert cores >= 1
        assert cores == budget.spark_cores

    def test_zero_total_memory_auto_detects(self):
        """total_memory_gb=0 should use auto-detected default (16)."""
        budget = ResourceBudget.compute(parallel=2, total_memory_gb=0, total_cores=8)
        # Default is 16GB, so parallel=2 gives 8g each (capped)
        assert budget.memory_gb == 8

    def test_zero_total_cores_auto_detects(self):
        """total_cores=0 should use os.cpu_count()."""
        budget = ResourceBudget.compute(parallel=2, total_memory_gb=16, total_cores=0)
        # Should not error, cores should be >= 1
        assert budget.spark_cores >= 1

    def test_memory_capped_at_8g(self):
        """Per-hub memory should not exceed 8g even with lots of RAM."""
        budget = ResourceBudget.compute(parallel=2, total_memory_gb=64, total_cores=16)
        assert budget.memory_gb <= 8

    def test_cores_capped_at_8(self):
        """Per-hub cores should not exceed 8 even with many cores."""
        budget = ResourceBudget.compute(parallel=2, total_memory_gb=32, total_cores=64)
        assert budget.spark_cores <= 8
