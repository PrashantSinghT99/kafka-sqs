"""Structural guardrails for the GitHub Actions test workflow."""

from pathlib import Path

import pytest
import yaml


@pytest.mark.unit
def test_ci_workflow_has_fast_kafka_and_sqs_evidence_gates() -> None:
    workflow_path = Path(__file__).parents[2] / ".github" / "workflows" / "test.yml"
    workflow = yaml.load(
        workflow_path.read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )

    jobs = workflow["jobs"]
    assert set(jobs) == {"unit-contract", "kafka-integration", "sqs-integration"}
    assert jobs["kafka-integration"]["needs"] == "unit-contract"
    assert jobs["sqs-integration"]["needs"] == "unit-contract"
    assert all(int(job["timeout-minutes"]) <= 20 for job in jobs.values())
    assert all(
        any(step.get("uses") == "actions/upload-artifact@v4" for step in job["steps"])
        for job in jobs.values()
    )
