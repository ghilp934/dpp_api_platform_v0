"""Tests for pack executors."""

import uuid

from dpp_worker.executor.stub_decision import StubDecisionExecutor


def test_stub_decision_executor_basic():
    """Test basic stub decision executor."""
    executor = StubDecisionExecutor()

    run_id = str(uuid.uuid4())
    inputs = {
        "question": "Should we proceed with the project?",
        "context": "Budget is limited",
        "mode": "brief",
    }
    timebox_sec = 90
    max_cost_usd_micros = 1_000_000  # $1.00

    envelope_data, actual_cost_usd_micros = executor.execute(
        run_id=run_id,
        inputs=inputs,
        timebox_sec=timebox_sec,
        max_cost_usd_micros=max_cost_usd_micros,
    )

    # Verify cost calculation
    assert actual_cost_usd_micros == 50_000  # $0.05 stub cost
    assert actual_cost_usd_micros <= max_cost_usd_micros

    # Verify envelope data structure
    assert "data" in envelope_data
    assert "artifacts" in envelope_data
    assert "logs" in envelope_data

    # Verify data fields
    data = envelope_data["data"]
    assert "answer_text" in data
    assert "confidence" in data
    assert "question" in data
    assert data["question"] == inputs["question"]
    assert data["mode"] == "brief"

    # Verify answer contains question snippet
    assert "Should we proceed" in data["answer_text"]


def test_stub_decision_executor_cost_cap():
    """Test that stub executor respects max_cost cap."""
    executor = StubDecisionExecutor()

    run_id = str(uuid.uuid4())
    inputs = {"question": "Test question"}
    timebox_sec = 90
    max_cost_usd_micros = 10_000  # $0.01 (less than stub cost)

    envelope_data, actual_cost_usd_micros = executor.execute(
        run_id=run_id,
        inputs=inputs,
        timebox_sec=timebox_sec,
        max_cost_usd_micros=max_cost_usd_micros,
    )

    # Stub cost is $0.05, but max is $0.01
    # Executor should cap at max_cost
    assert actual_cost_usd_micros == max_cost_usd_micros
    assert actual_cost_usd_micros == 10_000
