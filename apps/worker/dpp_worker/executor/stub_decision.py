"""Stub implementation of DecisionPack executor (v0.4.2.2 minimum)."""

from typing import Any

from dpp_worker.executor.base import PackExecutor


class StubDecisionExecutor(PackExecutor):
    """Minimal stub executor for Decision pack.

    This is a placeholder implementation for MS-3.
    Real implementation will integrate LLM/reasoning in later milestones.
    """

    def execute(
        self,
        run_id: str,
        inputs: dict[str, Any],
        timebox_sec: int,
        max_cost_usd_micros: int,
    ) -> tuple[dict[str, Any], int]:
        """Execute decision pack (stub implementation).

        Stub behavior:
        - Extracts question from inputs
        - Returns a dummy answer
        - Calculates minimal actual cost ($0.05 or less)

        Args:
            run_id: Run ID
            inputs: { "question": str, "context": str?, "mode": "brief|full"? }
            timebox_sec: Maximum execution time (unused in stub)
            max_cost_usd_micros: Maximum cost

        Returns:
            (envelope_data, actual_cost_usd_micros)
        """
        question = inputs.get("question", "")
        context = inputs.get("context", "")
        mode = inputs.get("mode", "brief")

        # Stub answer generation
        answer_text = f"[Stub] Decision for: {question[:50]}... Mode: {mode}"
        if context:
            answer_text += f" (with context: {len(context)} chars)"

        # Stub cost calculation: $0.05 or max_cost, whichever is smaller
        # DEC-4211: All money in USD_MICROS
        stub_cost_usd_micros = 50_000  # $0.05
        actual_cost_usd_micros = min(stub_cost_usd_micros, max_cost_usd_micros)

        # Build envelope data
        envelope_data = {
            "data": {
                "answer_text": answer_text,
                "confidence": 0.85,  # Display-only; not used for money calculation
                "question": question,
                "mode": mode,
            },
            "artifacts": {},
            "logs": {
                "discard_log": [],
                "blocked_log": [],
            },
        }

        return (envelope_data, actual_cost_usd_micros)
