"""Base class for pack executors."""

from abc import ABC, abstractmethod
from typing import Any


class PackExecutor(ABC):
    """Base class for all pack executors.

    Each pack type (decision, url, ocr, etc.) implements this interface.
    """

    @abstractmethod
    def execute(
        self,
        run_id: str,
        inputs: dict[str, Any],
        timebox_sec: int,
        max_cost_usd_micros: int,
    ) -> tuple[dict[str, Any], int]:
        """Execute the pack and return (envelope_data, actual_cost_usd_micros).

        Args:
            run_id: Run ID
            inputs: Pack-specific inputs from RunCreateRequest
            timebox_sec: Maximum execution time in seconds
            max_cost_usd_micros: Maximum cost (for cost calculation)

        Returns:
            Tuple of (envelope_data, actual_cost_usd_micros)
            - envelope_data: Dict containing data, artifacts, logs for pack_envelope.json
            - actual_cost_usd_micros: Actual cost in USD_MICROS (BIGINT)

        Raises:
            TimeoutError: If execution exceeds timebox_sec
            Exception: Pack-specific errors
        """
        pass
