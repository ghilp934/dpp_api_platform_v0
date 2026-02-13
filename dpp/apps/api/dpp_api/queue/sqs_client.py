"""SQS client for enqueueing runs."""

import json
import os
from datetime import datetime, timezone
from typing import Any

import boto3


class SQSClient:
    """SQS client wrapper for DPP."""

    def __init__(self):
        """Initialize SQS client."""
        sqs_endpoint = os.getenv("SQS_ENDPOINT_URL", "http://localhost:4566")
        self.queue_url = os.getenv("SQS_QUEUE_URL", "http://localhost:4566/000000000000/dpp-runs")

        # P0-2: Only use test credentials for LocalStack
        # Production uses boto3 default credential chain (IAM roles, env vars, etc.)
        sqs_kwargs = {
            "endpoint_url": sqs_endpoint,
            "region_name": "us-east-1",
        }

        # Check if endpoint is LocalStack (localhost or 127.0.0.1)
        if sqs_endpoint and ("localhost" in sqs_endpoint or "127.0.0.1" in sqs_endpoint):
            sqs_kwargs["aws_access_key_id"] = "test"
            sqs_kwargs["aws_secret_access_key"] = "test"

        self.client = boto3.client("sqs", **sqs_kwargs)

    def enqueue_run(self, run_id: str, tenant_id: str, pack_type: str, trace_id: str | None = None) -> str:
        """
        Enqueue a run for processing.

        Args:
            run_id: Run ID (UUID)
            tenant_id: Tenant ID
            pack_type: Pack type
            trace_id: Trace ID for observability (optional)

        Returns:
            SQS Message ID

        Raises:
            Exception: If enqueue fails
        """
        message_body = {
            "run_id": run_id,
            "tenant_id": tenant_id,
            "pack_type": pack_type,
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": "1",
            "trace_id": trace_id,  # Observability: trace across API → Worker → Reaper
        }

        response = self.client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=json.dumps(message_body),
        )

        return response["MessageId"]


# Singleton instance
_sqs_client: SQSClient | None = None


def get_sqs_client() -> SQSClient:
    """Get SQS client singleton."""
    global _sqs_client
    if _sqs_client is None:
        _sqs_client = SQSClient()
    return _sqs_client
