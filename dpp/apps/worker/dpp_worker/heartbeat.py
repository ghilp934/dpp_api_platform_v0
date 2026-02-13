"""Lease heartbeat for long-running worker tasks (P0-D).

Prevents zombie detection by periodically:
1. Extending DB lease_expires_at
2. Extending SQS message visibility timeout
"""

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from sqlalchemy.orm import Session

from dpp_api.db.repo_runs import RunRepository

logger = logging.getLogger(__name__)


class HeartbeatThread(threading.Thread):
    """Background thread that sends periodic heartbeats for a run.

    Extends:
    - DB lease_expires_at (prevents Reaper from timing out)
    - SQS visibility timeout (prevents duplicate processing)

    P0-1: Thread-safe session management.
    Creates a new Session for each heartbeat tick to avoid thread-safety issues.
    """

    def __init__(
        self,
        run_id: str,
        tenant_id: str,
        lease_token: str,
        current_version: int,
        session_factory: Callable[[], Session],
        sqs_client: Any,
        queue_url: str,
        receipt_handle: str,
        heartbeat_interval_sec: int = 30,
        lease_extension_sec: int = 120,
    ):
        """Initialize heartbeat thread.

        Args:
            run_id: Run ID
            tenant_id: Tenant ID
            lease_token: Lease token to verify ownership
            current_version: Current version for optimistic locking
            session_factory: SessionLocal factory for creating thread-safe sessions
            sqs_client: boto3 SQS client
            queue_url: SQS queue URL
            receipt_handle: SQS message receipt handle
            heartbeat_interval_sec: How often to send heartbeat (default 30s)
            lease_extension_sec: How much to extend lease/visibility (default 120s)
        """
        super().__init__(daemon=True, name=f"Heartbeat-{run_id[:8]}")
        self.run_id = run_id
        self.tenant_id = tenant_id
        self.lease_token = lease_token
        self.current_version = current_version
        self.session_factory = session_factory
        self.sqs = sqs_client
        self.queue_url = queue_url
        self.receipt_handle = receipt_handle
        self.heartbeat_interval_sec = heartbeat_interval_sec
        self.lease_extension_sec = lease_extension_sec
        self.stop_event = threading.Event()

    def run(self) -> None:
        """Run heartbeat loop until stopped."""
        logger.info(
            f"Heartbeat thread started for run {self.run_id} "
            f"(interval={self.heartbeat_interval_sec}s, extension={self.lease_extension_sec}s)"
        )

        while not self.stop_event.is_set():
            # Sleep in small intervals to allow quick shutdown
            for _ in range(self.heartbeat_interval_sec):
                if self.stop_event.is_set():
                    break
                time.sleep(1)

            if self.stop_event.is_set():
                break

            # Send heartbeat
            try:
                self._send_heartbeat()
            except Exception as e:
                logger.error(f"Heartbeat failed for run {self.run_id}: {e}", exc_info=True)
                # Don't stop thread on error - keep trying

        logger.info(f"Heartbeat thread stopped for run {self.run_id}")

    def _send_heartbeat(self) -> None:
        """Send heartbeat: extend DB lease and SQS visibility.

        P0-1: Creates a new Session for each tick to ensure thread-safety.
        """
        # 1. Extend DB lease_expires_at
        new_lease_expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=self.lease_extension_sec
        )

        # P0-1: Create new session for each heartbeat (thread-safe)
        with self.session_factory() as session:
            repo = RunRepository(session)

            # P0-D: Use optimistic locking to extend lease
            # Version will increment with each heartbeat (this is OK - not a state change)
            success = repo.update_with_version_check(
                run_id=self.run_id,
                tenant_id=self.tenant_id,
                expected_version=self.current_version,
                updates={
                    "lease_expires_at": new_lease_expires_at,
                },
                extra_conditions={
                    "lease_token": self.lease_token,  # Verify we still own the lease
                    "status": "PROCESSING",  # Only extend if still processing
                },
            )

            if success:
                # Update our version for next heartbeat
                self.current_version += 1
                logger.debug(
                    f"DB lease extended for run {self.run_id} until {new_lease_expires_at} "
                    f"(version={self.current_version})"
                )
            else:
                logger.warning(
                    f"DB lease extension failed for run {self.run_id} "
                    f"(version conflict, lease_token mismatch, or status changed)"
                )

        # 2. Extend SQS visibility timeout
        try:
            self.sqs.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=self.receipt_handle,
                VisibilityTimeout=self.lease_extension_sec,
            )
            logger.debug(
                f"SQS visibility extended for run {self.run_id} "
                f"by {self.lease_extension_sec}s"
            )
        except Exception as e:
            logger.error(
                f"SQS visibility extension failed for run {self.run_id}: {e}",
                exc_info=True,
            )
            # This is non-fatal - DB lease is more important

    def stop(self) -> None:
        """Stop heartbeat thread."""
        self.stop_event.set()
        self.join(timeout=2)  # Wait up to 2s for clean shutdown
