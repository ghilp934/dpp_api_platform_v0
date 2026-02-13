"""S3 Storage Client for DPP.

P1-1: Presigned URL generation for completed run results.
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


class S3Client:
    """S3 client for result storage and presigned URL generation."""

    def __init__(
        self,
        bucket: Optional[str] = None,
        region: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ):
        """Initialize S3 client.

        Args:
            bucket: S3 bucket name (default from env: DPP_RESULTS_BUCKET)
            region: AWS region (default from env: AWS_REGION or us-east-1)
            endpoint_url: Custom endpoint URL (for LocalStack/MinIO testing)
        """
        self.bucket = bucket or os.getenv("DPP_RESULTS_BUCKET", "dpp-results")
        self.region = region or os.getenv("AWS_REGION", "us-east-1")
        self.endpoint_url = endpoint_url or os.getenv("S3_ENDPOINT_URL")

        # Configure boto3 client
        config = Config(
            region_name=self.region,
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        )

        # Initialize S3 client
        self.client = boto3.client(
            "s3",
            config=config,
            endpoint_url=self.endpoint_url,
        )

    def generate_presigned_url(
        self,
        bucket: str,
        key: str,
        ttl_seconds: int = 600,
    ) -> tuple[str, datetime]:
        """Generate presigned URL for downloading S3 object.

        P1-1: Generate presigned URL with 600 second TTL for completed runs.

        Args:
            bucket: S3 bucket name
            key: S3 object key
            ttl_seconds: Time-to-live in seconds (default 600 = 10 minutes)

        Returns:
            Tuple of (presigned_url, expires_at)

        Raises:
            ClientError: If S3 operation fails
        """
        try:
            # Generate presigned URL
            presigned_url = self.client.generate_presigned_url(
                ClientMethod="get_object",
                Params={
                    "Bucket": bucket,
                    "Key": key,
                },
                ExpiresIn=ttl_seconds,
            )

            # Calculate expiration timestamp
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)

            return presigned_url, expires_at

        except ClientError as e:
            # Log error and re-raise
            import logging
            logger = logging.getLogger(__name__)
            logger.error(
                f"Failed to generate presigned URL for s3://{bucket}/{key}: {e}",
                exc_info=True,
            )
            raise

    def object_exists(self, bucket: str, key: str) -> bool:
        """Check if S3 object exists.

        P0-2: Used by Reconcile Loop to determine roll-forward vs roll-back.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            True if object exists, False otherwise
        """
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            # Other errors (permissions, etc.) should be raised
            raise

    def upload_file(
        self,
        file_path: str,
        bucket: str,
        key: str,
        metadata: Optional[dict[str, str]] = None,
    ) -> str:
        """Upload file to S3.

        Args:
            file_path: Local file path to upload
            bucket: S3 bucket name
            key: S3 object key
            metadata: Optional metadata dict

        Returns:
            S3 URI (s3://bucket/key)

        Raises:
            ClientError: If upload fails
        """
        try:
            extra_args = {}
            if metadata:
                extra_args["Metadata"] = metadata

            self.client.upload_file(file_path, bucket, key, ExtraArgs=extra_args)

            return f"s3://{bucket}/{key}"

        except ClientError as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(
                f"Failed to upload {file_path} to s3://{bucket}/{key}: {e}",
                exc_info=True,
            )
            raise

    def upload_bytes(
        self,
        data: bytes,
        bucket: str,
        key: str,
        content_type: str = "application/octet-stream",
        metadata: Optional[dict[str, str]] = None,
    ) -> str:
        """Upload bytes to S3.

        Args:
            data: Bytes to upload
            bucket: S3 bucket name
            key: S3 object key
            content_type: Content-Type header
            metadata: Optional metadata dict

        Returns:
            S3 URI (s3://bucket/key)

        Raises:
            ClientError: If upload fails
        """
        try:
            extra_args = {"ContentType": content_type}
            if metadata:
                extra_args["Metadata"] = metadata

            self.client.put_object(
                Bucket=bucket,
                Key=key,
                Body=data,
                **extra_args,
            )

            return f"s3://{bucket}/{key}"

        except ClientError as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(
                f"Failed to upload bytes to s3://{bucket}/{key}: {e}",
                exc_info=True,
            )
            raise


# Singleton instance
_s3_client: Optional[S3Client] = None


def get_s3_client() -> S3Client:
    """Get singleton S3 client instance.

    Returns:
        S3Client instance
    """
    global _s3_client
    if _s3_client is None:
        _s3_client = S3Client()
    return _s3_client
