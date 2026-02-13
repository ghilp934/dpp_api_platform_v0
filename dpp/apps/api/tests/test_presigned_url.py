"""Tests for Presigned URL generation (P1-1).

Tests S3 client presigned URL generation for completed runs.
"""

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from dpp_api.storage.s3_client import S3Client, get_s3_client


def test_s3_client_initialization() -> None:
    """Test S3Client initialization with defaults."""
    client = S3Client()

    assert client.bucket == "dpp-results"
    assert client.region == "us-east-1"
    assert client.client is not None


def test_s3_client_custom_config() -> None:
    """Test S3Client initialization with custom config."""
    client = S3Client(
        bucket="custom-bucket",
        region="us-west-2",
        endpoint_url="http://localhost:4566",
    )

    assert client.bucket == "custom-bucket"
    assert client.region == "us-west-2"
    assert client.endpoint_url == "http://localhost:4566"


def test_generate_presigned_url_success() -> None:
    """Test presigned URL generation succeeds.

    P1-1: Presigned URL should be generated with 600s TTL.
    """
    client = S3Client()

    # Mock boto3 client
    mock_url = "https://s3.amazonaws.com/bucket/key?signature=abc123&expires=600"
    client.client.generate_presigned_url = MagicMock(return_value=mock_url)

    # Generate presigned URL
    bucket = "dpp-results"
    key = "results/test-run-123.json"
    ttl_seconds = 600

    presigned_url, expires_at = client.generate_presigned_url(bucket, key, ttl_seconds)

    # Verify URL generated
    assert presigned_url == mock_url

    # Verify expiration timestamp (should be ~600 seconds from now)
    now = datetime.now(timezone.utc)
    time_diff = (expires_at - now).total_seconds()
    assert 595 <= time_diff <= 605  # Allow 5 second tolerance

    # Verify boto3 client was called correctly
    client.client.generate_presigned_url.assert_called_once_with(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=ttl_seconds,
    )


def test_generate_presigned_url_default_ttl() -> None:
    """Test presigned URL generation uses default 600s TTL."""
    client = S3Client()
    mock_url = "https://s3.amazonaws.com/bucket/key?signature=xyz"
    client.client.generate_presigned_url = MagicMock(return_value=mock_url)

    bucket = "dpp-results"
    key = "results/test-run-456.json"

    # Call without explicit TTL (should default to 600)
    presigned_url, expires_at = client.generate_presigned_url(bucket, key)

    # Verify default TTL was used
    client.client.generate_presigned_url.assert_called_once()
    call_args = client.client.generate_presigned_url.call_args
    assert call_args.kwargs["ExpiresIn"] == 600


def test_generate_presigned_url_custom_ttl() -> None:
    """Test presigned URL generation with custom TTL."""
    client = S3Client()
    mock_url = "https://s3.amazonaws.com/bucket/key?signature=custom"
    client.client.generate_presigned_url = MagicMock(return_value=mock_url)

    bucket = "dpp-results"
    key = "results/test-run-custom.json"
    custom_ttl = 1800  # 30 minutes

    presigned_url, expires_at = client.generate_presigned_url(bucket, key, custom_ttl)

    # Verify custom TTL was used
    now = datetime.now(timezone.utc)
    time_diff = (expires_at - now).total_seconds()
    assert 1795 <= time_diff <= 1805  # 30 minutes ± 5 seconds


def test_generate_presigned_url_error_handling() -> None:
    """Test presigned URL generation handles boto3 errors."""
    from botocore.exceptions import ClientError

    client = S3Client()

    # Mock boto3 client to raise error
    error_response = {"Error": {"Code": "NoSuchBucket", "Message": "Bucket not found"}}
    client.client.generate_presigned_url = MagicMock(
        side_effect=ClientError(error_response, "generate_presigned_url")
    )

    bucket = "nonexistent-bucket"
    key = "results/test.json"

    # Should propagate ClientError
    with pytest.raises(ClientError):
        client.generate_presigned_url(bucket, key)


def test_object_exists_true() -> None:
    """Test object_exists returns True when object exists."""
    client = S3Client()

    # Mock head_object to succeed
    client.client.head_object = MagicMock(return_value={"ContentLength": 1024})

    bucket = "dpp-results"
    key = "results/existing.json"

    assert client.object_exists(bucket, key) is True
    client.client.head_object.assert_called_once_with(Bucket=bucket, Key=key)


def test_object_exists_false() -> None:
    """Test object_exists returns False when object does not exist."""
    from botocore.exceptions import ClientError

    client = S3Client()

    # Mock head_object to raise 404
    error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
    client.client.head_object = MagicMock(
        side_effect=ClientError(error_response, "head_object")
    )

    bucket = "dpp-results"
    key = "results/nonexistent.json"

    assert client.object_exists(bucket, key) is False


def test_get_s3_client_singleton() -> None:
    """Test get_s3_client returns singleton instance."""
    # Clear singleton first
    from dpp_api.storage import s3_client
    s3_client._s3_client = None

    client1 = get_s3_client()
    client2 = get_s3_client()

    assert client1 is client2  # Same instance


def test_presigned_url_integration_with_env() -> None:
    """Test S3Client uses environment variables for configuration."""
    # Set environment variables
    os.environ["DPP_RESULTS_BUCKET"] = "test-env-bucket"
    os.environ["AWS_REGION"] = "eu-west-1"
    os.environ["S3_ENDPOINT_URL"] = "http://test-endpoint:4566"

    try:
        client = S3Client()

        assert client.bucket == "test-env-bucket"
        assert client.region == "eu-west-1"
        assert client.endpoint_url == "http://test-endpoint:4566"

    finally:
        # Clean up environment
        os.environ.pop("DPP_RESULTS_BUCKET", None)
        os.environ.pop("AWS_REGION", None)
        os.environ.pop("S3_ENDPOINT_URL", None)
