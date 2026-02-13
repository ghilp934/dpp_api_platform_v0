#!/bin/bash
set -e

echo "=== DPP LocalStack Initialization ==="

# Wait for LocalStack to be ready
echo "Waiting for LocalStack..."
until curl -s http://localhost:4566/_localstack/health | grep -q '"s3": "available"'; do
  sleep 1
done
echo "LocalStack is ready!"

# Create S3 bucket
BUCKET_NAME="dpp-results"
echo "Creating S3 bucket: ${BUCKET_NAME}"
awslocal s3 mb s3://${BUCKET_NAME} || echo "Bucket already exists"

# Set lifecycle policy (30 days expiration)
awslocal s3api put-bucket-lifecycle-configuration \
  --bucket ${BUCKET_NAME} \
  --lifecycle-configuration '{
    "Rules": [
      {
        "Id": "expire-results-30days",
        "Status": "Enabled",
        "Expiration": {
          "Days": 30
        },
        "Filter": {}
      },
      {
        "Id": "abort-multipart-7days",
        "Status": "Enabled",
        "AbortIncompleteMultipartUpload": {
          "DaysAfterInitiation": 7
        },
        "Filter": {}
      }
    ]
  }'

echo "S3 bucket lifecycle policy set"

# Create SQS queue
QUEUE_NAME="dpp-runs-queue"
echo "Creating SQS queue: ${QUEUE_NAME}"
QUEUE_URL=$(awslocal sqs create-queue --queue-name ${QUEUE_NAME} --attributes VisibilityTimeout=120,MessageRetentionPeriod=3600 --output text --query 'QueueUrl' || echo "")

if [ -z "$QUEUE_URL" ]; then
  QUEUE_URL=$(awslocal sqs get-queue-url --queue-name ${QUEUE_NAME} --output text --query 'QueueUrl')
fi

echo "SQS queue created: ${QUEUE_URL}"

# Create DLQ
DLQ_NAME="dpp-runs-dlq"
echo "Creating DLQ: ${DLQ_NAME}"
DLQ_URL=$(awslocal sqs create-queue --queue-name ${DLQ_NAME} --output text --query 'QueueUrl' || echo "")

if [ -z "$DLQ_URL" ]; then
  DLQ_URL=$(awslocal sqs get-queue-url --queue-name ${DLQ_NAME} --output text --query 'QueueUrl')
fi

DLQ_ARN=$(awslocal sqs get-queue-attributes --queue-url ${DLQ_URL} --attribute-names QueueArn --output text --query 'Attributes.QueueArn')

# Set redrive policy
awslocal sqs set-queue-attributes \
  --queue-url ${QUEUE_URL} \
  --attributes RedrivePolicy="{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"3\"}"

echo "DLQ configured with maxReceiveCount=3"

echo "=== LocalStack Initialization Complete ==="
echo "S3 Bucket: ${BUCKET_NAME}"
echo "SQS Queue: ${QUEUE_URL}"
echo "DLQ: ${DLQ_URL}"
