import base64
import binascii
import json
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from uuid import UUID

import boto3


VALID_DECISIONS = {"APPROVED", "REVIEW_REQUIRED", "REJECTED"}
VALID_REASONS = {
    "HIGH_VALUE",
    "COUNTRY_MISMATCH",
    "MULTIPLE_PAYMENT_ATTEMPTS",
    "NEW_ACCOUNT",
}
REQUIRED_FIELDS = {
    "assessmentId", "sourceEventId", "orderId", "customerId", "orderAmount",
    "currency", "riskScore", "decision", "reasons", "assessedAt", "modelVersion",
}


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT") or None,
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


s3_client = _s3_client()


def _required_text(assessment, field):
    value = assessment.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Field '{field}' must be a non-empty string")
    return value


def _parse_timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError("Field 'assessedAt' must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("Field 'assessedAt' must include a timezone")
    return parsed.astimezone(timezone.utc)


def validate_assessment(assessment):
    if not isinstance(assessment, dict):
        raise ValueError("FraudAssessment must be a JSON object")

    missing = sorted(REQUIRED_FIELDS.difference(assessment))
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    for field in (
        "assessmentId", "sourceEventId", "orderId", "customerId", "currency",
        "decision", "assessedAt", "modelVersion",
    ):
        _required_text(assessment, field)

    for field in ("assessmentId", "sourceEventId"):
        try:
            UUID(assessment[field])
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError(f"Field '{field}' must be a UUID") from exc

    decision = assessment["decision"]
    if decision not in VALID_DECISIONS:
        raise ValueError(f"Unknown fraud decision: {decision}")

    score = assessment["riskScore"]
    if isinstance(score, bool) or not isinstance(score, int) or not 0 <= score <= 100:
        raise ValueError("Field 'riskScore' must be an integer between 0 and 100")

    try:
        amount = Decimal(str(assessment["orderAmount"]))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("Field 'orderAmount' must be numeric") from exc
    if not amount.is_finite() or amount <= 0:
        raise ValueError("Field 'orderAmount' must be greater than zero")

    reasons = assessment["reasons"]
    if not isinstance(reasons, list) or any(reason not in VALID_REASONS for reason in reasons):
        raise ValueError("Field 'reasons' contains an unknown risk reason")

    _parse_timestamp(assessment["assessedAt"])
    return assessment


def decode_record(value):
    if not isinstance(value, str) or not value:
        raise ValueError("Kafka record must contain a non-empty base64 'value'")
    try:
        decoded = base64.b64decode(value, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError("Kafka record 'value' is not valid base64 UTF-8") from exc
    try:
        assessment = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise ValueError("Kafka record does not contain valid JSON") from exc
    return validate_assessment(assessment)


def object_key(assessment):
    assessed_at = _parse_timestamp(assessment["assessedAt"])
    prefix = os.environ.get("CURATED_PREFIX", "fraud-assessments").strip("/")
    partition = (
        f"decision={assessment['decision']}/"
        f"year={assessed_at.year:04d}/month={assessed_at.month:02d}/day={assessed_at.day:02d}/"
        f"{assessment['assessmentId']}.json"
    )
    return f"{prefix}/{partition}" if prefix else partition


def store_assessment(assessment, bucket_name):
    key = object_key(assessment)
    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(assessment, ensure_ascii=False, separators=(",", ":")),
        ContentType="application/json",
    )
    return key


def lambda_handler(event, context):
    bucket_name = os.environ.get("CURATED_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("CURATED_BUCKET_NAME is required")

    records = event.get("records") if isinstance(event, dict) else None
    if not isinstance(records, dict) or not records:
        raise ValueError("Kafka event must contain a non-empty 'records' object")

    stored_keys = []
    for partition_key, messages in records.items():
        if not isinstance(messages, list) or not messages:
            raise ValueError(f"Partition '{partition_key}' must contain records")
        for message in messages:
            if not isinstance(message, dict):
                raise ValueError(f"Partition '{partition_key}' contains an invalid record")
            assessment = decode_record(message.get("value"))
            stored_keys.append(store_assessment(assessment, bucket_name))

    return {"processed": len(stored_keys), "keys": stored_keys}
