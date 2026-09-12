import base64
import json
import os
import sys
import unittest
from copy import deepcopy
from collections import namedtuple
from unittest.mock import patch


os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("CURATED_BUCKET_NAME", "test-curated")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

import lambda_function
import local_kafka_consumer


def assessment(**overrides):
    value = {
        "assessmentId": "44444444-4444-4444-8444-444444444444",
        "sourceEventId": "33333333-3333-4333-8333-333333333333",
        "orderId": "order-rejected-001",
        "customerId": "customer-303",
        "orderAmount": 7999.90,
        "currency": "BRL",
        "riskScore": 100,
        "decision": "REJECTED",
        "reasons": ["HIGH_VALUE", "COUNTRY_MISMATCH", "MULTIPLE_PAYMENT_ATTEMPTS", "NEW_ACCOUNT"],
        "assessedAt": "2026-09-10T12:10:01Z",
        "modelVersion": "rules-v1",
    }
    value.update(overrides)
    return value


def encoded(value):
    raw = value if isinstance(value, str) else json.dumps(value)
    return base64.b64encode(raw.encode("utf-8")).decode("ascii")


def event(*values):
    return {
        "eventSource": "aws:kafka",
        "records": {
            "fraud-detection-topic-0": [
                {"topic": "fraud-detection-topic", "partition": 0, "offset": index, "value": value}
                for index, value in enumerate(values)
            ]
        },
    }


class TestFraudCurationLambda(unittest.TestCase):
    def setUp(self):
        os.environ["CURATED_BUCKET_NAME"] = "test-curated"
        os.environ.pop("CURATED_PREFIX", None)

    @patch("lambda_function.s3_client")
    def test_processes_records_from_multiple_partitions(self, mock_s3):
        first = assessment()
        second = assessment(
            assessmentId="55555555-5555-4555-8555-555555555555",
            decision="APPROVED",
            riskScore=0,
            reasons=[],
        )
        payload = event(encoded(first))
        payload["records"]["fraud-detection-topic-1"] = [
            {"topic": "fraud-detection-topic", "partition": 1, "offset": 4, "value": encoded(second)}
        ]

        result = lambda_function.lambda_handler(payload, None)

        self.assertEqual(result["processed"], 2)
        self.assertEqual(mock_s3.put_object.call_count, 2)
        first_call = mock_s3.put_object.call_args_list[0].kwargs
        self.assertEqual(
            first_call["Key"],
            "fraud-assessments/decision=REJECTED/year=2026/month=09/day=10/44444444-4444-4444-8444-444444444444.json",
        )
        self.assertEqual(json.loads(first_call["Body"]), first)

    def test_rejects_invalid_base64(self):
        with self.assertRaisesRegex(ValueError, "base64"):
            lambda_function.lambda_handler(event("%%%"), None)

    def test_rejects_invalid_json(self):
        with self.assertRaisesRegex(ValueError, "valid JSON"):
            lambda_function.lambda_handler(event(encoded("not-json")), None)

    def test_rejects_missing_fields(self):
        invalid = assessment()
        del invalid["assessmentId"]
        with self.assertRaisesRegex(ValueError, "assessmentId"):
            lambda_function.lambda_handler(event(encoded(invalid)), None)

    def test_rejects_unknown_decision(self):
        with self.assertRaisesRegex(ValueError, "Unknown fraud decision"):
            lambda_function.lambda_handler(event(encoded(assessment(decision="UNKNOWN"))), None)

    @patch("lambda_function.s3_client")
    def test_propagates_s3_failure(self, mock_s3):
        mock_s3.put_object.side_effect = RuntimeError("S3 unavailable")
        with self.assertRaisesRegex(RuntimeError, "S3 unavailable"):
            lambda_function.lambda_handler(event(encoded(assessment())), None)

    @patch("lambda_function.s3_client")
    def test_retry_uses_the_same_idempotent_key(self, mock_s3):
        payload = event(encoded(assessment()))

        first = lambda_function.lambda_handler(deepcopy(payload), None)
        second = lambda_function.lambda_handler(deepcopy(payload), None)

        self.assertEqual(first["keys"], second["keys"])
        self.assertEqual(
            mock_s3.put_object.call_args_list[0].kwargs["Key"],
            mock_s3.put_object.call_args_list[1].kwargs["Key"],
        )

    def test_local_worker_builds_the_same_aws_kafka_envelope(self):
        topic_partition = namedtuple("TopicPartition", "topic partition")("fraud-detection-topic", 2)
        message = namedtuple("Message", "topic partition offset timestamp key value")(
            "fraud-detection-topic", 2, 9, 1789042201000, b"order-rejected-001",
            json.dumps(assessment()).encode("utf-8"),
        )

        payload = local_kafka_consumer.as_lambda_event({topic_partition: [message]})

        record = payload["records"]["fraud-detection-topic-2"][0]
        self.assertEqual(json.loads(base64.b64decode(record["value"])), assessment())
        self.assertEqual(base64.b64decode(record["key"]), b"order-rejected-001")


if __name__ == "__main__":
    unittest.main()
