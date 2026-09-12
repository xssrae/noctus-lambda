import base64
import logging
import os

from kafka import KafkaConsumer

from lambda_function import lambda_handler


logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
LOGGER = logging.getLogger("noctus-lambda-worker")


def as_lambda_event(polled_records):
    records = {}
    for topic_partition, messages in polled_records.items():
        key = f"{topic_partition.topic}-{topic_partition.partition}"
        records[key] = [
            {
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "timestamp": message.timestamp,
                "timestampType": "CREATE_TIME",
                "key": base64.b64encode(message.key).decode("ascii") if message.key else None,
                "value": base64.b64encode(message.value).decode("ascii"),
                "headers": [],
            }
            for message in messages
        ]
    return {"eventSource": "SelfManagedKafka", "records": records}


def main():
    topic = os.environ.get("FRAUD_RESULTS_TOPIC", "fraud-detection-topic")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(","),
        group_id=os.environ.get("KAFKA_CONSUMER_GROUP", "noctus-curation-v1"),
        auto_offset_reset=os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        enable_auto_commit=False,
        max_poll_records=int(os.environ.get("KAFKA_MAX_POLL_RECORDS", "100")),
        value_deserializer=None,
        key_deserializer=None,
    )
    LOGGER.info("Consuming topic=%s group=%s", topic, consumer.config["group_id"])

    try:
        while True:
            polled = consumer.poll(timeout_ms=1000)
            if not polled:
                continue
            try:
                result = lambda_handler(as_lambda_event(polled), None)
                consumer.commit()
                LOGGER.info("Committed Kafka batch: processed=%s", result["processed"])
            except Exception:
                LOGGER.exception("Batch failed; offsets were not committed and will be retried")
                for topic_partition, messages in polled.items():
                    if messages:
                        consumer.seek(topic_partition, messages[0].offset)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
