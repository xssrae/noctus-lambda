FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY test_lambda.py ./test_lambda.py

RUN useradd --system --uid 10001 noctus
USER noctus

WORKDIR /app/src
ENTRYPOINT ["python", "local_kafka_consumer.py"]
