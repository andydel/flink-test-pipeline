#!/usr/bin/env python3

import json
import os
import random
import signal
import struct
import sys
import time
from io import BytesIO

import requests
from fastavro import parse_schema, schemaless_writer
from kafka import KafkaProducer


def load_sample_records(sample_path):
    with open(sample_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list) or not payload:
        raise ValueError("Sample data must be a non-empty list of records")
    return payload


def ensure_schema(schema_registry_url, subject, schema_dict):
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    payload = {"schema": json.dumps(schema_dict)}
    register_url = f"{schema_registry_url.rstrip('/')}/subjects/{subject}/versions"

    response = requests.post(register_url, headers=headers, data=json.dumps(payload), timeout=10)
    if response.status_code in (200, 201):
        return response.json()["id"], parse_schema(schema_dict)

    if response.status_code == 409:
        latest = requests.get(
            f"{schema_registry_url.rstrip('/')}/subjects/{subject}/versions/latest",
            timeout=10,
        )
        latest.raise_for_status()
        schema_id = latest.json()["id"]
        schema = latest.json()["schema"]
        return schema_id, parse_schema(json.loads(schema))

    response.raise_for_status()
    raise RuntimeError("Unexpected schema registry response")


def serialize_record(schema, schema_id, record):
    output = BytesIO()
    output.write(b"\x00")
    output.write(struct.pack(">I", schema_id))
    schemaless_writer(output, schema, record)
    return output.getvalue()


def randomize_record(base_record, counter):
    record = dict(base_record)
    record["employee_id"] = base_record.get("employee_id", 1000) + counter
    base_rate = base_record.get("hourly_rate_cents")
    if base_rate is None:
        base_rate = base_record.get("hourly_rate")
    if base_rate is None:
        base_rate = 2500
    record["hourly_rate_cents"] = int(base_rate)
    record["hourly_rate"] = int(base_rate)

    first = base_record.get("first_name", "Test")
    last = base_record.get("last_name", "Employee")
    record["first_name"] = first
    record["last_name"] = last
    record["age"] = max(16, min(75, base_record.get("age", 30) + random.randint(-25, 25)))
    record["gender"] = base_record.get("gender", "unspecified")
    record["ssn"] = f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}"
    record["email"] = (
        base_record.get("email")
        or f"{first.lower()}.{last.lower()}{counter}@example.com"
    )
    record["source_system"] = base_record.get("source_system") or "PAYROLL_GENERATOR"
    record["ingestion_timestamp"] = int(time.time() * 1000)
    record["pipeline_version"] = base_record.get("pipeline_version") or "1.0.0"
    return record


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    topic = os.getenv("PAYROLL_TOPIC", "payroll-employees")
    generation_rate = float(os.getenv("GENERATION_RATE", "100"))
    mode = os.getenv("GENERATION_MODE", "continuous").lower()

    sample_path = os.getenv("SAMPLE_DATA_PATH", "/app/test-data/payroll-employees.json")
    schema_path = os.getenv("SCHEMA_PATH", "/app/test-data/payroll-employee.avsc")

    sample_records = load_sample_records(sample_path)
    with open(schema_path, "r", encoding="utf-8") as handle:
        schema_dict = json.load(handle)

    subject = os.getenv("SCHEMA_SUBJECT", "payroll-employee-value")
    schema_id, parsed_schema = ensure_schema(schema_registry_url, subject, schema_dict)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: v,
        retries=5,
        linger_ms=50,
    )

    sleep_interval = 1.0 / max(generation_rate, 1.0)
    running = True

    def handle_signal(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    counter = 0
    print(
        f"[payroll-data-generator] Sending records to {topic} at ~{generation_rate:.2f}/sec (mode={mode})",
        flush=True,
    )

    try:
        while running:
            for base in sample_records:
                record = randomize_record(base, counter)
                payload = serialize_record(parsed_schema, schema_id, record)
                producer.send(topic, value=payload)
                counter += 1

                if mode != "burst":
                    time.sleep(sleep_interval)

                if not running:
                    break

            if mode == "burst":
                break

    finally:
        producer.flush()
        producer.close()
        print(f"[payroll-data-generator] Published {counter} records", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[payroll-data-generator] Failed: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
