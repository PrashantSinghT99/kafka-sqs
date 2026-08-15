"""Continuously running Kafka and SQS consumers for visual local mode."""

from __future__ import annotations

import argparse
import signal
from threading import Event

from order_app.local_lab.consumer_control_store import ConsumerControlStore
from order_app.local_lab.resource_setup import build_sqs_client, get_queue_url
from order_app.local_lab.config import LocalLabConfig
from order_app.order_processing import (
    ConsumerSettings,
    KafkaDeadLetterPublisher,
    KafkaOrderConsumer,
    OrderConsumerTimeout,
    PostgresOrderStore,
    SqsOrderConsumer,
)


def run_kafka_worker(settings: LocalLabConfig, stop: Event) -> None:
    store = PostgresOrderStore(settings.postgres_dsn, schema=settings.kafka_schema)
    store.initialize()
    controls = ConsumerControlStore(settings.postgres_dsn)
    controls.initialize()
    with KafkaOrderConsumer(
        ConsumerSettings(
            settings.kafka_bootstrap_servers,
            settings.kafka_consumer_group,
        ),
        settings.kafka_topic,
        store,
        dead_letter_publisher=KafkaDeadLetterPublisher(
            settings.kafka_bootstrap_servers
        ),
        dead_letter_topic=settings.kafka_dlq_topic,
    ) as consumer:
        print(
            f"Kafka worker ready: topic={settings.kafka_topic}, "
            f"group={settings.kafka_consumer_group}",
            flush=True,
        )
        while not stop.is_set():
            if controls.is_paused("kafka"):
                stop.wait(0.5)
                continue
            try:
                processed = consumer.process_one(timeout_seconds=2)
            except OrderConsumerTimeout:
                continue
            except Exception as exc:
                print(f"Kafka worker error: {type(exc).__name__}: {exc}", flush=True)
                stop.wait(1)
            else:
                print(
                    f"Kafka processed event={processed.event_id} "
                    f"partition={processed.partition} offset={processed.offset} "
                    f"duplicate={processed.duplicate}",
                    flush=True,
                )


def run_sqs_worker(settings: LocalLabConfig, stop: Event) -> None:
    client = build_sqs_client(settings)
    queue_url = get_queue_url(client, settings.sqs_queue_name)
    store = PostgresOrderStore(settings.postgres_dsn, schema=settings.sqs_schema)
    store.initialize()
    controls = ConsumerControlStore(settings.postgres_dsn)
    controls.initialize()
    consumer = SqsOrderConsumer(client, queue_url, store)
    print(f"SQS worker ready: queue={settings.sqs_queue_name}", flush=True)
    while not stop.is_set():
        if controls.is_paused("sqs"):
            stop.wait(0.5)
            continue
        try:
            event_id = consumer.process_one(wait_seconds=2)
        except TimeoutError:
            continue
        except Exception as exc:
            print(f"SQS worker error: {type(exc).__name__}: {exc}", flush=True)
            stop.wait(1)
        else:
            print(f"SQS processed and deleted event={event_id}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a local-lab order consumer.")
    parser.add_argument("broker", choices=("kafka", "sqs"))
    args = parser.parse_args()
    settings = LocalLabConfig.from_environment()
    stop = Event()

    def request_stop(*_: object) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    if args.broker == "kafka":
        run_kafka_worker(settings, stop)
    else:
        run_sqs_worker(settings, stop)


if __name__ == "__main__":
    main()
