import json

from confluent_kafka import Consumer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC,
)


consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
})


def main():
    consumer.subscribe([KAFKA_TOPIC])

    print(
        f"Listening to topic '{KAFKA_TOPIC}' "
        f"as group '{KAFKA_GROUP_ID}'..."
    )

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Consumer error: {message.error()}")
                continue

            try:
                event = json.loads(
                    message.value().decode("utf-8")
                )
            except (UnicodeDecodeError, json.JSONDecodeError):
                print(
                    f"Skipping invalid event | "
                    f"partition={message.partition()} "
                    f"offset={message.offset()} "
                    f"value={message.value()!r}"
                )
                continue

            print(
                f"Received | "
                f"partition={message.partition()} "
                f"offset={message.offset()} "
                f"user={event['user_id']} "
                f"event={event['event_type']}"
            )

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()