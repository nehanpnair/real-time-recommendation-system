import json

from confluent_kafka import Consumer
from state import create_user_state, update_user_state

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

    user_state = create_user_state()

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

            update_user_state(user_state, event)

            user = user_state[event["user_id"]]

            print(
                f"Received | "
                f"partition={message.partition()} "
                f"offset={message.offset()} "
                f"user={event['user_id']} "
                f"event={event['event_type']} "
                f"views_5m={user['views_5m']} "
                f"clicks_5m={user['clicks_5m']} "
                f"watermark={user['watermark'].isoformat()}"
            )

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()