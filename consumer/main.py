import json

from confluent_kafka import Consumer
from state import update_user_state
from checkpoint import load_checkpoint, save_checkpoint

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

    user_state = load_checkpoint()
    message_count = 0

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
            message_count += 1

            if message_count % 100 == 0:
                save_checkpoint(user_state)

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
        save_checkpoint(user_state)

    finally:
        consumer.close()


if __name__ == "__main__":
    main()