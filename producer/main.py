import json
import time

from confluent_kafka import Producer

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from events import generate_event


producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
})


def delivery_callback(error, message):
    if error is not None:
        print(f"Delivery failed: {error}")
        return

    print(
        f"Delivered | "
        f"topic={message.topic()} "
        f"partition={message.partition()} "
        f"offset={message.offset()}"
    )


def main():
    print("Starting event producer...")

    try:
        while True:
            event = generate_event()

            producer.produce(
                topic=KAFKA_TOPIC,
                key=str(event["user_id"]),
                value=json.dumps(event),
                callback=delivery_callback,
            )

            # Give librdkafka a chance to process delivery callbacks.
            producer.poll(0)

            time.sleep(0.2)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()


if __name__ == "__main__":
    main()