import os

from dotenv import load_dotenv


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "user-events",
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "streamrec-consumer",
)