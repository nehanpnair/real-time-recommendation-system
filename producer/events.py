import random
import uuid
from datetime import datetime, timezone


EVENT_TYPES = [
    "view",
    "click",
    "search",
    "add_to_cart",
    "purchase",
]

DEVICES = [
    "mobile",
    "desktop",
    "tablet",
]

COUNTRIES = [
    "IN",
    "US",
    "UK",
    "DE",
    "SG",
]


def generate_event():
    user_id = random.randint(1, 10_000)
    item_id = random.randint(1, 5_000)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "item_id": item_id,
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device": random.choice(DEVICES),
        "country": random.choice(COUNTRIES),
        "session_id": str(uuid.uuid4()),
    }