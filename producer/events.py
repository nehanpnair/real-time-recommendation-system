import random
import uuid
from datetime import datetime, timezone


USERS = list(range(1, 101))
ITEMS = list(range(1, 501))

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
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USERS),
        "item_id": random.choice(ITEMS),
        "event_type": random.choice(EVENT_TYPES),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "device": random.choice(DEVICES),
        "country": random.choice(COUNTRIES),
        "session_id": str(uuid.uuid4()),
    }