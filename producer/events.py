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


# Each user has a small set of preferred items.
USER_PREFERENCES = {}

for user_id in USERS:
    USER_PREFERENCES[user_id] = random.sample(
        ITEMS,
        30,
    )


def generate_event():
    user_id = random.choice(USERS)

    # Most interactions come from the user's preferred items, with occasional exploration.
    if random.random() < 0.8:
        item_id = random.choice(
            USER_PREFERENCES[user_id]
        )
    else:
        item_id = random.choice(ITEMS)

    event_type = random.choices(
        EVENT_TYPES,
        weights=[
            55,   # view
            25,   # click
            10,   # search
            7,    # add_to_cart
            3,    # purchase
        ],
        k=1,
    )[0]

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "item_id": item_id,
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device": random.choice(DEVICES),
        "country": random.choice(COUNTRIES),
        "session_id": str(uuid.uuid4()),
    }