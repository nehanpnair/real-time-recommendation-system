from collections import defaultdict
from datetime import datetime, timedelta


WINDOW_SIZE = timedelta(minutes=5)
ALLOWED_LATENESS = timedelta(seconds=30)


def create_user_state():
    return defaultdict(
        lambda: {
            "events": [],
            "max_event_time": None,
            "watermark": None,
            "views_5m": 0,
            "clicks_5m": 0,
            "searches_5m": 0,
            "add_to_carts_5m": 0,
            "purchases_5m": 0,
        }
    )


def update_user_state(state, event):
    user_id = event["user_id"]
    event_type = event["event_type"]

    event_time = datetime.fromisoformat(
        event["event_time"]
    )

    user = state[user_id]

    # Update maximum event time seen

    if (
        user["max_event_time"] is None
        or event_time > user["max_event_time"]
    ):
        user["max_event_time"] = event_time

    # Calculate watermark

    user["watermark"] = (
        user["max_event_time"] - ALLOWED_LATENESS
    )

    # Check whether this event is too late

    if event_time < user["watermark"]:
        print(
            f"Late event dropped | "
            f"user={user_id} "
            f"event_time={event_time.isoformat()} "
            f"watermark={user['watermark'].isoformat()}"
        )
        return

    # Store the event

    user["events"].append({
        "event_type": event_type,
        "event_time": event_time,
    })

    # Keep events ordered by event time.
    user["events"].sort(
        key=lambda event: event["event_time"]
    )

    # Remove events outside the 5-minute window

    window_start = (
        user["max_event_time"] - WINDOW_SIZE
    )

    user["events"] = [
        stored_event
        for stored_event in user["events"]
        if stored_event["event_time"] >= window_start
    ]

    # Recalculate 5-minute features

    user["views_5m"] = 0
    user["clicks_5m"] = 0
    user["searches_5m"] = 0
    user["add_to_carts_5m"] = 0
    user["purchases_5m"] = 0

    for stored_event in user["events"]:

        event_type = stored_event["event_type"]

        if event_type == "view":
            user["views_5m"] += 1

        elif event_type == "click":
            user["clicks_5m"] += 1

        elif event_type == "search":
            user["searches_5m"] += 1

        elif event_type == "add_to_cart":
            user["add_to_carts_5m"] += 1

        elif event_type == "purchase":
            user["purchases_5m"] += 1