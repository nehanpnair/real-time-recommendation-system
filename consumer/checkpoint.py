import json
import os

from datetime import datetime

from state import create_user_state


CHECKPOINT_DIR = "checkpoints"
CHECKPOINT_FILE = os.path.join(
    CHECKPOINT_DIR,
    "state.json",
)


def save_checkpoint(state):
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    serialized = {}

    for user_id, user in state.items():
        serialized[str(user_id)] = {
            "events": [
                {
                    "event_type": event["event_type"],
                    "event_time": event["event_time"].isoformat(),
                }
                for event in user["events"]
            ],
            "max_event_time": (
                user["max_event_time"].isoformat()
                if user["max_event_time"] is not None
                else None
            ),
            "watermark": (
                user["watermark"].isoformat()
                if user["watermark"] is not None
                else None
            ),
            "views_5m": user["views_5m"],
            "clicks_5m": user["clicks_5m"],
            "searches_5m": user["searches_5m"],
            "add_to_carts_5m": user["add_to_carts_5m"],
            "purchases_5m": user["purchases_5m"],
        }

    with open(CHECKPOINT_FILE, "w") as file:
        json.dump(serialized, file, indent=2)

    print(f"Checkpoint saved → {CHECKPOINT_FILE}")


def load_checkpoint():
    try:
        with open(CHECKPOINT_FILE, "r") as file:
            data = json.load(file)

    except FileNotFoundError:
        print("No checkpoint found. Starting with empty state.")
        return create_user_state()

    state = create_user_state()

    for user_id, user_data in data.items():
        user = state[int(user_id)]

        user["events"] = [
            {
                "event_type": event["event_type"],
                "event_time": datetime.fromisoformat(
                    event["event_time"]
                ),
            }
            for event in user_data["events"]
        ]

        user["max_event_time"] = (
            datetime.fromisoformat(
                user_data["max_event_time"]
            )
            if user_data["max_event_time"]
            else None
        )

        user["watermark"] = (
            datetime.fromisoformat(
                user_data["watermark"]
            )
            if user_data["watermark"]
            else None
        )

        user["views_5m"] = user_data["views_5m"]
        user["clicks_5m"] = user_data["clicks_5m"]
        user["searches_5m"] = user_data["searches_5m"]
        user["add_to_carts_5m"] = user_data["add_to_carts_5m"]
        user["purchases_5m"] = user_data["purchases_5m"]

    print(
        f"Checkpoint loaded ← {CHECKPOINT_FILE} "
        f"({len(state)} users)"
    )

    return state