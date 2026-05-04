"""
ingestion/cursor_store.py

Handles saving and loading the Plaid sync cursor for each institution.

The cursor is the bookmark Plaid uses to track which transactions have
already been delivered. It must be persisted between runs so that each
sync picks up only new transactions rather than re-downloading everything.

Storage: a JSON file at state/cursors.json in the repo root.
Format:  {"institution_a": "CAESFw...", "institution_b": "CAESGx..."}

In a production system this would live in GCS or a database so it is
accessible across machines. For this portfolio project, local file
storage is sufficient and keeps the dependency count low.
"""

import json
import os

STATE_DIR = os.path.join(os.path.dirname(__file__), "..", "state")
CURSOR_FILE = os.path.join(STATE_DIR, "cursors.json")


def _load_all() -> dict:
    """Load the full cursor file. Returns an empty dict if the file does not exist."""
    if not os.path.exists(CURSOR_FILE):
        return {}
    with open(CURSOR_FILE, "r") as f:
        return json.load(f)


def _save_all(cursors: dict) -> None:
    """Write the full cursor dict back to disk."""
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(CURSOR_FILE, "w") as f:
        json.dump(cursors, f, indent=2)


def load_cursor(institution_key: str) -> str | None:
    """
    Return the stored cursor for the given institution, or None if this is
    the first run.

    Args:
        institution_key: The institution identifier string, e.g. 'institution_a'.

    Returns:
        The cursor string, or None if no cursor has been stored yet.
    """
    cursors = _load_all()
    return cursors.get(institution_key)


def save_cursor(institution_key: str, cursor: str) -> None:
    """
    Persist the latest cursor for the given institution.

    Args:
        institution_key: The institution identifier string, e.g. 'institution_a'.
        cursor: The next_cursor value returned by Plaid after a sync.
    """
    cursors = _load_all()
    cursors[institution_key] = cursor
    _save_all(cursors)
