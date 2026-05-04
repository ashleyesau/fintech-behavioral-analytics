"""
ingestion/extract_transactions.py

Pulls transactions from the Plaid Sandbox API for both institutions using
the cursor-based /transactions/sync endpoint. Writes the raw JSON response
to Google Cloud Storage, partitioned by institution and ingestion date.

How it works:
    1. Load the stored cursor for the institution (None on first run).
    2. Call /transactions/sync in a loop until has_more is False.
       Each call uses the cursor returned by the previous call.
    3. Collect all added, modified, and removed transactions across pages.
    4. Write the raw response to GCS as a JSON file.
    5. Save the new cursor so the next run picks up from here.
    6. Log the result: institution, record counts, success or failure.

Usage:
    python ingestion/extract_transactions.py

    Runs a full incremental sync for both institutions and writes results
    to GCS. Safe to run multiple times -- each run only fetches new data.
"""

import json
import logging
import os
import time
from datetime import date, datetime
from typing import Any

from dotenv import load_dotenv
from google.cloud import storage
from plaid import ApiException
from plaid.model.transactions_sync_request import TransactionsSyncRequest

from cursor_store import load_cursor, save_cursor
from plaid_client import get_plaid_client

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INSTITUTIONS = {
    "institution_a": {
        "label": "StableBank",
        "token_env_var": "ACCESS_TOKEN_INSTITUTION_A",
    },
    "institution_b": {
        "label": "VolatileBank",
        "token_env_var": "ACCESS_TOKEN_INSTITUTION_B",
    },
}

GCS_BUCKET = os.getenv("GCS_BUCKET", "plaid-495309-raw-data")

# Retry settings for transient API failures.
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1  # seconds -- doubles with each attempt (1, 2, 4)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(LOG_DIR, "ingestion.log")),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# JSON serialisation helper
# ---------------------------------------------------------------------------


def _serialise(obj: Any) -> Any:
    """
    Custom JSON serialiser for types that the standard library cannot handle.

    Plaid returns Python datetime.date objects for transaction dates, not
    strings. This function converts them to ISO format strings so they can
    be written to JSON without error.
    """
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not JSON serialisable")


# ---------------------------------------------------------------------------
# GCS write
# ---------------------------------------------------------------------------


def write_to_gcs(
    institution_key: str,
    run_date: str,
    payload: dict,
    record_count: int,
) -> str:
    """
    Write the sync payload to GCS as an immutable partitioned JSON file.

    Partition structure:
        gs://<bucket>/raw/transactions/institution=<key>/date=<YYYY-MM-DD>/
            transactions_<timestamp>.json

    A metadata sidecar file is written alongside the data file, recording
    the record count and run ID. This feeds mart_operational_monitoring.

    Args:
        institution_key: e.g. 'institution_a'
        run_date: ISO date string, e.g. '2026-05-04'
        payload: The full sync response dict to write.
        record_count: Number of transactions in the added array.

    Returns:
        The GCS blob path that was written.
    """
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    prefix = f"raw/transactions/institution={institution_key}/date={run_date}"
    data_blob_name = f"{prefix}/transactions_{timestamp}.json"
    meta_blob_name = f"{prefix}/metadata_{timestamp}.json"

    # Write the main data file.
    data_blob = bucket.blob(data_blob_name)
    data_blob.upload_from_string(
        json.dumps(payload, default=_serialise, indent=2),
        content_type="application/json",
    )

    # Write the metadata sidecar.
    metadata = {
        "run_id": timestamp,
        "institution": institution_key,
        "run_date": run_date,
        "record_count": record_count,
        "blob_path": data_blob_name,
        "written_at": datetime.utcnow().isoformat(),
    }
    meta_blob = bucket.blob(meta_blob_name)
    meta_blob.upload_from_string(
        json.dumps(metadata, indent=2),
        content_type="application/json",
    )

    return data_blob_name


# ---------------------------------------------------------------------------
# Sync logic
# ---------------------------------------------------------------------------


def sync_institution(institution_key: str, label: str, access_token: str) -> dict:
    """
    Run a full incremental sync for one institution.

    Calls /transactions/sync in a loop until has_more is False, collecting
    all added, modified, and removed transactions across pages. Each page
    call includes retry logic for transient failures.

    Args:
        institution_key: e.g. 'institution_a'
        label: Human-readable name for logging, e.g. 'StableBank'
        access_token: Plaid access token for this institution.

    Returns:
        A dict summarising the sync result:
            institution, added_count, modified_count, removed_count,
            success (bool), error (str or None)
    """
    log.info("Starting sync for %s (%s)", label, institution_key)

    client = get_plaid_client()
    cursor = load_cursor(institution_key)

    if cursor is None:
        log.info("  No cursor found -- this is the first run. Fetching full history.")
    else:
        log.info("  Cursor loaded. Fetching incremental updates since last run.")

    all_added = []
    all_modified = []
    all_removed = []
    new_cursor = cursor
    page_number = 0

    while True:
        page_number += 1
        log.info("  Fetching page %d...", page_number)

        # Build the request. If cursor is None, Plaid starts from the
        # beginning of the transaction history.
        request = TransactionsSyncRequest(
            access_token=access_token,
            **({"cursor": new_cursor} if new_cursor else {}),
        )

        # Call the API with retry logic.
        response = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = client.transactions_sync(request)
                break
            except ApiException as e:
                error_body = json.loads(e.body)
                error_code = error_body.get("error_code", "UNKNOWN")

                # PRODUCT_NOT_READY means Plaid is still preparing the
                # transaction data. This is expected on first sync and
                # resolves within a few seconds.
                if error_code == "PRODUCT_NOT_READY":
                    log.warning(
                        "  Attempt %d/%d: PRODUCT_NOT_READY -- waiting %ds before retry.",
                        attempt,
                        MAX_RETRIES,
                        RETRY_BASE_DELAY * (2 ** (attempt - 1)),
                    )
                else:
                    log.error(
                        "  Attempt %d/%d: API error %s -- %s",
                        attempt,
                        MAX_RETRIES,
                        error_code,
                        error_body.get("error_message"),
                    )

                if attempt == MAX_RETRIES:
                    raise

                time.sleep(RETRY_BASE_DELAY * (2 ** (attempt - 1)))

        # Collect this page's transactions.
        # Plaid responses are typed model objects -- convert to dict first.
        added = [t.to_dict() for t in response.added]
        modified = [t.to_dict() for t in response.modified]
        removed = [t.to_dict() for t in response.removed]

        all_added.extend(added)
        all_modified.extend(modified)
        all_removed.extend(removed)
        new_cursor = response.next_cursor

        log.info(
            "  Page %d: added=%d, modified=%d, removed=%d, has_more=%s",
            page_number,
            len(added),
            len(modified),
            len(removed),
            response.has_more,
        )

        if not response.has_more:
            break

    log.info(
        "Sync complete for %s. Total: added=%d, modified=%d, removed=%d across %d page(s).",
        label,
        len(all_added),
        len(all_modified),
        len(all_removed),
        page_number,
    )

    # Build the payload to write to GCS.
    run_date = date.today().isoformat()
    payload = {
        "institution": institution_key,
        "run_date": run_date,
        "added": all_added,
        "modified": all_modified,
        "removed": all_removed,
        "next_cursor": new_cursor,
        "page_count": page_number,
    }

    # Write to GCS.
    blob_path = write_to_gcs(
        institution_key=institution_key,
        run_date=run_date,
        payload=payload,
        record_count=len(all_added),
    )
    log.info("  Written to GCS: gs://%s/%s", GCS_BUCKET, blob_path)

    # Save the cursor only after GCS write succeeds. If the write fails,
    # the cursor is not advanced and the next run will retry.
    save_cursor(institution_key, new_cursor)
    log.info("  Cursor saved for next run.")

    return {
        "institution": institution_key,
        "added_count": len(all_added),
        "modified_count": len(all_modified),
        "removed_count": len(all_removed),
        "success": True,
        "error": None,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    run_summary = []

    for institution_key, config in INSTITUTIONS.items():
        access_token = os.getenv(config["token_env_var"])
        if not access_token:
            log.error(
                "Missing %s in .env. Run generate_tokens.py first.",
                config["token_env_var"],
            )
            run_summary.append({
                "institution": institution_key,
                "success": False,
                "error": f"Missing {config['token_env_var']}",
            })
            continue

        try:
            result = sync_institution(
                institution_key=institution_key,
                label=config["label"],
                access_token=access_token,
            )
            run_summary.append(result)
        except Exception as e:
            log.error("Sync failed for %s: %s", config["label"], str(e))
            run_summary.append({
                "institution": institution_key,
                "success": False,
                "error": str(e),
            })

    print("\n" + "=" * 55)
    print("Run summary")
    print("=" * 55)
    for r in run_summary:
        status = "OK" if r["success"] else "FAILED"
        print(f"  {r['institution']:<20} {status}")
        if r["success"]:
            print(f"    added={r['added_count']}  modified={r['modified_count']}  removed={r['removed_count']}")
        else:
            print(f"    error: {r['error']}")
    print("=" * 55)


if __name__ == "__main__":
    main()
