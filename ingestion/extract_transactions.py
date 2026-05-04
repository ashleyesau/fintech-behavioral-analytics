"""
ingestion/extract_transactions.py

Pulls all raw financial data from the Plaid Sandbox API for both institutions
and writes it to Google Cloud Storage, partitioned by institution and date.

Three data types are extracted in sequence:

    1. Transactions  -- /transactions/sync (cursor-based, incremental)
    2. Accounts      -- /accounts/get (full snapshot each run)
    3. Balances      -- /accounts/balances/get (full snapshot each run)

Each type is written to a separate GCS partition:
    raw/transactions/institution=<key>/date=<YYYY-MM-DD>/
    raw/accounts/institution=<key>/date=<YYYY-MM-DD>/
    raw/balances/institution=<key>/date=<YYYY-MM-DD>/

A metadata sidecar is written alongside each data file for use by
mart_operational_monitoring downstream.

Usage:
    python ingestion/extract_transactions.py
"""

import json
import logging
import os
import time
from datetime import UTC, date, datetime
from typing import Any

from dotenv import load_dotenv
from google.cloud import storage
from plaid import ApiException
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest
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
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    """Return the current UTC datetime as a timezone-aware object."""
    return datetime.now(UTC)


def _serialise(obj: Any) -> Any:
    """
    Custom JSON serialiser for types the standard library cannot handle.

    Plaid returns Python datetime.date and datetime.datetime objects rather
    than strings. This converts them to ISO 8601 strings before JSON write.
    """
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not JSON serialisable")


def _print_stage_summary(stage: str, results: list) -> None:
    """
    Print a formatted summary table for one extraction stage.

    Args:
        stage: Stage label, e.g. 'TRANSACTIONS', 'ACCOUNTS', 'BALANCES'
        results: List of per-institution result dicts from that stage.
    """
    width = 55
    print("\n" + "=" * width)
    print(f"  Stage complete: {stage}")
    print("=" * width)
    for r in results:
        status = "OK" if r["success"] else "FAILED"
        print(f"  {r['institution']:<22} {status}")
        if r["success"]:
            for k, v in r.items():
                if k not in ("institution", "success", "error"):
                    print(f"    {k}: {v}")
        else:
            print(f"    error: {r['error']}")
    print("=" * width)


# ---------------------------------------------------------------------------
# GCS write
# ---------------------------------------------------------------------------


def write_to_gcs(
    data_type: str,
    institution_key: str,
    run_date: str,
    payload: dict,
    record_count: int,
) -> str:
    """
    Write an extraction payload to GCS as an immutable partitioned JSON file.

    Partition structure:
        gs://<bucket>/raw/<data_type>/institution=<key>/date=<YYYY-MM-DD>/

    A metadata sidecar is written alongside every data file. It records the
    record count, run ID, and blob path for use by mart_operational_monitoring.

    Args:
        data_type: 'transactions', 'accounts', or 'balances'
        institution_key: e.g. 'institution_a'
        run_date: ISO date string, e.g. '2026-05-04'
        payload: The response dict to serialise and write.
        record_count: Number of records in the primary list.

    Returns:
        The GCS blob path that was written.
    """
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(GCS_BUCKET)

    timestamp = _now().strftime("%Y%m%dT%H%M%SZ")
    prefix = f"raw/{data_type}/institution={institution_key}/date={run_date}"
    data_blob_name = f"{prefix}/{data_type}_{timestamp}.json"
    meta_blob_name = f"{prefix}/metadata_{timestamp}.json"

    data_blob = bucket.blob(data_blob_name)
    data_blob.upload_from_string(
        json.dumps(payload, default=_serialise, indent=2),
        content_type="application/json",
    )

    metadata = {
        "run_id": timestamp,
        "data_type": data_type,
        "institution": institution_key,
        "run_date": run_date,
        "record_count": record_count,
        "blob_path": data_blob_name,
        "written_at": _now().isoformat(),
    }
    meta_blob = bucket.blob(meta_blob_name)
    meta_blob.upload_from_string(
        json.dumps(metadata, indent=2),
        content_type="application/json",
    )

    return data_blob_name


# ---------------------------------------------------------------------------
# Stage 1: Transactions
# ---------------------------------------------------------------------------


def sync_transactions(institution_key: str, label: str, access_token: str) -> dict:
    """
    Incremental transaction sync via /transactions/sync.

    Calls the endpoint in a loop until has_more is False. Each call uses
    the cursor returned by the previous call. On first run, no cursor is
    passed and Plaid returns the full transaction history.

    Cursor is saved only after a successful GCS write so that a partial
    failure does not advance the bookmark and silently skip transactions.

    Args:
        institution_key: e.g. 'institution_a'
        label: Human-readable label for logging.
        access_token: Plaid access token for this institution.

    Returns:
        Result dict with institution, added_count, modified_count,
        removed_count, pages, gcs_path, success, error.
    """
    log.info("[TRANSACTIONS] Starting sync for %s", label)

    client = get_plaid_client()
    cursor = load_cursor(institution_key)

    if cursor is None:
        log.info("  No cursor found -- first run, fetching full history.")
    else:
        log.info("  Cursor loaded. Fetching incremental updates.")

    all_added, all_modified, all_removed = [], [], []
    new_cursor = cursor
    page_number = 0

    while True:
        page_number += 1
        log.info("  Fetching page %d...", page_number)

        request = TransactionsSyncRequest(
            access_token=access_token,
            **({"cursor": new_cursor} if new_cursor else {}),
        )

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = client.transactions_sync(request)
                break
            except ApiException as e:
                error_body = json.loads(e.body)
                error_code = error_body.get("error_code", "UNKNOWN")
                if error_code == "PRODUCT_NOT_READY":
                    log.warning(
                        "  Attempt %d/%d: PRODUCT_NOT_READY -- retrying in %ds.",
                        attempt, MAX_RETRIES, RETRY_BASE_DELAY * (2 ** (attempt - 1)),
                    )
                else:
                    log.error(
                        "  Attempt %d/%d: API error %s -- %s",
                        attempt, MAX_RETRIES, error_code,
                        error_body.get("error_message"),
                    )
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(RETRY_BASE_DELAY * (2 ** (attempt - 1)))

        added = [t.to_dict() for t in response.added]
        modified = [t.to_dict() for t in response.modified]
        removed = [t.to_dict() for t in response.removed]
        all_added.extend(added)
        all_modified.extend(modified)
        all_removed.extend(removed)
        new_cursor = response.next_cursor

        log.info(
            "  Page %d: added=%d, modified=%d, removed=%d, has_more=%s",
            page_number, len(added), len(modified), len(removed), response.has_more,
        )

        if not response.has_more:
            break

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

    blob_path = write_to_gcs(
        data_type="transactions",
        institution_key=institution_key,
        run_date=run_date,
        payload=payload,
        record_count=len(all_added),
    )
    log.info("  Written to GCS: gs://%s/%s", GCS_BUCKET, blob_path)

    save_cursor(institution_key, new_cursor)
    log.info("  Cursor saved.")

    return {
        "institution": institution_key,
        "added_count": len(all_added),
        "modified_count": len(all_modified),
        "removed_count": len(all_removed),
        "pages": page_number,
        "gcs_path": blob_path,
        "success": True,
        "error": None,
    }


# ---------------------------------------------------------------------------
# Stage 2: Accounts
# ---------------------------------------------------------------------------


def sync_accounts(institution_key: str, label: str, access_token: str) -> dict:
    """
    Full account snapshot via /accounts/get.

    No cursor or pagination -- Plaid returns all accounts in a single call.
    A new snapshot is written to GCS on every run, providing a point-in-time
    record of account metadata (type, subtype, institution).

    Args:
        institution_key: e.g. 'institution_a'
        label: Human-readable label for logging.
        access_token: Plaid access token for this institution.

    Returns:
        Result dict with institution, account_count, gcs_path, success, error.
    """
    log.info("[ACCOUNTS] Starting sync for %s", label)

    client = get_plaid_client()
    request = AccountsGetRequest(access_token=access_token)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.accounts_get(request)
            break
        except ApiException as e:
            error_body = json.loads(e.body)
            log.error(
                "  Attempt %d/%d: API error %s -- %s",
                attempt, MAX_RETRIES,
                error_body.get("error_code"),
                error_body.get("error_message"),
            )
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_BASE_DELAY * (2 ** (attempt - 1)))

    accounts = [a.to_dict() for a in response.accounts]
    log.info("  Retrieved %d accounts for %s.", len(accounts), label)

    run_date = date.today().isoformat()
    payload = {
        "institution": institution_key,
        "run_date": run_date,
        "accounts": accounts,
    }

    blob_path = write_to_gcs(
        data_type="accounts",
        institution_key=institution_key,
        run_date=run_date,
        payload=payload,
        record_count=len(accounts),
    )
    log.info("  Written to GCS: gs://%s/%s", GCS_BUCKET, blob_path)

    return {
        "institution": institution_key,
        "account_count": len(accounts),
        "gcs_path": blob_path,
        "success": True,
        "error": None,
    }


# ---------------------------------------------------------------------------
# Stage 3: Balances
# ---------------------------------------------------------------------------


def sync_balances(institution_key: str, label: str, access_token: str) -> dict:
    """
    Real-time balance snapshot via /accounts/balances/get.

    Similar to /accounts/get but returns live balance figures rather than
    cached values. Plaid contacts the institution directly on each call.

    A new snapshot is written on every run. The daily time series this
    produces feeds balance_stress_index and overdraft_proximity in
    mart_risk_signals downstream.

    Args:
        institution_key: e.g. 'institution_a'
        label: Human-readable label for logging.
        access_token: Plaid access token for this institution.

    Returns:
        Result dict with institution, account_count, gcs_path, success, error.
    """
    log.info("[BALANCES] Starting sync for %s", label)

    client = get_plaid_client()
    request = AccountsBalanceGetRequest(access_token=access_token)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.accounts_balance_get(request)
            break
        except ApiException as e:
            error_body = json.loads(e.body)
            log.error(
                "  Attempt %d/%d: API error %s -- %s",
                attempt, MAX_RETRIES,
                error_body.get("error_code"),
                error_body.get("error_message"),
            )
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_BASE_DELAY * (2 ** (attempt - 1)))

    accounts = [a.to_dict() for a in response.accounts]
    log.info("  Retrieved balances for %d accounts for %s.", len(accounts), label)

    run_date = date.today().isoformat()
    payload = {
        "institution": institution_key,
        "run_date": run_date,
        "snapshot_date": run_date,
        "accounts": accounts,
    }

    blob_path = write_to_gcs(
        data_type="balances",
        institution_key=institution_key,
        run_date=run_date,
        payload=payload,
        record_count=len(accounts),
    )
    log.info("  Written to GCS: gs://%s/%s", GCS_BUCKET, blob_path)

    return {
        "institution": institution_key,
        "account_count": len(accounts),
        "gcs_path": blob_path,
        "success": True,
        "error": None,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    transaction_results = []
    account_results = []
    balance_results = []

    for institution_key, config in INSTITUTIONS.items():
        access_token = os.getenv(config["token_env_var"])
        if not access_token:
            msg = f"Missing {config['token_env_var']} in .env. Run generate_tokens.py first."
            log.error(msg)
            error_result = {"institution": institution_key, "success": False, "error": msg}
            transaction_results.append(error_result)
            account_results.append(error_result)
            balance_results.append(error_result)
            continue

        label = config["label"]

        try:
            transaction_results.append(sync_transactions(institution_key, label, access_token))
        except Exception as e:
            log.error("Transaction sync failed for %s: %s", label, str(e))
            transaction_results.append({"institution": institution_key, "success": False, "error": str(e)})

        try:
            account_results.append(sync_accounts(institution_key, label, access_token))
        except Exception as e:
            log.error("Account sync failed for %s: %s", label, str(e))
            account_results.append({"institution": institution_key, "success": False, "error": str(e)})

        try:
            balance_results.append(sync_balances(institution_key, label, access_token))
        except Exception as e:
            log.error("Balance sync failed for %s: %s", label, str(e))
            balance_results.append({"institution": institution_key, "success": False, "error": str(e)})

    _print_stage_summary("TRANSACTIONS", transaction_results)
    _print_stage_summary("ACCOUNTS", account_results)
    _print_stage_summary("BALANCES", balance_results)

    all_results = transaction_results + account_results + balance_results
    total = len(all_results)
    passed = sum(1 for r in all_results if r["success"])

    print(f"\n  {passed}/{total} stages passed.")
    if passed == total:
        print("  All data written to GCS successfully.")
    else:
        print("  One or more stages failed. Check logs/ingestion.log for details.")


if __name__ == "__main__":
    main()
