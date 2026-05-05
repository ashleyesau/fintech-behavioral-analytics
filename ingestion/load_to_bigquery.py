import argparse
import json
import logging
import os
from datetime import date

from google.cloud import bigquery, storage

GCP_PROJECT = "plaid-495309"
GCS_BUCKET = "plaid-495309-raw-data"
BQ_DATASET = "raw"
SERVICE_ACCOUNT_KEY = os.path.expanduser("~/.gcp/plaid-pipeline-sa-key.json")

INSTITUTIONS = ["institution_a", "institution_b"]
DATA_TYPES = ["transactions", "accounts", "balances"]

BQ_TABLE_MAP = {
    "transactions": "raw_transactions",
    "accounts": "raw_accounts",
    "balances": "raw_balances",
}

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/load.log"),
    ],
)
log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load raw GCS JSON files into BigQuery raw tables."
    )
    parser.add_argument(
        "--date",
        type=str,
        default=date.today().isoformat(),
        help="Date partition to load in YYYY-MM-DD format (default: today)",
    )
    return parser.parse_args()


def get_clients():
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_KEY)
    bq_client = bigquery.Client.from_service_account_json(
        SERVICE_ACCOUNT_KEY, project=GCP_PROJECT
    )
    return storage_client, bq_client


def find_data_blobs(storage_client, institution, data_type, run_date):
    prefix = f"raw/{data_type}/institution={institution}/date={run_date}/"
    blobs = list(storage_client.list_blobs(GCS_BUCKET, prefix=prefix))

    # Exclude metadata sidecars -- they are named metadata_TIMESTAMP.json
    data_blobs = [
        b for b in blobs
        if not os.path.basename(b.name).startswith("metadata_")
    ]

    if not data_blobs:
        raise FileNotFoundError(
            f"No {data_type} file found for {institution} on {run_date}. "
            f"Run extract_transactions.py first."
        )

    # Return sorted ascending so latest is last
    return sorted(data_blobs, key=lambda b: b.name)


def download_blob(blob):
    content = blob.download_as_text()
    return json.loads(content)


def flatten_transactions(raw, institution_id, ingestion_date):
    rows = raw.get("added", [])
    if not rows:
        log.warning(f"No 'added' transactions found for {institution_id} on {ingestion_date}.")
    for row in rows:
        row["institution_id"] = institution_id
        row["ingestion_date"] = ingestion_date
    return rows


def flatten_accounts(raw, institution_id, ingestion_date):
    rows = raw.get("accounts", [])
    if not rows:
        log.warning(f"No accounts found for {institution_id} on {ingestion_date}.")
    for row in rows:
        row["institution_id"] = institution_id
        row["ingestion_date"] = ingestion_date
    return rows


def flatten_balances(raw, institution_id, ingestion_date):
    # Balances payload uses 'accounts' as the key, same as the accounts file
    rows = raw.get("accounts", [])
    if not rows:
        log.warning(f"No balances found for {institution_id} on {ingestion_date}.")
    for row in rows:
        row["institution_id"] = institution_id
        row["ingestion_date"] = ingestion_date
    return rows


FLATTEN_FN = {
    "transactions": flatten_transactions,
    "accounts": flatten_accounts,
    "balances": flatten_balances,
}


def load_rows(bq_client, rows, data_type, run_date):
    if not rows:
        log.info(f"No rows to load for {data_type}. Skipping.")
        return 0

    # Partition decorator scopes the write to this date's partition only
    date_nodash = run_date.replace("-", "")
    table_id = (
        f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_MAP[data_type]}${date_nodash}"
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        ignore_unknown_values=True,
    )

    job = bq_client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()

    if job.errors:
        raise RuntimeError(
            f"BigQuery load failed for {data_type}: {job.errors}"
        )

    return job.output_rows


def _print_stage_summary(results):
    print("\n" + "=" * 65)
    print("LOAD SUMMARY")
    print("=" * 65)
    print(f"{'Institution':<22} {'Data Type':<15} {'Rows':<10} {'Destination'}")
    print("-" * 65)
    total = 0
    for r in results:
        print(
            f"{r['institution']:<22} {r['data_type']:<15} "
            f"{r['rows_loaded']:<10} {r['destination']}"
        )
        total += r["rows_loaded"]
    print("-" * 65)
    print(f"{'TOTAL':<22} {'':<15} {total:<10}")
    print("=" * 65 + "\n")


def main():
    args = parse_args()
    run_date = args.date
    log.info(f"Starting BigQuery load for date: {run_date}")

    storage_client, bq_client = get_clients()
    results = []

    for institution in INSTITUTIONS:
        for data_type in DATA_TYPES:
            log.info(f"Processing {data_type} for {institution}")

            blobs = find_data_blobs(storage_client, institution, data_type, run_date)

            if data_type == "transactions":
                # Concatenate added rows across all files for the date.
                # Multiple extraction runs per day each produce a separate file
                # with their slice of added transactions via cursor-based sync.
                rows = []
                for blob in blobs:
                    log.info(f"Reading file: {blob.name}")
                    raw = download_blob(blob)
                    rows.extend(flatten_transactions(raw, institution, run_date))
            else:
                # Accounts and balances are full snapshots -- use latest file only
                blob = blobs[-1]
                log.info(f"Found file: {blob.name}")
                raw = download_blob(blob)
                rows = FLATTEN_FN[data_type](raw, institution, run_date)

            log.info(f"Flattened {len(rows)} rows")

            destination = f"{BQ_DATASET}.{BQ_TABLE_MAP[data_type]}"
            rows_loaded = load_rows(bq_client, rows, data_type, run_date)
            log.info(f"Loaded {rows_loaded} rows into {destination}")

            results.append(
                {
                    "institution": institution,
                    "data_type": data_type,
                    "rows_loaded": rows_loaded,
                    "destination": destination,
                }
            )

    _print_stage_summary(results)
    log.info("Load complete.")


if __name__ == "__main__":
    main()
