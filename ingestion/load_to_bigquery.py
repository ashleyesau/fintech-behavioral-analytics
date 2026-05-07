import argparse
import json
import logging
import os
from datetime import UTC, date, datetime

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

BQ_SCHEMAS = {
    "transactions": [
        bigquery.SchemaField("transaction_id", "STRING"),
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("institution_id", "STRING"),
        bigquery.SchemaField("amount", "FLOAT64"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("merchant_name", "STRING"),
        bigquery.SchemaField("merchant_category_id", "STRING"),
        bigquery.SchemaField("transaction_type", "STRING"),
        bigquery.SchemaField("pending", "BOOL"),
        bigquery.SchemaField("payment_channel", "STRING"),
        bigquery.SchemaField("raw_json", "STRING"),
        bigquery.SchemaField("ingestion_date", "DATE"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("_source_file", "STRING"),
    ],
    "accounts": [
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("institution_id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("official_name", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("subtype", "STRING"),
        bigquery.SchemaField("mask", "STRING"),
        bigquery.SchemaField("raw_json", "STRING"),
        bigquery.SchemaField("ingestion_date", "DATE"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("_source_file", "STRING"),
    ],
    "balances": [
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("institution_id", "STRING"),
        bigquery.SchemaField("balance_available", "FLOAT64"),
        bigquery.SchemaField("balance_current", "FLOAT64"),
        bigquery.SchemaField("balance_limit", "FLOAT64"),
        bigquery.SchemaField("iso_currency_code", "STRING"),
        bigquery.SchemaField("raw_json", "STRING"),
        bigquery.SchemaField("ingestion_date", "DATE"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("_source_file", "STRING"),
    ],
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

    data_blobs = [
        b for b in blobs
        if not os.path.basename(b.name).startswith("metadata_")
    ]

    if not data_blobs:
        raise FileNotFoundError(
            f"No {data_type} file found for {institution} on {run_date}. "
            f"Run extract_transactions.py first."
        )

    return sorted(data_blobs, key=lambda b: b.name)


def download_blob(blob):
    content = blob.download_as_text()
    return json.loads(content)


def flatten_transactions(raw, institution_id, ingestion_date):
    rows = raw.get("added", [])
    if not rows:
        log.warning(f"No 'added' transactions found for {institution_id} on {ingestion_date}.")
    return rows


def flatten_accounts(raw, institution_id, ingestion_date):
    rows = raw.get("accounts", [])
    if not rows:
        log.warning(f"No accounts found for {institution_id} on {ingestion_date}.")
    projected = []
    for row in rows:
        projected.append({
            "account_id": row.get("account_id"),
            "name": row.get("name"),
            "official_name": row.get("official_name"),
            "type": row.get("type"),
            "subtype": row.get("subtype"),
            "mask": str(row["mask"]) if row.get("mask") is not None else None,
        })
    return projected


def flatten_balances(raw, institution_id, ingestion_date):
    rows = raw.get("accounts", [])
    if not rows:
        log.warning(f"No balances found for {institution_id} on {ingestion_date}.")
    projected = []
    for row in rows:
        balances = row.get("balances") or {}
        projected.append({
            "account_id": row.get("account_id"),
            "balance_available": balances.get("available"),
            "balance_current": balances.get("current"),
            "balance_limit": balances.get("limit"),
            "iso_currency_code": balances.get("iso_currency_code"),
        })
    return projected


def _add_metadata(rows, institution_id, ingestion_date, blob_name):
    ingested_at = datetime.now(UTC).isoformat()
    for row in rows:
        row["raw_json"] = json.dumps(row, default=str)
        row["institution_id"] = institution_id
        row["ingestion_date"] = ingestion_date
        row["_ingested_at"] = ingested_at
        row["_source_file"] = blob_name
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

    date_nodash = run_date.replace("-", "")
    table_id = (
        f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_MAP[data_type]}${date_nodash}"
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=BQ_SCHEMAS[data_type],
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

    # Outer loop is data_type so all institutions are accumulated before the
    # single WRITE_TRUNCATE load. Looping institutions in the inner position
    # previously caused each institution load to overwrite the prior one.
    for data_type in DATA_TYPES:
        all_rows = []

        for institution in INSTITUTIONS:
            log.info(f"Processing {data_type} for {institution}")

            blobs = find_data_blobs(storage_client, institution, data_type, run_date)

            if data_type == "transactions":
                rows = []
                for blob in blobs:
                    log.info(f"Reading file: {blob.name}")
                    raw = download_blob(blob)
                    file_rows = flatten_transactions(raw, institution, run_date)
                    rows.extend(_add_metadata(file_rows, institution, run_date, blob.name))
            else:
                blob = blobs[-1]
                log.info(f"Found file: {blob.name}")
                raw = download_blob(blob)
                rows = FLATTEN_FN[data_type](raw, institution, run_date)
                rows = _add_metadata(rows, institution, run_date, blob.name)

            log.info(f"Flattened {len(rows)} rows for {institution}")
            all_rows.extend(rows)

        destination = f"{BQ_DATASET}.{BQ_TABLE_MAP[data_type]}"
        rows_loaded = load_rows(bq_client, all_rows, data_type, run_date)
        log.info(f"Loaded {rows_loaded} rows into {destination}")

        results.append(
            {
                "institution": "institution_a + institution_b",
                "data_type": data_type,
                "rows_loaded": rows_loaded,
                "destination": destination,
            }
        )

    _print_stage_summary(results)
    log.info("Load complete.")


if __name__ == "__main__":
    main()
