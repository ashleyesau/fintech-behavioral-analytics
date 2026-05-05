"""
ingestion/seed_institution_b.py

Generates and loads synthetic transaction and balance data for institution_b
into BigQuery raw tables.

The Plaid Sandbox returns identical transaction data for both institutions
when the same test user credentials are used. This script replaces
institution_b's Plaid data with realistically designed synthetic data that
demonstrates the headline finding:

  Customers with high merchant concentration (top 3 merchants > 60% of spend)
  combined with 2+ consecutive months of negative net cash flow show elevated
  balance stress rates.

Institution B (VolatileBank) behavioural profile:
  - Gig and freelance income: irregular timing and variable payment amounts
  - High merchant concentration: top 3 merchants account for ~67% of spend
  - Negative cashflow streak 1: Sep-Oct 2024 (2 consecutive months)
  - Negative cashflow streak 2: Mar-May 2025 (3 consecutive months)
  - Isolated negative month: Oct 2025
  - Recent negative streak: Mar-Apr 2026
  - Checking balance stressed to 285 at snapshot date (2026-05-04)

All amounts follow Plaid convention: positive = money out, negative = money in.
Random seed is fixed at 42 for reproducibility. Re-running this script produces
identical data.

Usage:
  python ingestion/seed_institution_b.py
"""

import calendar
import json
import os
import random
import string
from datetime import UTC, date, datetime

from google.cloud import bigquery

random.seed(42)

GCP_PROJECT = "plaid-495309"
BQ_DATASET = "raw"
SERVICE_ACCOUNT_KEY = os.path.expanduser("~/.gcp/plaid-pipeline-sa-key.json")
INSTITUTION_ID = "institution_b"
INGESTION_DATE = "2026-05-04"
INGESTED_AT = datetime.now(UTC).isoformat()
SOURCE_FILE = "synthetic/institution_b/seed_20260505.json"

# Account IDs from raw.raw_accounts for institution_b
ACCOUNTS = {
    "checking":             "7q6nzXZmd6Tba5dXDXG3CrNvKj3jKACdMND4G",
    "savings":              "eEdBNwA37dCQxna5Z51ei3AjMzVzMbSr7MAyj",
    "cd":                   "QBELbZx6vESZ1glqRqNKUowbmZ6Zmluw3kbrx",
    "credit_card":          "Z7vnlBQ6rvCW5EP46438CLnXG8w8G5ier8Zn3",
    "money_market":         "MqpQWPr6bpToykDWQWPEixoDGgQgGAfLeDabW",
    "ira":                  "1ZbgXWN5KbhmKlkeEeBGijn1ZeReZ3CpZQ6bK",
    "401k":                 "LlLeEPK6nLI43Z9nznPJH8B9jplpjruklLZg8",
    "student_loan":         "pw1QoAKZE1t6nbDVXVQ7HZe9adgdaptpEDP9G",
    "mortgage":             "oEMepyqQ8MCDdL1EgEVzFdWJxpgpxXCokX64m",
    "hsa":                  "g94xj7ndQ4trlGbn7np3HoLE7dzd7AuE4lAjG",
    "cash_management":      "8KlbBZMAalUojZnJ1JmpiJ6lVGpGVruWKxDvQ",
    "business_credit_card": "EejdM3mbojfQdJVx1xzliBnVA9E9AJc49jWZZ",
}

# Top 3 merchants -- target ~67% of total monthly spend
# weight: proportion of monthly spend this merchant receives
# min/max: individual transaction amount range in USD
TOP_MERCHANTS = [
    {
        "name": "FreshMart Grocery",
        "channel": "in store",
        "type": "place",
        "weight": 0.35,
        "min": 55,
        "max": 130,
    },
    {
        "name": "CityFuel Station",
        "channel": "in store",
        "type": "place",
        "weight": 0.20,
        "min": 40,
        "max": 80,
    },
    {
        "name": "LinkMobile Airtime",
        "channel": "online",
        "type": "special",
        "weight": 0.12,
        "min": 28,
        "max": 55,
    },
]

# Other merchants -- share remaining ~33% of monthly spend
OTHER_MERCHANTS = [
    {"name": "QuickByte Cafe",     "channel": "in store", "type": "place",   "min": 8,  "max": 22},
    {"name": "Metro Pharmacy",     "channel": "in store", "type": "place",   "min": 12, "max": 45},
    {"name": "Sunrise Bakery",     "channel": "in store", "type": "place",   "min": 6,  "max": 18},
    {"name": "EasyPay Utilities",  "channel": "online",   "type": "special", "min": 80, "max": 140},
    {"name": "StreamFlix",         "channel": "online",   "type": "special", "min": 15, "max": 20},
    {"name": "Corner Laundry",     "channel": "in store", "type": "place",   "min": 10, "max": 25},
    {"name": "Fast Lane Takeaway", "channel": "in store", "type": "place",   "min": 9,  "max": 30},
    {"name": "City Gym",           "channel": "other",    "type": "special", "min": 30, "max": 50},
]

# Monthly cashflow targets: (income, spend) in USD
# Covers 2024-05 through 2026-04 (24 months)
# Positive net = income > spend. Negative net = spend > income.
CASHFLOW_SCHEDULE = [
    (2800, 2200),  # 2024-05  net +600
    (3400, 2600),  # 2024-06  net +800
    (2600, 2100),  # 2024-07  net +500
    (3200, 2700),  # 2024-08  net +500
    (1300, 2800),  # 2024-09  net -1500  STREAK 1 START
    (1500, 2900),  # 2024-10  net -1400  STREAK 1 END
    (3100, 2300),  # 2024-11  net +800
    (3800, 3100),  # 2024-12  net +700   holiday spend elevated
    (2700, 2000),  # 2025-01  net +700
    (3000, 2300),  # 2025-02  net +700
    (1400, 2600),  # 2025-03  net -1200  STREAK 2 START
    (1600, 2700),  # 2025-04  net -1100  STREAK 2 MIDDLE
    (1800, 2700),  # 2025-05  net -900   STREAK 2 END
    (3500, 2400),  # 2025-06  net +1100  recovery
    (3100, 2200),  # 2025-07  net +900
    (2800, 2600),  # 2025-08  net +200
    (3200, 2500),  # 2025-09  net +700
    (1200, 2700),  # 2025-10  net -1500  ISOLATED NEGATIVE
    (3600, 2300),  # 2025-11  net +1300
    (4100, 3200),  # 2025-12  net +900   holiday spend elevated
    (2500, 2300),  # 2026-01  net +200
    (3000, 2400),  # 2026-02  net +600
    (1100, 2600),  # 2026-03  net -1500  RECENT STREAK START
    (1300, 2800),  # 2026-04  net -1500  RECENT STREAK END
]

# Balance snapshot for 2026-05-04
# Liquid accounts are stressed. Investment and loan accounts are left near
# their Plaid Sandbox values since they are not the focus of the risk signals.
BALANCE_SNAPSHOT = [
    {"account": "checking",             "available": 185.0,   "current": 285.0,    "limit": None},
    {"account": "savings",              "available": 420.0,   "current": 420.0,    "limit": None},
    {"account": "cd",                   "available": None,    "current": 1000.0,   "limit": None},
    {"account": "credit_card",          "available": 150.0,   "current": 1850.0,   "limit": 2000.0},
    {"account": "money_market",         "available": 2800.0,  "current": 2800.0,   "limit": None},
    {"account": "ira",                  "available": None,    "current": 320.76,   "limit": None},
    {"account": "401k",                 "available": None,    "current": 23631.98, "limit": None},
    {"account": "student_loan",         "available": None,    "current": 65262.0,  "limit": None},
    {"account": "mortgage",             "available": None,    "current": 56302.06, "limit": None},
    {"account": "hsa",                  "available": 380.0,   "current": 380.0,    "limit": None},
    {"account": "cash_management",      "available": 950.0,   "current": 950.0,    "limit": None},
    {"account": "business_credit_card", "available": 4800.0,  "current": 5200.0,   "limit": 10000.0},
]


def get_bq_client():
    return bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_KEY, project=GCP_PROJECT)


def gen_transaction_id():
    """Generate a 35-character alphanumeric ID matching Plaid transaction ID format."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=35))


def random_date_in_month(year, month):
    """Return a random date within the given month."""
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, random.randint(1, last_day))


def build_tx_row(account_id, tx_date, amount, merchant_name, channel, tx_type):
    """Build a single transaction row dict matching the raw_transactions schema."""
    core = {
        "transaction_id": gen_transaction_id(),
        "account_id": account_id,
        "amount": round(amount, 2),
        "date": tx_date.isoformat(),
        "merchant_name": merchant_name,
        "merchant_category_id": None,
        "transaction_type": tx_type,
        "pending": False,
        "payment_channel": channel,
    }
    row = dict(core)
    row["raw_json"] = json.dumps(core)
    row["institution_id"] = INSTITUTION_ID
    row["ingestion_date"] = INGESTION_DATE
    row["_ingested_at"] = INGESTED_AT
    row["_source_file"] = SOURCE_FILE
    return row


def generate_spend_transactions(year, month, target_spend):
    """
    Generate spending transactions for the checking account for one month.

    Top 3 merchants receive weight-proportional shares of total spend, each
    split into multiple individual transactions. Remaining spend is distributed
    across a random selection of other merchants.
    """
    rows = []
    account_id = ACCOUNTS["checking"]
    remaining = target_spend

    for merchant in TOP_MERCHANTS:
        allocation = round(target_spend * merchant["weight"], 2)
        remaining -= allocation
        while allocation > merchant["min"]:
            amount = min(
                round(random.uniform(merchant["min"], merchant["max"]), 2),
                allocation,
            )
            rows.append(build_tx_row(
                account_id,
                random_date_in_month(year, month),
                amount,
                merchant["name"],
                merchant["channel"],
                merchant["type"],
            ))
            allocation = round(allocation - amount, 2)

    # Distribute remaining spend across randomly selected other merchants
    while remaining > 8:
        merchant = random.choice(OTHER_MERCHANTS)
        amount = min(
            round(random.uniform(merchant["min"], merchant["max"]), 2),
            remaining,
        )
        rows.append(build_tx_row(
            account_id,
            random_date_in_month(year, month),
            amount,
            merchant["name"],
            merchant["channel"],
            merchant["type"],
        ))
        remaining = round(remaining - amount, 2)

    return rows


def generate_income_transactions(year, month, target_income):
    """
    Generate income (credit) transactions for the checking account for one month.

    1-3 payments per month arrive on distinct random days, simulating gig and
    freelance income patterns. Amounts are split unevenly across payments.
    Income is negative in Plaid convention (negative = money coming in).
    """
    rows = []
    account_id = ACCOUNTS["checking"]
    num_payments = random.randint(1, 3)

    # Split income into uneven chunks using random cut points
    cuts = sorted([random.random() for _ in range(num_payments - 1)] + [0.0, 1.0])
    amounts = [
        round((cuts[i + 1] - cuts[i]) * target_income, 2)
        for i in range(num_payments)
    ]

    # Assign each payment a unique day within the month
    last_day = calendar.monthrange(year, month)[1]
    days_used = set()
    for amount in amounts:
        if amount < 50:
            continue
        for _ in range(20):
            day = random.randint(1, last_day)
            if day not in days_used:
                days_used.add(day)
                break
        rows.append(build_tx_row(
            account_id,
            date(year, month, day),
            -round(amount, 2),  # negative = credit in Plaid convention
            None,               # income has no merchant name
            "other",
            "special",
        ))

    return rows


def generate_balance_rows():
    """
    Generate one balance row per account for the 2026-05-04 snapshot.

    balance_available, balance_current, and balance_limit are populated as
    top-level columns (matching the pre-provisioned schema) as well as being
    captured inside raw_json.
    """
    rows = []
    for entry in BALANCE_SNAPSHOT:
        account_id = ACCOUNTS[entry["account"]]
        core = {
            "account_id": account_id,
            "balance_available": entry["available"],
            "balance_current": entry["current"],
            "balance_limit": entry["limit"],
            "iso_currency_code": "USD",
        }
        row = dict(core)
        row["raw_json"] = json.dumps(core)
        row["institution_id"] = INSTITUTION_ID
        row["ingestion_date"] = INGESTION_DATE
        row["_ingested_at"] = INGESTED_AT
        row["_source_file"] = SOURCE_FILE
        rows.append(row)
    return rows


def delete_existing_data(bq_client):
    """Delete existing institution_b rows from raw_transactions and raw_balances."""
    for table in ["raw_transactions", "raw_balances"]:
        sql = f"""
            DELETE FROM `{GCP_PROJECT}.{BQ_DATASET}.{table}`
            WHERE institution_id = '{INSTITUTION_ID}'
            AND ingestion_date = '{INGESTION_DATE}'
        """
        bq_client.query(sql).result()
        print(f"Deleted existing {INSTITUTION_ID} rows from {table}.")


def load_rows(bq_client, rows, table_name):
    """Load a list of row dicts into the correct date partition of a BigQuery table."""
    if not rows:
        print(f"No rows to load for {table_name}. Skipping.")
        return 0

    date_nodash = INGESTION_DATE.replace("-", "")
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}${date_nodash}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        ignore_unknown_values=True,
    )

    job = bq_client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()

    if job.errors:
        raise RuntimeError(f"Load failed for {table_name}: {job.errors}")

    return job.output_rows


def _print_summary(tx_rows, balance_rows):
    width = 55
    print("\n" + "=" * width)
    print("  SEED SUMMARY")
    print("=" * width)
    print(f"  Institution:      {INSTITUTION_ID}")
    print(f"  Ingestion date:   {INGESTION_DATE}")
    print(f"  Transactions:     {len(tx_rows)} rows")
    print(f"  Balance rows:     {len(balance_rows)} rows")
    print("=" * width + "\n")


def main():
    bq_client = get_bq_client()

    print("Deleting existing institution_b data...")
    delete_existing_data(bq_client)

    print("Generating transactions for 24 months...")
    tx_rows = []
    year, month = 2024, 5
    for income, spend in CASHFLOW_SCHEDULE:
        tx_rows.extend(generate_income_transactions(year, month, income))
        tx_rows.extend(generate_spend_transactions(year, month, spend))
        month += 1
        if month > 12:
            month = 1
            year += 1

    print("Generating balance snapshot...")
    balance_rows = generate_balance_rows()

    print(f"Loading {len(tx_rows)} transaction rows into raw_transactions...")
    load_rows(bq_client, tx_rows, "raw_transactions")

    print(f"Loading {len(balance_rows)} balance rows into raw_balances...")
    load_rows(bq_client, balance_rows, "raw_balances")

    _print_summary(tx_rows, balance_rows)
    print("Seed complete.")


if __name__ == "__main__":
    main()
