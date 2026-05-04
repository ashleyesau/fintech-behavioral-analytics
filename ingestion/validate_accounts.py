"""
ingestion/validate_accounts.py

Validates that both Plaid access tokens are working by calling /accounts/get
for each institution. Logs account IDs, names, types, and current balances.

Run this after generate_tokens.py to confirm auth is working before building
the full ingestion layer.

Usage:
    python ingestion/validate_accounts.py
"""

import json
import os
from dotenv import load_dotenv
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid import ApiException

from plaid_client import get_plaid_client

load_dotenv()

INSTITUTIONS = {
    "INSTITUTION_A": {
        "label": "StableBank",
        "token_env_var": "ACCESS_TOKEN_INSTITUTION_A",
    },
    "INSTITUTION_B": {
        "label": "VolatileBank",
        "token_env_var": "ACCESS_TOKEN_INSTITUTION_B",
    },
}


def validate_institution(client, label: str, access_token: str) -> None:
    """
    Call /accounts/get for one institution and print a summary of all accounts.

    Args:
        client: Authenticated PlaidApi client.
        label: Human-readable institution label.
        access_token: Plaid access token for this institution.
    """
    print(f"\n{'=' * 55}")
    print(f"  {label}")
    print(f"{'=' * 55}")

    request = AccountsGetRequest(access_token=access_token)
    response = client.accounts_get(request)

    accounts = response["accounts"]
    print(f"  Accounts returned: {len(accounts)}\n")

    for acct in accounts:
        balances = acct["balances"]
        print(f"  Account ID   : {acct['account_id']}")
        print(f"  Name         : {acct['name']}")
        print(f"  Official Name: {acct.get('official_name', 'N/A')}")
        print(f"  Type         : {acct['type']} / {acct['subtype']}")
        print(f"  Balance      : current={balances.get('current')}  "
              f"available={balances.get('available')}  "
              f"limit={balances.get('limit')}")
        print()


def main():
    client = get_plaid_client()

    all_ok = True
    for env_key, config in INSTITUTIONS.items():
        token = os.getenv(config["token_env_var"])
        if not token:
            print(
                f"\n  MISSING: {config['token_env_var']} not found in .env. "
                "Run generate_tokens.py first."
            )
            all_ok = False
            continue

        try:
            validate_institution(client, config["label"], token)
        except ApiException as e:
            error_body = json.loads(e.body)
            print(
                f"\n  ERROR for {config['label']}: "
                f"{error_body.get('error_code')} - {error_body.get('error_message')}"
            )
            all_ok = False

    print("\n" + ("Both institutions validated successfully." if all_ok
                  else "One or more institutions failed validation."))


if __name__ == "__main__":
    main()
