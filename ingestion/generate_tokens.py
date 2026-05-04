"""
ingestion/generate_tokens.py

Generates Plaid Sandbox access tokens for both institutions using the
public token exchange flow (bypasses the Link UI in Sandbox).

Run once per environment. Store the resulting access tokens in your .env file.

Usage:
    python ingestion/generate_tokens.py

Output:
    Prints ACCESS_TOKEN_INSTITUTION_A and ACCESS_TOKEN_INSTITUTION_B
    ready to paste into your .env file.
"""

import json
from plaid.model.sandbox_public_token_create_request import (
    SandboxPublicTokenCreateRequest,
)
from plaid.model.item_public_token_exchange_request import (
    ItemPublicTokenExchangeRequest,
)
from plaid.model.products import Products
from plaid import ApiException

from plaid_client import get_plaid_client

# Sandbox institution IDs.
# ins_109508 -> Chase (used as StableBank / Institution A)
# ins_109511 -> Wells Fargo (used as VolatileBank / Institution B)
INSTITUTIONS = {
    "INSTITUTION_A": {
        "label": "StableBank (Chase)",
        "institution_id": "ins_109508",
    },
    "INSTITUTION_B": {
        "label": "VolatileBank (Wells Fargo)",
        "institution_id": "ins_109511",
    },
}

# Products needed for this project.
PRODUCTS = [Products("transactions"), Products("auth")]

# Sandbox test credentials - documented by Plaid.
# user_transactions_dynamic gives a richer 24-month transaction history.
SANDBOX_CREDENTIALS = {
    "username": "user_transactions_dynamic",
    "password": "pass_good",
    "mfa": "1234",  # used when Plaid prompts for 2FA
}


def generate_access_token(client, institution_id: str, institution_label: str) -> str:
    """
    Exchange a Plaid Sandbox public token for a permanent access token.

    In production this flow is handled by the Plaid Link UI. In Sandbox,
    the public token can be created directly via the API.

    Args:
        client: Authenticated PlaidApi client.
        institution_id: Plaid institution ID string.
        institution_label: Human-readable label for logging.

    Returns:
        str: The access token for the institution.
    """
    print(f"\n--- Generating token for {institution_label} ({institution_id}) ---")

    # Step 1: Create a public token for the institution in Sandbox.
    public_token_request = SandboxPublicTokenCreateRequest(
        institution_id=institution_id,
        initial_products=PRODUCTS,
    )
    public_token_response = client.sandbox_public_token_create(public_token_request)
    public_token = public_token_response["public_token"]
    print(f"  Public token created: {public_token[:20]}...")

    # Step 2: Exchange the public token for a durable access token.
    exchange_request = ItemPublicTokenExchangeRequest(public_token=public_token)
    exchange_response = client.item_public_token_exchange(exchange_request)
    access_token = exchange_response["access_token"]
    item_id = exchange_response["item_id"]

    print(f"  Access token:         {access_token[:20]}...")
    print(f"  Item ID:              {item_id}")
    return access_token


def main():
    client = get_plaid_client()
    tokens = {}

    for env_key, config in INSTITUTIONS.items():
        try:
            token = generate_access_token(
                client,
                institution_id=config["institution_id"],
                institution_label=config["label"],
            )
            tokens[env_key] = token
        except ApiException as e:
            error_body = json.loads(e.body)
            print(
                f"  ERROR for {config['label']}: "
                f"{error_body.get('error_code')} - {error_body.get('error_message')}"
            )

    print("\n\n" + "=" * 60)
    print("Add these lines to your .env file:")
    print("=" * 60)
    for env_key, token in tokens.items():
        print(f"ACCESS_TOKEN_{env_key}={token}")
    print("=" * 60)


if __name__ == "__main__":
    main()
