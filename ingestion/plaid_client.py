"""
ingestion/plaid_client.py

Returns a configured PlaidApi client for the Sandbox environment.
Reads credentials from environment variables (loaded via python-dotenv).
"""

import os
import plaid
from plaid.api import plaid_api
from dotenv import load_dotenv

load_dotenv()


def get_plaid_client() -> plaid_api.PlaidApi:
    """
    Initialise and return a PlaidApi client configured for the Sandbox.

    Required environment variables:
        PLAID_CLIENT_ID  -- from dashboard.plaid.com
        PLAID_SECRET     -- Sandbox secret from dashboard.plaid.com
        PLAID_ENV        -- must be 'sandbox'

    Returns:
        plaid_api.PlaidApi: Authenticated Plaid API client.

    Raises:
        ValueError: If any required environment variables are missing.
    """
    client_id = os.getenv("PLAID_CLIENT_ID")
    secret = os.getenv("PLAID_SECRET")
    env = os.getenv("PLAID_ENV", "sandbox").lower()

    missing = [k for k, v in {
        "PLAID_CLIENT_ID": client_id,
        "PLAID_SECRET": secret,
    }.items() if not v]

    if missing:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing)}. "
            "Check your .env file."
        )

    if env != "sandbox":
        raise ValueError(
            f"PLAID_ENV is '{env}'. This project is Sandbox-only. "
            "Set PLAID_ENV=sandbox in your .env file."
        )

    host_map = {
        "sandbox": plaid.Environment.Sandbox,
        "production": plaid.Environment.Production,
    }

    configuration = plaid.Configuration(
        host=host_map[env],
        api_key={
            "clientId": client_id,
            "secret": secret,
        },
    )

    api_client = plaid.ApiClient(configuration)
    return plaid_api.PlaidApi(api_client)
