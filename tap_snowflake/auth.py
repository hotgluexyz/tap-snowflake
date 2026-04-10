"""Snowflake OAuth authenticator."""

import requests

from hotglue_singer_sdk.authenticators import OAuthAuthenticator


def fetch_snowflake_access_token(config: dict) -> str:
    """Exchange a Snowflake refresh token for a new access token."""
    account = config.get("account")
    client_id = config.get("client_id")
    client_secret = config.get("client_secret")
    url = f"https://{account}.snowflakecomputing.com/oauth/token-request"
    payload = {
        "client_id": client_id,
        "refresh_token": config.get("refresh_token"),
        "grant_type": "refresh_token",
    }
    response = requests.post(url, data=payload, auth=requests.auth.HTTPBasicAuth(client_id, client_secret))
    response_json = response.json()
    if response_json.get("error"):
        raise ConnectionError(response_json.get("message"))
    return response_json["access_token"]


class SnowflakeOAuthAuthenticator(OAuthAuthenticator):
    """OAuth authenticator for Snowflake using refresh_token grant."""

    @property
    def auth_endpoint(self) -> str:
        account = self.config.get("account")
        return f"https://{account}.snowflakecomputing.com/oauth/token-request"

    @property
    def oauth_request_payload(self) -> dict:
        return {
            "client_id": self.config.get("client_id"),
            "refresh_token": self.config.get("refresh_token"),
            "grant_type": "refresh_token",
        }

    def request_auth(self):
        return requests.auth.HTTPBasicAuth(
            self.config.get("client_id"),
            self.config.get("client_secret"),
        )
