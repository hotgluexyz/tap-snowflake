"""Snowflake OAuth authenticator."""

import requests

from hotglue_singer_sdk.authenticators import OAuthAuthenticator


def _snowflake_token_url(account: str) -> str:
    return f"https://{account}.snowflakecomputing.com/oauth/token-request"


def _snowflake_oauth_payload(config: dict) -> dict:
    return {
        "client_id": config.get("client_id"),
        "refresh_token": config.get("refresh_token"),
        "grant_type": "refresh_token",
    }


def _snowflake_oauth_auth(config: dict) -> requests.auth.HTTPBasicAuth:
    return requests.auth.HTTPBasicAuth(config.get("client_id"), config.get("client_secret"))


def fetch_snowflake_access_token(config: dict) -> str:
    """Exchange a Snowflake refresh token for a new access token."""
    response = requests.post(
        _snowflake_token_url(config.get("account")),
        data=_snowflake_oauth_payload(config),
        auth=_snowflake_oauth_auth(config),
    )
    response_json = response.json()
    if response_json.get("error"):
        raise ConnectionError(response_json.get("message"))
    return response_json["access_token"]


class SnowflakeOAuthAuthenticator(OAuthAuthenticator):
    """OAuth authenticator for Snowflake using refresh_token grant."""

    @property
    def auth_endpoint(self) -> str:
        return _snowflake_token_url(self.config.get("account"))

    @property
    def oauth_request_payload(self) -> dict:
        return _snowflake_oauth_payload(self.config)

    def request_auth(self):
        return _snowflake_oauth_auth(self.config)
