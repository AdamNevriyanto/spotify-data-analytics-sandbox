# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Verify Spotify API access
# MAGIC Run this notebook in workspace `dbc-7bca3ce8-2b59.cloud.databricks.com`.
# MAGIC It reads app credentials, refreshes an access token, and fetches one page
# MAGIC containing up to five recently played tracks. It does not write to S3.
# MAGIC If Spotify rotates the refresh token, it saves the replacement in the same
# MAGIC secret scope. Your notebook identity needs READ and WRITE on that scope.
# MAGIC Do not display token responses, request headers, or secret values.

# COMMAND ----------

import requests
from databricks.sdk import WorkspaceClient

EXPECTED_HOST = "https://dbc-7bca3ce8-2b59.cloud.databricks.com"
SECRET_SCOPE = "spotify-secrets"

# Notebook authentication uses the workspace context, not your local CLI profile.
workspace = WorkspaceClient()
if workspace.config.host.rstrip("/") != EXPECTED_HOST:
    raise RuntimeError("Wrong workspace. Open this notebook in dbc-7bca3ce8-2b59.")

print("Workspace verified. Ready to check Spotify access.")

# COMMAND ----------

def checked_json(response, operation):
    """Report safe diagnostics without printing bodies or credential headers."""
    if response.status_code != 200:
        hint = "Check Spotify availability and application configuration."
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "")
            delay = f" Wait at least {int(retry_after)} seconds." if retry_after.isdigit() else ""
            hint = "Rate or quota limit reached. Do not repeatedly rerun." + delay
        elif response.status_code == 403:
            hint = "Check app access restrictions, account eligibility, and granted scope."
        elif response.status_code == 401:
            hint = "Authentication rejected. Check app credentials and authorization."
        elif operation == "Token refresh" and response.status_code == 400:
            try:
                invalid_grant = response.json().get("error") == "invalid_grant"
            except (ValueError, AttributeError):
                invalid_grant = False
            if invalid_grant:
                hint = "Refresh token expired, revoked, or invalid. Rerun local authorization."
        raise RuntimeError(f"{operation}: HTTP {response.status_code}. {hint}")
    try:
        body = response.json()
    except ValueError:
        raise RuntimeError(f"{operation}: expected a JSON response.") from None
    if not isinstance(body, dict):
        raise RuntimeError(f"{operation}: unexpected response structure.")
    return body


def check_spotify():
    """Keep credentials local to the function; return only data and safe metadata."""
    client_id = dbutils.secrets.get(SECRET_SCOPE, "spotify-client-id")
    client_secret = dbutils.secrets.get(SECRET_SCOPE, "spotify-client-secret")
    refresh_token = dbutils.secrets.get(SECRET_SCOPE, "spotify-refresh-token")

    try:
        response = requests.post(
            "https://accounts.spotify.com/api/token",
            auth=(client_id, client_secret),
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            timeout=(10, 30), allow_redirects=False,
        )
    except requests.RequestException:
        raise RuntimeError("Token request could not complete. Check outbound HTTPS connectivity.") from None

    tokens = checked_json(response, "Token refresh")
    access_token = tokens.get("access_token")
    if not isinstance(access_token, str) or not access_token:
        raise RuntimeError("Token response did not contain an access token.")

    replacement = tokens.get("refresh_token")
    rotated = bool(replacement and replacement != refresh_token)
    if rotated:
        try:
            workspace.secrets.put_secret(
                scope=SECRET_SCOPE, key="spotify-refresh-token", string_value=replacement,
            )
        except Exception:
            raise RuntimeError(
                "Could not persist the replacement refresh token. Check scope WRITE permission; "
                "reauthorization may be needed."
            ) from None

    try:
        response = requests.get(
            "https://api.spotify.com/v1/me/player/recently-played",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"limit": 5}, timeout=(10, 30), allow_redirects=False,
        )
    except requests.RequestException:
        raise RuntimeError("Recently-played request could not complete. Check outbound HTTPS connectivity.") from None

    payload = checked_json(response, "Recently played")
    items = payload.get("items")
    if not isinstance(items, list):
        raise RuntimeError("Recently-played response is missing the expected items array.")
    if any(not isinstance(item, dict) or "played_at" not in item or "track" not in item for item in items):
        raise RuntimeError("A playback item is missing expected fields. Inspect the response schema privately.")
    return payload, tokens.get("expires_in"), rotated


# Clear any previous successful response before trying again.
spotify_payload = None
spotify_payload, token_lifetime_seconds, refresh_token_rotated = check_spotify()
print("Token refresh: HTTP 200")
print(f"Access-token lifetime: {token_lifetime_seconds} seconds")
print(f"Replacement refresh token saved: {refresh_token_rotated}")
print("Recently played: HTTP 200")
print(f"Items returned: {len(spotify_payload['items'])}")
print(f"More pages indicated: {bool(spotify_payload.get('next'))}")
print("Expected response fields verified. No S3 objects written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop here
# MAGIC A successful response with zero items is valid. This is a connectivity and
# MAGIC authorization check, not a complete history extraction or a paginated ingest.
# MAGIC The unmodified response is available in `spotify_payload` for the next step.
# MAGIC Share only the printed status summary, not credentials or token responses.
