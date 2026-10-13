"""Run locally to authorize Spotify and save its refresh token in Databricks.

Uses Python's standard library and an authenticated modern Databricks CLI.
No tokens are printed or written to local files. This does not ingest data.
"""

import base64
import json
import os
import secrets
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer


PROFILE = "dbx-workspace"
HOST = "https://dbc-7bca3ce8-2b59.cloud.databricks.com"
SCOPE = "spotify-secrets"
REDIRECT_URI = "http://127.0.0.1:8888/callback"
PERMISSION = "user-read-recently-played"


def cli(*args, input_text=None):
    # Avoid inherited credentials and the template bundle's different host.
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith("DATABRICKS_") or key == "DATABRICKS_CONFIG_FILE"
    }
    env.update(DATABRICKS_HOST=HOST, DATABRICKS_AUTH_TYPE="databricks-cli")
    result = subprocess.run(
        ["databricks", *args, "--profile", PROFILE],
        input=input_text, capture_output=True, text=True, encoding="utf-8",
        env=env, cwd=tempfile.gettempdir(), timeout=90,
    )
    if result.returncode:
        # CLI output may contain sensitive data; deliberately do not echo it.
        raise RuntimeError(
            "Databricks operation failed. Check profile login and scope READ/WRITE "
            "permissions. CLI output suppressed to protect credentials."
        )
    return result.stdout


def read_secret(key):
    response = json.loads(cli("secrets", "get-secret", SCOPE, key, "--output", "json"))
    value = base64.b64decode(response["value"]).decode("utf-8").strip()
    if not value:
        raise RuntimeError("A required app credential is empty.")
    return value


def main():
    print(f"Workspace: {HOST}\nSecret scope: {SCOPE}")
    client_id = read_secret("spotify-client-id")
    client_secret = read_secret("spotify-client-secret")
    state = secrets.token_urlsafe(32)
    outcome = {}

    class Callback(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass  # Never log the callback URL containing the authorization code.

        def do_GET(self):
            parsed = urllib.parse.urlsplit(self.path)
            params = urllib.parse.parse_qs(parsed.query)
            status, message = 400, "Invalid callback. Return to the terminal."
            if parsed.path != "/callback":
                status, message = 404, "Not found."
            elif not secrets.compare_digest(params.get("state", [""])[0], state):
                message = "State validation failed. Authorization was not accepted."
            elif "error" in params:
                outcome["error"] = "Spotify authorization was declined or failed."
                message = outcome["error"]
            elif params.get("code"):
                outcome["code"] = params["code"][0]
                status, message = 200, "Authorization received. Check the terminal for token storage status."
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Referrer-Policy", "no-referrer")
            self.end_headers()
            self.wfile.write(message.encode("utf-8"))

    url = "https://accounts.spotify.com/authorize?" + urllib.parse.urlencode({
        "client_id": client_id, "response_type": "code",
        "redirect_uri": REDIRECT_URI, "scope": PERMISSION,
        "state": state, "show_dialog": "true",
    })
    with HTTPServer(("127.0.0.1", 8888), Callback) as server:
        server.timeout = 1
        print("Have the account owner approve access in the browser on THIS computer.")
        print("If no browser opens, open this authorization URL manually:\n" + url)
        webbrowser.open(url)
        deadline = time.monotonic() + 300
        while not outcome and time.monotonic() < deadline:
            server.handle_request()
    if "code" not in outcome:
        raise RuntimeError(outcome.get("error", "Authorization timed out after five minutes. Rerun to retry."))

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    request = urllib.request.Request(
        "https://accounts.spotify.com/api/token",
        data=urllib.parse.urlencode({
            "grant_type": "authorization_code", "code": outcome["code"],
            "redirect_uri": REDIRECT_URI,
        }).encode(),
        headers={"Authorization": "Basic " + basic,
                 "Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            tokens = json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"Spotify token exchange failed (HTTP {error.code}). Rerun authorization after checking app settings.") from None
    if PERMISSION not in tokens.get("scope", "").split():
        raise RuntimeError("Spotify did not grant the required listening-history permission.")
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        raise RuntimeError("Spotify returned no refresh token. Rerun authorization.")
    # Pass the secret through stdin, never command-line arguments or a local file.
    cli("secrets", "put-secret", SCOPE, "spotify-refresh-token", input_text=refresh_token)
    saved = read_secret("spotify-refresh-token")
    if not secrets.compare_digest(saved, refresh_token):
        raise RuntimeError("Refresh-token storage verification failed. Rerun authorization.")
    print("Success: spotify-refresh-token saved and verified in spotify-secrets.")
    print("No Spotify listening data was fetched. No S3 objects were written.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Authorization cancelled.")
        raise SystemExit(1)
    except Exception as error:
        # Do not print arbitrary exception details that could contain credentials.
        print(str(error) if isinstance(error, RuntimeError) else
              f"Operation failed ({type(error).__name__}); check CLI availability, network, and port 8888.")
        raise SystemExit(1)
