"""
API Key Rotation Client
Wraps requests.get() with automatic key rotation.
If a request fails with 403, 429, or a network error,
it tries the next key in the pool before giving up.

Usage:
    from api_client import RotatingClient

    client = RotatingClient(
        keys=["key1", "key2", "key3"],
        header_name="x-apisports-key"   # or "Authorization" for BallDontLie
    )
    resp = client.get("https://api.example.com/endpoint", params={...})
"""

import os
import requests
import time


# Status codes that trigger key rotation
ROTATE_ON = {401, 403, 429, 500, 502, 503}

# Seconds to wait between retries on the same key (rate limit backoff)
RATE_LIMIT_WAIT = 2


class RotatingClient:

    def __init__(self, keys: list, header_name: str = "x-apisports-key",
                 bearer: bool = False, timeout: int = 10, trust_env: bool | None = None):
        """
        keys        : list of API key strings (tries in order)
        header_name : HTTP header to inject the key into
        bearer      : if True, format as "Bearer <key>" (BallDontLie style)
        timeout     : request timeout in seconds
        trust_env   : if False, ignores HTTP(S)_PROXY env vars (common cause of "all keys failed")
        """
        if not keys:
            raise ValueError("RotatingClient requires at least one API key.")

        self.keys        = [k for k in keys if k and k.strip()]
        self.header_name = header_name
        self.bearer      = bearer
        self.timeout     = timeout
        # Default: don't trust env proxies unless explicitly enabled.
        if trust_env is None:
            v = os.getenv("REQUESTS_TRUST_ENV", "").strip().lower()
            self.trust_env = True if v in ("1", "true", "yes") else False
        else:
            self.trust_env = bool(trust_env)
        self._active_idx = 0   # index of currently active key

    @property
    def active_key(self):
        return self.keys[self._active_idx]

    def _make_headers(self, key):
        value = f"Bearer {key}" if self.bearer else key
        return {self.header_name: value}

    def get(self, url: str, params: dict = None, extra_headers: dict = None) -> requests.Response:
        """
        GET request with automatic key rotation on failure.
        Tries every key once before giving up and returning the last response.
        """
        last_resp   = None
        tried_keys  = set()

        # Start from active key, rotate through all
        start_idx = self._active_idx
        indices   = list(range(start_idx, len(self.keys))) + list(range(0, start_idx))

        for idx in indices:
            key = self.keys[idx]
            if key in tried_keys:
                continue
            tried_keys.add(key)

            headers = self._make_headers(key)
            if extra_headers:
                headers.update(extra_headers)

            try:
                session = requests.Session()
                session.trust_env = self.trust_env
                resp = session.get(url, headers=headers, params=params, timeout=self.timeout)
                last_resp = resp

                if resp.status_code == 200:
                    # Stick with this key going forward
                    self._active_idx = idx
                    return resp

                elif resp.status_code == 429:
                    # Rate limited — brief wait then try next key
                    print(f"[ROTATE] Key #{idx+1} rate-limited (429). Trying next key...")
                    time.sleep(RATE_LIMIT_WAIT)
                    continue

                elif resp.status_code in (401, 403):
                    print(f"[ROTATE] Key #{idx+1} unauthorised ({resp.status_code}). Trying next key...")
                    continue

                elif resp.status_code in ROTATE_ON:
                    detail = ""
                    try:
                        if resp.text:
                            detail = resp.text.strip().replace("\n", " ")
                            if len(detail) > 140:
                                detail = detail[:140] + "..."
                            detail = f" ({detail})"
                    except Exception:
                        detail = ""
                    print(f"[ROTATE] Key #{idx+1} returned {resp.status_code}. Trying next key...{detail}")
                    continue

                else:
                    # Non-retryable error (e.g. 404) — return immediately
                    return resp

            except requests.Timeout:
                print(f"[ROTATE] Key #{idx+1} timed out. Trying next key...")
                continue
            except requests.ConnectionError as e:
                print(f"[ROTATE] Key #{idx+1} connection error: {e}. Trying next key...")
                continue
            except Exception as e:
                print(f"[ROTATE] Key #{idx+1} unexpected error: {e}. Trying next key...")
                continue

        # All keys exhausted
        print(f"[ROTATE] All {len(self.keys)} key(s) failed for {url}")
        return last_resp   # return last response so caller can inspect status code

    def status(self):
        """Print current key pool status."""
        print(f"[ROTATE] Pool: {len(self.keys)} key(s) | Active: key #{self._active_idx + 1}")
