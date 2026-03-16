"""
Telegram Sender — with retry on timeout/connection error.
"""

import time
import requests


class TelegramSender:
    def __init__(self, token: str, chat_id: str):
        self.token    = token
        self.chat_id  = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"

    def send_message(self, text: str, parse_mode: str = "Markdown",
                     retries: int = 3, delay: int = 5) -> bool:
        """Send message with automatic retry on timeout."""
        url     = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id":                  self.chat_id,
            "text":                     text,
            "parse_mode":               parse_mode,
            "disable_web_page_preview": True,
        }
        for attempt in range(1, retries + 1):
            try:
                resp = requests.post(url, json=payload, timeout=15)
                if resp.status_code == 200:
                    print("[TELEGRAM] Message sent successfully.")
                    return True
                elif resp.status_code == 429:
                    # Rate limited — wait longer
                    wait = int(resp.json().get("parameters", {}).get("retry_after", 30))
                    print(f"[TELEGRAM] Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                elif resp.status_code == 400:
                    # Bad request — usually Markdown parse error, retry as plain text
                    print(f"[TELEGRAM] Bad request (400) — retrying as plain text.")
                    payload["parse_mode"] = ""
                else:
                    print(f"[TELEGRAM ERROR] HTTP {resp.status_code}: {resp.text[:100]}")
            except requests.Timeout:
                print(f"[TELEGRAM] Timeout on attempt {attempt}/{retries}. Retrying in {delay}s...")
                time.sleep(delay)
            except requests.ConnectionError as e:
                print(f"[TELEGRAM] Connection error attempt {attempt}/{retries}: {e}")
                time.sleep(delay)
            except Exception as e:
                print(f"[TELEGRAM] Unexpected error: {e}")
                time.sleep(delay)

        print(f"[TELEGRAM] Failed after {retries} attempts — message dropped.")
        return False

    def test_connection(self) -> bool:
        """Test bot token is valid."""
        for attempt in range(3):
            try:
                resp = requests.get(f"{self.base_url}/getMe", timeout=10)
                if resp.status_code == 200:
                    name = resp.json().get("result", {}).get("username", "?")
                    print(f"[TELEGRAM] Connected as @{name}")
                    return True
            except Exception as e:
                print(f"[TELEGRAM] Connection test attempt {attempt+1}/3: {e}")
                time.sleep(3)
        print("[TELEGRAM] Could not connect after 3 attempts.")
        return False