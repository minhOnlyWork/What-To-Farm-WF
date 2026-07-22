from pathlib import Path
import json
import urllib.error
import urllib.request


API_URL = "https://api.warframe.market/v2/items"

OUTPUT_FILE = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "items.json"
)

TIMEOUT_SECONDS = 30

USER_AGENT = (
    "What-To-Farm-WF/0.1 "
    "(https://github.com/minhOnlyWork/What-To-Farm-WF)"
)

def fetch_items() -> dict:
    request = urllib.request.Request(
        API_URL,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=TIMEOUT_SECONDS,
        ) as response:
            status_code = response.getcode()
            response_body = response.read()

    except urllib.error.HTTPError as error:
        raise RuntimeError(
            f"Warframe Market returned HTTP {error.code}."
        ) from error

    except urllib.error.URLError as error:
        raise RuntimeError(
            f"Could not connect to Warframe Market: {error.reason}"
        ) from error

    except TimeoutError as error:
        raise RuntimeError(
            "Warframe Market request timed out."
        ) from error

    if status_code != 200:
        raise RuntimeError(
            f"Unexpected HTTP status code: {status_code}"
        )

    try:
        data = json.loads(response_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError(
            "Warframe Market returned invalid JSON."
        ) from error

    if not isinstance(data, dict):
        raise RuntimeError(
            "Warframe Market returned an unexpected data type."
        )

    return data
