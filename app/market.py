import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


API_URL = "https://api.warframe.market/v2/items"

TIMEOUT_SECONDS = 30
MAX_ATTEMPTS = 3
MAX_RESPONSE_BYTES = 25 * 1024 * 1024

RETRYABLE_STATUS_CODES = {
    429,
    500,
    502,
    503,
    504,
}

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_FILE = PROJECT_ROOT / "data" / "items.json"

USER_AGENT = (
    "What-To-Farm-WF/0.1 "
    "(https://github.com/minhOnlyWork/What-To-Farm-WF)"
)


class MarketDataError(RuntimeError):
    """Raised when market data cannot be downloaded or saved."""


def build_request() -> urllib.request.Request:
    """Create the request for the complete tradable item catalog."""

    return urllib.request.Request(
        API_URL,
        headers={
            "Accept": "application/json",
            "Language": "en",
            "User-Agent": USER_AGENT,
        },
        method="GET",
    )


def get_retry_delay(
    attempt: int,
    retry_after: Optional[str],
) -> float:
    """
    Return the delay before another request.

    Expected return:
    - Retry-After value when it is valid.
    - Otherwise 1, 2, then 4 seconds.
    """

    if retry_after is not None:
        try:
            delay = float(retry_after)

            if 0 <= delay <= 60:
                return delay

        except ValueError:
            pass

    return float(2 ** (attempt - 1))


def fetch_items_response() -> Dict[str, Any]:
    """
    Download and decode the Warframe.market item response.

    Expected return:
    {
        "apiVersion": "...",
        "data": [...],
        "error": None
    }
    """

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            request = build_request()

            with urllib.request.urlopen(
                request,
                timeout=TIMEOUT_SECONDS,
            ) as response:
                raw_data = response.read(
                    MAX_RESPONSE_BYTES + 1
                )

            if len(raw_data) > MAX_RESPONSE_BYTES:
                raise MarketDataError(
                    "The API response exceeded the 25 MB "
                    "safety limit."
                )

            try:
                decoded_data = raw_data.decode("utf-8")

            except UnicodeDecodeError as error:
                raise MarketDataError(
                    "The API response was not valid UTF-8."
                ) from error

            try:
                payload = json.loads(decoded_data)

            except json.JSONDecodeError as error:
                raise MarketDataError(
                    "The API returned invalid JSON."
                ) from error

            if not isinstance(payload, dict):
                raise MarketDataError(
                    "The API response root was not "
                    "a JSON object."
                )

            return payload

        except urllib.error.HTTPError as error:
            if (
                error.code not in RETRYABLE_STATUS_CODES
                or attempt == MAX_ATTEMPTS
            ):
                raise MarketDataError(
                    "Warframe.market returned HTTP {}.".format(
                        error.code
                    )
                ) from error

            retry_after = None

            if error.headers is not None:
                retry_after = error.headers.get(
                    "Retry-After"
                )

            delay = get_retry_delay(
                attempt,
                retry_after,
            )

        except (
            urllib.error.URLError,
            TimeoutError,
            socket.timeout,
        ) as error:
            if attempt == MAX_ATTEMPTS:
                raise MarketDataError(
                    "Could not connect to Warframe.market "
                    "after {} attempts.".format(
                        MAX_ATTEMPTS
                    )
                ) from error

            delay = get_retry_delay(
                attempt,
                None,
            )

        print(
            "[RETRY] Attempt {} failed. "
            "Retrying in {} seconds...".format(
                attempt,
                format(delay, "g"),
            )
        )

        time.sleep(delay)

    raise MarketDataError(
        "The API request failed unexpectedly."
    )


def validate_items_response(
    payload: Dict[str, Any],
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Validate the API response and every returned item.

    Expected return:
    (
        API version,
        validated list of items
    )
    """

    api_error = payload.get("error")

    if api_error is not None:
        raise MarketDataError(
            "The API returned an error payload: {!r}".format(
                api_error
            )
        )

    api_version = payload.get("apiVersion")

    if (
        not isinstance(api_version, str)
        or not api_version.strip()
    ):
        raise MarketDataError(
            "The response has a missing or invalid "
            "'apiVersion'."
        )

    items = payload.get("data")

    if not isinstance(items, list):
        raise MarketDataError(
            "The response field 'data' was not a list."
        )

    if not items:
        raise MarketDataError(
            "The API returned an empty item list."
        )

    seen_ids: Set[str] = set()
    seen_slugs: Set[str] = set()

    validated_items: List[Dict[str, Any]] = []

    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise MarketDataError(
                "Item at index {} was not "
                "a JSON object.".format(index)
            )

        item_id = item.get("id")
        slug = item.get("slug")

        if (
            not isinstance(item_id, str)
            or not item_id.strip()
        ):
            raise MarketDataError(
                "Item at index {} has an "
                "invalid 'id'.".format(index)
            )

        if (
            not isinstance(slug, str)
            or not slug.strip()
        ):
            raise MarketDataError(
                "Item at index {} has an "
                "invalid 'slug'.".format(index)
            )

        if item_id in seen_ids:
            raise MarketDataError(
                "Duplicate item id found: {}".format(
                    item_id
                )
            )

        if slug in seen_slugs:
            raise MarketDataError(
                "Duplicate item slug found: {}".format(
                    slug
                )
            )

        seen_ids.add(item_id)
        seen_slugs.add(slug)

        validated_items.append(item)

    return api_version, validated_items


def current_utc_time() -> str:
    """
    Return a UTC timestamp.

    Expected return example:
    2026-07-22T13:30:00Z
    """

    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def save_items(
    api_version: str,
    items: List[Dict[str, Any]],
    output_file: Path = OUTPUT_FILE,
) -> None:
    """
    Save item data atomically.

    The temporary file is replaced only after the complete
    JSON document has been written successfully.
    """

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    document = {
        "source": API_URL,
        "downloaded_at": current_utc_time(),
        "api_version": api_version,
        "item_count": len(items),
        "items": items,
    }

    temporary_file = output_file.with_suffix(
        output_file.suffix + ".tmp"
    )

    try:
        with temporary_file.open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as file:
            json.dump(
                document,
                file,
                ensure_ascii=False,
                indent=2,
            )

            file.write("\n")
            file.flush()

            os.fsync(file.fileno())

        os.replace(
            str(temporary_file),
            str(output_file),
        )

    except OSError as error:
        try:
            if temporary_file.exists():
                temporary_file.unlink()

        except OSError:
            pass

        raise MarketDataError(
            "Could not save data to '{}'.".format(
                output_file
            )
        ) from error


def download_and_save_items() -> int:
    """
    Download, validate, and save the item catalog.

    Expected return:
    Number of validated items.
    """

    payload = fetch_items_response()

    api_version, items = validate_items_response(
        payload
    )

    save_items(
        api_version,
        items,
    )

    print(
        "[SUCCESS] Item catalog downloaded "
        "and validated."
    )

    print(
        "[SUCCESS] API version: {}".format(
            api_version
        )
    )

    print(
        "[SUCCESS] Item count: {}".format(
            len(items)
        )
    )

    print(
        "[SUCCESS] Saved to: {}".format(
            OUTPUT_FILE
        )
    )

    return len(items)


def main() -> int:
    """Run the item catalog collector."""

    try:
        download_and_save_items()
        return 0

    except MarketDataError as error:
        print(
            "[ERROR] {}".format(error),
            file=sys.stderr,
        )

        return 1

    except KeyboardInterrupt:
        print(
            "\n[CANCELLED] Download cancelled by user.",
            file=sys.stderr,
        )

        return 130


if __name__ == "__main__":
    sys.exit(main())
