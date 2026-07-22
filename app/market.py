import json
import os
import re
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


ITEMS_URL = "https://api.warframe.market/v2/items"
STATS_URL = "https://api.warframe.market/v1/items/{slug}/statistics"

ROOT = Path(__file__).resolve().parent.parent
ITEMS_FILE = ROOT / "data" / "items.json"
STATS_DIR = ROOT / "data" / "statistics"

TIMEOUT_SECONDS = 30
MAX_ATTEMPTS = 3
MAX_RESPONSE_BYTES = 25 * 1024 * 1024

RETRYABLE_CODES = {
    429,
    500,
    502,
    503,
    504,
}

USER_AGENT = (
    "What-To-Farm-WF/0.1 "
    "(https://github.com/minhOnlyWork/What-To-Farm-WF)"
)

SLUG_PATTERN = re.compile(r"^[a-z0-9_]+$")


class MarketDataError(RuntimeError):
    pass


def utc_now() -> str:
    """
    Expected return:
    2026-07-22T14:30:00Z
    """

    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def retry_delay(
    attempt: int,
    retry_after: Optional[str],
) -> float:
    """
    Expected return:
    Retry-After when valid, otherwise 1, 2, then 4 seconds.
    """

    if retry_after is not None:
        try:
            value = float(retry_after)

            if 0 <= value <= 60:
                return value

        except ValueError:
            pass

    return float(2 ** (attempt - 1))


def fetch_json(url: str) -> Dict[str, Any]:
    """
    Expected return:
    A non-empty JSON object from the requested API URL.
    """

    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Language": "en",
            "User-Agent": USER_AGENT,
        },
        method="GET",
    )

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(
                request,
                timeout=TIMEOUT_SECONDS,
            ) as response:
                raw = response.read(
                    MAX_RESPONSE_BYTES + 1
                )

            if len(raw) > MAX_RESPONSE_BYTES:
                raise MarketDataError(
                    "API response exceeded the 25 MB "
                    "safety limit."
                )

            try:
                decoded = raw.decode("utf-8")

            except UnicodeDecodeError as error:
                raise MarketDataError(
                    "API response was not valid UTF-8."
                ) from error

            try:
                result = json.loads(decoded)

            except json.JSONDecodeError as error:
                raise MarketDataError(
                    "API returned invalid JSON."
                ) from error

            if not isinstance(result, dict):
                raise MarketDataError(
                    "API response root was not "
                    "a JSON object."
                )

            if not result:
                raise MarketDataError(
                    "API returned an empty JSON object."
                )

            return result

        except urllib.error.HTTPError as error:
            if (
                error.code not in RETRYABLE_CODES
                or attempt == MAX_ATTEMPTS
            ):
                raise MarketDataError(
                    "Warframe.market returned HTTP {} "
                    "for '{}'.".format(
                        error.code,
                        url,
                    )
                ) from error

            retry_after = None

            if error.headers is not None:
                retry_after = error.headers.get(
                    "Retry-After"
                )

            delay = retry_delay(
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
                    "Could not connect after {} "
                    "attempts.".format(
                        MAX_ATTEMPTS
                    )
                ) from error

            delay = retry_delay(
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
        "API request failed unexpectedly."
    )


def save_json(
    document: Dict[str, Any],
    output_file: Path,
) -> None:
    """
    Expected result:
    The complete JSON document is saved atomically.
    """

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

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
            "Could not save '{}'.".format(
                output_file
            )
        ) from error


def validate_item_catalog(
    response: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Expected return:
    A validated, non-empty item list.
    """

    api_error = response.get("error")

    if api_error is not None:
        raise MarketDataError(
            "Item API returned an error: {!r}".format(
                api_error
            )
        )

    api_version = response.get("apiVersion")
    items = response.get("data")

    if (
        not isinstance(api_version, str)
        or not api_version
    ):
        raise MarketDataError(
            "Missing or invalid API version."
        )

    if not isinstance(items, list) or not items:
        raise MarketDataError(
            "Missing or empty item list."
        )

    seen_ids = set()
    seen_slugs = set()

    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise MarketDataError(
                "Item {} was not a JSON object.".format(
                    index
                )
            )

        item_id = item.get("id")
        slug = item.get("slug")

        if (
            not isinstance(item_id, str)
            or not item_id
        ):
            raise MarketDataError(
                "Item {} has an invalid id.".format(
                    index
                )
            )

        if (
            not isinstance(slug, str)
            or not SLUG_PATTERN.fullmatch(slug)
        ):
            raise MarketDataError(
                "Item {} has an invalid slug.".format(
                    index
                )
            )

        if item_id in seen_ids:
            raise MarketDataError(
                "Duplicate item id: {}".format(
                    item_id
                )
            )

        if slug in seen_slugs:
            raise MarketDataError(
                "Duplicate item slug: {}".format(
                    slug
                )
            )

        seen_ids.add(item_id)
        seen_slugs.add(slug)

    return items


def download_items() -> None:
    """
    Download and save the complete tradable item catalog.
    """

    response = fetch_json(ITEMS_URL)
    items = validate_item_catalog(response)

    document = {
        "source": ITEMS_URL,
        "downloaded_at": utc_now(),
        "api_version": response["apiVersion"],
        "item_count": len(items),
        "items": items,
    }

    save_json(
        document,
        ITEMS_FILE,
    )

    print(
        "[SUCCESS] Item catalog downloaded "
        "and validated."
    )

    print(
        "[SUCCESS] API version: {}".format(
            response["apiVersion"]
        )
    )

    print(
        "[SUCCESS] Item count: {}".format(
            len(items)
        )
    )

    print(
        "[SUCCESS] Saved to: {}".format(
            ITEMS_FILE
        )
    )


def load_item(
    slug: str,
) -> Dict[str, Any]:
    """
    Expected return:
    The catalog item whose slug exactly matches.
    """

    if not ITEMS_FILE.is_file():
        raise MarketDataError(
            "data/items.json is missing. Run "
            "'python app\\market.py items' first."
        )

    try:
        with ITEMS_FILE.open(
            "r",
            encoding="utf-8",
        ) as file:
            document = json.load(file)

    except json.JSONDecodeError as error:
        raise MarketDataError(
            "data/items.json contains invalid JSON."
        ) from error

    except OSError as error:
        raise MarketDataError(
            "Could not read data/items.json."
        ) from error

    if not isinstance(document, dict):
        raise MarketDataError(
            "data/items.json root is not "
            "a JSON object."
        )

    items = document.get("items")

    if not isinstance(items, list):
        raise MarketDataError(
            "data/items.json has no valid "
            "items list."
        )

    for item in items:
        if (
            isinstance(item, dict)
            and item.get("slug") == slug
        ):
            return item

    raise MarketDataError(
        "Slug '{}' was not found in "
        "data/items.json.".format(slug)
    )


def get_item_name(
    item: Dict[str, Any],
) -> str:
    """
    Expected return:
    English item name or its slug as fallback.
    """

    i18n = item.get("i18n")

    if isinstance(i18n, dict):
        english = i18n.get("en")

        if isinstance(english, dict):
            name = english.get("name")

            if (
                isinstance(name, str)
                and name.strip()
            ):
                return name.strip()

    slug = item.get("slug")

    if isinstance(slug, str):
        return slug

    return "Unknown item"


def validate_statistics(
    response: Dict[str, Any],
) -> Dict[
    str,
    Dict[str, List[Dict[str, Any]]],
]:
    """
    Validate both statistics sections and every returned
    time window.

    The original API response is still saved unchanged.
    """

    payload = response.get("payload")

    if not isinstance(payload, dict):
        raise MarketDataError(
            "Statistics response has no valid "
            "payload object."
        )

    validated = {}

    for section_name in (
        "statistics_closed",
        "statistics_live",
    ):
        section = payload.get(section_name)

        if not isinstance(section, dict):
            raise MarketDataError(
                "Missing or invalid '{}'.".format(
                    section_name
                )
            )

        windows = {}

        for window_name, records in section.items():
            if not isinstance(window_name, str):
                raise MarketDataError(
                    "A statistics window name "
                    "was not text."
                )

            if not isinstance(records, list):
                raise MarketDataError(
                    "'{}.{}' was not a list.".format(
                        section_name,
                        window_name,
                    )
                )

            for index, record in enumerate(records):
                if not isinstance(record, dict):
                    raise MarketDataError(
                        "Record {} in '{}.{}' was "
                        "not a JSON object.".format(
                            index,
                            section_name,
                            window_name,
                        )
                    )

            windows[window_name] = records

        validated[section_name] = windows

    if not any(validated.values()):
        raise MarketDataError(
            "Statistics response contained "
            "no time windows."
        )

    return validated


def download_statistics(
    raw_slug: str,
) -> None:
    """
    Download every field returned for one item's
    Statistics page.
    """

    slug = raw_slug.strip().lower()

    if not SLUG_PATTERN.fullmatch(slug):
        raise MarketDataError(
            "Invalid slug '{}'. Use lowercase "
            "letters, numbers, and underscores "
            "only.".format(raw_slug)
        )

    item = load_item(slug)

    encoded_slug = urllib.parse.quote(
        slug,
        safe="",
    )

    url = STATS_URL.format(
        slug=encoded_slug
    )

    response = fetch_json(url)

    sections = validate_statistics(
        response
    )

    output_file = (
        STATS_DIR
        / "{}.json".format(slug)
    )

    document = {
        "source": url,
        "downloaded_at": utc_now(),
        "item": {
            "id": item.get("id"),
            "slug": slug,
            "name": get_item_name(item),
            "tags": item.get("tags", []),
        },
        "raw_response": response,
    }

    save_json(
        document,
        output_file,
    )

    print(
        "[SUCCESS] Statistics downloaded "
        "and validated."
    )

    print(
        "[SUCCESS] Item: {}".format(
            get_item_name(item)
        )
    )

    print(
        "[SUCCESS] Slug: {}".format(
            slug
        )
    )

    for section_name, windows in sections.items():
        if not windows:
            print(
                "[SUCCESS] {}: no windows".format(
                    section_name
                )
            )

        for window_name, records in windows.items():
            print(
                "[SUCCESS] {}.{}: {} records".format(
                    section_name,
                    window_name,
                    len(records),
                )
            )

    print(
        "[SUCCESS] Saved to: {}".format(
            output_file
        )
    )


def print_usage() -> None:
    print("Usage:")
    print(r"  python app\market.py items")
    print(r"  python app\market.py stats <item_slug>")
    print()
    print("Example:")
    print(
        r"  python app\market.py "
        r"stats secura_dual_cestra"
    )


def main() -> int:
    """
    Run one supported command.

    Expected return:
    0 for success, non-zero for an error.
    """

    try:
        if (
            len(sys.argv) == 2
            and sys.argv[1] == "items"
        ):
            download_items()
            return 0

        if (
            len(sys.argv) == 3
            and sys.argv[1] == "stats"
        ):
            download_statistics(
                sys.argv[2]
            )

            return 0

        print_usage()
        return 2

    except MarketDataError as error:
        print(
            "[ERROR] {}".format(error),
            file=sys.stderr,
        )

        return 1

    except KeyboardInterrupt:
        print(
            "\n[CANCELLED] Operation "
            "cancelled by user.",
            file=sys.stderr,
        )

        return 130


if __name__ == "__main__":
    sys.exit(main())
