import http.client
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


ITEMS_URL = "https://api.warframe.market/v2/items"
STATISTICS_URL = (
    "https://api.warframe.market/v1/items/"
    "{slug}/statistics"
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

ITEMS_FILE = PROJECT_ROOT / "data" / "items.json"
STATISTICS_DIR = PROJECT_ROOT / "data" / "statistics"

BATCH_SUMMARY_FILE = (
    STATISTICS_DIR / "_download_summary.json"
)

BATCH_ERROR_FILE = (
    STATISTICS_DIR / "_download_errors.jsonl"
)

TIMEOUT_SECONDS = 30
MAX_ATTEMPTS = 3
MAX_RESPONSE_BYTES = 25 * 1024 * 1024

# 0.45 seconds means no more than about 2.22 requests
# per second. This stays below the known 3 requests/second
# public API limit.
MIN_REQUEST_INTERVAL_SECONDS = 0.45

RETRYABLE_HTTP_CODES = {
    429,
    500,
    502,
    503,
    504,
}

USER_AGENT = (
    "What-To-Farm-WF/0.3 "
    "(https://github.com/minhOnlyWork/What-To-Farm-WF)"
)

# Windows and Linux both treat slash as a path separator.
# The remaining characters are invalid in Windows filenames.
INVALID_SLUG_CHARACTERS = set('<>:"/\\|?*')

WINDOWS_RESERVED_FILENAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *("COM{}".format(number) for number in range(1, 10)),
    *("LPT{}".format(number) for number in range(1, 10)),
}

_last_request_time: Optional[float] = None


class MarketDataError(RuntimeError):
    """Base exception for market data failures."""


class MarketHttpError(MarketDataError):
    """HTTP error containing its response status code."""

    def __init__(
        self,
        status_code: int,
        url: str,
    ) -> None:
        self.status_code = status_code
        self.url = url

        super().__init__(
            "Warframe.market returned HTTP {} for '{}'.".format(
                status_code,
                url,
            )
        )


def utc_now() -> str:
    """
    Return the current UTC time.

    Expected return:
    2026-07-22T15:30:00Z
    """

    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def wait_for_rate_limit() -> None:
    """
    Wait long enough to respect the request interval.

    Expected result:
    Consecutive API requests begin at least 0.45 seconds
    apart.
    """

    global _last_request_time

    now = time.monotonic()

    if _last_request_time is not None:
        elapsed = now - _last_request_time
        remaining = (
            MIN_REQUEST_INTERVAL_SECONDS - elapsed
        )

        if remaining > 0:
            time.sleep(remaining)

    _last_request_time = time.monotonic()


def get_retry_delay(
    attempt: int,
    retry_after: Optional[str],
) -> float:
    """
    Return the retry delay.

    Expected return:
    A valid Retry-After delay, otherwise 1, 2, or 4
    seconds.
    """

    if retry_after is not None:
        try:
            parsed_delay = float(retry_after)

            if 0 <= parsed_delay <= 120:
                return parsed_delay

        except ValueError:
            pass

    return float(2 ** (attempt - 1))


def build_request(
    url: str,
) -> urllib.request.Request:
    """
    Build one identifiable JSON request.

    Expected return:
    A GET request with the required headers.
    """

    return urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Language": "en",
            "User-Agent": USER_AGENT,
        },
        method="GET",
    )


def decode_json_response(
    raw_data: bytes,
) -> Dict[str, Any]:
    """
    Decode and validate a JSON response.

    Expected return:
    A non-empty JSON object.
    """

    if len(raw_data) > MAX_RESPONSE_BYTES:
        raise MarketDataError(
            "API response exceeded the 25 MB safety limit."
        )

    try:
        text = raw_data.decode("utf-8")

    except UnicodeDecodeError as error:
        raise MarketDataError(
            "API response was not valid UTF-8."
        ) from error

    try:
        document = json.loads(text)

    except json.JSONDecodeError as error:
        raise MarketDataError(
            "API returned invalid JSON."
        ) from error

    if not isinstance(document, dict):
        raise MarketDataError(
            "API response root was not a JSON object."
        )

    if not document:
        raise MarketDataError(
            "API returned an empty JSON object."
        )

    return document


def fetch_json(
    url: str,
) -> Dict[str, Any]:
    """
    Download one JSON response with retries.

    Expected return:
    A decoded, non-empty JSON object.
    """

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            wait_for_rate_limit()

            request = build_request(url)

            with urllib.request.urlopen(
                request,
                timeout=TIMEOUT_SECONDS,
            ) as response:
                raw_data = response.read(
                    MAX_RESPONSE_BYTES + 1
                )

            return decode_json_response(raw_data)

        except urllib.error.HTTPError as error:
            if (
                error.code not in RETRYABLE_HTTP_CODES
                or attempt == MAX_ATTEMPTS
            ):
                raise MarketHttpError(
                    status_code=error.code,
                    url=url,
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
            ConnectionError,
            http.client.HTTPException,
        ) as error:
            if attempt == MAX_ATTEMPTS:
                raise MarketDataError(
                    "Could not connect to Warframe.market "
                    "after {} attempts for '{}'.".format(
                        MAX_ATTEMPTS,
                        url,
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
        "API request failed unexpectedly."
    )


def save_json_atomic(
    document: Dict[str, Any],
    output_file: Path,
) -> None:
    """
    Save a JSON file atomically.

    Expected result:
    The previous file remains intact unless the complete
    new file is written successfully.
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


def read_json_file(
    input_file: Path,
) -> Dict[str, Any]:
    """
    Read one local JSON object.

    Expected return:
    A JSON object loaded from the file.
    """

    try:
        with input_file.open(
            "r",
            encoding="utf-8",
        ) as file:
            document = json.load(file)

    except FileNotFoundError as error:
        raise MarketDataError(
            "File not found: '{}'.".format(
                input_file
            )
        ) from error

    except json.JSONDecodeError as error:
        raise MarketDataError(
            "File contains invalid JSON: '{}'.".format(
                input_file
            )
        ) from error

    except OSError as error:
        raise MarketDataError(
            "Could not read '{}'.".format(
                input_file
            )
        ) from error

    if not isinstance(document, dict):
        raise MarketDataError(
            "JSON root was not an object in '{}'.".format(
                input_file
            )
        )

    return document


def validate_slug(
    raw_slug: Any,
) -> str:
    """
    Validate an API slug without assuming ASCII-only text.

    Expected return:
    The original non-empty slug, including valid Unicode,
    parentheses, hyphens, and curly apostrophes.
    """

    if not isinstance(raw_slug, str):
        raise MarketDataError(
            "Item slug was not text."
        )

    slug = raw_slug.strip()

    if not slug:
        raise MarketDataError(
            "Item slug was empty."
        )

    if slug != raw_slug:
        raise MarketDataError(
            "Item slug contains leading or trailing spaces: "
            "'{}'.".format(raw_slug)
        )

    if slug in {".", ".."}:
        raise MarketDataError(
            "Item slug cannot be '.' or '..'."
        )

    for character in slug:
        if ord(character) < 32:
            raise MarketDataError(
                "Item slug contains a control character: "
                "'{}'.".format(slug)
            )

        if character in INVALID_SLUG_CHARACTERS:
            raise MarketDataError(
                "Item slug contains an unsafe character "
                "'{}': '{}'.".format(
                    character,
                    slug,
                )
            )

    if slug.endswith((".", " ")):
        raise MarketDataError(
            "Item slug cannot end with a dot or space: "
            "'{}'.".format(slug)
        )

    filename_stem = slug.split(".", 1)[0].upper()

    if filename_stem in WINDOWS_RESERVED_FILENAMES:
        raise MarketDataError(
            "Item slug is a reserved Windows filename: "
            "'{}'.".format(slug)
        )

    return slug


def validate_catalog_items(
    items: Any,
) -> List[Dict[str, Any]]:
    """
    Validate catalog items and reject duplicate identifiers.

    Expected return:
    A non-empty list of item dictionaries.
    """

    if not isinstance(items, list) or not items:
        raise MarketDataError(
            "Item catalog is missing or empty."
        )

    validated_items: List[Dict[str, Any]] = []

    seen_ids: Set[str] = set()
    seen_slugs: Set[str] = set()

    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise MarketDataError(
                "Catalog item {} was not a JSON object.".format(
                    index
                )
            )

        item_id = item.get("id")

        if (
            not isinstance(item_id, str)
            or not item_id.strip()
        ):
            raise MarketDataError(
                "Catalog item {} has an invalid id.".format(
                    index
                )
            )

        try:
            slug = validate_slug(item.get("slug"))

        except MarketDataError as error:
            raise MarketDataError(
                "Catalog item {} has an invalid slug: {}".format(
                    index,
                    error,
                )
            ) from error

        if item_id in seen_ids:
            raise MarketDataError(
                "Duplicate catalog item id: '{}'.".format(
                    item_id
                )
            )

        if slug in seen_slugs:
            raise MarketDataError(
                "Duplicate catalog item slug: '{}'.".format(
                    slug
                )
            )

        seen_ids.add(item_id)
        seen_slugs.add(slug)

        validated_items.append(item)

    return validated_items


def validate_item_catalog_response(
    response: Dict[str, Any],
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Validate the item-catalog API response.

    Expected return:
    The API version and validated item list.
    """

    api_error = response.get("error")

    if api_error is not None:
        raise MarketDataError(
            "Item API returned an error: {!r}.".format(
                api_error
            )
        )

    api_version = response.get("apiVersion")

    if (
        not isinstance(api_version, str)
        or not api_version.strip()
    ):
        raise MarketDataError(
            "Item API returned an invalid apiVersion."
        )

    items = validate_catalog_items(
        response.get("data")
    )

    return api_version, items


def download_items() -> int:
    """
    Download and save the complete item catalog.

    Expected return:
    Number of validated catalog items.
    """

    response = fetch_json(ITEMS_URL)

    api_version, items = (
        validate_item_catalog_response(response)
    )

    document = {
        "source": ITEMS_URL,
        "downloaded_at": utc_now(),
        "api_version": api_version,
        "item_count": len(items),
        "items": items,
    }

    save_json_atomic(
        document,
        ITEMS_FILE,
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
            ITEMS_FILE
        )
    )

    return len(items)


def load_catalog_items() -> List[Dict[str, Any]]:
    """
    Load the local item catalog.

    Expected return:
    The validated list stored in data/items.json.
    """

    if not ITEMS_FILE.is_file():
        raise MarketDataError(
            "data/items.json is missing. Run "
            "'python app\\market.py items' first."
        )

    document = read_json_file(ITEMS_FILE)

    return validate_catalog_items(
        document.get("items")
    )


def find_catalog_item(
    items: List[Dict[str, Any]],
    slug: str,
) -> Dict[str, Any]:
    """
    Find one catalog item by exact slug.

    Expected return:
    The matching catalog item.
    """

    for item in items:
        if item.get("slug") == slug:
            return item

    raise MarketDataError(
        "Slug '{}' was not found in data/items.json.".format(
            slug
        )
    )


def get_item_name(
    item: Dict[str, Any],
) -> str:
    """
    Return the English item name.

    Expected return:
    English name, or the slug as a fallback.
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

    if isinstance(slug, str) and slug:
        return slug

    return "Unknown item"


def validate_statistics_response(
    response: Dict[str, Any],
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    Validate every statistics section and time window.

    The raw response is still saved without removing fields.

    Expected return:
    Validated statistics sections and their record lists.
    """

    payload = response.get("payload")

    if not isinstance(payload, dict):
        raise MarketDataError(
            "Statistics response has no valid payload."
        )

    validated_sections: Dict[
        str,
        Dict[str, List[Dict[str, Any]]],
    ] = {}

    for section_name in (
        "statistics_closed",
        "statistics_live",
    ):
        section = payload.get(section_name)

        if not isinstance(section, dict):
            raise MarketDataError(
                "Statistics response is missing '{}'.".format(
                    section_name
                )
            )

        validated_windows: Dict[
            str,
            List[Dict[str, Any]],
        ] = {}

        for window_name, records in section.items():
            if not isinstance(window_name, str):
                raise MarketDataError(
                    "Statistics window name was not text."
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
                        "Record {} in '{}.{}' was not "
                        "a JSON object.".format(
                            index,
                            section_name,
                            window_name,
                        )
                    )

                datetime_value = record.get("datetime")

                if (
                    not isinstance(datetime_value, str)
                    or not datetime_value
                ):
                    raise MarketDataError(
                        "Record {} in '{}.{}' has an "
                        "invalid datetime.".format(
                            index,
                            section_name,
                            window_name,
                        )
                    )

            validated_windows[window_name] = records

        validated_sections[section_name] = (
            validated_windows
        )

    return validated_sections


def statistics_file_for_slug(
    slug: str,
) -> Path:
    """
    Build the local statistics filename.

    Expected return:
    data/statistics/<slug>.json
    """

    safe_slug = validate_slug(slug)

    return STATISTICS_DIR / "{}.json".format(
        safe_slug
    )


def build_statistics_url(
    slug: str,
) -> str:
    """
    Build the encoded statistics endpoint.

    Expected return:
    A complete Warframe.market statistics URL.
    """

    safe_slug = validate_slug(slug)

    encoded_slug = urllib.parse.quote(
        safe_slug,
        safe="",
    )

    return STATISTICS_URL.format(
        slug=encoded_slug
    )


def download_statistics_for_item(
    item: Dict[str, Any],
) -> Tuple[
    Path,
    Dict[str, Dict[str, List[Dict[str, Any]]]],
]:
    """
    Download and save every raw statistics field for one item.

    Expected return:
    Output path and validated statistics sections.
    """

    slug = validate_slug(item.get("slug"))
    url = build_statistics_url(slug)

    response = fetch_json(url)

    sections = validate_statistics_response(
        response
    )

    output_file = statistics_file_for_slug(slug)

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

    save_json_atomic(
        document,
        output_file,
    )

    return output_file, sections


def download_statistics(
    raw_slug: str,
) -> None:
    """
    Download statistics for one selected item.
    """

    slug = validate_slug(raw_slug)

    items = load_catalog_items()

    item = find_catalog_item(
        items,
        slug,
    )

    output_file, sections = (
        download_statistics_for_item(item)
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


def saved_statistics_are_valid(
    output_file: Path,
    expected_slug: str,
) -> bool:
    """
    Check whether a previously saved statistics file can
    safely be skipped.

    Expected return:
    True only when the file is readable, matches the slug,
    and contains valid statistics sections.
    """

    if not output_file.is_file():
        return False

    try:
        document = read_json_file(output_file)

        item = document.get("item")

        if not isinstance(item, dict):
            return False

        if item.get("slug") != expected_slug:
            return False

        raw_response = document.get("raw_response")

        if not isinstance(raw_response, dict):
            return False

        validate_statistics_response(raw_response)

        return True

    except MarketDataError:
        return False


def reset_batch_error_log() -> None:
    """
    Remove the previous batch error log.

    Expected result:
    A new batch starts with an empty error log.
    """

    STATISTICS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    try:
        if BATCH_ERROR_FILE.exists():
            BATCH_ERROR_FILE.unlink()

    except OSError as error:
        raise MarketDataError(
            "Could not reset '{}'.".format(
                BATCH_ERROR_FILE
            )
        ) from error


def append_batch_error(
    position: int,
    total: int,
    slug: str,
    error: Exception,
) -> None:
    """
    Append one failed item to the JSON Lines error log.

    Expected result:
    One independent JSON object is appended.
    """

    record = {
        "recorded_at": utc_now(),
        "position": position,
        "total": total,
        "slug": slug,
        "error_type": type(error).__name__,
        "error": str(error),
    }

    if isinstance(error, MarketHttpError):
        record["http_status"] = error.status_code

    try:
        BATCH_ERROR_FILE.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with BATCH_ERROR_FILE.open(
            "a",
            encoding="utf-8",
            newline="\n",
        ) as file:
            file.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                )
            )
            file.write("\n")
            file.flush()

    except OSError as log_error:
        raise MarketDataError(
            "Could not write batch error log."
        ) from log_error


def save_batch_summary(
    started_at: str,
    status: str,
    total_items: int,
    downloaded: int,
    skipped: int,
    failed: int,
    force_refresh: bool,
) -> None:
    """
    Save the current all-item download status.

    Expected result:
    A summary JSON file that survives interruptions.
    """

    summary = {
        "started_at": started_at,
        "updated_at": utc_now(),
        "status": status,
        "total_items": total_items,
        "processed": downloaded + skipped + failed,
        "downloaded": downloaded,
        "skipped": skipped,
        "failed": failed,
        "force_refresh": force_refresh,
        "error_log": (
            str(BATCH_ERROR_FILE)
            if failed > 0
            else None
        ),
    }

    save_json_atomic(
        summary,
        BATCH_SUMMARY_FILE,
    )


def save_running_summary_if_needed(
    position: int,
    started_at: str,
    total_items: int,
    downloaded: int,
    skipped: int,
    failed: int,
    force_refresh: bool,
) -> None:
    """
    Save batch progress after every 25 catalog positions.

    Expected result:
    The summary is updated at positions 25, 50, 75, etc.
    """

    if position % 25 != 0:
        return

    save_batch_summary(
        started_at=started_at,
        status="running",
        total_items=total_items,
        downloaded=downloaded,
        skipped=skipped,
        failed=failed,
        force_refresh=force_refresh,
    )


def download_all_statistics(
    force_refresh: bool,
) -> int:
    """
    Download statistics for every item in the catalog.

    Existing valid files are skipped unless --force is used.

    Expected return:
    Number of failed items.
    """

    items = load_catalog_items()

    total_items = len(items)
    started_at = utc_now()

    downloaded = 0
    skipped = 0
    failed = 0

    reset_batch_error_log()

    save_batch_summary(
        started_at=started_at,
        status="running",
        total_items=total_items,
        downloaded=downloaded,
        skipped=skipped,
        failed=failed,
        force_refresh=force_refresh,
    )

    print(
        "[START] Downloading statistics for "
        "{} items.".format(total_items)
    )

    if force_refresh:
        print(
            "[START] Force refresh is enabled."
        )
    else:
        print(
            "[START] Valid existing files will be skipped."
        )

    try:
        for position, item in enumerate(
            items,
            start=1,
        ):
            raw_slug = item.get("slug")

            try:
                slug = validate_slug(raw_slug)

            except MarketDataError as error:
                failed += 1

                append_batch_error(
                    position=position,
                    total=total_items,
                    slug=str(raw_slug),
                    error=error,
                )

                print(
                    "[{}/{}] [ERROR] Invalid catalog item: "
                    "{}".format(
                        position,
                        total_items,
                        error,
                    )
                )

                save_running_summary_if_needed(
                    position=position,
                    started_at=started_at,
                    total_items=total_items,
                    downloaded=downloaded,
                    skipped=skipped,
                    failed=failed,
                    force_refresh=force_refresh,
                )

                continue

            output_file = statistics_file_for_slug(
                slug
            )

            if (
                not force_refresh
                and saved_statistics_are_valid(
                    output_file,
                    slug,
                )
            ):
                skipped += 1

                print(
                    "[{}/{}] [SKIP] {}".format(
                        position,
                        total_items,
                        slug,
                    )
                )

                save_running_summary_if_needed(
                    position=position,
                    started_at=started_at,
                    total_items=total_items,
                    downloaded=downloaded,
                    skipped=skipped,
                    failed=failed,
                    force_refresh=force_refresh,
                )

                continue

            if (
                output_file.exists()
                and not force_refresh
            ):
                print(
                    "[{}/{}] [REDOWNLOAD] {} "
                    "has an invalid local file.".format(
                        position,
                        total_items,
                        slug,
                    )
                )

            try:
                download_statistics_for_item(item)

                downloaded += 1

                print(
                    "[{}/{}] [OK] {}".format(
                        position,
                        total_items,
                        slug,
                    )
                )

            except MarketDataError as error:
                failed += 1

                append_batch_error(
                    position=position,
                    total=total_items,
                    slug=slug,
                    error=error,
                )

                print(
                    "[{}/{}] [ERROR] {}: {}".format(
                        position,
                        total_items,
                        slug,
                        error,
                    )
                )

            save_running_summary_if_needed(
                position=position,
                started_at=started_at,
                total_items=total_items,
                downloaded=downloaded,
                skipped=skipped,
                failed=failed,
                force_refresh=force_refresh,
            )

    except KeyboardInterrupt:
        save_batch_summary(
            started_at=started_at,
            status="interrupted",
            total_items=total_items,
            downloaded=downloaded,
            skipped=skipped,
            failed=failed,
            force_refresh=force_refresh,
        )

        raise

    save_batch_summary(
        started_at=started_at,
        status="completed",
        total_items=total_items,
        downloaded=downloaded,
        skipped=skipped,
        failed=failed,
        force_refresh=force_refresh,
    )

    print()
    print("[COMPLETE] Batch download finished.")
    print(
        "[COMPLETE] Downloaded: {}".format(
            downloaded
        )
    )
    print(
        "[COMPLETE] Skipped: {}".format(
            skipped
        )
    )
    print(
        "[COMPLETE] Failed: {}".format(
            failed
        )
    )
    print(
        "[COMPLETE] Summary: {}".format(
            BATCH_SUMMARY_FILE
        )
    )

    if failed > 0:
        print(
            "[COMPLETE] Error log: {}".format(
                BATCH_ERROR_FILE
            )
        )

    return failed


def print_usage() -> None:
    """Print all supported commands."""

    print("Usage:")
    print(r"  python app\market.py items")
    print(r"  python app\market.py stats <item_slug>")
    print(r"  python app\market.py all-stats")
    print(r"  python app\market.py all-stats --force")
    print()
    print("Examples:")
    print(
        r"  python app\market.py "
        r"stats secura_dual_cestra"
    )
    print(
        r"  python app\market.py all-stats"
    )


def main() -> int:
    """
    Run one market-data command.

    Expected return:
    0 for success and non-zero for failure.
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

        if (
            len(sys.argv) == 2
            and sys.argv[1] == "all-stats"
        ):
            failure_count = download_all_statistics(
                force_refresh=False
            )

            return 1 if failure_count > 0 else 0

        if (
            len(sys.argv) == 3
            and sys.argv[1] == "all-stats"
            and sys.argv[2] == "--force"
        ):
            failure_count = download_all_statistics(
                force_refresh=True
            )

            return 1 if failure_count > 0 else 0

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
            "\n[CANCELLED] Operation cancelled by user.",
            file=sys.stderr,
        )
        return 130


if __name__ == "__main__":
    sys.exit(main())
