import http.client
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
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

SCHEMA_VERSION = 2

TIMEOUT_SECONDS = 30
MAX_ATTEMPTS = 3
MAX_RESPONSE_BYTES = 25 * 1024 * 1024

MIN_REQUEST_INTERVAL_SECONDS = 0.45

FRESHNESS_DURATION = timedelta(hours=24)
CLOCK_SKEW_TOLERANCE = timedelta(minutes=5)

ROLLING_HISTORY_DURATION = timedelta(days=90)

HISTORY_WINDOW = "90days"
LATEST_WINDOW = "48hours"

RETRYABLE_HTTP_CODES = {
    429,
    500,
    502,
    503,
    504,
}

USER_AGENT = (
    "What-To-Farm-WF/0.4 "
    "(https://github.com/minhOnlyWork/What-To-Farm-WF)"
)

INVALID_SLUG_CHARACTERS = set('<>:"/\\|?*')

WINDOWS_RESERVED_FILENAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(
        "COM{}".format(number)
        for number in range(1, 10)
    ),
    *(
        "LPT{}".format(number)
        for number in range(1, 10)
    ),
}

STATISTICS_SECTIONS = (
    "statistics_closed",
    "statistics_live",
)

_last_request_time: Optional[float] = None


class MarketDataError(RuntimeError):
    """Base exception for market data errors."""


class MarketHttpError(MarketDataError):
    """HTTP error containing the status code."""

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


@dataclass
class FetchResult:
    """
    Result returned by one HTTP request.

    Expected values:
    - document contains JSON for HTTP 200.
    - not_modified is True for HTTP 304.
    """

    document: Optional[Dict[str, Any]]
    not_modified: bool
    etag: Optional[str]
    last_modified: Optional[str]


@dataclass
class RefreshResult:
    """
    Result returned by one item refresh.

    Expected status:
    - downloaded
    - not_modified
    """

    output_file: Path
    sections: Dict[
        str,
        Dict[str, List[Dict[str, Any]]],
    ]
    status: str


def utc_now_datetime() -> datetime:
    """
    Return the current UTC datetime.

    Expected return:
    A timezone-aware datetime in UTC.
    """

    return datetime.now(timezone.utc)


def format_utc(
    value: datetime,
) -> str:
    """
    Convert a datetime to UTC ISO-8601 text.

    Expected return:
    2026-07-22T15:30:00Z
    """

    if value.tzinfo is None:
        raise MarketDataError(
            "Cannot format a timezone-naive datetime."
        )

    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def utc_now() -> str:
    """
    Return the current UTC time as text.

    Expected return:
    2026-07-22T15:30:00Z
    """

    return format_utc(
        utc_now_datetime()
    )


def parse_utc_datetime(
    raw_value: Any,
    field_name: str,
) -> datetime:
    """
    Parse an ISO-8601 datetime.

    Expected return:
    A timezone-aware UTC datetime.
    """

    if (
        not isinstance(raw_value, str)
        or not raw_value
    ):
        raise MarketDataError(
            "{} was not a non-empty datetime string.".format(
                field_name
            )
        )

    normalized = raw_value

    if normalized.endswith("Z"):
        normalized = (
            normalized[:-1] + "+00:00"
        )

    try:
        parsed = datetime.fromisoformat(
            normalized
        )

    except ValueError as error:
        raise MarketDataError(
            "{} was not valid ISO-8601: {!r}.".format(
                field_name,
                raw_value,
            )
        ) from error

    if parsed.tzinfo is None:
        raise MarketDataError(
            "{} did not include a timezone.".format(
                field_name
            )
        )

    return parsed.astimezone(
        timezone.utc
    )


def wait_for_rate_limit() -> None:
    """
    Wait before starting another request.

    Expected result:
    Consecutive requests begin at least 0.45 seconds apart.
    """

    global _last_request_time

    now = time.monotonic()

    if _last_request_time is not None:
        elapsed = (
            now - _last_request_time
        )

        remaining = (
            MIN_REQUEST_INTERVAL_SECONDS
            - elapsed
        )

        if remaining > 0:
            time.sleep(remaining)

    _last_request_time = (
        time.monotonic()
    )


def get_retry_delay(
    attempt: int,
    retry_after: Optional[str],
) -> float:
    """
    Calculate the retry delay.

    Expected return:
    Retry-After value or exponential backoff.
    """

    if retry_after:
        try:
            delay = float(
                retry_after
            )

            if 0 <= delay <= 120:
                return delay

        except ValueError:
            try:
                retry_time = (
                    parsedate_to_datetime(
                        retry_after
                    )
                )

                if retry_time.tzinfo is None:
                    retry_time = (
                        retry_time.replace(
                            tzinfo=timezone.utc
                        )
                    )

                delay = (
                    retry_time.astimezone(
                        timezone.utc
                    )
                    - utc_now_datetime()
                ).total_seconds()

                if 0 <= delay <= 120:
                    return delay

            except (
                TypeError,
                ValueError,
                OverflowError,
            ):
                pass

    return float(
        2 ** (attempt - 1)
    )


def build_request(
    url: str,
    etag: Optional[str] = None,
    last_modified: Optional[str] = None,
) -> urllib.request.Request:
    """
    Build one HTTP GET request.

    Expected result:
    Request includes JSON and optional cache headers.
    """

    headers = {
        "Accept": "application/json",
        "Language": "en",
        "User-Agent": USER_AGENT,
    }

    if etag:
        headers["If-None-Match"] = (
            etag
        )

    if last_modified:
        headers["If-Modified-Since"] = (
            last_modified
        )

    return urllib.request.Request(
        url,
        headers=headers,
        method="GET",
    )


def decode_json_response(
    raw_data: bytes,
) -> Dict[str, Any]:
    """
    Decode and validate JSON bytes.

    Expected return:
    A non-empty JSON object.
    """

    if (
        len(raw_data)
        > MAX_RESPONSE_BYTES
    ):
        raise MarketDataError(
            "API response exceeded the "
            "25 MB safety limit."
        )

    try:
        text = raw_data.decode(
            "utf-8"
        )

    except UnicodeDecodeError as error:
        raise MarketDataError(
            "API response was not valid UTF-8."
        ) from error

    try:
        document = json.loads(
            text
        )

    except json.JSONDecodeError as error:
        raise MarketDataError(
            "API returned invalid JSON."
        ) from error

    if (
        not isinstance(document, dict)
        or not document
    ):
        raise MarketDataError(
            "API response root was not a "
            "non-empty JSON object."
        )

    return document


def fetch_json(
    url: str,
    etag: Optional[str] = None,
    last_modified: Optional[str] = None,
    allow_not_modified: bool = False,
) -> FetchResult:
    """
    Fetch JSON with retries.

    Expected return:
    JSON for HTTP 200 or not_modified=True for HTTP 304.
    """

    for attempt in range(
        1,
        MAX_ATTEMPTS + 1,
    ):
        try:
            wait_for_rate_limit()

            request = build_request(
                url=url,
                etag=etag,
                last_modified=last_modified,
            )

            with urllib.request.urlopen(
                request,
                timeout=TIMEOUT_SECONDS,
            ) as response:
                raw_data = response.read(
                    MAX_RESPONSE_BYTES + 1
                )

                response_etag = (
                    response.headers.get(
                        "ETag"
                    )
                )

                response_last_modified = (
                    response.headers.get(
                        "Last-Modified"
                    )
                )

            return FetchResult(
                document=decode_json_response(
                    raw_data
                ),
                not_modified=False,
                etag=response_etag,
                last_modified=(
                    response_last_modified
                ),
            )

        except urllib.error.HTTPError as error:
            if (
                error.code == 304
                and allow_not_modified
            ):
                headers = (
                    error.headers
                    if error.headers is not None
                    else {}
                )

                return FetchResult(
                    document=None,
                    not_modified=True,
                    etag=headers.get(
                        "ETag"
                    ),
                    last_modified=headers.get(
                        "Last-Modified"
                    ),
                )

            if (
                error.code
                not in RETRYABLE_HTTP_CODES
                or attempt == MAX_ATTEMPTS
            ):
                raise MarketHttpError(
                    status_code=error.code,
                    url=url,
                ) from error

            retry_after = None

            if error.headers is not None:
                retry_after = (
                    error.headers.get(
                        "Retry-After"
                    )
                )

            delay = get_retry_delay(
                attempt=attempt,
                retry_after=retry_after,
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
                    "Could not connect after "
                    "{} attempts for '{}'.".format(
                        MAX_ATTEMPTS,
                        url,
                    )
                ) from error

            delay = get_retry_delay(
                attempt=attempt,
                retry_after=None,
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
    Save JSON atomically.

    Expected result:
    Existing file is only replaced after the complete new file
    has been written successfully.
    """

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_file = (
        output_file.with_name(
            output_file.name + ".tmp"
        )
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
            os.fsync(
                file.fileno()
            )

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
    Read a JSON file.

    Expected return:
    A JSON object.
    """

    try:
        with input_file.open(
            "r",
            encoding="utf-8",
        ) as file:
            document = json.load(
                file
            )

    except FileNotFoundError as error:
        raise MarketDataError(
            "File not found: '{}'.".format(
                input_file
            )
        ) from error

    except json.JSONDecodeError as error:
        raise MarketDataError(
            "Invalid JSON in '{}'.".format(
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
            "JSON root was not an object "
            "in '{}'.".format(
                input_file
            )
        )

    return document


def validate_slug(
    raw_slug: Any,
) -> str:
    """
    Validate an item slug.

    Expected return:
    Original valid slug, including supported Unicode.
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
            "Item slug contains leading "
            "or trailing spaces."
        )

    if slug in {".", ".."}:
        raise MarketDataError(
            "Item slug cannot be '.' or '..'."
        )

    for character in slug:
        if ord(character) < 32:
            raise MarketDataError(
                "Item slug contains a "
                "control character."
            )

        if (
            character
            in INVALID_SLUG_CHARACTERS
        ):
            raise MarketDataError(
                "Item slug contains unsafe "
                "character '{}': '{}'.".format(
                    character,
                    slug,
                )
            )

    if slug.endswith((".", " ")):
        raise MarketDataError(
            "Item slug cannot end with "
            "a dot or space."
        )

    filename_stem = (
        slug.split(".", 1)[0].upper()
    )

    if (
        filename_stem
        in WINDOWS_RESERVED_FILENAMES
    ):
        raise MarketDataError(
            "Reserved Windows filename: "
            "'{}'.".format(slug)
        )

    return slug


def validate_catalog_items(
    items: Any,
) -> List[Dict[str, Any]]:
    """
    Validate catalog items.

    Expected return:
    Non-empty list with unique ids and slugs.
    """

    if (
        not isinstance(items, list)
        or not items
    ):
        raise MarketDataError(
            "Item catalog is missing or empty."
        )

    validated: List[
        Dict[str, Any]
    ] = []

    seen_ids: Set[str] = set()
    seen_slugs: Set[str] = set()

    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise MarketDataError(
                "Catalog item {} was "
                "not an object.".format(
                    index
                )
            )

        item_id = item.get("id")

        if (
            not isinstance(item_id, str)
            or not item_id.strip()
        ):
            raise MarketDataError(
                "Catalog item {} has "
                "an invalid id.".format(
                    index
                )
            )

        slug = validate_slug(
            item.get("slug")
        )

        if item_id in seen_ids:
            raise MarketDataError(
                "Duplicate catalog id: "
                "'{}'.".format(item_id)
            )

        if slug in seen_slugs:
            raise MarketDataError(
                "Duplicate catalog slug: "
                "'{}'.".format(slug)
            )

        seen_ids.add(item_id)
        seen_slugs.add(slug)

        validated.append(item)

    return validated


def validate_item_catalog_response(
    response: Dict[str, Any],
) -> Tuple[
    str,
    List[Dict[str, Any]],
]:
    """
    Validate the item catalog API response.

    Expected return:
    API version and validated item list.
    """

    api_error = response.get(
        "error"
    )

    if api_error is not None:
        raise MarketDataError(
            "Item API returned an error: "
            "{!r}.".format(api_error)
        )

    api_version = response.get(
        "apiVersion"
    )

    if (
        not isinstance(api_version, str)
        or not api_version.strip()
    ):
        raise MarketDataError(
            "Item API returned an "
            "invalid apiVersion."
        )

    items = validate_catalog_items(
        response.get("data")
    )

    return api_version, items


def download_items() -> int:
    """
    Download the complete item catalog.

    Expected return:
    Number of validated items.
    """

    result = fetch_json(
        ITEMS_URL
    )

    if result.document is None:
        raise MarketDataError(
            "Item catalog request "
            "returned no document."
        )

    api_version, items = (
        validate_item_catalog_response(
            result.document
        )
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
        "[SUCCESS] Item catalog "
        "downloaded and validated."
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
    Validated list from data/items.json.
    """

    if not ITEMS_FILE.is_file():
        raise MarketDataError(
            "data/items.json is missing. "
            "Run 'python app\\market.py items' first."
        )

    document = read_json_file(
        ITEMS_FILE
    )

    return validate_catalog_items(
        document.get("items")
    )


def find_catalog_item(
    items: List[Dict[str, Any]],
    slug: str,
) -> Dict[str, Any]:
    """
    Find a catalog item.

    Expected return:
    Item matching the exact slug.
    """

    for item in items:
        if item.get("slug") == slug:
            return item

    raise MarketDataError(
        "Slug '{}' was not found "
        "in data/items.json.".format(
            slug
        )
    )


def get_item_name(
    item: Dict[str, Any],
) -> str:
    """
    Get the English item name.

    Expected return:
    English name or slug fallback.
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

    if (
        isinstance(slug, str)
        and slug
    ):
        return slug

    return "Unknown item"


def validate_statistics_response(
    response: Dict[str, Any],
) -> Dict[
    str,
    Dict[str, List[Dict[str, Any]]],
]:
    """
    Validate the statistics API response.

    Expected return:
    Closed and live statistics windows.
    """

    payload = response.get(
        "payload"
    )

    if not isinstance(payload, dict):
        raise MarketDataError(
            "Statistics response has "
            "no valid payload."
        )

    validated: Dict[
        str,
        Dict[str, List[Dict[str, Any]]],
    ] = {}

    for section_name in STATISTICS_SECTIONS:
        section = payload.get(
            section_name
        )

        if not isinstance(section, dict):
            raise MarketDataError(
                "Missing statistics section "
                "'{}'.".format(
                    section_name
                )
            )

        for required_window in (
            LATEST_WINDOW,
            HISTORY_WINDOW,
        ):
            if (
                required_window
                not in section
            ):
                raise MarketDataError(
                    "Missing statistics window "
                    "'{}.{}'.".format(
                        section_name,
                        required_window,
                    )
                )

        validated_windows: Dict[
            str,
            List[Dict[str, Any]],
        ] = {}

        for (
            window_name,
            records,
        ) in section.items():
            if (
                not isinstance(window_name, str)
                or not isinstance(records, list)
            ):
                raise MarketDataError(
                    "Invalid window "
                    "'{}.{}'.".format(
                        section_name,
                        window_name,
                    )
                )

            for index, record in enumerate(
                records
            ):
                if not isinstance(record, dict):
                    raise MarketDataError(
                        "Record {} in '{}.{}' "
                        "is invalid.".format(
                            index,
                            section_name,
                            window_name,
                        )
                    )

                parse_utc_datetime(
                    record.get("datetime"),
                    (
                        "{}.{}[{}].datetime"
                    ).format(
                        section_name,
                        window_name,
                        index,
                    ),
                )

            validated_windows[
                window_name
            ] = records

        validated[
            section_name
        ] = validated_windows

    return validated


def statistics_file_for_slug(
    slug: str,
) -> Path:
    """
    Build the statistics file path.

    Expected return:
    data/statistics/<slug>.json
    """

    safe_slug = validate_slug(
        slug
    )

    return (
        STATISTICS_DIR
        / "{}.json".format(safe_slug)
    )


def build_statistics_url(
    slug: str,
) -> str:
    """
    Build the statistics API URL.

    Expected return:
    URL-encoded item statistics URL.
    """

    safe_slug = validate_slug(
        slug
    )

    encoded_slug = urllib.parse.quote(
        safe_slug,
        safe="",
    )

    return STATISTICS_URL.format(
        slug=encoded_slug
    )


def canonical_key_value(
    value: Any,
) -> str:
    """
    Convert a JSON value to stable key text.

    Expected return:
    Distinguishes null, numbers, and strings.
    """

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    except (
        TypeError,
        ValueError,
    ) as error:
        raise MarketDataError(
            "Statistics key contains "
            "a non-JSON value."
        ) from error


def statistics_record_key(
    section_name: str,
    record: Dict[str, Any],
) -> Tuple[str, ...]:
    """
    Build the identity of one statistics row.

    Expected return:
    Closed:
    datetime + mod_rank

    Live:
    datetime + order_type + mod_rank
    """

    datetime_value = record.get(
        "datetime"
    )

    if not isinstance(
        datetime_value,
        str,
    ):
        raise MarketDataError(
            "Statistics datetime "
            "was not text."
        )

    mod_rank = canonical_key_value(
        record.get("mod_rank")
    )

    if (
        section_name
        == "statistics_closed"
    ):
        return (
            datetime_value,
            mod_rank,
        )

    if (
        section_name
        == "statistics_live"
    ):
        return (
            datetime_value,
            canonical_key_value(
                record.get("order_type")
            ),
            mod_rank,
        )

    raise MarketDataError(
        "Unsupported statistics section "
        "'{}'.".format(section_name)
    )


def records_to_map(
    section_name: str,
    records: Any,
    source_name: str,
) -> Dict[
    Tuple[str, ...],
    Dict[str, Any],
]:
    """
    Convert records to a unique-key map.

    Expected result:
    Exact duplicates collapse.
    Conflicting duplicates raise an error.
    """

    if not isinstance(records, list):
        raise MarketDataError(
            "{} was not a list.".format(
                source_name
            )
        )

    output: Dict[
        Tuple[str, ...],
        Dict[str, Any],
    ] = {}

    for index, record in enumerate(
        records
    ):
        if not isinstance(record, dict):
            raise MarketDataError(
                "Record {} in {} was "
                "not an object.".format(
                    index,
                    source_name,
                )
            )

        parse_utc_datetime(
            record.get("datetime"),
            "{}[{}].datetime".format(
                source_name,
                index,
            ),
        )

        key = statistics_record_key(
            section_name,
            record,
        )

        existing = output.get(key)

        if (
            existing is not None
            and existing != record
        ):
            raise MarketDataError(
                "Conflicting duplicate key "
                "in {}: {}.".format(
                    source_name,
                    key,
                )
            )

        output[key] = record

    return output


def sort_statistics_records(
    section_name: str,
    records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Sort statistics rows.

    Expected return:
    Rows sorted by their logical identity.
    """

    return sorted(
        records,
        key=lambda row: (
            statistics_record_key(
                section_name,
                row,
            )
        ),
    )


def history_rows(
    raw_response: Dict[str, Any],
    section_name: str,
) -> List[Dict[str, Any]]:
    """
    Get 90-day rows from raw API data.

    Expected return:
    Raw section 90days list.
    """

    sections = (
        validate_statistics_response(
            raw_response
        )
    )

    return sections[
        section_name
    ][HISTORY_WINDOW]


def empty_history_archive() -> Dict[str, Any]:
    """
    Create an empty history archive.

    Expected return:
    Empty closed and live lists.
    """

    return {
        "window": HISTORY_WINDOW,
        "rolling_days": 90,
        "statistics_closed": [],
        "statistics_live": [],
    }


def validate_history_archive(
    archive: Any,
) -> Dict[str, Any]:
    """
    Validate and normalize the history archive.

    Expected return:
    Normalized archive object.
    """

    if archive is None:
        return empty_history_archive()

    if not isinstance(archive, dict):
        raise MarketDataError(
            "history_archive was "
            "not an object."
        )

    normalized = (
        empty_history_archive()
    )

    for section_name in STATISTICS_SECTIONS:
        records = archive.get(
            section_name,
            [],
        )

        row_map = records_to_map(
            section_name=section_name,
            records=records,
            source_name=(
                "history_archive.{}"
            ).format(section_name),
        )

        normalized[
            section_name
        ] = sort_statistics_records(
            section_name,
            list(row_map.values()),
        )

    return normalized


def build_history_archive(
    existing_document: Optional[
        Dict[str, Any]
    ],
    new_raw_response: Dict[str, Any],
    fetched_at: datetime,
) -> Dict[str, Any]:
    """
    Preserve daily rows outside the new rolling window.

    Expected result:
    - Current 90 days stay in raw_response.
    - Older daily rows stay in history_archive.
    - Current rows are not stored twice.
    """

    previous_archive = (
        empty_history_archive()
    )

    previous_raw: Optional[
        Dict[str, Any]
    ] = None

    if existing_document is not None:
        previous_archive = (
            validate_history_archive(
                existing_document.get(
                    "history_archive"
                )
            )
        )

        raw_candidate = (
            existing_document.get(
                "raw_response"
            )
        )

        if isinstance(
            raw_candidate,
            dict,
        ):
            validate_statistics_response(
                raw_candidate
            )

            previous_raw = (
                raw_candidate
            )

    validate_statistics_response(
        new_raw_response
    )

    cutoff = (
        fetched_at
        - ROLLING_HISTORY_DURATION
    )

    output = (
        empty_history_archive()
    )

    for section_name in STATISTICS_SECTIONS:
        previous_rows = records_to_map(
            section_name=section_name,
            records=previous_archive[
                section_name
            ],
            source_name=(
                "previous history_archive.{}"
            ).format(section_name),
        )

        if previous_raw is not None:
            previous_current_rows = (
                records_to_map(
                    section_name=section_name,
                    records=history_rows(
                        previous_raw,
                        section_name,
                    ),
                    source_name=(
                        "previous raw_response."
                        "{}.{}"
                    ).format(
                        section_name,
                        HISTORY_WINDOW,
                    ),
                )
            )

            previous_rows.update(
                previous_current_rows
            )

        new_current_rows = (
            records_to_map(
                section_name=section_name,
                records=history_rows(
                    new_raw_response,
                    section_name,
                ),
                source_name=(
                    "new raw_response."
                    "{}.{}"
                ).format(
                    section_name,
                    HISTORY_WINDOW,
                ),
            )
        )

        archived: List[
            Dict[str, Any]
        ] = []

        for (
            key,
            record,
        ) in previous_rows.items():
            record_time = (
                parse_utc_datetime(
                    record.get("datetime"),
                    (
                        "historical statistics "
                        "datetime"
                    ),
                )
            )

            if (
                record_time < cutoff
                and key
                not in new_current_rows
            ):
                archived.append(
                    record
                )

        output[
            section_name
        ] = sort_statistics_records(
            section_name,
            archived,
        )

    return output


def build_item_metadata(
    item: Dict[str, Any],
    slug: str,
) -> Dict[str, Any]:
    """
    Build item metadata for a saved file.

    Expected return:
    id, slug, name, and tags.
    """

    tags = item.get(
        "tags",
        [],
    )

    if not isinstance(tags, list):
        tags = []

    return {
        "id": item.get("id"),
        "slug": slug,
        "name": get_item_name(item),
        "tags": tags,
    }


def validate_saved_statistics_document(
    document: Dict[str, Any],
    expected_slug: str,
) -> None:
    """
    Validate a saved statistics file.

    Expected result:
    No exception for valid legacy or new files.
    """

    item = document.get(
        "item"
    )

    if (
        not isinstance(item, dict)
        or item.get("slug")
        != expected_slug
    ):
        raise MarketDataError(
            "Saved statistics item does not "
            "match the expected slug."
        )

    raw_response = document.get(
        "raw_response"
    )

    if not isinstance(
        raw_response,
        dict,
    ):
        raise MarketDataError(
            "Saved statistics file "
            "has no raw_response."
        )

    validate_statistics_response(
        raw_response
    )

    parse_utc_datetime(
        document.get("downloaded_at"),
        "downloaded_at",
    )

    if "last_checked_at" in document:
        parse_utc_datetime(
            document.get(
                "last_checked_at"
            ),
            "last_checked_at",
        )

    validate_history_archive(
        document.get(
            "history_archive"
        )
    )

    gaps = document.get(
        "collection_coverage_gaps",
        [],
    )

    if not isinstance(gaps, list):
        raise MarketDataError(
            "collection_coverage_gaps "
            "was not a list."
        )


def migrate_statistics_document(
    document: Dict[str, Any],
    item: Dict[str, Any],
    slug: str,
) -> Tuple[
    Dict[str, Any],
    bool,
]:
    """
    Upgrade a legacy statistics file locally.

    Expected return:
    Schema version 2 document and whether it changed.
    """

    validate_saved_statistics_document(
        document,
        slug,
    )

    migrated = dict(document)
    changed = False

    expected_values = {
        "schema_version": SCHEMA_VERSION,
        "source": build_statistics_url(
            slug
        ),
        "item": build_item_metadata(
            item,
            slug,
        ),
    }

    for (
        key,
        value,
    ) in expected_values.items():
        if migrated.get(key) != value:
            migrated[key] = value
            changed = True

    downloaded_at = migrated.get(
        "downloaded_at"
    )

    if not isinstance(
        migrated.get("last_checked_at"),
        str,
    ):
        migrated[
            "last_checked_at"
        ] = downloaded_at

        changed = True

    if not isinstance(
        migrated.get(
            "first_collected_at"
        ),
        str,
    ):
        migrated[
            "first_collected_at"
        ] = downloaded_at

        changed = True

    http_cache = migrated.get(
        "http_cache"
    )

    normalized_cache = {
        "etag": None,
        "last_modified": None,
    }

    if isinstance(http_cache, dict):
        etag = http_cache.get(
            "etag"
        )

        last_modified = http_cache.get(
            "last_modified"
        )

        if isinstance(etag, str):
            normalized_cache[
                "etag"
            ] = etag

        if isinstance(
            last_modified,
            str,
        ):
            normalized_cache[
                "last_modified"
            ] = last_modified

    if http_cache != normalized_cache:
        migrated[
            "http_cache"
        ] = normalized_cache

        changed = True

    normalized_archive = (
        validate_history_archive(
            migrated.get(
                "history_archive"
            )
        )
    )

    if (
        migrated.get(
            "history_archive"
        )
        != normalized_archive
    ):
        migrated[
            "history_archive"
        ] = normalized_archive

        changed = True

    gaps = migrated.get(
        "collection_coverage_gaps"
    )

    if not isinstance(gaps, list):
        migrated[
            "collection_coverage_gaps"
        ] = []

        changed = True

    validate_saved_statistics_document(
        migrated,
        slug,
    )

    return migrated, changed


def load_existing_statistics_document(
    output_file: Path,
    item: Dict[str, Any],
    slug: str,
) -> Tuple[
    Optional[Dict[str, Any]],
    bool,
]:
    """
    Load and migrate an existing statistics file.

    Expected return:
    - Valid document and migration flag.
    - None for missing or invalid files.
    """

    if not output_file.is_file():
        return None, False

    try:
        document = read_json_file(
            output_file
        )

        migrated, changed = (
            migrate_statistics_document(
                document=document,
                item=item,
                slug=slug,
            )
        )

    except MarketDataError:
        return None, False

    if changed:
        save_json_atomic(
            migrated,
            output_file,
        )

    return migrated, changed


def freshness_reference_time(
    document: Dict[str, Any],
) -> datetime:
    """
    Get the latest successful server-check time.

    Expected return:
    last_checked_at or downloaded_at fallback.
    """

    raw_value = document.get(
        "last_checked_at"
    )

    if not isinstance(raw_value, str):
        raw_value = document.get(
            "downloaded_at"
        )

    return parse_utc_datetime(
        raw_value,
        "statistics freshness timestamp",
    )


def statistics_document_is_fresh(
    document: Dict[str, Any],
    now: Optional[datetime] = None,
) -> bool:
    """
    Check whether a file is younger than 24 hours.

    Expected return:
    True when recently checked.
    """

    current_time = (
        now
        if now is not None
        else utc_now_datetime()
    )

    checked_time = (
        freshness_reference_time(
            document
        )
    )

    age = (
        current_time
        - checked_time
    )

    if (
        age
        < -CLOCK_SKEW_TOLERANCE
    ):
        return False

    return (
        age < FRESHNESS_DURATION
    )


def get_http_cache_headers(
    document: Optional[
        Dict[str, Any]
    ],
) -> Tuple[
    Optional[str],
    Optional[str],
]:
    """
    Get saved HTTP cache validators.

    Expected return:
    ETag and Last-Modified.
    """

    if document is None:
        return None, None

    cache = document.get(
        "http_cache"
    )

    if not isinstance(cache, dict):
        return None, None

    etag = cache.get("etag")

    last_modified = cache.get(
        "last_modified"
    )

    valid_etag = (
        etag
        if isinstance(etag, str) and etag
        else None
    )

    valid_last_modified = (
        last_modified
        if (
            isinstance(last_modified, str)
            and last_modified
        )
        else None
    )

    return (
        valid_etag,
        valid_last_modified,
    )


def merge_http_cache_headers(
    existing_document: Optional[
        Dict[str, Any]
    ],
    response_etag: Optional[str],
    response_last_modified: Optional[str],
) -> Dict[str, Optional[str]]:
    """
    Merge old and new HTTP cache validators.

    Expected return:
    New values when provided, otherwise previous values.
    """

    (
        old_etag,
        old_last_modified,
    ) = get_http_cache_headers(
        existing_document
    )

    return {
        "etag": (
            response_etag
            or old_etag
        ),
        "last_modified": (
            response_last_modified
            or old_last_modified
        ),
    }


def append_collection_gap(
    existing_gaps: Any,
    previous_check: datetime,
    current_check: datetime,
) -> List[Dict[str, Any]]:
    """
    Record an unrecoverable collection gap.

    Expected result:
    Gap is added when successful checks are over 90 days apart.
    """

    gaps: List[
        Dict[str, Any]
    ] = []

    if isinstance(
        existing_gaps,
        list,
    ):
        for gap in existing_gaps:
            if isinstance(gap, dict):
                gaps.append(gap)

    recoverable_start = (
        current_check
        - ROLLING_HISTORY_DURATION
    )

    if (
        previous_check
        >= recoverable_start
    ):
        return gaps

    gap = {
        "after_last_successful_check": (
            format_utc(
                previous_check
            )
        ),
        "before_current_recoverable_window": (
            format_utc(
                recoverable_start
            )
        ),
        "detected_at": format_utc(
            current_check
        ),
        "reason": (
            "Statistics were not collected within "
            "the rolling 90-day recovery window. "
            "Missing rows were not fabricated or "
            "converted to zero."
        ),
    }

    identity = (
        gap[
            "after_last_successful_check"
        ],
        gap[
            "before_current_recoverable_window"
        ],
    )

    existing_identities = {
        (
            value.get(
                "after_last_successful_check"
            ),
            value.get(
                "before_current_recoverable_window"
            ),
        )
        for value in gaps
    }

    if (
        identity
        not in existing_identities
    ):
        gaps.append(gap)

    return gaps


def refresh_statistics_for_item(
    item: Dict[str, Any],
    existing_document: Optional[
        Dict[str, Any]
    ],
    force_refresh: bool,
) -> RefreshResult:
    """
    Refresh one item's statistics.

    Expected result:
    - Current API response replaces raw_response.
    - Old daily rows move into history_archive.
    - Current 90-day rows are not duplicated.
    """

    slug = validate_slug(
        item.get("slug")
    )

    url = build_statistics_url(
        slug
    )

    output_file = (
        statistics_file_for_slug(
            slug
        )
    )

    etag = None
    last_modified = None

    if not force_refresh:
        (
            etag,
            last_modified,
        ) = get_http_cache_headers(
            existing_document
        )

    result = fetch_json(
        url=url,
        etag=etag,
        last_modified=last_modified,
        allow_not_modified=(
            existing_document is not None
        ),
    )

    checked_at = (
        utc_now_datetime()
    )

    checked_at_text = (
        format_utc(checked_at)
    )

    if result.not_modified:
        if existing_document is None:
            raise MarketDataError(
                "HTTP 304 was returned "
                "without a local file."
            )

        document = dict(
            existing_document
        )

        previous_check = (
            freshness_reference_time(
                document
            )
        )

        document[
            "last_checked_at"
        ] = checked_at_text

        document[
            "http_cache"
        ] = merge_http_cache_headers(
            existing_document=(
                existing_document
            ),
            response_etag=result.etag,
            response_last_modified=(
                result.last_modified
            ),
        )

        document[
            "collection_coverage_gaps"
        ] = append_collection_gap(
            existing_gaps=document.get(
                "collection_coverage_gaps"
            ),
            previous_check=previous_check,
            current_check=checked_at,
        )

        validate_saved_statistics_document(
            document,
            slug,
        )

        save_json_atomic(
            document,
            output_file,
        )

        raw_response = document.get(
            "raw_response"
        )

        if not isinstance(
            raw_response,
            dict,
        ):
            raise MarketDataError(
                "Local raw_response disappeared "
                "during HTTP 304 handling."
            )

        sections = (
            validate_statistics_response(
                raw_response
            )
        )

        return RefreshResult(
            output_file=output_file,
            sections=sections,
            status="not_modified",
        )

    if result.document is None:
        raise MarketDataError(
            "Statistics request "
            "returned no document."
        )

    sections = (
        validate_statistics_response(
            result.document
        )
    )

    archive = build_history_archive(
        existing_document=(
            existing_document
        ),
        new_raw_response=(
            result.document
        ),
        fetched_at=checked_at,
    )

    first_collected_at = (
        checked_at_text
    )

    gaps: List[
        Dict[str, Any]
    ] = []

    if existing_document is not None:
        old_first = (
            existing_document.get(
                "first_collected_at"
            )
        )

        if isinstance(old_first, str):
            parse_utc_datetime(
                old_first,
                "first_collected_at",
            )

            first_collected_at = (
                old_first
            )

        previous_check = (
            freshness_reference_time(
                existing_document
            )
        )

        gaps = append_collection_gap(
            existing_gaps=(
                existing_document.get(
                    "collection_coverage_gaps"
                )
            ),
            previous_check=previous_check,
            current_check=checked_at,
        )

    document = {
        "schema_version": SCHEMA_VERSION,
        "source": url,
        "downloaded_at": checked_at_text,
        "last_checked_at": checked_at_text,
        "first_collected_at": (
            first_collected_at
        ),
        "item": build_item_metadata(
            item,
            slug,
        ),
        "http_cache": (
            merge_http_cache_headers(
                existing_document=(
                    existing_document
                ),
                response_etag=result.etag,
                response_last_modified=(
                    result.last_modified
                ),
            )
        ),
        "raw_response": result.document,
        "history_archive": archive,
        "collection_coverage_gaps": gaps,
    }

    validate_saved_statistics_document(
        document,
        slug,
    )

    save_json_atomic(
        document,
        output_file,
    )

    return RefreshResult(
        output_file=output_file,
        sections=sections,
        status="downloaded",
    )


def download_statistics(
    raw_slug: str,
) -> None:
    """
    Refresh one selected item.

    Expected result:
    Server is checked even when the local file is fresh.
    """

    slug = validate_slug(
        raw_slug
    )

    items = load_catalog_items()

    item = find_catalog_item(
        items,
        slug,
    )

    output_file = (
        statistics_file_for_slug(
            slug
        )
    )

    (
        existing_document,
        migrated,
    ) = load_existing_statistics_document(
        output_file=output_file,
        item=item,
        slug=slug,
    )

    if migrated:
        print(
            "[MIGRATED] Upgraded {} "
            "to schema version {}.".format(
                slug,
                SCHEMA_VERSION,
            )
        )

    refresh = (
        refresh_statistics_for_item(
            item=item,
            existing_document=(
                existing_document
            ),
            force_refresh=False,
        )
    )

    if (
        refresh.status
        == "not_modified"
    ):
        print(
            "[NOT MODIFIED] Server confirmed "
            "the local data is current."
        )

    else:
        print(
            "[SUCCESS] Statistics downloaded, "
            "merged, and validated."
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

    for (
        section_name,
        windows,
    ) in refresh.sections.items():
        for (
            window_name,
            records,
        ) in windows.items():
            print(
                "[SUCCESS] {}.{}: "
                "{} records".format(
                    section_name,
                    window_name,
                    len(records),
                )
            )

    print(
        "[SUCCESS] Saved to: {}".format(
            refresh.output_file
        )
    )


def reset_batch_error_log() -> None:
    """
    Reset the batch error log.

    Expected result:
    Previous error log is removed.
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
    Append one batch error.

    Expected result:
    One JSON object is appended to the JSONL file.
    """

    record: Dict[str, Any] = {
        "recorded_at": utc_now(),
        "position": position,
        "total": total,
        "slug": slug,
        "error_type": (
            type(error).__name__
        ),
        "error": str(error),
    }

    if isinstance(
        error,
        MarketHttpError,
    ):
        record[
            "http_status"
        ] = error.status_code

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
            "Could not write "
            "batch error log."
        ) from log_error


def save_batch_summary(
    started_at: str,
    status: str,
    total_items: int,
    downloaded: int,
    skipped_fresh: int,
    not_modified: int,
    failed: int,
    force_refresh: bool,
) -> None:
    """
    Save the batch summary.

    Expected result:
    Progress survives interruption.
    """

    summary = {
        "started_at": started_at,
        "updated_at": utc_now(),
        "status": status,
        "total_items": total_items,
        "processed": (
            downloaded
            + skipped_fresh
            + not_modified
            + failed
        ),
        "downloaded": downloaded,
        "skipped_fresh": (
            skipped_fresh
        ),
        "not_modified": (
            not_modified
        ),
        "failed": failed,
        "force_refresh": (
            force_refresh
        ),
        "freshness_hours": 24,
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


def download_all_statistics(
    force_refresh: bool,
) -> int:
    """
    Refresh all catalog statistics.

    Expected behavior:
    - Fresh valid files are skipped.
    - Stale valid files are refreshed.
    - Invalid files are downloaded again.
    - Force refresh ignores freshness and cache validators.
    """

    items = load_catalog_items()

    total_items = len(items)
    started_at = utc_now()

    downloaded = 0
    skipped_fresh = 0
    not_modified = 0
    failed = 0

    reset_batch_error_log()

    save_batch_summary(
        started_at=started_at,
        status="running",
        total_items=total_items,
        downloaded=downloaded,
        skipped_fresh=skipped_fresh,
        not_modified=not_modified,
        failed=failed,
        force_refresh=force_refresh,
    )

    print(
        "[START] Refreshing statistics "
        "for {} items.".format(
            total_items
        )
    )

    if force_refresh:
        print(
            "[START] Force refresh is enabled."
        )

    else:
        print(
            "[START] Files checked within "
            "24 hours will be skipped."
        )

    try:
        for (
            position,
            item,
        ) in enumerate(
            items,
            start=1,
        ):
            raw_slug = item.get(
                "slug"
            )

            try:
                slug = validate_slug(
                    raw_slug
                )

            except MarketDataError as error:
                failed += 1

                append_batch_error(
                    position=position,
                    total=total_items,
                    slug=str(raw_slug),
                    error=error,
                )

                print(
                    "[{}/{}] [ERROR] "
                    "Invalid item: {}".format(
                        position,
                        total_items,
                        error,
                    )
                )

                continue

            output_file = (
                statistics_file_for_slug(
                    slug
                )
            )

            file_existed = (
                output_file.exists()
            )

            (
                existing_document,
                migrated,
            ) = (
                load_existing_statistics_document(
                    output_file=output_file,
                    item=item,
                    slug=slug,
                )
            )

            if migrated:
                print(
                    "[{}/{}] [MIGRATED] "
                    "{}".format(
                        position,
                        total_items,
                        slug,
                    )
                )

            if (
                not force_refresh
                and existing_document
                is not None
                and statistics_document_is_fresh(
                    existing_document
                )
            ):
                skipped_fresh += 1

                print(
                    "[{}/{}] [SKIP FRESH] "
                    "{}".format(
                        position,
                        total_items,
                        slug,
                    )
                )

            else:
                if (
                    file_existed
                    and existing_document
                    is None
                ):
                    print(
                        "[{}/{}] [REDOWNLOAD] "
                        "{} has an invalid "
                        "local file.".format(
                            position,
                            total_items,
                            slug,
                        )
                    )

                elif (
                    existing_document
                    is not None
                ):
                    print(
                        "[{}/{}] [REFRESH] "
                        "{}".format(
                            position,
                            total_items,
                            slug,
                        )
                    )

                try:
                    refresh = (
                        refresh_statistics_for_item(
                            item=item,
                            existing_document=(
                                existing_document
                            ),
                            force_refresh=(
                                force_refresh
                            ),
                        )
                    )

                    if (
                        refresh.status
                        == "not_modified"
                    ):
                        not_modified += 1

                        print(
                            "[{}/{}] "
                            "[NOT MODIFIED] "
                            "{}".format(
                                position,
                                total_items,
                                slug,
                            )
                        )

                    else:
                        downloaded += 1

                        print(
                            "[{}/{}] [OK] "
                            "{}".format(
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
                        "[{}/{}] [ERROR] "
                        "{}: {}".format(
                            position,
                            total_items,
                            slug,
                            error,
                        )
                    )

            if position % 25 == 0:
                save_batch_summary(
                    started_at=started_at,
                    status="running",
                    total_items=total_items,
                    downloaded=downloaded,
                    skipped_fresh=(
                        skipped_fresh
                    ),
                    not_modified=(
                        not_modified
                    ),
                    failed=failed,
                    force_refresh=(
                        force_refresh
                    ),
                )

    except KeyboardInterrupt:
        save_batch_summary(
            started_at=started_at,
            status="interrupted",
            total_items=total_items,
            downloaded=downloaded,
            skipped_fresh=(
                skipped_fresh
            ),
            not_modified=(
                not_modified
            ),
            failed=failed,
            force_refresh=(
                force_refresh
            ),
        )

        raise

    save_batch_summary(
        started_at=started_at,
        status="completed",
        total_items=total_items,
        downloaded=downloaded,
        skipped_fresh=skipped_fresh,
        not_modified=not_modified,
        failed=failed,
        force_refresh=force_refresh,
    )

    print()
    print(
        "[COMPLETE] Batch refresh finished."
    )

    print(
        "[COMPLETE] Downloaded: {}".format(
            downloaded
        )
    )

    print(
        "[COMPLETE] Skipped fresh: {}".format(
            skipped_fresh
        )
    )

    print(
        "[COMPLETE] Not modified: {}".format(
            not_modified
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
    """Print supported commands."""

    print("Usage:")
    print(
        r"  python app\market.py items"
    )
    print(
        r"  python app\market.py "
        r"stats <item_slug>"
    )
    print(
        r"  python app\market.py all-stats"
    )
    print(
        r"  python app\market.py "
        r"all-stats --force"
    )


def main() -> int:
    """
    Run one command.

    Expected return:
    0 for success.
    Non-zero for failure.
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
            failure_count = (
                download_all_statistics(
                    force_refresh=False
                )
            )

            return (
                1
                if failure_count > 0
                else 0
            )

        if (
            len(sys.argv) == 3
            and sys.argv[1] == "all-stats"
            and sys.argv[2] == "--force"
        ):
            failure_count = (
                download_all_statistics(
                    force_refresh=True
                )
            )

            return (
                1
                if failure_count > 0
                else 0
            )

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
