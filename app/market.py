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
