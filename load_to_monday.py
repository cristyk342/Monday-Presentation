"""
load_to_monday.py

Uploads engagements and deliverables CSVs from the Desktop into Monday.com.

Usage:
    py load_to_monday.py <MONDAY_API_KEY>

Load order:
    1. Upload each engagement to the Nexus Engagements board
    2. Build an in-memory lookup: engagement_id -> Monday item ID
    3. Upload each deliverable to the Deliverables board, linking it to
       its parent engagement via the board_relation column
    4. Save the lookup table as engagement_lookup.json on the Desktop
       so future scripts can reference Monday item IDs directly.

Notes:
    - The 'Client' field maps to the 'name' column (item title) in the
      Nexus Engagements board.
    - Engagement Lead and Assignee are plain text columns.
    - The 'Engagement Lead' mirror column (lookup_mm2jqd2f) is read-only
      and is auto-populated by Monday once the board relation is set.
"""

import csv
import json
import os
import sys
import time
from datetime import datetime

import requests

# ── Board / column IDs ────────────────────────────────────────────────────────

ENGAGEMENTS_BOARD_ID = "18409314022"
DELIVERABLES_BOARD_ID = "18409315323"

# column IDs are kept here so they can be updated in one place if the board changes
ENG_COLS = {
    "engagement_id": "text_mm2jqk29",
    "lead":          "text_mm2jve4b",
    "status":        "status",
    "start_date":    "date4",
    "end_date":      "date_mm2jdj4k",
    "budget":        "numeric_mm2jjxht",
}

DEL_COLS = {
    "deliverable_id":  "text_mm2j29yw",
    "assignee":        "text_mm2jstey",
    "status":          "status",
    "due_date":        "date4",
    "hours_estimated": "numeric_mm2j6ccv",
    "priority":        "color_mm2jc77q",
    "engagement_link": "board_relation_mm2jep2j",
    # lookup_mm2jqd2f (Engagement Lead mirror) is read-only — omitted
}

MONDAY_API_URL = "https://api.monday.com/v2"
DESKTOP = os.path.join(os.path.expanduser("~"), "Desktop")
ENGAGEMENTS_CSV = os.path.join(DESKTOP, "engagements.csv")
DELIVERABLES_CSV = os.path.join(DESKTOP, "deliverables.csv")
LOOKUP_OUTPUT = os.path.join(DESKTOP, "engagement_lookup.json")

REQUEST_DELAY = 0.4


# ── API helpers ───────────────────────────────────────────────────────────────

def monday_request(api_key, query, variables=None):
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
        "API-Version": "2024-10",
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    resp = requests.post(MONDAY_API_URL, json=payload, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"Monday API error: {data['errors']}")
    return data


def create_item(api_key, board_id, item_name, col_values_dict):
    """
    col_values_dict: {column_id: python value/dict}
    Monday expects column_values as a single JSON-serialized string.
    """
    # strip out any None values before sending — avoids passing empty fields to Monday
    filtered = {k: v for k, v in col_values_dict.items() if v is not None}
    # Monday expects column_values as a single JSON string, not a dict
    col_values_json = json.dumps(filtered)

    query = """
    mutation ($board_id: ID!, $item_name: String!, $column_values: JSON!) {
        create_item(
            board_id: $board_id
            item_name: $item_name
            column_values: $column_values
        ) {
            id
        }
    }
    """
    variables = {
        "board_id": board_id,
        "item_name": item_name,
        "column_values": col_values_json,
    }
    data = monday_request(api_key, query, variables)
    # return the new Monday item ID so we can store it in the lookup
    return int(data["data"]["create_item"]["id"])


# ── Column value builders ─────────────────────────────────────────────────────

# each function below formats values the way Monday's API expects for that column type
# hardcoded to Monday's current spec — if Monday changes the format, update here only
def date_val(date_str):
    """MM/DD/YYYY -> {"date": "YYYY-MM-DD"}, or None on bad input."""
    try:
        iso = datetime.strptime(date_str.strip(), "%m/%d/%Y").strftime("%Y-%m-%d")
        return {"date": iso}
    except ValueError:
        return None


def status_val(label):
    return {"label": label}


def number_val(value):
    return float(value)


def text_val(value):
    return str(value)


def board_relation_val(monday_item_id):
    return {"item_ids": [monday_item_id]}

#follows monday formatting values
#can easily be changed

# ── Upload logic ──────────────────────────────────────────────────────────────

def upload_engagements(api_key):
    """
    Upload every row from engagements.csv to the Nexus Engagements board.
    Returns {engagement_id: monday_item_id} lookup dict.
    """
    lookup = {}

    with open(ENGAGEMENTS_CSV, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"\nUploading {len(rows)} engagements to board {ENGAGEMENTS_BOARD_ID}...")

    for row in rows:
        eng_id = row["engagement_id"].strip()
        client = row["client"].strip()  # 'name' column = Client (item title)
        print(f"  {eng_id}: {client}")

        col_values = {
            ENG_COLS["engagement_id"]: text_val(eng_id),
            ENG_COLS["lead"]:          text_val(row["engagement_lead"]),
            ENG_COLS["status"]:        status_val(row["engagement_status"].strip()),
            ENG_COLS["start_date"]:    date_val(row["engagement_start"]),
            ENG_COLS["end_date"]:      date_val(row["engagement_end"]),
            ENG_COLS["budget"]:        number_val(row["budget"]),
        }

        monday_id = create_item(api_key, ENGAGEMENTS_BOARD_ID, client, col_values)
        # store the returned Monday ID against our engagement ID for the deliverable upload
        lookup[eng_id] = monday_id
        print(f"    -> Monday item ID: {monday_id}")
        time.sleep(REQUEST_DELAY)  # stay under Monday's rate limit

    return lookup


def upload_deliverables(api_key, engagement_lookup):
    """
    Upload every row from deliverables.csv to the Deliverables board,
    linking each to its parent engagement via the board_relation column.
    The Engagement Lead mirror column is read-only and populates automatically.
    """
    with open(DELIVERABLES_CSV, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"\nUploading {len(rows)} deliverables to board {DELIVERABLES_BOARD_ID}...")

    for row in rows:
        del_id = row["deliverable_id"].strip()
        eng_id = row["engagement_id"].strip()
        name = row["deliverable_name"].strip()

        parent_id = engagement_lookup.get(eng_id)
        if parent_id is None:
            # engagement wasn't in the lookup — something went wrong during the engagement upload
            # log it and skip rather than crashing the whole run
            print(f"  [SKIP] {del_id}: no Monday item found for engagement {eng_id}")
            continue

        print(f"  {del_id}: {name}  (-> {eng_id} / Monday ID {parent_id})")

        col_values = {
            DEL_COLS["deliverable_id"]:  text_val(del_id),
            DEL_COLS["assignee"]:        text_val(row["assignee"]),
            DEL_COLS["status"]:          status_val(row["deliverable_status"].strip()),
            DEL_COLS["due_date"]:        date_val(row["due_date"]),
            DEL_COLS["hours_estimated"]: number_val(row["hours_estimated"]),
            DEL_COLS["priority"]:        status_val(row["priority"].strip()),
            DEL_COLS["engagement_link"]: board_relation_val(parent_id),  # links deliverable to its parent engagement
        }

        monday_id = create_item(api_key, DELIVERABLES_BOARD_ID, name, col_values)
        print(f"    -> Monday item ID: {monday_id}")
        time.sleep(REQUEST_DELAY)


# ── Entry point ───────────────────────────────────────────────────────────────

def main(api_key):
    engagement_lookup = upload_engagements(api_key)

    # save lookup to disk so deliverable upload can be re-run independently if it fails
    with open(LOOKUP_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(engagement_lookup, f, indent=2)
    print(f"\nEngagement lookup saved -> {LOOKUP_OUTPUT}")

    upload_deliverables(api_key, engagement_lookup)

    print("\nAll done.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: py load_to_monday.py <MONDAY_API_KEY>")
        sys.exit(1)
    main(sys.argv[1])

