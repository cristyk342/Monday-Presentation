"""
validate_migration.py - Data migration validation report

Compares three data layers:
  1. Original flat source CSV  (Downloads/nexus_smartsheet_export.csv)
  2. Cleaned split CSVs        (Desktop/engagements.csv, deliverables.csv)
  3. Live Monday.com boards    (queried via API)

Usage:
    py validate_migration.py <MONDAY_API_KEY>
"""

import csv
import io
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime

import requests

# ── Paths ─────────────────────────────────────────────────────────────────────

MONDAY_API_URL   = "https://api.monday.com/v2"
HOME             = os.path.expanduser("~")
DESKTOP          = os.path.join(HOME, "Desktop")
SOURCE_CSV       = os.path.join(HOME, "Downloads", "nexus_smartsheet_export.csv")
ENGAGEMENTS_CSV  = os.path.join(DESKTOP, "engagements.csv")
DELIVERABLES_CSV = os.path.join(DESKTOP, "deliverables.csv")
LOOKUP_JSON      = os.path.join(DESKTOP, "engagement_lookup.json")
REPORT_OUTPUT    = os.path.join(DESKTOP, "migration_validation_report.txt")

ENGAGEMENTS_BOARD_ID  = "18409314022"
DELIVERABLES_BOARD_ID = "18409315323"

# ── Monday column_id → readable field name ────────────────────────────────────

ENG_COL_MAP = {
    "name":              "client",
    "text_mm2jqk29":     "engagement_id",
    "text_mm2jve4b":     "engagement_lead",
    "status":            "engagement_status",
    "date4":             "engagement_start",
    "date_mm2jdj4k":     "engagement_end",
    "numeric_mm2jjxht":  "budget",
}

DEL_COL_MAP = {
    "name":                    "deliverable_name",
    "text_mm2j29yw":           "deliverable_id",
    "text_mm2jstey":           "assignee",
    "status":                  "deliverable_status",
    "date4":                   "due_date",
    "numeric_mm2j6ccv":        "hours_estimated",
    "color_mm2jc77q":          "priority",
    "board_relation_mm2jep2j": "engagement_link",
}

# ── Status normalisation maps (mirrors transform_data_nx.py) ─────────────────

ENGAGEMENT_STATUS_MAP = {
    "in progress": "Active",
    "active":      "Active",
    "complete":    "Completed",
    "not started": "Planned",
    "on hold":     "On Hold",
}

DELIVERABLE_STATUS_MAP = {
    "working on it": "In Progress",
    "in progress":   "In Progress",
    "to do":         "To Do",
    "not started":   "To Do",
    "in review":     "Internal Review",
    "done":          "Delivered",
}

REQUIRED_ENG_FIELDS = [
    "engagement_id", "engagement_name", "client", "engagement_lead",
    "engagement_start", "engagement_end", "budget", "engagement_status",
]
REQUIRED_DEL_FIELDS = [
    "deliverable_id", "engagement_id", "deliverable_name", "assignee",
    "due_date", "priority", "deliverable_status", "hours_estimated",
]


# ── Report ────────────────────────────────────────────────────────────────────

class Report:
    def __init__(self):
        self._lines = []
        self.passed = self.warnings = self.failures = 0

    def section(self, title):
        pad = "─" * max(0, 66 - len(title))
        self._lines.append(("section", f"── {title} {pad}"))

    def ok(self,   msg): self._lines.append(("ok",   msg)); self.passed   += 1
    def warn(self, msg): self._lines.append(("warn", msg)); self.warnings += 1
    def fail(self, msg): self._lines.append(("fail", msg)); self.failures += 1
    def info(self, msg): self._lines.append(("info", msg))

    def _render_lines(self):
        """Return the full report as a list of plain strings."""
        W = 70
        icons = {"ok": "✓", "warn": "⚠", "fail": "✗", "info": " ", "section": ""}
        out = []
        out.append("=" * W)
        out.append("  DATA MIGRATION VALIDATION REPORT")
        out.append(f"  Generated : {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}")
        out.append("=" * W)
        for kind, msg in self._lines:
            if kind == "section":
                out.append(f"\n{msg}")
            else:
                out.append(f"  {icons[kind]}  {msg}")
        out.append("\n" + "=" * W)
        status = "PASS" if self.failures == 0 else "FAIL"
        out.append(f"  {status}  —  {self.passed} passed | {self.warnings} warnings | {self.failures} failures")
        out.append("=" * W)
        return out

    def print_report(self):
        print()
        for line in self._render_lines():
            print(line)
        print()

    def save_report(self, path):
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(self._render_lines()) + "\n")
        print(f"Report saved -> {path}")


# ── Source CSV parser (handles wrapped lines) ─────────────────────────────────

def _reassemble_rows(raw_lines):
    non_empty = [l.rstrip("\r\n") for l in raw_lines if l.strip()]
    logical, current, in_header = [], None, True
    for line in non_empty:
        if line.startswith("engagement_id"):
            if current is not None:
                logical.append(current)
            current, in_header = line, True
        elif re.match(r"^ENG-\d+,", line):
            if current is not None:
                logical.append(current)
            current, in_header = line, False
        else:
            if current is not None:
                current += ("" if in_header else " ") + line
    if current is not None:
        logical.append(current)
    return logical


def load_source(path):
    """Returns (engagements_dict, deliverables_list) from the raw source CSV."""
    with open(path, encoding="utf-8") as f:
        raw = f.readlines()
    reader = csv.DictReader(io.StringIO("\n".join(_reassemble_rows(raw))))
    engagements, deliverables = {}, []
    for row in reader:
        eid = row["engagement_id"].strip()
        if eid not in engagements:
            engagements[eid] = {k: v.strip() for k, v in row.items()
                                if k in REQUIRED_ENG_FIELDS + ["engagement_name"]}
        deliverables.append({k: v.strip() for k, v in row.items()
                              if k in REQUIRED_DEL_FIELDS})
    return engagements, deliverables


def load_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ── Monday.com API ────────────────────────────────────────────────────────────

def monday_request(api_key, query, variables=None):
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
        "API-Version": "2024-10",
    }
    resp = requests.post(MONDAY_API_URL,
                         json={"query": query, **({"variables": variables} if variables else {})},
                         headers=headers)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"Monday API error: {data['errors']}")
    return data


def fetch_board_items(api_key, board_id):
    query = """
    query ($board_id: ID!) {
        boards(ids: [$board_id]) {
            items_page(limit: 500) {
                items {
                    id
                    name
                    column_values {
                        id text value
                        ... on BoardRelationValue { linked_item_ids }
                    }
                }
            }
        }
    }
    """
    data = monday_request(api_key, query, {"board_id": board_id})
    return data["data"]["boards"][0]["items_page"]["items"]


def flatten_monday_item(item, col_map):
    """Turn a Monday item into a flat dict using the column map."""
    row = {"_monday_id": item["id"], col_map.get("name", "name"): item["name"]}
    for cv in item["column_values"]:
        field = col_map.get(cv["id"])
        if field:
            row[field] = cv["text"] or ""
            if field == "engagement_link":
                row["_linked_ids"] = [str(i) for i in cv.get("linked_item_ids") or []]
    return row


# ── Helpers ───────────────────────────────────────────────────────────────────

def normalise_date(date_str):
    """Accepts MM/DD/YYYY or YYYY-MM-DD, returns YYYY-MM-DD or None."""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def normalise_number(val):
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


# ── Validation checks ─────────────────────────────────────────────────────────

def check_record_counts(r, src_eng, src_del, clean_eng, clean_del, mon_eng, mon_del):
    r.section("1 · RECORD COUNTS")

    src_e, src_d = len(src_eng), len(src_del)
    cln_e, cln_d = len(clean_eng), len(clean_del)
    mon_e, mon_d = len(mon_eng), len(mon_del)

    r.info(f"{'Layer':<25} {'Engagements':>12} {'Deliverables':>14}")
    r.info(f"{'Source CSV':<25} {src_e:>12} {src_d:>14}")
    r.info(f"{'Cleaned CSVs':<25} {cln_e:>12} {cln_d:>14}")
    r.info(f"{'Monday.com':<25} {mon_e:>12} {mon_d:>14}")

    if src_e == cln_e == mon_e:
        r.ok(f"Engagement count consistent across all layers ({src_e})")
    else:
        if src_e != cln_e:
            r.fail(f"Engagement count mismatch: source={src_e}, cleaned={cln_e}")
        if src_e != mon_e:
            r.fail(f"Engagement count mismatch: source={src_e}, Monday={mon_e}")

    if src_d == cln_d == mon_d:
        r.ok(f"Deliverable count consistent across all layers ({src_d})")
    else:
        if src_d != cln_d:
            r.fail(f"Deliverable count mismatch: source={src_d}, cleaned={cln_d}")
        if src_d != mon_d:
            r.fail(f"Deliverable count mismatch: source={src_d}, Monday={mon_d}")


def check_single_deliverables(r, clean_del):
    r.section("2 · SINGLE-DELIVERABLE ENGAGEMENTS")
    counts = defaultdict(int)
    for row in clean_del:
        counts[row["engagement_id"]] += 1
    singles = [eid for eid, c in counts.items() if c == 1]
    if singles:
        r.warn(f"{len(singles)} engagement(s) have only one deliverable:")
        for eid in singles:
            r.info(f"  {eid}")
    else:
        r.ok("No single-deliverable engagements found")


def check_missing_fields(r, src_eng, src_del, clean_eng, clean_del, mon_eng_rows, mon_del_rows):
    r.section("3 · MISSING / EMPTY FIELDS")

    # Source engagements
    issues = []
    for eid, eng in src_eng.items():
        for f in REQUIRED_ENG_FIELDS + ["engagement_name"]:
            if not eng.get(f, "").strip():
                issues.append(f"Source engagement {eid}: '{f}' is empty")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("Source CSV — no missing engagement fields")

    # Source deliverables
    issues = []
    for row in src_del:
        for f in REQUIRED_DEL_FIELDS:
            if not row.get(f, "").strip():
                issues.append(f"Source deliverable {row.get('deliverable_id','?')}: '{f}' is empty")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("Source CSV — no missing deliverable fields")

    # Cleaned engagements
    issues = []
    for row in clean_eng:
        for f in ["engagement_id", "client", "engagement_lead", "engagement_start",
                  "engagement_end", "budget", "engagement_status"]:
            if not row.get(f, "").strip():
                issues.append(f"Cleaned engagement {row.get('engagement_id','?')}: '{f}' is empty")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("Cleaned engagements.csv — no missing fields")

    # Cleaned deliverables
    issues = []
    for row in clean_del:
        for f in REQUIRED_DEL_FIELDS:
            if not row.get(f, "").strip():
                issues.append(f"Cleaned deliverable {row.get('deliverable_id','?')}: '{f}' is empty")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("Cleaned deliverables.csv — no missing fields")

    # Monday engagements
    mon_eng_required = ["engagement_id", "client", "engagement_lead",
                         "engagement_status", "engagement_start", "engagement_end", "budget"]
    issues = []
    for item in mon_eng_rows:
        for f in mon_eng_required:
            if not item.get(f, "").strip():
                issues.append(f"Monday engagement '{item.get('client','?')}': '{f}' is empty")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("Monday.com engagements — no missing fields")

    # Monday deliverables
    mon_del_required = ["deliverable_id", "deliverable_name", "assignee",
                         "deliverable_status", "due_date", "hours_estimated", "priority"]
    issues = []
    for item in mon_del_rows:
        for f in mon_del_required:
            if not item.get(f, "").strip():
                issues.append(f"Monday deliverable '{item.get('deliverable_name','?')}': '{f}' is empty")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("Monday.com deliverables — no missing fields")


def check_status_normalisation(r, src_eng, src_del, clean_eng, clean_del):
    r.section("4 · STATUS NORMALISATION  (source → cleaned CSVs)")

    clean_eng_idx = {row["engagement_id"]: row for row in clean_eng}
    clean_del_idx = {row["deliverable_id"]: row for row in clean_del}

    issues = []
    for eid, eng in src_eng.items():
        raw   = eng["engagement_status"].lower()
        expected = ENGAGEMENT_STATUS_MAP.get(raw)
        if expected is None:
            issues.append(f"Engagement {eid}: unknown source status '{eng['engagement_status']}'")
            continue
        actual = clean_eng_idx.get(eid, {}).get("engagement_status", "")
        if actual != expected:
            issues.append(f"Engagement {eid}: expected '{expected}', got '{actual}'")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("All engagement statuses normalised correctly")

    issues = []
    for row in src_del:
        did  = row["deliverable_id"]
        raw  = row["deliverable_status"].lower()
        expected = DELIVERABLE_STATUS_MAP.get(raw)
        if expected is None:
            issues.append(f"Deliverable {did}: unknown source status '{row['deliverable_status']}'")
            continue
        actual = clean_del_idx.get(did, {}).get("deliverable_status", "")
        if actual != expected:
            issues.append(f"Deliverable {did}: expected '{expected}', got '{actual}'")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("All deliverable statuses normalised correctly")


def check_field_values(r, src_eng, src_del, clean_eng, clean_del):
    r.section("5 · FIELD CROSS-CHECK  (source vs cleaned CSVs)")

    clean_eng_idx = {row["engagement_id"]: row for row in clean_eng}
    clean_del_idx = {row["deliverable_id"]: row for row in clean_del}

    issues = []
    for eid, src in src_eng.items():
        cln = clean_eng_idx.get(eid)
        if not cln:
            issues.append(f"Engagement {eid} missing from cleaned CSV"); continue
        for src_f, cln_f in [("client", "client"), ("engagement_lead", "engagement_lead"),
                               ("engagement_name", "engagement_name")]:
            if src.get(src_f, "").strip() != cln.get(cln_f, "").strip():
                issues.append(f"Engagement {eid} '{cln_f}': "
                              f"source='{src.get(src_f)}' vs cleaned='{cln.get(cln_f)}'")
        # Budget as number
        if normalise_number(src.get("budget")) != normalise_number(cln.get("budget")):
            issues.append(f"Engagement {eid} 'budget': "
                          f"source='{src.get('budget')}' vs cleaned='{cln.get('budget')}'")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("Engagement non-status fields match between source and cleaned CSV")

    issues = []
    for src in src_del:
        did = src["deliverable_id"]
        cln = clean_del_idx.get(did)
        if not cln:
            issues.append(f"Deliverable {did} missing from cleaned CSV"); continue
        for f in ["deliverable_name", "assignee", "engagement_id", "priority"]:
            if src.get(f, "").strip() != cln.get(f, "").strip():
                issues.append(f"Deliverable {did} '{f}': "
                              f"source='{src.get(f)}' vs cleaned='{cln.get(f)}'")
        if normalise_number(src.get("hours_estimated")) != normalise_number(cln.get("hours_estimated")):
            issues.append(f"Deliverable {did} 'hours_estimated': "
                          f"source='{src.get('hours_estimated')}' vs cleaned='{cln.get('hours_estimated')}'")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("Deliverable non-status fields match between source and cleaned CSV")


def check_date_formats(r, src_eng, src_del, clean_eng, clean_del, mon_eng_rows, mon_del_rows):
    r.section("6 · DATE FORMAT VALIDATION")

    mdy = re.compile(r"^\d{2}/\d{2}/\d{4}$")
    iso = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    # Source & cleaned CSVs should be MM/DD/YYYY
    issues = []
    for eid, eng in src_eng.items():
        for f in ("engagement_start", "engagement_end"):
            v = eng.get(f, "")
            if not mdy.match(v):
                issues.append(f"Source engagement {eid} '{f}': unexpected format '{v}'")
    for row in src_del:
        if not mdy.match(row.get("due_date", "")):
            issues.append(f"Source deliverable {row['deliverable_id']} 'due_date': "
                          f"unexpected format '{row.get('due_date')}'")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("Source CSV dates are all MM/DD/YYYY")

    issues = []
    for row in clean_eng:
        for f in ("engagement_start", "engagement_end"):
            if not mdy.match(row.get(f, "")):
                issues.append(f"Cleaned engagement {row['engagement_id']} '{f}': "
                              f"unexpected format '{row.get(f)}'")
    for row in clean_del:
        if not mdy.match(row.get("due_date", "")):
            issues.append(f"Cleaned deliverable {row['deliverable_id']} 'due_date': "
                          f"unexpected format '{row.get('due_date')}'")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("Cleaned CSVs dates are all MM/DD/YYYY")

    # Monday should be YYYY-MM-DD
    issues = []
    for item in mon_eng_rows:
        for f in ("engagement_start", "engagement_end"):
            v = item.get(f, "")
            if v and not iso.match(v):
                issues.append(f"Monday engagement '{item.get('client')}' '{f}': "
                              f"unexpected format '{v}'")
    for item in mon_del_rows:
        v = item.get("due_date", "")
        if v and not iso.match(v):
            issues.append(f"Monday deliverable '{item.get('deliverable_name')}' 'due_date': "
                          f"unexpected format '{v}'")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("Monday.com dates are all YYYY-MM-DD")


def check_numeric_fields(r, clean_eng, clean_del, mon_eng_rows, mon_del_rows):
    r.section("7 · NUMERIC FIELD VALIDATION")

    issues = []
    for row in clean_eng:
        if normalise_number(row.get("budget")) is None:
            issues.append(f"Cleaned engagement {row['engagement_id']}: non-numeric budget '{row.get('budget')}'")
    for row in clean_del:
        if normalise_number(row.get("hours_estimated")) is None:
            issues.append(f"Cleaned deliverable {row['deliverable_id']}: "
                          f"non-numeric hours_estimated '{row.get('hours_estimated')}'")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("Cleaned CSV — all numeric fields are valid numbers")

    issues = []
    for item in mon_eng_rows:
        if normalise_number(item.get("budget")) is None:
            issues.append(f"Monday engagement '{item.get('client')}': non-numeric budget '{item.get('budget')}'")
    for item in mon_del_rows:
        if normalise_number(item.get("hours_estimated")) is None:
            issues.append(f"Monday deliverable '{item.get('deliverable_name')}': "
                          f"non-numeric hours_estimated '{item.get('hours_estimated')}'")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok("Monday.com — all numeric fields are valid numbers")


def check_relationships(r, clean_eng, clean_del, mon_del_rows, lookup):
    r.section("8 · RELATIONSHIP INTEGRITY")

    eng_ids = {row["engagement_id"] for row in clean_eng}

    # Every deliverable points to a known engagement
    orphans = [row["deliverable_id"] for row in clean_del
               if row["engagement_id"] not in eng_ids]
    if orphans:
        r.fail(f"Cleaned CSV: {len(orphans)} orphaned deliverable(s) with unknown engagement_id:")
        for d in orphans: r.info(f"  {d}")
    else:
        r.ok("Cleaned CSV — every deliverable links to a valid engagement")

    # Monday board_relation links point to known Monday item IDs
    known_monday_ids = {str(v) for v in lookup.values()}
    unlinked, wrong_link = [], []
    for item in mon_del_rows:
        linked = item.get("_linked_ids", [])
        if not linked:
            unlinked.append(item.get("deliverable_name", "?"))
        else:
            for lid in linked:
                if lid not in known_monday_ids:
                    wrong_link.append(
                        f"{item.get('deliverable_name','?')} links to unknown item ID {lid}"
                    )
    if unlinked:
        r.fail(f"Monday.com: {len(unlinked)} deliverable(s) have no board_relation set:")
        for d in unlinked: r.info(f"  {d}")
    else:
        r.ok("Monday.com — every deliverable has a board_relation set")

    if wrong_link:
        for w in wrong_link: r.fail(w)
    else:
        r.ok("Monday.com — all board_relation links point to known engagement items")

    # Reverse: every engagement has at least one deliverable on Monday
    linked_eng_ids = set()
    for item in mon_del_rows:
        for lid in item.get("_linked_ids", []):
            linked_eng_ids.add(lid)
    missing_links = [eid for eid, mid in lookup.items()
                     if str(mid) not in linked_eng_ids]
    if missing_links:
        r.warn(f"{len(missing_links)} Monday engagement(s) have no linked deliverables:")
        for e in missing_links: r.info(f"  {e}")
    else:
        r.ok("Monday.com — every engagement has at least one linked deliverable")


def check_monday_field_mapping(r, clean_eng, clean_del, mon_eng_rows, mon_del_rows, lookup):
    r.section("9 · MONDAY.COM FIELD MAPPING  (cleaned CSVs vs live data)")

    # Index Monday rows by engagement_id / deliverable_id text columns
    mon_eng_idx = {item.get("engagement_id", ""): item for item in mon_eng_rows}
    mon_del_idx = {item.get("deliverable_id", ""): item for item in mon_del_rows}

    issues = []
    for row in clean_eng:
        eid  = row["engagement_id"]
        mrow = mon_eng_idx.get(eid)
        if not mrow:
            issues.append(f"Engagement {eid} not found on Monday by engagement_id column")
            continue
        # client (item name)
        if row["client"].strip() != mrow.get("client", "").strip():
            issues.append(f"Engagement {eid} 'client': "
                          f"CSV='{row['client']}' vs Monday='{mrow.get('client')}'")
        # engagement_lead
        if row["engagement_lead"].strip() != mrow.get("engagement_lead", "").strip():
            issues.append(f"Engagement {eid} 'engagement_lead': "
                          f"CSV='{row['engagement_lead']}' vs Monday='{mrow.get('engagement_lead')}'")
        # status
        if row["engagement_status"].strip() != mrow.get("engagement_status", "").strip():
            issues.append(f"Engagement {eid} 'engagement_status': "
                          f"CSV='{row['engagement_status']}' vs Monday='{mrow.get('engagement_status')}'")
        # dates (normalise both to ISO)
        for csv_f, mon_f in [("engagement_start", "engagement_start"),
                              ("engagement_end",   "engagement_end")]:
            cv = normalise_date(row.get(csv_f, ""))
            mv = normalise_date(mrow.get(mon_f, ""))
            if cv != mv:
                issues.append(f"Engagement {eid} '{csv_f}': CSV='{row.get(csv_f)}' vs Monday='{mrow.get(mon_f)}'")
        # budget
        if normalise_number(row.get("budget")) != normalise_number(mrow.get("budget")):
            issues.append(f"Engagement {eid} 'budget': "
                          f"CSV='{row.get('budget')}' vs Monday='{mrow.get('budget')}'")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok(f"All {len(clean_eng)} engagement fields match Monday.com exactly")

    issues = []
    for row in clean_del:
        did  = row["deliverable_id"]
        mrow = mon_del_idx.get(did)
        if not mrow:
            issues.append(f"Deliverable {did} not found on Monday by deliverable_id column")
            continue
        # name
        if row["deliverable_name"].strip() != mrow.get("deliverable_name", "").strip():
            issues.append(f"Deliverable {did} 'deliverable_name': "
                          f"CSV='{row['deliverable_name']}' vs Monday='{mrow.get('deliverable_name')}'")
        # assignee
        if row["assignee"].strip() != mrow.get("assignee", "").strip():
            issues.append(f"Deliverable {did} 'assignee': "
                          f"CSV='{row['assignee']}' vs Monday='{mrow.get('assignee')}'")
        # status
        if row["deliverable_status"].strip() != mrow.get("deliverable_status", "").strip():
            issues.append(f"Deliverable {did} 'deliverable_status': "
                          f"CSV='{row['deliverable_status']}' vs Monday='{mrow.get('deliverable_status')}'")
        # priority
        if row["priority"].strip() != mrow.get("priority", "").strip():
            issues.append(f"Deliverable {did} 'priority': "
                          f"CSV='{row['priority']}' vs Monday='{mrow.get('priority')}'")
        # due_date
        if normalise_date(row.get("due_date", "")) != normalise_date(mrow.get("due_date", "")):
            issues.append(f"Deliverable {did} 'due_date': "
                          f"CSV='{row.get('due_date')}' vs Monday='{mrow.get('due_date')}'")
        # hours_estimated
        if normalise_number(row.get("hours_estimated")) != normalise_number(mrow.get("hours_estimated")):
            issues.append(f"Deliverable {did} 'hours_estimated': "
                          f"CSV='{row.get('hours_estimated')}' vs Monday='{mrow.get('hours_estimated')}'")
    if issues:
        for i in issues: r.fail(i)
    else:
        r.ok(f"All {len(clean_del)} deliverable fields match Monday.com exactly")


# ── Entry point ───────────────────────────────────────────────────────────────

def main(api_key):
    r = Report()

    print("Loading source CSV...")
    src_eng, src_del = load_source(SOURCE_CSV)

    print("Loading cleaned CSVs...")
    clean_eng = load_csv(ENGAGEMENTS_CSV)
    clean_del = load_csv(DELIVERABLES_CSV)

    print("Loading engagement lookup...")
    with open(LOOKUP_JSON, encoding="utf-8") as f:
        lookup = json.load(f)   # {engagement_id: monday_item_id}

    print("Querying Monday.com boards...")
    raw_mon_eng = fetch_board_items(api_key, ENGAGEMENTS_BOARD_ID)
    raw_mon_del = fetch_board_items(api_key, DELIVERABLES_BOARD_ID)

    mon_eng_rows = [flatten_monday_item(i, ENG_COL_MAP) for i in raw_mon_eng]
    mon_del_rows = [flatten_monday_item(i, DEL_COL_MAP) for i in raw_mon_del]

    print("Running validation checks...\n")

    check_record_counts(r, src_eng, src_del, clean_eng, clean_del, mon_eng_rows, mon_del_rows)
    check_single_deliverables(r, clean_del)
    check_missing_fields(r, src_eng, src_del, clean_eng, clean_del, mon_eng_rows, mon_del_rows)
    check_status_normalisation(r, src_eng, src_del, clean_eng, clean_del)
    check_field_values(r, src_eng, src_del, clean_eng, clean_del)
    check_date_formats(r, src_eng, src_del, clean_eng, clean_del, mon_eng_rows, mon_del_rows)
    check_numeric_fields(r, clean_eng, clean_del, mon_eng_rows, mon_del_rows)
    check_relationships(r, clean_eng, clean_del, mon_del_rows, lookup)
    check_monday_field_mapping(r, clean_eng, clean_del, mon_eng_rows, mon_del_rows, lookup)

    r.print_report()
    r.save_report(REPORT_OUTPUT)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: py validate_migration.py <MONDAY_API_KEY>")
        sys.exit(1)
    main(sys.argv[1])
