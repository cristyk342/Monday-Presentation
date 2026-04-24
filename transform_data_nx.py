import csv
import io
import re

# maps old inconsistent smartsheet status values to clean standardized monday labels
# dictionary used instead of if/else so new statuses can be added in one line
ENGAGEMENT_STATUS_MAP = {
    'in progress': 'Active',
    'active': 'Active',
    'complete': 'Completed',
    'not started': 'Planned',
    'on hold': 'On Hold',
}

# same approach for deliverable statuses — separate map because engagement and
# deliverable statuses mean different things and should never be mixed
DELIVERABLE_STATUS_MAP = {
    'working on it': 'In Progress',
    'in progress': 'In Progress',
    'to do': 'To Do',
    'not started': 'To Do',
    'in review': 'Internal Review',
    'done': 'Delivered',
}


def _reassemble_rows(raw_lines):
    """
    Re-join lines that were wrapped mid-cell by the CSV exporter.
    Header continuations are joined without a space (mid-word breaks like 'engagemen\nt_end').
    Data continuations are joined with a space (inter-word breaks like 'Sarah\nChen').
    """
    # strip blank lines first so we're only working with real content
    non_empty = [line.rstrip('\r\n') for line in raw_lines if line.strip()]
    logical_rows = []
    current = None
    in_header = True

    for line in non_empty:
        if line.startswith('engagement_id'):
            # this is the header row — save whatever we had and start fresh
            if current is not None:
                logical_rows.append(current)
            current = line
            in_header = True
        elif re.match(r'^ENG-\d+,', line):
            # new data row starting — save previous and start building this one
            if current is not None:
                logical_rows.append(current)
            current = line
            in_header = False
        else:
            if current is not None:
                # continuation of the previous line — join with space for data, no space for headers
                sep = '' if in_header else ' '
                current += sep + line

    if current is not None:
        logical_rows.append(current)

    return logical_rows


def transform_data_nx(csv_file_path):
    """
    Parse a flat Smartsheet CSV export and split it into two relational structures.

    Returns:
        engagements (dict): {engagement_id -> engagement attribute dict}
        deliverables (list): list of deliverable dicts, each with engagement_id as FK
    """
    with open(csv_file_path, 'r', encoding='utf-8') as f:
        raw_lines = f.readlines()

    # fix wrapped lines before passing to the CSV parser
    logical_rows = _reassemble_rows(raw_lines)
    reader = csv.DictReader(io.StringIO('\n'.join(logical_rows)))

    # dict for engagements so we can check for duplicates by ID instantly
    engagements = {}
    # list for deliverables since we just need to collect them all in order
    deliverables = []

    for row in reader:
        eng_id = row['engagement_id'].strip()

        # normalize to lowercase before looking up — handles 'Active' vs 'active' etc.
        eng_status_raw = row['engagement_status'].strip().lower()
        # if status isn't in the map, fall back to the raw value rather than crashing
        eng_status = ENGAGEMENT_STATUS_MAP.get(eng_status_raw, row['engagement_status'].strip())

        if eng_id not in engagements:
            # only store the first occurrence — engagement info is repeated on every
            # deliverable row in the flat export so we dedupe here
            engagements[eng_id] = {
                'engagement_id': eng_id,
                'engagement_name': row['engagement_name'].strip(),
                'client': row['client'].strip(),
                'engagement_lead': row['engagement_lead'].strip(),
                'engagement_start': row['engagement_start'].strip(),
                'engagement_end': row['engagement_end'].strip(),
                'budget': int(row['budget'].strip()),  # cast to int — no fractional budgets
                'engagement_status': eng_status,
            }

        del_status_raw = row['deliverable_status'].strip().lower()
        # unknown status falls back to raw value — validation script will flag it
        del_status = DELIVERABLE_STATUS_MAP.get(del_status_raw, row['deliverable_status'].strip())

        deliverables.append({
            'deliverable_id': row['deliverable_id'].strip(),
            'engagement_id': eng_id,  # foreign key linking deliverable to its parent engagement
            'deliverable_name': row['deliverable_name'].strip(),
            'assignee': row['assignee'].strip(),
            'due_date': row['due_date'].strip(),
            'priority': row['priority'].strip(),
            'deliverable_status': del_status,
            'hours_estimated': int(row['hours_estimated'].strip()),  # whole hours only
        })

    return engagements, deliverables


def export_to_csv(engagements, deliverables, output_dir):
    # writing to CSV creates a checkpoint — if the monday upload fails later
    # we can re-run from here without re-transforming the raw source data
    import os

    eng_path = os.path.join(output_dir, 'engagements.csv')
    eng_fields = ['engagement_id', 'engagement_name', 'client', 'engagement_lead',
                  'engagement_start', 'engagement_end', 'budget', 'engagement_status']
    with open(eng_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=eng_fields)
        writer.writeheader()
        writer.writerows(engagements.values())

    del_path = os.path.join(output_dir, 'deliverables.csv')
    del_fields = ['deliverable_id', 'engagement_id', 'deliverable_name', 'assignee',
                  'due_date', 'priority', 'deliverable_status', 'hours_estimated']
    with open(del_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=del_fields)
        writer.writeheader()
        writer.writerows(deliverables)

    return eng_path, del_path


# stops the migration from running automatically if this file is imported by another script
# keeps the functions reusable without triggering the whole pipeline by accident
if __name__ == '__main__':
    import sys
    import os

    path = sys.argv[1] if len(sys.argv) > 1 else r'C:\Users\nicol\Downloads\nexus_smartsheet_export.csv'
    desktop = os.path.join(os.path.expanduser('~'), 'Desktop')

    engagements, deliverables = transform_data_nx(path)
    eng_path, del_path = export_to_csv(engagements, deliverables, desktop)

    print(f'Engagements ({len(engagements)} rows) -> {eng_path}')
    print(f'Deliverables ({len(deliverables)} rows) -> {del_path}')
