#!/usr/bin/env python3
"""
Audit performer Commons imagery for wrong-category contamination.

Background: earlier runs of the Commons imagery enricher fell back to a blind
``Category:<Name>`` guess when Wikidata had no Commons-category claim. For
common names that picked up an *unrelated* same-named person's category (e.g.
an archaeologist's catalogued coin finds rather than photos of a musician).
The resolver no longer does this — it requires either the performer's own
Wikipedia article or a Wikidata hit verified as a human (P31=Q5) — but images
already linked by the old path are still in the database.

This script is READ-ONLY. For every performer that currently has at least one
``wikimedia_commons`` image it:

  1. Re-runs the *current* resolver (core.commons_imagery.resolve_commons_category)
     with the same Wikipedia/external-link inputs the worker uses.
  2. Classifies each existing Commons image:
       NO_CATEGORY            - resolver now returns nothing, so the image could
                                only have come from the removed guess path.
                                Whole performer is suspect.
       NOT_IN_RESOLVED_CATEGORY - resolver returns a category, but this image's
                                Commons pageid is NOT a member of it (walked at
                                the worker's recurse depth of 0). Strong signal
                                the image came from a different/old category.
       OK                     - image's pageid is in the resolved category.

Flagged rows are written to a CSV worklist for manual review / cleanup. The
script never deletes anything.

Usage:
    python scripts/audit_commons_imagery.py                     # full sweep
    python scripts/audit_commons_imagery.py --limit 200
    python scripts/audit_commons_imagery.py --name "Andrew Williams"
    python scripts/audit_commons_imagery.py --id <performer-uuid>
    python scripts/audit_commons_imagery.py --since 2026-06-09T17:00:00
    python scripts/audit_commons_imagery.py --all -o all_commons.csv
"""

import csv
from datetime import datetime

from script_base import ScriptBase, run_script
from db_utils import get_db_connection
from core import commons_imagery as ci

# Mirror the worker's GatherConfig recurse depth (research_worker/handlers/commons.py
# builds GatherConfig() without overriding recurse_subcats, so it stays 0).
_RECURSE_SUBCATS = ci.GatherConfig().recurse_subcats
_ACCEPTED_LICENSES = list(ci.GatherConfig().licenses)

_PERFORMERS_WITH_COMMONS_SQL = """
    SELECT
        p.id,
        p.name,
        p.wikipedia_url,
        p.external_links,
        i.id   AS image_id,
        i.url  AS image_url,
        i.source_identifier,
        i.source_page_url,
        ai.is_primary,
        ai.created_at AS linked_at
    FROM artist_images ai
    JOIN images i      ON i.id = ai.image_id
    JOIN performers p  ON p.id = ai.performer_id
    WHERE i.source = 'wikimedia_commons'
    {where}
    ORDER BY p.name, ai.display_order
"""


def _wikipedia_url(row) -> str | None:
    """Same precedence the worker uses: explicit column, then external_links."""
    direct = (row.get("wikipedia_url") or "").strip()
    if direct:
        return direct
    links = row.get("external_links") or {}
    if isinstance(links, dict):
        return (links.get("wikipedia") or "").strip() or None
    return None


def _load_rows(name=None, performer_id=None, since=None, limit=None):
    clauses, params = [], []
    if performer_id:
        clauses.append("p.id = %s")
        params.append(performer_id)
    if name:
        clauses.append("LOWER(p.name) = LOWER(%s)")
        params.append(name)
    if since:
        clauses.append("ai.created_at >= %s")
        params.append(since)
    where = ("AND " + " AND ".join(clauses)) if clauses else ""
    sql = _PERFORMERS_WITH_COMMONS_SQL.format(where=where)
    if limit:
        sql += "\n    LIMIT %s"
        params.append(limit)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall()


def _group_by_performer(rows):
    """rows -> {performer_id: {"meta": row, "images": [rows]}} preserving order."""
    grouped = {}
    for r in rows:
        pid = str(r["id"])
        grouped.setdefault(pid, {"meta": r, "images": []})["images"].append(r)
    return grouped


def _resolved_category_pageids(session, category):
    """Return the set of Commons pageids in `category` (as strings)."""
    records = ci.fetch_commons_category_files(
        session, category, _ACCEPTED_LICENSES, include_nkcr=False,
        recurse_subcats=_RECURSE_SUBCATS,
    )
    return {str(r.source_identifier) for r in records}


def main() -> bool:
    script = ScriptBase(
        name="audit_commons_imagery",
        description="Audit performer Commons imagery for wrong-category contamination",
        epilog=__doc__,
    )
    group = script.parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--name", help="Audit a single performer by name")
    group.add_argument("--id", help="Audit a single performer by UUID")
    script.parser.add_argument("--since", default=None,
                               help="Only images linked at/after this ISO timestamp "
                                    "(e.g. 2026-06-09T17:00:00). Useful to focus on "
                                    "a specific enrichment run.")
    script.parser.add_argument("--limit", type=int, default=None,
                               help="Cap the number of image rows scanned")
    script.parser.add_argument("--all", action="store_true",
                               help="Include OK rows in the CSV (default: flagged only)")
    script.parser.add_argument("-o", "--output", default=None,
                               help="Output CSV path (default: "
                                    "commons_imagery_audit_<ts>.csv)")
    args = script.parse_args()

    script.print_header({
        "SINGLE": args.name or args.id or False,
        "SINCE": args.since or False,
        "LIMIT": args.limit or False,
        "INCLUDE OK": args.all,
    })

    rows = _load_rows(name=args.name, performer_id=args.id,
                      since=args.since, limit=args.limit)
    if not rows:
        script.logger.info("No wikimedia_commons images matched the filters.")
        return True

    grouped = _group_by_performer(rows)
    script.logger.info("Scanning %d image(s) across %d performer(s)",
                       len(rows), len(grouped))

    session = ci.make_session()
    out_path = args.output or (
        f"commons_imagery_audit_{datetime.now():%Y%m%d_%H%M%S}.csv")

    counts = {"NO_CATEGORY": 0, "NOT_IN_RESOLVED_CATEGORY": 0, "OK": 0}
    fieldnames = [
        "performer_id", "performer_name", "verdict", "resolved_category",
        "image_id", "image_url", "source_identifier", "source_page_url",
        "is_primary", "linked_at",
    ]
    written = 0

    with open(out_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()

        for pid, bundle in grouped.items():
            meta = bundle["meta"]
            name = meta["name"]
            category = ci.resolve_commons_category(
                session, name, _wikipedia_url(meta))

            member_pageids = set()
            if category:
                try:
                    member_pageids = _resolved_category_pageids(session, category)
                except Exception as e:  # network/category hiccup -> don't crash the sweep
                    script.logger.warning(
                        "Could not list %s for %s (%s); treating members as unknown",
                        category, name, e)

            for img in bundle["images"]:
                if category is None:
                    verdict = "NO_CATEGORY"
                elif str(img["source_identifier"]) in member_pageids:
                    verdict = "OK"
                else:
                    # Either the image isn't in the resolved category, or the
                    # category couldn't be enumerated above (member_pageids
                    # empty) — both warrant a manual look rather than a pass.
                    verdict = "NOT_IN_RESOLVED_CATEGORY"
                counts[verdict] += 1

                if verdict == "OK" and not args.all:
                    continue
                writer.writerow({
                    "performer_id": pid,
                    "performer_name": name,
                    "verdict": verdict,
                    "resolved_category": category or "",
                    "image_id": str(img["image_id"]),
                    "image_url": img["image_url"],
                    "source_identifier": img["source_identifier"],
                    "source_page_url": img["source_page_url"] or "",
                    "is_primary": img["is_primary"],
                    "linked_at": img["linked_at"].isoformat() if img["linked_at"] else "",
                })
                written += 1

    flagged = counts["NO_CATEGORY"] + counts["NOT_IN_RESOLVED_CATEGORY"]
    script.logger.info("Done. flagged=%d (NO_CATEGORY=%d, "
                       "NOT_IN_RESOLVED_CATEGORY=%d), ok=%d",
                       flagged, counts["NO_CATEGORY"],
                       counts["NOT_IN_RESOLVED_CATEGORY"], counts["OK"])
    script.logger.info("Wrote %d row(s) to %s", written, out_path)
    return True


if __name__ == "__main__":
    run_script(main)
