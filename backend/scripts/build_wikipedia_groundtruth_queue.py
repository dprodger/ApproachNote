#!/usr/bin/env python3
"""
Build a verification *queue* for manually grounding performer -> Wikipedia links.

Target: performers that HAVE Commons imagery but NO Wikipedia link on record.
These are the highest-value enrichment opportunities — we already have visual
evidence of who the person is (the Commons photos), we just haven't recorded the
authoritative Wikipedia article. A human can confirm the link quickly and that
confirmation becomes ground truth (a different, more trustworthy class of data
than anything a crawler guesses).

For each such performer this script derives candidate Wikipedia links:

  1. CATEGORY-DERIVED (the "implicit" link): walk the Commons categories the
     performer's own image files sit in, map each category to its Wikidata item
     (Commons pageprops.wikibase_item), and take that item's English Wikipedia
     sitelink. This ties the candidate directly to the evidence we already hold.
  2. NAME-SEARCH FALLBACK: when no category yields a real biography article,
     search Wikidata by the performer's name and surface human/group hits that
     do have an English Wikipedia article.

Junk is filtered: "Wikimedia category" items (P31=Q4167836) and sitelinks that
are themselves Category: pages are dropped — they are topic categories, not
people (e.g. Category:Public speaking).

Output is a queue JSON under data/ground_truth/, consumed by
build_wikipedia_groundtruth_viewer.py to produce the human-verification UI.
This script only READS the database and public Wikimedia APIs; it writes nothing
back. The human's decisions become the ground-truth file (exported from the
viewer) — this is just the worklist.

Usage:
    python scripts/build_wikipedia_groundtruth_queue.py --limit 50
    python scripts/build_wikipedia_groundtruth_queue.py            # full subset
    python scripts/build_wikipedia_groundtruth_queue.py -o /tmp/queue.json
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))  # make core.* / db_utils importable (script_base does this too)
load_dotenv(BACKEND_DIR / ".env")

from core import commons_imagery as ci  # noqa: E402  (session + endpoint constants)
from db_utils import get_db_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("groundtruth_queue")

WIKIMEDIA_CATEGORY_QID = "Q4167836"   # "Wikimedia category" — a topic category, not a person
MAX_FILES_FOR_CATEGORIES = 3          # category-walk this many of a performer's files
MAX_EVIDENCE_IMAGES = 8               # thumbnails shown to the reviewer
NAME_SEARCH_LIMIT = 7

_SUBSET_SQL = """
    SELECT p.id, p.name, i.url, i.source_page_url, ai.is_primary, ai.display_order
    FROM performers p
    JOIN artist_images ai ON ai.performer_id = p.id
    JOIN images i         ON i.id = ai.image_id
    WHERE i.source = 'wikimedia_commons'
      AND btrim(COALESCE(p.wikipedia_url, '')) = ''
      AND btrim(COALESCE(p.external_links->>'wikipedia', '')) = ''
    ORDER BY p.name, ai.is_primary DESC, ai.display_order
"""


# --------------------------------------------------------------------------- #
# Wikimedia helpers (with in-run caches; many performers share categories/QIDs)
# --------------------------------------------------------------------------- #

class WM:
    def __init__(self, session, delay: float):
        self.s = session
        self.delay = delay
        self._cat_qid: dict = {}
        self._qid_info: dict = {}
        self._name_search: dict = {}

    def _get(self, url, params):
        if self.delay:
            time.sleep(self.delay)
        try:
            return self.s.get(url, params=params, timeout=20).json()
        except Exception as e:
            logger.debug("API error %s: %s", url, e)
            return {}

    def file_categories(self, file_title: str) -> list[str]:
        j = self._get(ci.COMMONS_API, {
            "action": "query", "format": "json", "titles": file_title,
            "prop": "categories", "cllimit": "max", "clshow": "!hidden",
        })
        pg = next(iter(j.get("query", {}).get("pages", {}).values()), {})
        return [c["title"] for c in pg.get("categories", [])]

    def category_qid(self, cat_title: str) -> str | None:
        if cat_title in self._cat_qid:
            return self._cat_qid[cat_title]
        j = self._get(ci.COMMONS_API, {
            "action": "query", "format": "json", "titles": cat_title,
            "prop": "pageprops", "ppprop": "wikibase_item",
        })
        pg = next(iter(j.get("query", {}).get("pages", {}).values()), {})
        qid = pg.get("pageprops", {}).get("wikibase_item")
        self._cat_qid[cat_title] = qid
        return qid

    def qid_info(self, qid: str) -> dict:
        if qid in self._qid_info:
            return self._qid_info[qid]
        j = self._get(ci.WIKIDATA_API, {
            "action": "wbgetentities", "ids": qid, "format": "json",
            "props": "sitelinks|descriptions|claims",
        })
        e = j.get("entities", {}).get(qid, {})
        claims = e.get("claims", {})
        def _ids(prop):
            out = []
            for s in claims.get(prop, []):
                v = s.get("mainsnak", {}).get("datavalue", {}).get("value")
                if isinstance(v, dict) and "id" in v:
                    out.append(v["id"])
            return out
        p18 = None
        for s in claims.get("P18", []):
            v = s.get("mainsnak", {}).get("datavalue", {}).get("value")
            if isinstance(v, str):
                p18 = v
                break
        enwiki = e.get("sitelinks", {}).get("enwiki", {}).get("title")
        info = {
            "qid": qid,
            "enwiki_title": enwiki,
            "description": e.get("descriptions", {}).get("en", {}).get("value"),
            "p31": _ids("P31"),
            "p106": _ids("P106"),
            "image": p18,
        }
        self._qid_info[qid] = info
        return info

    def name_search_qids(self, name: str) -> list[str]:
        if name in self._name_search:
            return self._name_search[name]
        j = self._get(ci.WIKIDATA_API, {
            "action": "wbsearchentities", "search": name, "language": "en",
            "format": "json", "type": "item", "limit": NAME_SEARCH_LIMIT,
        })
        qids = [h["id"] for h in j.get("search", []) if h.get("id")]
        self._name_search[name] = qids
        return qids


# --------------------------------------------------------------------------- #
# Candidate construction
# --------------------------------------------------------------------------- #

def _wiki_url(enwiki_title: str | None) -> str | None:
    """A real biography article URL, or None for missing / Category: sitelinks."""
    if not enwiki_title or enwiki_title.startswith("Category:"):
        return None
    return "https://en.wikipedia.org/wiki/" + enwiki_title.replace(" ", "_")


def _candidate_from_info(info: dict, *, method: str, commons_category: str | None) -> dict | None:
    if WIKIMEDIA_CATEGORY_QID in info["p31"]:
        return None  # a topic category, not a person/group
    url = _wiki_url(info["enwiki_title"])
    if not url:
        return None  # no usable biography article -> not a Wikipedia-link candidate
    thumb = None
    if info["image"]:
        thumb = (f"https://commons.wikimedia.org/wiki/Special:FilePath/"
                 f"{info['image'].replace(' ', '_')}?width=180")
    return {
        "method": method,
        "commons_category": commons_category,
        "wikidata_qid": info["qid"],
        "wikipedia_url": url,
        "title": info["enwiki_title"],
        "description": info["description"],
        "is_human": "Q5" in info["p31"],
        "thumb": thumb,
    }


def _norm(s: str) -> str:
    return "".join(ch for ch in s.lower() if ch.isalnum())


def derive_candidates(wm: WM, name: str, files: list[str]) -> list[dict]:
    candidates: list[dict] = []
    seen_qids: set[str] = set()

    # 1. Category-derived (the implicit link).
    categories: list[str] = []
    for ftitle in files[:MAX_FILES_FOR_CATEGORIES]:
        for c in wm.file_categories(ftitle):
            if c not in categories:
                categories.append(c)
    for cat in categories:
        qid = wm.category_qid(cat)
        if not qid or qid in seen_qids:
            continue
        cand = _candidate_from_info(wm.qid_info(qid), method="category",
                                    commons_category=cat)
        if cand:
            seen_qids.add(qid)
            candidates.append(cand)

    # 2. Name-search fallback — only when no category produced a candidate.
    if not candidates:
        for qid in wm.name_search_qids(name):
            if qid in seen_qids:
                continue
            cand = _candidate_from_info(wm.qid_info(qid), method="name_search",
                                        commons_category=None)
            if cand:
                seen_qids.add(qid)
                candidates.append(cand)

    # Rank: category before name-search; human before non-human; exact-name match first.
    target = _norm(name)
    def key(c):
        return (
            0 if c["method"] == "category" else 1,
            0 if c["is_human"] else 1,
            0 if _norm(c["title"]) == target else 1,
        )
    candidates.sort(key=key)
    return candidates


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def _load_subset() -> list[dict]:
    """Returns [{performer_id, name, images:[{url,page,title}]}] for the subset."""
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(_SUBSET_SQL)
        rows = cur.fetchall()
    grouped: dict[str, dict] = {}
    order: list[str] = []
    for r in rows:
        pid = str(r["id"])
        g = grouped.get(pid)
        if g is None:
            g = {"performer_id": pid, "name": r["name"], "images": []}
            grouped[pid] = g
            order.append(pid)
        spu = r["source_page_url"] or ""
        title = spu.split("/wiki/", 1)[1] if "/wiki/" in spu else None
        g["images"].append({"url": r["url"], "page": spu, "title": title})
    return [grouped[pid] for pid in order]


MAX_INSTRUMENTS = 6
MAX_SAMPLE_RECORDINGS = 5


def _empty_context() -> dict:
    return {"instruments": [], "recording_count": 0, "year_min": None,
            "year_max": None, "sample_recordings": [], "artist_type": None,
            "disambiguation": None, "birth_date": None, "death_date": None,
            "biography": None}


def _load_system_context(performer_ids: list[str]) -> dict[str, dict]:
    """What our own DB knows about each performer — instrument(s), recording
    history, and bio fields. This is often the decisive disambiguator: the
    Commons photos can be wrong (old guess path), but "guitarist, 3 recordings
    in the 1990s" unambiguously points at the right Wikipedia article."""
    if not performer_ids:
        return {}
    ctx = {pid: _empty_context() for pid in performer_ids}
    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, artist_type, disambiguation, birth_date, death_date,
                   LEFT(COALESCE(biography, ''), 280) AS bio
            FROM performers WHERE id = ANY(%s)
        """, (performer_ids,))
        for r in cur.fetchall():
            c = ctx[str(r["id"])]
            c["artist_type"] = r["artist_type"]
            c["disambiguation"] = r["disambiguation"]
            c["birth_date"] = str(r["birth_date"]) if r["birth_date"] else None
            c["death_date"] = str(r["death_date"]) if r["death_date"] else None
            c["biography"] = (r["bio"] or "").strip() or None

        cur.execute("""
            SELECT rp.performer_id AS pid, i.name AS name, COUNT(*) AS n
            FROM recording_performers rp
            JOIN instruments i ON i.id = rp.instrument_id
            WHERE rp.performer_id = ANY(%s)
            GROUP BY rp.performer_id, i.name
            ORDER BY rp.performer_id, n DESC
        """, (performer_ids,))
        for r in cur.fetchall():
            insts = ctx[str(r["pid"])]["instruments"]
            if len(insts) < MAX_INSTRUMENTS:
                insts.append(r["name"])

        cur.execute("""
            SELECT rp.performer_id AS pid, COUNT(DISTINCT r.id) AS n,
                   MIN(r.recording_year) AS ymin, MAX(r.recording_year) AS ymax
            FROM recording_performers rp
            JOIN recordings r ON r.id = rp.recording_id
            WHERE rp.performer_id = ANY(%s)
            GROUP BY rp.performer_id
        """, (performer_ids,))
        for r in cur.fetchall():
            c = ctx[str(r["pid"])]
            c["recording_count"] = r["n"]
            c["year_min"], c["year_max"] = r["ymin"], r["ymax"]

        cur.execute("""
            SELECT pid, year, title FROM (
                SELECT rp.performer_id AS pid, r.recording_year AS year,
                       COALESCE(NULLIF(r.title, ''), s.title) AS title,
                       ROW_NUMBER() OVER (PARTITION BY rp.performer_id
                           ORDER BY r.recording_year NULLS LAST, r.id) AS rn
                FROM recording_performers rp
                JOIN recordings r ON r.id = rp.recording_id
                JOIN songs s ON s.id = r.song_id
                WHERE rp.performer_id = ANY(%s)
            ) t WHERE rn <= %s
            ORDER BY pid, year NULLS LAST
        """, (performer_ids, MAX_SAMPLE_RECORDINGS))
        for r in cur.fetchall():
            ctx[str(r["pid"])]["sample_recordings"].append(
                {"year": r["year"], "title": r["title"]})
    return ctx


def main() -> None:
    p = argparse.ArgumentParser(
        description="Build the performer->Wikipedia verification queue JSON.",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    p.add_argument("--limit", type=int, default=None,
                   help="Cap the number of performers (alphabetical) for a first pass")
    p.add_argument("--delay", type=float, default=0.1,
                   help="Seconds between Wikimedia API calls (politeness; default 0.1)")
    p.add_argument("-o", "--output", default=None,
                   help="Output queue JSON path (default: "
                        "data/ground_truth/wikipedia_queue_<ts>.json)")
    p.add_argument("--backfill-context", metavar="QUEUE_JSON", default=None,
                   help="Augment an EXISTING queue JSON in place with system_context "
                        "(DB-only, no Wikimedia calls). Keeps the same filename so the "
                        "viewer's saved decisions are preserved. Then re-run the viewer.")
    args = p.parse_args()

    if args.backfill_context:
        path = Path(args.backfill_context)
        data = json.loads(path.read_text(encoding="utf-8"))
        ids = [r["performer_id"] for r in data.get("records", [])]
        ctx = _load_system_context(ids)
        for r in data["records"]:
            r["system_context"] = ctx.get(r["performer_id"], _empty_context())
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Backfilled system_context into %s (%d performer(s))", path, len(ids))
        print(path)
        return

    subset = _load_subset()
    logger.info("Subset: %d performer(s) with Commons imagery and no Wikipedia link",
                len(subset))
    if args.limit:
        subset = subset[: args.limit]
        logger.info("Capped to %d performer(s)", len(subset))

    system_ctx = _load_system_context([p["performer_id"] for p in subset])

    wm = WM(ci.make_session(), delay=args.delay)
    records = []
    with_candidate = 0
    for idx, perf in enumerate(subset, 1):
        file_titles = [img["title"] and f"File:{img['title'].split('File:')[-1]}"
                       for img in perf["images"] if img.get("title")]
        file_titles = [f for f in file_titles if f]
        candidates = derive_candidates(wm, perf["name"], file_titles)
        if candidates:
            with_candidate += 1
        evidence = []
        for img in perf["images"][:MAX_EVIDENCE_IMAGES]:
            t = img.get("title")
            thumb = (f"https://commons.wikimedia.org/wiki/Special:FilePath/"
                     f"{t.split('File:')[-1]}?width=180") if t and "File:" in t else img["url"]
            evidence.append({"thumb": thumb, "page": img["page"], "title": t})
        records.append({
            "performer_id": perf["performer_id"],
            "name": perf["name"],
            "system_context": system_ctx.get(perf["performer_id"], _empty_context()),
            "evidence_images": evidence,
            "candidates": candidates,
        })
        if idx % 25 == 0 or idx == len(subset):
            logger.info("Processed %d/%d performer(s); %d with a candidate so far",
                        idx, len(subset), with_candidate)

    out_dir = REPO_ROOT / "data" / "ground_truth"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.output) if args.output else out_dir / f"wikipedia_queue_{ts}.json"
    payload = {
        "schema": "performer_wikipedia_queue/v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "performer_count": len(records),
        "with_candidate": with_candidate,
        "records": records,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Wrote %s — %d performer(s), %d with >=1 candidate",
                out_path, len(records), with_candidate)
    print(out_path)


if __name__ == "__main__":
    main()
