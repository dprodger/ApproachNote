# Ground-truth datasets

Human-verified reference data, kept deliberately separate from anything a
crawler produces automatically. The distinguishing marker is provenance: every
record here carries `"method": "manual"`, meaning a person looked at the
evidence and confirmed it. These files are the authoritative source for
re-ingest and for diffing against automated crawlers.

## What's tracked vs ignored

Committed (authoritative):
- `README.md` — this file.
- `performer_wikipedia_groundtruth*.json` — verified performer → Wikipedia links.

Ignored (regenerable scratch — see `.gitignore`):
- `wikipedia_queue_*.json` — the verification *worklist* (rebuilt from the DB).
- `*.html` — generated verification viewers.

## Pipeline: performer → Wikipedia links

Goal: for performers that have Commons imagery but **no** Wikipedia link on
record, confirm the correct Wikipedia article so the system gets smarter about
them going forward.

```
backend/scripts/build_wikipedia_groundtruth_queue.py     # DB + Wikimedia  -> queue JSON
backend/scripts/build_wikipedia_groundtruth_viewer.py    # queue JSON      -> verification HTML
# (review in browser, click Export)                       # decisions       -> ground-truth JSON
```

Candidates are **category-derived** first — the Commons category the
performer's own photos sit in → its Wikidata item → the English Wikipedia
sitelink (the "implicit" link) — falling back to a Wikidata name search when no
category yields a real biography article.

### Schema: `performer_wikipedia_queue/v1` (worklist, ignored)

```jsonc
{
  "schema": "performer_wikipedia_queue/v1",
  "generated_at": "<iso8601>",
  "performer_count": 1224,
  "with_candidate": 812,
  "records": [
    {
      "performer_id": "<uuid>",
      "name": "?uestlove",
      "evidence_images": [ { "thumb": "<url>", "page": "<commons File: url>", "title": "File:…" } ],
      "candidates": [
        {
          "method": "category" | "name_search",
          "commons_category": "Category:Questlove" | null,
          "wikidata_qid": "Q263024",
          "wikipedia_url": "https://en.wikipedia.org/wiki/Questlove",
          "title": "Questlove",
          "description": "American hip hop musician, record producer and DJ",
          "is_human": true,
          "thumb": "<wikidata P18 thumbnail url>" | null
        }
      ]
    }
  ]
}
```

### Schema: `performer_wikipedia_groundtruth/v1` (authoritative, committed)

Exported from the viewer; only performers the reviewer actually decided on are
included. `no_match` is a real, useful decision (a crawler proposing a link
there is wrong).

```jsonc
{
  "schema": "performer_wikipedia_groundtruth/v1",
  "exported_at": "<iso8601>",
  "source_queue": "wikipedia_queue_<ts>.json",
  "record_count": 137,
  "records": {
    "<performer_id>": {
      "name": "?uestlove",
      "status": "verified" | "no_match",
      "wikipedia_url": "https://en.wikipedia.org/wiki/Questlove" | null,
      "wikidata_qid": "Q263024" | null,
      "method": "manual",
      "candidate_method": "category" | "name_search" | "custom" | null,
      "evidence": { "commons_category": "Category:Questlove" | null },
      "verified_at": "<iso8601>"
    }
  }
}
```

Re-ingest (not yet built): read this file, `UPDATE performers SET wikipedia_url`
for `status == "verified"` rows, stamping a manual-provenance marker
(e.g. `updated_by = 'groundtruth_manual'`) so the distinction survives in the DB.
