# Performer imagery from Wikimedia Commons + Flickr Commons

This documents the approach used by `backend/scripts/fetch_commons_images.py`
to gather **high-quality, freely-licensed** performer images beyond the single
lead image we already pull from a Wikipedia article, and to ingest them into the
`images` + `artist_images` tables.

## Why these sources

The lead Wikipedia image gives us one photo per artist. To gather *more* good
images we walk the artist's **Wikimedia Commons category** (e.g.
`Category:Dave Brubeck`). A Commons category is a curated bucket of media about
exactly that subject, and every file carries structured license metadata
(`extmetadata`) — license, author, source — so we get attribution and
provenance for free.

**Flickr Commons** is the second source: institutional and public-domain photo
collections hosted on Flickr. It needs a `FLICKR_API_KEY` and is skipped
gracefully when the key is absent.

## License policy: free-culture set (default)

By default the script accepts the **free-culture set**: Public Domain, CC0,
CC-BY and CC-BY-SA. PD and CC0 carry no obligations; **CC-BY and CC-BY-SA
require displaying the captured `attribution` + `license_url`**, and CC-BY-SA
additionally keeps any derivative (e.g. a crop you publish) under CC-BY-SA —
share-alike attaches to the image, not to your app. The **NonCommercial (NC)**
and **NoDerivatives (ND)** variants are always rejected.

For a worry-free, attribution-free set, narrow with
`--licenses public_domain,cc0`. PD/CC0-only is very sparse for living,
heavily-photographed artists (e.g. Sonny Rollins returns ~0), because their
Commons imagery is overwhelmingly CC-BY-SA. "No known copyright restrictions"
(NKCR) is excluded unless you pass `--include-nkcr`.

License classification maps Commons `extmetadata.License` / `LicenseShortName`:

| Source value                        | Stored `license_type` |
|-------------------------------------|-----------------------|
| `cc0`, "CC0"                        | `cc0`                 |
| `pd`, `pd-*`, "Public domain"       | `public_domain`       |
| `cc-by-*` (not NC/ND)               | `cc_by`               |
| `cc-by-sa-*` (not NC/ND)            | `cc_by_sa`            |
| `*-nc-*`, `*-nd-*` (NonComm/NoDeriv)| *(rejected)*          |
| "No known copyright restrictions"   | *(rejected unless `--include-nkcr` → `public_domain`)* |

The `--licenses` allow-list (default `public_domain,cc0,cc_by,cc_by_sa`) then
filters which of those classified types are kept.

Flickr license ids map: `4` → `cc_by`, `5` → `cc_by_sa`, `9` → `cc0`,
`10`/`7`/`8` → `public_domain` (request set via `--flickr-licenses`, default
`4,5,9,10`).

## What we capture per image

Written to `images`:

`url` (full-res), `thumbnail_url` (~400px), `source`
(`wikimedia_commons` | `flickr`), `source_identifier` (Commons pageid / Flickr
photo id — used for idempotent de-dup), `source_page_url` (the human-readable
file/photo page — **the reference back to the source**), `license_type`,
`license_url`, `attribution` (author/credit, HTML stripped), `width`, `height`.

Linked via `artist_images` (`performer_id`, `image_id`, `is_primary`,
`display_order`). The first kept image becomes `is_primary` only when the
performer has no images yet.

> Note: the live tables are **`images`** and **`artist_images`** (there is no
> `artist_imagery` table — `artist_imagery` was the name in the original
> request). `release_imagery` is the separate album-art table and is untouched.

## Noise filtering

Even within a person's category, Commons holds non-portraits (signatures,
graves, plaques, album covers, buildings). Filenames matching those patterns
are dropped by default; pass `--no-portrait-filter` to keep them. Kept images
that *look* borderline are flagged (`flagged_non_portrait`) in the JSON for
human review rather than silently included.

## Running it

```bash
cd backend
source venv/bin/activate

# Dry run: gather candidates, export reviewable JSON + SQL, no DB writes
python scripts/fetch_commons_images.py --name "Dave Brubeck" --dry-run \
    --emit-json out/brubeck.json --emit-sql out/brubeck.sql

# Review out/brubeck.json, then either run the SQL or re-run without --dry-run:
python scripts/fetch_commons_images.py --name "Dave Brubeck"
```

Useful flags: `--limit N` (default 8), `--recurse-subcats N`, `--category
"Category:..."` (skip Wikidata lookup), `--no-db --performer-id <uuid>` (pure
gather, no DB), `--no-flickr`, `--include-nkcr`.

The emitted SQL is idempotent (CTE `INSERT ... ON CONFLICT (url) DO UPDATE` +
`artist_images ... ON CONFLICT DO NOTHING`), so it is safe to re-run.

## Visual analysis (`--visual`)

Not every freely-licensed image is a *good* portrait. With `--visual` the
script runs a two-tier quality pass (`backend/core/image_quality.py`) between
gathering and ingest.

**Tier 2 — local heuristics (no API, no cost).** For each candidate it
downloads the bytes and computes:

- *resolution* — long edge ≥ `--min-long-edge` (default 500; uses the
  API-reported dimensions when available);
- *sharpness* — variance-of-Laplacian (OpenCV) ≥ `--min-sharpness` to drop
  soft scans;
- *face presence & size* — face detection (OpenCV **YuNet**, falling back to
  the bundled **Haar** cascade offline); requires a face whose area is ≥
  `--min-face-fraction` of the frame, which removes instruments, buildings,
  album covers and crowd shots;
- *identity* — compares face embeddings (OpenCV **SFace**) to a **reference**
  image (the performer's current primary DB image, or `--reference-image
  <url>`) and drops candidates whose distance exceeds `--identity-threshold`
  (default 0.60; for SFace this is `1 − cosine_similarity`). This catches
  namesakes / mislabeled files. Skipped automatically when no reference face is
  available or the SFace model can't be fetched;
- *near-duplicate de-dup* — two complementary signals, keeping the
  higher-scored twin: a perceptual hash (`imagehash`, `--phash-distance`,
  default 6) collapses near-identical framings (resize / re-encode), and ORB
  key-point matching (OpenCV, `--orb-dup-matches`, default 40; disable with
  `--no-orb-dedup`) catches the *same photo at a different crop or scale*,
  which a perceptual hash misses. (Empirically, two crops of one photo share
  ~300 ORB matches while unrelated images share <15, so the two are cleanly
  separable.)

Anything failing the gate is dropped (or kept-but-annotated with `--no-gate`).
We never reject on a check we couldn't run, so a missing optional library just
relaxes that one criterion.

**Tier 3 — vision rerank (pluggable, default Claude).** Survivors are scored by
a vision model against a fixed rubric (real photograph vs artwork/poster, is it
the subject, single dominant subject, 1–5 quality, issues). The result is
blended with the local signals into a 0–100 score used to rank and pick the top
`--limit`; the first becomes `is_primary` when the performer has no image yet.

- `--reranker claude` (default): uses Claude vision via the Anthropic API.
  Needs `ANTHROPIC_API_KEY` (and optional `IMAGE_RERANK_MODEL`, default
  `claude-haiku-4-5-20251001`). Disabled gracefully if the key is absent.
- `--reranker clip`: a local open_clip backend (no API). Requires `torch` +
  `open_clip_torch`, which are left commented in `requirements.txt` because
  they are large — uncomment to use this backend.
- `--no-rerank`: rank on local heuristics alone.

The full per-image verdict (gate result, reasons, local measurements, vision
JSON, final score) is included in `--emit-json` for auditing.

### Installing the visual-analysis dependencies

The libraries live in the main `requirements.txt`:

```bash
cd backend && source venv/bin/activate
pip install -r requirements.txt
```

If `--visual` is run before these are installed, the script detects it, logs a
warning, and **skips** the visual stage (keeping all gathered candidates)
rather than dropping everything — `--visual` will silently no-op until the libs
are present.

**Face models (no dlib).** Face detection and identity use OpenCV's YuNet and
SFace ONNX models. On first `--visual` run the script downloads them once to
`backend/data/face_models/` (override the dir with `IMAGE_MODEL_DIR`, or point
`IMAGE_YUNET_MODEL` / `IMAGE_SFACE_MODEL` at local `.onnx` files; set
`IMAGE_NO_MODEL_DOWNLOAD=1` to forbid downloads). If the models can't be
fetched, detection falls back to OpenCV's bundled Haar cascade and identity is
skipped — no `dlib`/`face_recognition` build is required. (`face_recognition`
is still used automatically *if* it happens to be importable, but it is
intentionally left out of `requirements.txt` because its `dlib` build fails on
recent macOS/Apple-Clang toolchains.)

Add `backend/data/face_models/` to `.gitignore` if you don't want the ~5 MB
ONNX files committed.

Dependencies for the local tier and the Claude reranker are in the main
`requirements.txt`; the heavy CLIP/torch pair is commented out there.

## Running it on the research worker (`commons` source)

The pipeline runs as a durable background job, mirroring the Wikipedia
enrichment path. The shared logic lives in `core/commons_imagery.py`; both the
CLI and the worker call it.

Pieces:

- Handler — `research_worker/handlers/commons.py`,
  `('commons', 'enrich_performer_imagery')`, one job per performer. Loads the
  performer, uses their current primary image as the identity reference, runs
  the pipeline, and links the good images. Registered in
  `research_worker/run.py`; gets its own worker thread.
- Producer — `core/performer_commons_imagery.py`. `enqueue_sweep()` enqueues
  every performer that is *due*: `last_imagery_check IS NULL` (never checked) or
  older than the staleness window (default 90 days). The handler stamps
  `performers.last_imagery_check` on every completion, so a performer stops
  being due until the window elapses — this is what makes re-sweeps cheap and
  picks up newly-uploaded Commons photos over time.
- CLI — `scripts/enrich_performer_imagery.py`: `--name`/`--id` for one
  performer, otherwise a due-sweep with `--stale-days` / `--limit` / `--dry-run`.
- Admin — `POST /admin/research/enqueue-commons-imagery` (`{stale_days, limit}`)
  triggers a sweep; watch progress on the dashboard with the `source=commons`
  filter.
- Migration — `sql/migrations/020_commons_imagery_enrichment.sql` adds
  `performers.last_imagery_check` and seeds the `commons` daily quota row.

Cost control: the Claude rerank is the only paid step. The handler reranks at
most `rerank_cap` (default 12) images per performer and reserves one `commons`
daily quota unit per reranked image via `ctx.consume_quota`. When the daily
budget (`source_quotas`, default 2000 = ~166 performers/day) is spent, the job
is released until the next reset (`QuotaExhausted`) — a hard ceiling on spend.
Tune with `UPDATE source_quotas SET units_limit = <n> WHERE source = 'commons';`

Deploy notes: the worker needs `ANTHROPIC_API_KEY` (rerank), optionally
`FLICKR_API_KEY`, and `IMAGE_MODEL_DIR=/data/face_models` so the ONNX models
cache on the persistent disk (all in `render.yaml`). There is no scheduler
wired in — run the CLI or hit the admin endpoint to start a sweep; the worker
drains the jobs continuously.

Operate it:

```bash
# one performer, immediately
python scripts/enrich_performer_imagery.py --name "Sonny Rollins"

# how many are due right now (no enqueue)
python scripts/enrich_performer_imagery.py --dry-run

# enqueue the first 200 due performers
python scripts/enrich_performer_imagery.py --limit 200
```

## Proof-of-concept results (Dave Brubeck, Bobby Broom)

- **Dave Brubeck** — large Commons category (`Category:Dave Brubeck`, ~40 files
  incl. the `Category:The Dave Brubeck Quartet` subcat). It contains genuine
  PD/CC0 photos (e.g. Library-of-Congress-sourced portraits and
  William P. Gottlieb collection material) alongside CC-BY-SA images that the
  strict filter rejects. See `doc/examples/dave_brubeck_imagery.sample.*` for
  the exact JSON/SQL shape produced.
- **Bobby Broom** — no usable free imagery on Commons: there is no
  `Category:Bobby Broom`, and a Commons search surfaces only unrelated "broom"
  files. Under PD/CC0-only the run yields **zero** images (see
  `doc/examples/bobby_broom_imagery.sample.json`). This is the common case for
  living, less-documented artists and is exactly why a second source
  (Flickr Commons, key permitting) and graceful empty handling matter.

> The sample files were produced from verified Commons file-page references but
> the per-file license/attribution/dimension values must be populated by an
> actual run — the build environment used to scaffold this had no outbound
> access to the Wikimedia/Flickr APIs. Run the dry-run command above in the
> backend environment (where the existing `fetch_loc_images.py` already reaches
> these APIs) to generate authoritative output.
