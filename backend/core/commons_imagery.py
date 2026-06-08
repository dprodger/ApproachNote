"""
Commons / Flickr performer-imagery pipeline (library).
======================================================

Single source of truth for gathering high-quality, freely-licensed performer
images and turning them into a ranked, deduped, gated set ready to persist into
`images` + `artist_images`. Both the CLI (`scripts/fetch_commons_images.py`)
and the research-worker handler
(`research_worker/handlers/commons.py`) call into here so the logic lives in
exactly one place.

Pipeline:
  gather_candidates()   resolve the performer's Wikimedia Commons category
                        (Wikidata P373), walk it, license-filter, optionally
                        add Flickr Commons, drop filename-obvious non-portraits,
                        de-dup by URL.
  analyze_and_rank()    download each candidate, run the tier-2 local gate
                        (resolution / sharpness / face / identity), de-dup
                        (perceptual hash + ORB crop-dup), rerank the survivors
                        with a vision model (Claude by default, cost-bounded by
                        `rerank_cap` and an optional quota budget callback), and
                        rank solo-portraits-first.
  persist_images()      idempotent upsert into images + artist_images, with the
                        "group photos are never primary" rule.

LICENSE POLICY (default): the free-culture set -- PD, CC0, CC-BY, CC-BY-SA.
The NonCommercial (NC) and NoDerivatives (ND) variants are always rejected.
CC-BY / CC-BY-SA carry attribution (and, for -SA, share-alike) obligations; the
captured `attribution` + `license_url` must be displayed in the app.

See doc/commons-flickr-imagery.md for the full design.
"""

from __future__ import annotations

import os
import re
import html
import time
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import requests

from db_utils import get_db_connection
from core import image_quality as iq

logger = logging.getLogger("commons_imagery")

USER_AGENT = "ApproachNote/1.0 (+support@approachnote.com)"
COMMONS_API = "https://commons.wikimedia.org/w/api.php"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
FLICKR_API = "https://api.flickr.com/services/rest/"

# Filenames that are usually *not* a portrait of the person. Tunable.
NON_PORTRAIT_PATTERNS = re.compile(
    r"\b(signature|autograph|grave|tomb|headstone|plaque|memorial|statue|bust|"
    r"mural|star|walk of fame|house|home|residence|birthplace|building|"
    r"poster|flyer|ticket|album|cover|sleeve|label|record|logo|map|"
    r"document|letter|score|sheet music|stamp)\b",
    re.IGNORECASE,
)

# Flickr license id -> (license_type, license_url)
FLICKR_LICENSE_MAP = {
    "9": ("cc0", "https://creativecommons.org/publicdomain/zero/1.0/"),
    "10": ("public_domain", "https://creativecommons.org/publicdomain/mark/1.0/"),
    "7": ("public_domain", "https://www.flickr.com/commons/usage/"),   # NKCR
    "8": ("public_domain", "https://www.usa.gov/government-works"),     # US Gov
    "4": ("cc_by", "https://creativecommons.org/licenses/by/2.0/"),
    "5": ("cc_by_sa", "https://creativecommons.org/licenses/by-sa/2.0/"),
}

DEFAULT_LICENSES = ("public_domain", "cc0", "cc_by", "cc_by_sa")


# ---------------------------------------------------------------------------
# Config + record types
# ---------------------------------------------------------------------------

@dataclass
class GatherConfig:
    # gathering
    licenses: tuple = DEFAULT_LICENSES
    include_nkcr: bool = False
    recurse_subcats: int = 0
    portrait_filter: bool = True
    category: Optional[str] = None        # explicit Commons category override
    use_flickr: bool = True
    flickr_licenses: str = "4,5,9,10"
    limit: int = 8                        # max images kept per performer
    # visual analysis
    visual: bool = True
    reranker: str = "claude"
    do_rerank: bool = True
    do_gate: bool = True
    identity: bool = True
    allow_faceless: bool = False
    min_long_edge: int = 500
    min_sharpness: float = 40.0
    min_face_fraction: float = 0.015
    identity_threshold: float = 0.60
    phash_distance: int = 6
    orb_dup_matches: int = 40
    orb_dedup: bool = True
    rerank_cap: int = 50                  # max images sent to the vision model

    def gate(self) -> "iq.GateConfig":
        return iq.GateConfig(
            min_long_edge=self.min_long_edge,
            min_sharpness=self.min_sharpness,
            min_face_fraction=self.min_face_fraction,
            require_face=not self.allow_faceless,
            identity_threshold=self.identity_threshold,
            enforce_identity=self.identity,
        )


@dataclass
class ImageRecord:
    url: str
    source: str
    source_identifier: str
    source_page_url: str
    license_type: str
    license_url: Optional[str] = None
    attribution: Optional[str] = None
    thumbnail_url: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    title: Optional[str] = None
    flagged_non_portrait: bool = False
    quality_score: Optional[float] = None
    analysis: Optional[Dict[str, Any]] = None
    # NOTE: `_orb` (ORB descriptors) and `_img_bytes` are attached dynamically
    # during analyze_and_rank(); they are deliberately NOT dataclass fields so
    # asdict()/JSON emit never tries to serialize a numpy array.

    def db_fields(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "source": self.source,
            "source_identifier": self.source_identifier,
            "license_type": self.license_type,
            "license_url": self.license_url,
            "attribution": self.attribution,
            "width": self.width,
            "height": self.height,
            "thumbnail_url": self.thumbnail_url,
            "source_page_url": self.source_page_url,
        }


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return s


def download(session: requests.Session, url: Optional[str],
             max_bytes: int = 15_000_000, timeout: int = 30) -> Optional[bytes]:
    """Download image bytes with a size cap. Returns None on any failure."""
    if not url:
        return None
    try:
        r = session.get(url, timeout=timeout, stream=True)
        r.raise_for_status()
        data = bytearray()
        for chunk in r.iter_content(64 * 1024):
            data.extend(chunk)
            if len(data) > max_bytes:
                logger.debug("skipping oversized download (%s)", url)
                return None
        return bytes(data)
    except Exception as e:
        logger.debug("download failed %s: %s", url, e)
        return None


def _strip_html(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _to_int(v) -> Optional[int]:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Category resolution
# ---------------------------------------------------------------------------

def resolve_commons_category(session: requests.Session, artist_name: str,
                             wikipedia_url: Optional[str] = None) -> Optional[str]:
    """Find the performer's Wikimedia Commons category via Wikidata P373,
    falling back to a verified "Category:<Name>" guess."""
    qid = None
    if wikipedia_url:
        m = re.search(r"/wiki/(.+)$", wikipedia_url)
        if m:
            title = requests.utils.unquote(m.group(1))
            try:
                r = session.get(WIKIPEDIA_API, params={
                    "action": "query", "format": "json", "titles": title,
                    "prop": "pageprops", "ppprop": "wikibase_item",
                }, timeout=15)
                pages = r.json().get("query", {}).get("pages", {})
                page = next(iter(pages.values()), {})
                qid = page.get("pageprops", {}).get("wikibase_item")
            except Exception as e:
                logger.debug("Wikipedia->QID lookup failed: %s", e)

    if not qid:
        try:
            r = session.get(WIKIDATA_API, params={
                "action": "wbsearchentities", "search": artist_name,
                "language": "en", "format": "json", "type": "item", "limit": 5,
            }, timeout=15)
            hits = r.json().get("search", [])
            qid = hits[0]["id"] if hits else None
        except Exception as e:
            logger.debug("Wikidata search failed: %s", e)

    if qid:
        try:
            r = session.get(WIKIDATA_API, params={
                "action": "wbgetentities", "ids": qid, "format": "json",
                "props": "claims",
            }, timeout=15)
            claims = r.json().get("entities", {}).get(qid, {}).get("claims", {})
            p373 = claims.get("P373")
            if p373:
                cat = p373[0]["mainsnak"]["datavalue"]["value"]
                logger.info("Resolved Commons category via Wikidata %s: %s", qid, cat)
                return f"Category:{cat}"
        except Exception as e:
            logger.debug("Wikidata P373 lookup failed: %s", e)

    guess = f"Category:{artist_name}"
    if _category_exists(session, guess):
        logger.info("Using guessed Commons category: %s", guess)
        return guess

    logger.warning("Could not resolve a Commons category for %r", artist_name)
    return None


def _category_exists(session: requests.Session, category: str) -> bool:
    try:
        r = session.get(COMMONS_API, params={
            "action": "query", "format": "json", "titles": category,
            "prop": "info",
        }, timeout=15)
        pages = r.json().get("query", {}).get("pages", {})
        return all(int(pid) > 0 for pid in pages)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Commons category -> image records
# ---------------------------------------------------------------------------

def fetch_commons_category_files(
    session: requests.Session, category: str, accepted_types: List[str],
    include_nkcr: bool, recurse_subcats: int = 0,
    _seen_cats: Optional[set] = None,
) -> List[ImageRecord]:
    """List file members of a Commons category and keep those whose license is
    in the accepted set."""
    _seen_cats = _seen_cats if _seen_cats is not None else set()
    if category in _seen_cats:
        return []
    _seen_cats.add(category)

    records: List[ImageRecord] = []
    cont: Dict[str, Any] = {}
    subcats: List[str] = []

    while True:
        params = {
            "action": "query", "format": "json",
            "generator": "categorymembers",
            "gcmtitle": category,
            "gcmtype": "file|subcat" if recurse_subcats > 0 else "file",
            "gcmlimit": "200",
            "prop": "imageinfo",
            "iiprop": "url|size|extmetadata|mime",
            "iiurlwidth": "400",
            "iiextmetadatafilter": "License|LicenseShortName|LicenseUrl|"
                                   "Artist|Credit|ObjectName|ImageDescription",
        }
        params.update(cont)
        try:
            time.sleep(0.4)  # be polite
            r = session.get(COMMONS_API, params=params, timeout=30)
            data = r.json()
        except Exception as e:
            logger.error("Commons query failed for %s: %s", category, e)
            break

        pages = data.get("query", {}).get("pages", {})
        for page in pages.values():
            title = page.get("title", "")
            if title.startswith("Category:"):
                subcats.append(title)
                continue
            ii = page.get("imageinfo")
            if not ii:
                continue
            rec = _commons_record_from_imageinfo(
                page.get("pageid"), title, ii[0], accepted_types, include_nkcr
            )
            if rec:
                records.append(rec)

        cont = data.get("continue", {})
        if not cont:
            break

    if recurse_subcats > 0:
        for sub in subcats:
            records.extend(fetch_commons_category_files(
                session, sub, accepted_types, include_nkcr,
                recurse_subcats - 1, _seen_cats,
            ))
    return records


def _commons_record_from_imageinfo(
    pageid, title, info: Dict[str, Any],
    accepted_types: List[str], include_nkcr: bool,
) -> Optional[ImageRecord]:
    mime = info.get("mime", "")
    if mime and (not mime.startswith("image/") or mime == "image/svg+xml"):
        return None
    ext = info.get("extmetadata", {}) or {}

    raw_license = (ext.get("License", {}) or {}).get("value", "").lower()
    short = (ext.get("LicenseShortName", {}) or {}).get("value", "")
    license_type = classify_license(raw_license, short.lower(), include_nkcr)
    if license_type is None or license_type not in accepted_types:
        return None

    license_url = (ext.get("LicenseUrl", {}) or {}).get("value")
    if not license_url and license_type == "cc0":
        license_url = "https://creativecommons.org/publicdomain/zero/1.0/"
    attribution = _strip_html(
        (ext.get("Artist", {}) or {}).get("value")
        or (ext.get("Credit", {}) or {}).get("value")
    )
    object_name = _strip_html((ext.get("ObjectName", {}) or {}).get("value")) or title

    return ImageRecord(
        url=info.get("url"),
        source="wikimedia_commons",
        source_identifier=str(pageid),
        source_page_url=info.get("descriptionurl") or info.get("descriptionshorturl"),
        license_type=license_type,
        license_url=license_url,
        attribution=attribution,
        thumbnail_url=info.get("thumburl"),
        width=info.get("width"),
        height=info.get("height"),
        title=object_name,
        flagged_non_portrait=bool(NON_PORTRAIT_PATTERNS.search(title)),
    )


def classify_license(raw: str, short: str, include_nkcr: bool) -> Optional[str]:
    """Map Commons license metadata -> our license_type, or None if not an
    accepted free license. Recognizes PD, CC0, CC-BY and CC-BY-SA, and rejects
    the NonCommercial (NC) and NoDerivatives (ND) variants."""
    code = (raw or short.replace(" ", "-")).lower()
    if "cc0" in code or "cc-zero" in code:
        return "cc0"
    if code == "pd" or code.startswith("pd-") or "public-domain" in code \
            or "public domain" in short:
        return "public_domain"
    if include_nkcr and "no known copyright" in short:
        return "public_domain"
    if "nc" in code or "nd" in code:
        return None
    if "by-sa" in code:
        return "cc_by_sa"
    if "by" in code:
        return "cc_by"
    return None


# ---------------------------------------------------------------------------
# Flickr Commons
# ---------------------------------------------------------------------------

def fetch_flickr_images(session: requests.Session, artist_name: str,
                        flickr_licenses: str, limit: int) -> List[ImageRecord]:
    api_key = os.environ.get("FLICKR_API_KEY")
    if not api_key:
        logger.info("FLICKR_API_KEY not set -- skipping Flickr Commons")
        return []

    try:
        time.sleep(0.4)
        r = session.get(FLICKR_API, params={
            "method": "flickr.photos.search",
            "api_key": api_key, "text": artist_name,
            "license": flickr_licenses, "content_type": "1",
            "sort": "relevance", "per_page": str(min(limit * 3, 100)),
            "extras": "license,owner_name,url_o,url_l,url_c,o_dims,description",
            "format": "json", "nojsoncallback": "1",
        }, timeout=30)
        photos = r.json().get("photos", {}).get("photo", [])
    except Exception as e:
        logger.error("Flickr search failed: %s", e)
        return []

    records: List[ImageRecord] = []
    for p in photos:
        mapped = FLICKR_LICENSE_MAP.get(str(p.get("license")))
        if not mapped:
            continue
        license_type, license_url = mapped
        url = p.get("url_o") or p.get("url_l") or p.get("url_c")
        if not url:
            continue
        photo_id = p.get("id")
        owner = p.get("owner")
        records.append(ImageRecord(
            url=url, source="flickr", source_identifier=str(photo_id),
            source_page_url=f"https://www.flickr.com/photos/{owner}/{photo_id}",
            license_type=license_type, license_url=license_url,
            attribution=p.get("ownername") or owner,
            thumbnail_url=p.get("url_c") or p.get("url_l"),
            width=_to_int(p.get("width_o") or p.get("o_width")),
            height=_to_int(p.get("height_o") or p.get("o_height")),
            title=p.get("title"),
            flagged_non_portrait=bool(NON_PORTRAIT_PATTERNS.search(p.get("title", ""))),
        ))
    return records


# ---------------------------------------------------------------------------
# Stage 1: gather candidates (no download / analysis)
# ---------------------------------------------------------------------------

def gather_candidates(performer_name: str, wikipedia_url: Optional[str], *,
                      session: requests.Session,
                      config: GatherConfig) -> List[ImageRecord]:
    """Resolve the Commons category, walk it, add Flickr, license-filter,
    drop filename-obvious non-portraits and URL duplicates."""
    accepted = list(config.licenses)
    category = config.category or resolve_commons_category(
        session, performer_name, wikipedia_url)
    commons: List[ImageRecord] = []
    if category:
        logger.info("Walking Commons category: %s", category)
        commons = fetch_commons_category_files(
            session, category, accepted, config.include_nkcr,
            config.recurse_subcats)
        logger.info("Commons: %d freely-licensed image(s)", len(commons))

    flickr: List[ImageRecord] = []
    if config.use_flickr:
        flickr = fetch_flickr_images(
            session, performer_name, config.flickr_licenses, config.limit)
        if flickr:
            logger.info("Flickr: %d freely-licensed image(s)", len(flickr))

    records = commons + flickr
    if config.portrait_filter:
        kept = [r for r in records if not r.flagged_non_portrait]
        if len(kept) != len(records):
            logger.info("Portrait filter dropped %d non-portrait file(s)",
                        len(records) - len(kept))
        records = kept

    seen, deduped = set(), []
    for r in records:
        if r.url and r.url not in seen:
            seen.add(r.url)
            deduped.append(r)
    return deduped


# ---------------------------------------------------------------------------
# Stage 2: analyze, gate, de-dup, rerank, rank
# ---------------------------------------------------------------------------

def is_single_subject(rec: ImageRecord) -> bool:
    """True if the image is a lone-subject portrait. Uses the vision verdict
    when available, else falls back to 'exactly one detected face'."""
    a = rec.analysis or {}
    v = a.get("vision") or {}
    if v.get("single_subject") is not None:
        return bool(v["single_subject"])
    return (a.get("local") or {}).get("face_count") == 1


def primary_index(records: List[ImageRecord]) -> Optional[int]:
    """Index of the is_primary image: highest-ranked single-subject portrait.
    None if there is no single-subject candidate (group photos never primary)."""
    for i, r in enumerate(records):
        if is_single_subject(r):
            return i
    return None


def _phash(rec: ImageRecord) -> Optional[str]:
    return (rec.analysis or {}).get("local", {}).get("phash") if rec.analysis else None


def analyze_and_rank(
    records: List[ImageRecord], *,
    session: requests.Session, config: GatherConfig,
    reference_urls: Optional[List[str]] = None,
    performer_name: str = "",
    rerank_budget: Optional[Callable[[int], None]] = None,
) -> List[ImageRecord]:
    """Download + gate + de-dup + (cost-bounded) rerank + rank.

    `rerank_budget(n)` is called once with the number of images about to be
    reranked, before any vision call. It may raise (e.g. QuotaExhausted) to
    abort; the exception propagates to the caller.

    Returns survivors sorted best-first (the caller applies `config.limit`).
    """
    if not config.visual:
        return records

    caps = iq.local_capabilities()
    if not caps["pillow"]:
        logger.warning("Visual analysis needs Pillow+numpy (not installed); "
                       "keeping all %d candidate(s) un-gated", len(records))
        return records
    missing = [k for k in ("numpy", "opencv", "imagehash") if not caps[k]]
    if missing:
        logger.warning("Visual analysis with REDUCED signals; missing: %s",
                       ", ".join(missing))

    gate = config.gate()

    # Identity reference embeddings
    ref_encs = []
    if config.identity and reference_urls:
        ref_bytes = [b for b in (download(session, u) for u in reference_urls) if b]
        ref_encs = iq.encode_reference_faces(ref_bytes)
        logger.info("Identity reference: %d face embedding(s) from %d image(s)",
                    len(ref_encs), len(ref_bytes))

    # Phase 1: download + local analysis + gate (no vision cost yet)
    for r in records:
        img_bytes = download(session, r.url) or download(session, r.thumbnail_url)
        if not img_bytes:
            r.analysis = {"passed_gate": False, "reasons": ["download failed"],
                          "local": None, "vision": None}
            r.quality_score = 0.0
            r._orb = None
            continue
        local = iq.compute_local_analysis(img_bytes, ref_encs or None)
        r._orb = iq.orb_signature(img_bytes)
        if r.width and r.height:
            local.width, local.height = r.width, r.height
        passed, reasons = iq.evaluate_gate(local, gate)
        score = iq.aggregate_score(local, None)
        r.analysis = iq.QualityVerdict(passed_gate=passed, reasons=reasons,
                                       score=score, local=local,
                                       vision=None).to_dict()
        r.quality_score = score
        r._img_bytes = img_bytes  # transient, kept for the rerank phase

    # Phase 2: gate filter
    if config.do_gate:
        before = len(records)
        records = [r for r in records if (r.analysis or {}).get("passed_gate")]
        logger.info("Gate kept %d/%d", len(records), before)

    # Phase 3: cost-bounded vision rerank of the top survivors
    reranker = None
    if config.do_rerank:
        reranker = iq.get_reranker(config.reranker)
    if reranker is not None and records:
        ranked = sorted(records, key=lambda r: r.quality_score or 0.0, reverse=True)
        to_rerank = ranked[: max(0, config.rerank_cap)]
        if to_rerank and rerank_budget is not None:
            rerank_budget(len(to_rerank))  # may raise (quota); propagates
        for r in to_rerank:
            img_bytes = getattr(r, "_img_bytes", None)
            if not img_bytes:
                continue
            vision = reranker.score(img_bytes, {"performer_name": performer_name})
            if vision:
                local = (r.analysis or {}).get("local")
                local_obj = _local_from_dict(local)
                r.quality_score = iq.aggregate_score(local_obj, vision)
                r.analysis["vision"] = vision
                r.analysis["score"] = round(r.quality_score, 1)

    # free the transient bytes
    for r in records:
        if hasattr(r, "_img_bytes"):
            delattr(r, "_img_bytes")

    # Phase 4: solo-first ranking, then de-dup (perceptual + ORB crop-dup)
    records.sort(key=lambda r: (1 if is_single_subject(r) else 0,
                                r.quality_score or 0.0), reverse=True)
    kept: List[ImageRecord] = []
    removed = 0
    for r in records:
        dup = False
        for k in kept:
            d = iq.phash_distance(_phash(r), _phash(k))
            if d is not None and d <= config.phash_distance:
                dup = True
                break
            if (config.orb_dedup and iq.orb_good_matches(
                    getattr(r, "_orb", None), getattr(k, "_orb", None))
                    >= config.orb_dup_matches):
                logger.info("ORB de-dup: '%s' == '%s' (kept higher-scored)",
                            r.title, k.title)
                dup = True
                break
        if dup:
            removed += 1
        else:
            kept.append(r)
    if removed:
        logger.info("De-dup removed %d near/crop-duplicate(s)", removed)
    return kept


def _local_from_dict(d: Optional[dict]):
    """Rebuild a LocalAnalysis from its serialized dict (for score re-blend)."""
    if not d:
        return None
    la = iq.LocalAnalysis()
    for k, v in d.items():
        if hasattr(la, k):
            setattr(la, k, v)
    return la


# ---------------------------------------------------------------------------
# Stage 3: persistence (idempotent; group photos never primary)
# ---------------------------------------------------------------------------

def persist_images(performer_id: str, records: List[ImageRecord], *,
                   had_any_image: bool) -> Dict[str, Any]:
    """Upsert each image and link it to the performer, deduped by
    (source, source_identifier) / URL. is_primary lands only on the top
    single-subject portrait, and only when the performer had no images.
    Returns {'saved': n, 'primary_set': bool}."""
    if not records:
        return {"saved": 0, "primary_set": False}

    primary_idx = None if had_any_image else primary_index(records)
    next_order = _next_display_order(performer_id)
    saved = 0
    primary_set = False
    for i, r in enumerate(records):
        is_primary = (i == primary_idx)
        if _save_one(performer_id, r, is_primary=is_primary,
                     display_order=next_order):
            saved += 1
            next_order += 1
            if is_primary:
                primary_set = True
    return {"saved": saved, "primary_set": primary_set}


def _next_display_order(performer_id: str) -> int:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(display_order), -1) AS m "
                "FROM artist_images WHERE performer_id = %s",
                (performer_id,),
            )
            return cur.fetchone()["m"] + 1


def _save_one(performer_id: str, rec: ImageRecord, *,
              is_primary: bool, display_order: int) -> bool:
    """Insert (or reuse by URL) the image row and link it to the performer.
    Returns True iff a new performer<->image link was created."""
    fields = rec.db_fields()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM images WHERE source = %s AND source_identifier = %s",
                (fields["source"], fields["source_identifier"]),
            )
            existing = cur.fetchone()
            if existing:
                image_id = existing["id"]
            else:
                cur.execute(
                    """
                    INSERT INTO images (
                        url, source, source_identifier, license_type, license_url,
                        attribution, width, height, thumbnail_url, source_page_url
                    ) VALUES (
                        %(url)s, %(source)s, %(source_identifier)s, %(license_type)s,
                        %(license_url)s, %(attribution)s, %(width)s, %(height)s,
                        %(thumbnail_url)s, %(source_page_url)s
                    )
                    ON CONFLICT (url) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                    RETURNING id
                    """,
                    fields,
                )
                image_id = cur.fetchone()["id"]

            cur.execute(
                "SELECT 1 FROM artist_images WHERE performer_id = %s AND image_id = %s",
                (performer_id, image_id),
            )
            if cur.fetchone():
                conn.commit()  # image row may have been freshly inserted
                return False

            cur.execute(
                "INSERT INTO artist_images "
                "(performer_id, image_id, is_primary, display_order) "
                "VALUES (%s, %s, %s, %s)",
                (performer_id, image_id, is_primary, display_order),
            )
            conn.commit()
    return True
