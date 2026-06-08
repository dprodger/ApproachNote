"""
Visual analysis for candidate performer imagery.
====================================================

Two stages, used to turn a pile of "freely-licensed" candidate images into a
ranked set of *good* ones:

  Tier 2 -- local heuristics (no API, no cost):
      * resolution            (Pillow)
      * sharpness / blur       (OpenCV variance-of-Laplacian)
      * face presence & size   (face_recognition)
      * identity match         (face_recognition embeddings vs a reference image)
      * near-duplicate de-dup  (imagehash perceptual hash)

  Tier 3 -- pluggable vision rerank (per-image judgement):
      * ClaudeReranker (default) -- Claude vision scores each survivor against a
        rubric and returns structured JSON.
      * ClipReranker (optional)  -- local open_clip zero-shot scoring.

Design notes
------------
* Every heavy dependency (PIL, cv2, numpy, face_recognition, imagehash,
  anthropic, torch/open_clip) is imported lazily *inside* the function that
  needs it, so importing this module is cheap and a missing optional library
  degrades to a logged warning instead of an ImportError at import time.
* The gate (`evaluate_gate`) and the score blend (`aggregate_score`) are pure
  functions over the dataclasses below, so they are unit-testable without any
  of the heavy libraries installed.
"""

from __future__ import annotations

import os
import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any, Tuple

logger = logging.getLogger("image_quality")


# ===========================================================================
# Data
# ===========================================================================

@dataclass
class GateConfig:
    min_long_edge: int = 500          # px on the longer side
    min_sharpness: float = 40.0       # variance-of-Laplacian; soft scans score low
    min_face_fraction: float = 0.015  # largest face area / image area
    require_face: bool = True
    identity_threshold: float = 0.60  # face_recognition distance; lower = stricter
    enforce_identity: bool = True     # only applies when a reference is available


@dataclass
class LocalAnalysis:
    width: Optional[int] = None
    height: Optional[int] = None
    sharpness: Optional[float] = None
    face_detection_ran: bool = False  # distinguishes "no face" from "lib absent"
    face_count: int = 0
    largest_face_fraction: float = 0.0
    phash: Optional[str] = None
    identity_distance: Optional[float] = None  # None => no reference or no face
    error: Optional[str] = None

    @property
    def long_edge(self) -> int:
        return max(self.width or 0, self.height or 0)


@dataclass
class QualityVerdict:
    passed_gate: bool = False
    reasons: List[str] = field(default_factory=list)   # why it failed / notes
    score: float = 0.0                                  # 0-100, for ranking
    local: Optional[LocalAnalysis] = None
    vision: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed_gate": self.passed_gate,
            "reasons": self.reasons,
            "score": round(self.score, 1),
            "local": asdict(self.local) if self.local else None,
            "vision": self.vision,
        }


# ===========================================================================
# Tier 2 -- local heuristics
# ===========================================================================

def local_capabilities() -> Dict[str, bool]:
    """Probe which optional analysis libraries are importable. Pillow is the
    floor: without it we cannot decode images at all."""
    caps: Dict[str, bool] = {}
    for key, mod in (("pillow", "PIL"), ("numpy", "numpy"), ("opencv", "cv2"),
                     ("imagehash", "imagehash"), ("face_recognition", "face_recognition")):
        try:
            __import__(mod)
            caps[key] = True
        except Exception:
            caps[key] = False
    return caps


def _load_pil(image_bytes: bytes):
    from PIL import Image  # lazy
    import io
    img = Image.open(io.BytesIO(image_bytes))
    img.load()
    return img


# ---- face engine (dlib-free) ----------------------------------------------
# Detection + identity are pluggable. Preference order:
#   1. face_recognition (dlib)  -- used only if already importable
#   2. OpenCV  -- YuNet detector + SFace identity (small ONNX models, auto-
#      downloaded once); falls back to the bundled Haar cascade for detection
#      when the models are unavailable (offline), in which case identity is
#      simply skipped.

@dataclass
class _Face:
    fraction: float   # face area / analyzed-image area (scale-invariant)
    raw: Any = None   # engine-specific payload used by embed()


_MODEL_URLS = {
    "yunet": (
        "face_detection_yunet_2023mar.onnx",
        "https://github.com/opencv/opencv_zoo/raw/main/models/"
        "face_detection_yunet/face_detection_yunet_2023mar.onnx",
    ),
    "sface": (
        "face_recognition_sface_2021dec.onnx",
        "https://github.com/opencv/opencv_zoo/raw/main/models/"
        "face_recognition_sface/face_recognition_sface_2021dec.onnx",
    ),
}


def _resolve_model_path(kind: str) -> Optional[str]:
    """Return a local path to the requested ONNX model, downloading it once to
    a cache dir if needed. Returns None if unavailable (e.g. offline). Override
    with IMAGE_YUNET_MODEL / IMAGE_SFACE_MODEL, the cache dir with
    IMAGE_MODEL_DIR, or disable downloads with IMAGE_NO_MODEL_DOWNLOAD=1."""
    from pathlib import Path
    env_key = {"yunet": "IMAGE_YUNET_MODEL", "sface": "IMAGE_SFACE_MODEL"}[kind]
    override = os.environ.get(env_key)
    if override and os.path.exists(override):
        return override
    cache = Path(os.environ.get(
        "IMAGE_MODEL_DIR",
        str(Path(__file__).resolve().parent.parent / "data" / "face_models"),
    ))
    fname, url = _MODEL_URLS[kind]
    dest = cache / fname
    if dest.exists():
        return str(dest)
    if os.environ.get("IMAGE_NO_MODEL_DOWNLOAD"):
        return None
    try:
        import urllib.request
        cache.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading %s face model -> %s", kind, dest)
        urllib.request.urlretrieve(url, str(dest))
        return str(dest)
    except Exception as e:
        logger.warning("Could not obtain %s model (%s); set %s to a local .onnx "
                       "to enable it. Continuing without.", kind, e, env_key)
        return None


class _FaceRecognitionEngine:
    name = "face_recognition"

    def detect(self, img) -> List[_Face]:
        import numpy as np
        import face_recognition
        rgb = np.array(img.convert("RGB"))
        h, w = rgb.shape[:2]
        area = float(w * h) or 1.0
        locs = face_recognition.face_locations(rgb)
        return [_Face(fraction=((b - t) * (r - l)) / area, raw=(rgb, (t, r, b, l)))
                for (t, r, b, l) in locs]

    def embed(self, img, faces: List[_Face]) -> list:
        import face_recognition
        out = []
        for f in faces:
            rgb, loc = f.raw
            out.extend(face_recognition.face_encodings(rgb, [loc]))
        return out

    def min_distance(self, ref: list, cand: list) -> Optional[float]:
        import face_recognition
        best = None
        for c in cand:
            d = face_recognition.face_distance(ref, c)
            if len(d):
                m = float(min(d))
                best = m if best is None else min(best, m)
        return best


class _OpenCVFaceEngine:
    name = "opencv"

    def __init__(self, detector, recognizer, haar):
        self.detector = detector      # cv2.FaceDetectorYN or None
        self.recognizer = recognizer  # cv2.FaceRecognizerSF or None
        self.haar = haar              # cv2.CascadeClassifier or None

    def _bgr(self, img):
        import numpy as np
        import cv2
        return cv2.cvtColor(np.array(img.convert("RGB")), cv2.COLOR_RGB2BGR)

    def detect(self, img) -> List[_Face]:
        import cv2
        bgr = self._bgr(img)
        # Downscale large images: YuNet recall is best at a moderate size, and
        # face *fraction* is scale-invariant so the gate is unaffected.
        h0, w0 = bgr.shape[:2]
        long_edge = max(h0, w0)
        if long_edge > 1024:
            s = 1024.0 / long_edge
            bgr = cv2.resize(bgr, (int(round(w0 * s)), int(round(h0 * s))))
        h, w = bgr.shape[:2]
        area = float(w * h) or 1.0
        faces: List[_Face] = []
        if self.detector is not None:
            self.detector.setInputSize((w, h))
            _, dets = self.detector.detect(bgr)
            if dets is not None:
                for row in dets:
                    fw, fh = float(row[2]), float(row[3])
                    faces.append(_Face(fraction=(fw * fh) / area,
                                       raw=("yunet", row, bgr)))
        # Fall back to Haar only if YuNet found nothing (catches some B&W /
        # off-angle vintage portraits YuNet misses).
        if not faces and self.haar is not None:
            gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
            for (x, y, fw, fh) in self.haar.detectMultiScale(gray, 1.1, 5):
                faces.append(_Face(fraction=float(fw * fh) / area,
                                   raw=("haar", (x, y, fw, fh), bgr)))
        return faces

    def embed(self, img, faces: List[_Face]) -> list:
        if self.recognizer is None:
            return []
        import cv2
        out = []
        for f in faces:
            try:
                kind, data, bgr = f.raw
                if kind == "yunet":
                    aligned = self.recognizer.alignCrop(bgr, data)
                else:  # haar bbox: crop + resize (no landmarks => rougher align)
                    x, y, fw, fh = data
                    crop = bgr[max(0, y):y + fh, max(0, x):x + fw]
                    aligned = cv2.resize(crop, (112, 112))
                out.append(self.recognizer.feature(aligned))
            except Exception as e:
                logger.debug("SFace embed failed: %s", e)
        return out

    def min_distance(self, ref: list, cand: list) -> Optional[float]:
        if self.recognizer is None:
            return None
        import cv2
        best = None
        for c in cand:
            for r in ref:
                try:
                    cos = self.recognizer.match(r, c, cv2.FaceRecognizerSF_FR_COSINE)
                    d = 1.0 - float(cos)  # -> lower is more similar (like FR distance)
                    best = d if best is None else min(best, d)
                except Exception:
                    pass
        return best


_FACE_ENGINE: Any = "uninit"


def _get_face_engine():
    global _FACE_ENGINE
    if _FACE_ENGINE != "uninit":
        return _FACE_ENGINE
    _FACE_ENGINE = _build_face_engine()
    return _FACE_ENGINE


def _build_face_engine():
    # 1) face_recognition if it happens to be installed
    try:
        import face_recognition  # noqa: F401
        import numpy  # noqa: F401
        logger.debug("face engine: face_recognition")
        return _FaceRecognitionEngine()
    except Exception:
        pass
    # 2) OpenCV
    try:
        import cv2
        import numpy  # noqa: F401
    except Exception:
        logger.warning("No face engine available (need opencv or face_recognition)")
        return None
    detector = recognizer = haar = None
    yp = _resolve_model_path("yunet")
    if yp:
        try:
            # Lower the score threshold from YuNet's strict 0.9 default to
            # improve recall on vintage / off-angle portraits. Tune via
            # IMAGE_YUNET_SCORE.
            score_thr = float(os.environ.get("IMAGE_YUNET_SCORE", "0.6"))
            detector = cv2.FaceDetectorYN_create(yp, "", (320, 320),
                                                 score_thr, 0.3, 5000)
        except Exception as e:
            logger.debug("YuNet init failed: %s", e)
    sp = _resolve_model_path("sface")
    if sp:
        try:
            recognizer = cv2.FaceRecognizerSF_create(sp, "")
        except Exception as e:
            logger.debug("SFace init failed: %s", e)
    try:
        cascade = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
        haar = cv2.CascadeClassifier(cascade)
        if haar.empty():
            haar = None
    except Exception:
        haar = None
    if detector is None and haar is None:
        logger.warning("OpenCV face detection unavailable")
        return None
    logger.info("Face engine: OpenCV (%s detection%s)",
                "YuNet" if detector is not None else "Haar",
                " + SFace identity" if recognizer is not None
                else "; identity disabled (no SFace model)")
    return _OpenCVFaceEngine(detector, recognizer, haar)


def compute_local_analysis(
    image_bytes: bytes,
    reference_encodings: Optional[list] = None,
) -> LocalAnalysis:
    """Run all local heuristics over one image. Never raises -- any failure is
    captured in `LocalAnalysis.error` so the caller can decide what to do."""
    out = LocalAnalysis()
    try:
        img = _load_pil(image_bytes)
        out.width, out.height = img.size
    except Exception as e:  # not a decodable image
        out.error = f"decode: {e}"
        return out

    # perceptual hash (dedup)
    try:
        import imagehash  # lazy
        out.phash = str(imagehash.phash(img))
    except Exception as e:
        logger.debug("phash unavailable: %s", e)

    # sharpness — downscale very large images first so the float64 Laplacian
    # can't balloon memory (a 24MP image would be ~190MB as CV_64F). Normal-
    # sized photos are untouched, so gate behavior is unchanged.
    try:
        import numpy as np  # lazy
        import cv2  # lazy
        g = img.convert("L")
        long_edge = max(g.size)
        if long_edge > 1600:
            s = 1600.0 / long_edge
            g = g.resize((max(1, int(g.size[0] * s)), max(1, int(g.size[1] * s))))
        gray = np.array(g)
        out.sharpness = float(cv2.Laplacian(gray, cv2.CV_64F).var())
    except Exception as e:
        logger.debug("sharpness unavailable: %s", e)

    # faces + identity (pluggable engine: face_recognition if present, else OpenCV)
    try:
        engine = _get_face_engine()
        if engine is not None:
            faces = engine.detect(img)
            out.face_detection_ran = True
            out.face_count = len(faces)
            if faces:
                out.largest_face_fraction = max(f.fraction for f in faces)
                if reference_encodings:
                    cand = engine.embed(img, faces)
                    if cand:
                        out.identity_distance = engine.min_distance(
                            reference_encodings, cand)
    except Exception as e:
        logger.debug("face analysis unavailable: %s", e)

    return out


def encode_reference_faces(image_bytes_list: List[bytes]) -> list:
    """Build face-embedding list from one or more reference images (e.g. the
    artist's current primary image / Wikipedia lead). Returns [] if nothing
    usable, in which case identity checking is simply skipped downstream."""
    encs: list = []
    engine = _get_face_engine()
    if engine is None:
        logger.warning("No face engine available -- identity check disabled")
        return encs
    for b in image_bytes_list:
        try:
            img = _load_pil(b)
            faces = engine.detect(img)
            encs.extend(engine.embed(img, faces))
        except Exception as e:
            logger.debug("reference encode failed: %s", e)
    return encs


def evaluate_gate(local: LocalAnalysis, cfg: GateConfig) -> Tuple[bool, List[str]]:
    """Pure gate decision. Returns (passed, reasons). Reasons describe failures;
    an empty list with passed=True means a clean pass. Missing measurements
    (heuristic lib not installed) are treated leniently -- we don't reject on a
    check we couldn't run."""
    reasons: List[str] = []

    if local.error:
        return False, [f"undecodable image ({local.error})"]

    if local.long_edge and local.long_edge < cfg.min_long_edge:
        reasons.append(f"low resolution ({local.long_edge}px < {cfg.min_long_edge})")

    if local.sharpness is not None and local.sharpness < cfg.min_sharpness:
        reasons.append(f"too blurry (sharpness {local.sharpness:.0f} < {cfg.min_sharpness:.0f})")

    if cfg.require_face and local.face_detection_ran and local.face_count == 0:
        reasons.append("no face detected")
    elif local.face_count and local.largest_face_fraction < cfg.min_face_fraction:
        reasons.append(
            f"face too small ({local.largest_face_fraction*100:.1f}% < "
            f"{cfg.min_face_fraction*100:.1f}%)"
        )

    if (cfg.enforce_identity and local.identity_distance is not None
            and local.identity_distance > cfg.identity_threshold):
        reasons.append(
            f"identity mismatch (distance {local.identity_distance:.2f} > "
            f"{cfg.identity_threshold:.2f})"
        )

    return (len(reasons) == 0), reasons


# ---- near-duplicate de-dup ------------------------------------------------

def phash_distance(a: Optional[str], b: Optional[str]) -> Optional[int]:
    """Hamming distance between two perceptual-hash hex strings, or None if
    either is missing / imagehash unavailable."""
    if not a or not b:
        return None
    try:
        import imagehash  # lazy
        return imagehash.hex_to_hash(a) - imagehash.hex_to_hash(b)
    except Exception:
        # Fallback: raw hex hamming (works because phash hex is fixed width)
        try:
            xa = bin(int(a, 16))[2:].zfill(len(a) * 4)
            xb = bin(int(b, 16))[2:].zfill(len(b) * 4)
            return sum(c1 != c2 for c1, c2 in zip(xa, xb))
        except Exception:
            return None


def orb_signature(image_bytes: bytes, max_edge: int = 512, n: int = 800):
    """ORB keypoint descriptors for crop/scale-tolerant duplicate detection.
    Returns an (N,32) uint8 array, or None. Unlike a perceptual hash, ORB keeps
    matching when one image is a crop of another."""
    try:
        import numpy as np
        import cv2
        gray = np.array(_load_pil(image_bytes).convert("L"))
        h, w = gray.shape[:2]
        s = max_edge / float(max(h, w))
        if s < 1.0:
            gray = cv2.resize(gray, (int(w * s), int(h * s)))
        _, des = cv2.ORB_create(n).detectAndCompute(gray, None)
        return des
    except Exception as e:
        logger.debug("ORB signature failed: %s", e)
        return None


def orb_good_matches(des_a, des_b, ratio: float = 0.75) -> int:
    """Count Lowe-ratio-passing matches between two ORB descriptor sets. Same
    photo (even different crops) -> hundreds; unrelated images -> single digits."""
    if des_a is None or des_b is None:
        return 0
    try:
        import cv2
        matches = cv2.BFMatcher(cv2.NORM_HAMMING).knnMatch(des_a, des_b, k=2)
        return sum(1 for m in matches
                   if len(m) == 2 and m[0].distance < ratio * m[1].distance)
    except Exception:
        return 0


def dedup_by_phash(phashes: List[Optional[str]], max_distance: int = 6) -> List[int]:
    """Greedy near-duplicate removal. Given a list of phash hex strings (index
    = candidate position), returns the indices to KEEP, preferring earlier
    items (callers should pre-sort by desirability). Items without a phash are
    always kept."""
    kept: List[int] = []
    for i, h in enumerate(phashes):
        if h is None:
            kept.append(i)
            continue
        dup = False
        for j in kept:
            d = phash_distance(h, phashes[j])
            if d is not None and d <= max_distance:
                dup = True
                break
        if not dup:
            kept.append(i)
    return kept


# ===========================================================================
# Score blend (pure)
# ===========================================================================

def aggregate_score(local: Optional[LocalAnalysis],
                    vision: Optional[Dict[str, Any]]) -> float:
    """Blend local heuristics + vision judgement into a 0-100 rank score.

    Vision quality (1-5) dominates when present; otherwise we fall back to a
    heuristic blend of face prominence, sharpness and identity confidence."""
    if vision and isinstance(vision.get("quality"), (int, float)):
        q = float(vision["quality"])               # 1-5
        score = (q / 5.0) * 80.0                    # up to 80 from the model
        if vision.get("is_photograph") is False:
            score -= 30
        if vision.get("is_subject") is False:
            score -= 40
        if vision.get("single_subject") is True:
            score += 5
        # small boost for a confident identity match
        if local and local.identity_distance is not None:
            score += max(0.0, (0.6 - local.identity_distance)) * 25.0
        return _clamp(score)

    # heuristics-only fallback
    score = 0.0
    if local:
        score += min(local.largest_face_fraction / 0.15, 1.0) * 45.0   # framing
        if local.sharpness is not None:
            score += min(local.sharpness / 300.0, 1.0) * 25.0          # crispness
        if local.identity_distance is not None:
            score += max(0.0, (0.6 - local.identity_distance) / 0.6) * 30.0
        elif local.face_count:
            score += 15.0
        # damp tiny images so they don't top the local-only ranking
        if local.long_edge:
            score *= min(local.long_edge / 500.0, 1.0)
    return _clamp(score)


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


# ===========================================================================
# Tier 3 -- pluggable vision rerank
# ===========================================================================

RUBRIC = (
    "You are grading a candidate photo for use as a performer's portrait in a "
    "music reference app. Judge ONLY what you can see. Respond with a single "
    "JSON object, no prose, with keys:\n"
    '  "is_photograph": boolean (true for a real photograph, false for artwork, '
    "poster, album cover, illustration, screenshot, text/document),\n"
    '  "is_subject": boolean (does it plausibly show the named performer as the '
    "main subject; if you cannot tell, true),\n"
    '  "single_subject": boolean (one clearly dominant person vs a crowd/group),\n'
    '  "quality": integer 1-5 (overall portrait quality: focus, lighting, '
    "composition, resolution),\n"
    '  "issues": array of short strings (e.g. "motion blur", "watermark", '
    '"heavy crop", "low light"),\n'
    '  "rationale": one short sentence.\n'
)


class VisionReranker:
    """Interface: score(image_bytes, context) -> dict matching the rubric keys."""

    name = "base"

    def score(self, image_bytes: bytes, context: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class ClaudeReranker(VisionReranker):
    name = "claude"

    def __init__(self, model: Optional[str] = None, max_edge: int = 768):
        self.model = model or os.environ.get(
            "IMAGE_RERANK_MODEL", "claude-haiku-4-5-20251001"
        )
        self.max_edge = max_edge
        self._client = None

    def _client_or_none(self):
        if self._client is not None:
            return self._client
        try:
            from anthropic import Anthropic  # lazy
        except Exception as e:
            logger.warning("anthropic SDK not installed -- Claude rerank disabled (%s)", e)
            return None
        if not os.environ.get("ANTHROPIC_API_KEY"):
            logger.warning("ANTHROPIC_API_KEY not set -- Claude rerank disabled")
            return None
        self._client = Anthropic()
        return self._client

    def _downscaled_b64(self, image_bytes: bytes) -> Tuple[str, str]:
        """Return (media_type, base64) of a downscaled JPEG to keep tokens low."""
        import io
        from PIL import Image  # lazy
        import base64
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        img.thumbnail((self.max_edge, self.max_edge))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        return "image/jpeg", base64.b64encode(buf.getvalue()).decode("ascii")

    def score(self, image_bytes: bytes, context: Dict[str, Any]) -> Dict[str, Any]:
        client = self._client_or_none()
        if client is None:
            return {}
        try:
            media_type, b64 = self._downscaled_b64(image_bytes)
            performer = context.get("performer_name", "the performer")
            msg = client.messages.create(
                model=self.model,
                max_tokens=400,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image", "source": {
                            "type": "base64", "media_type": media_type, "data": b64}},
                        {"type": "text",
                         "text": f"Performer: {performer}.\n{RUBRIC}"},
                    ],
                }],
            )
            text = "".join(
                blk.text for blk in msg.content if getattr(blk, "type", "") == "text"
            )
            return parse_vision_json(text)
        except Exception as e:
            logger.warning("Claude rerank failed: %s", e)
            return {}


class ClipReranker(VisionReranker):
    """Optional local backend. Needs torch + open_clip_torch (not installed by
    default). Scores relevance/quality with zero-shot prompts and maps to the
    same rubric shape (quality 1-5)."""

    name = "clip"

    def __init__(self, model_name: str = "ViT-B-32", pretrained: str = "openai"):
        self.model_name = model_name
        self.pretrained = pretrained
        self._model = None
        self._preprocess = None
        self._tokenizer = None

    def _ensure(self):
        if self._model is not None:
            return True
        try:
            import open_clip  # lazy, heavy
            import torch  # noqa: F401
        except Exception as e:
            logger.warning("open_clip/torch not installed -- CLIP rerank disabled (%s)", e)
            return False
        self._model, _, self._preprocess = open_clip.create_model_and_transforms(
            self.model_name, pretrained=self.pretrained)
        self._tokenizer = open_clip.get_tokenizer(self.model_name)
        self._model.eval()
        return True

    def score(self, image_bytes: bytes, context: Dict[str, Any]) -> Dict[str, Any]:
        if not self._ensure():
            return {}
        try:
            import io
            import torch
            from PIL import Image
            performer = context.get("performer_name", "a person")
            prompts = [
                f"a clear, high-quality photograph of {performer}",
                "a blurry or low-quality photo",
                "an album cover, poster, or artwork",
                "a photo of a crowd or group of people",
            ]
            img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
            image = self._preprocess(img).unsqueeze(0)
            text = self._tokenizer(prompts)
            with torch.no_grad():
                im_f = self._model.encode_image(image)
                tx_f = self._model.encode_text(text)
                im_f /= im_f.norm(dim=-1, keepdim=True)
                tx_f /= tx_f.norm(dim=-1, keepdim=True)
                probs = (100.0 * im_f @ tx_f.T).softmax(dim=-1)[0].tolist()
            good = probs[0]
            quality = 1 + round(good * 4)  # 1..5
            return {
                "is_photograph": probs[2] < 0.4,
                "is_subject": True,
                "single_subject": probs[3] < 0.4,
                "quality": int(quality),
                "issues": (["likely low quality"] if probs[1] > 0.4 else []),
                "rationale": f"CLIP good-match prob={good:.2f}",
            }
        except Exception as e:
            logger.warning("CLIP rerank failed: %s", e)
            return {}


def get_reranker(name: str = "claude", **kwargs) -> VisionReranker:
    name = (name or "claude").lower()
    if name == "clip":
        return ClipReranker(**kwargs)
    return ClaudeReranker(**kwargs)


def parse_vision_json(text: str) -> Dict[str, Any]:
    """Tolerantly extract the JSON object from a model reply (handles code
    fences / stray prose)."""
    if not text:
        return {}
    s = text.strip()
    if s.startswith("```"):
        s = s.strip("`")
        if s[:4].lower() == "json":
            s = s[4:]
    start, end = s.find("{"), s.rfind("}")
    if start != -1 and end != -1 and end > start:
        s = s[start:end + 1]
    try:
        return json.loads(s)
    except Exception as e:
        logger.debug("vision JSON parse failed: %s", e)
        return {}
