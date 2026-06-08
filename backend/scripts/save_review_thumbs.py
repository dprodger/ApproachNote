#!/usr/bin/env python3
"""
Save ranked review thumbnails from a fetch_commons_images.py --emit-json file.

Reads the emitted JSON and downloads each candidate's thumbnail (in the ranked
order they appear) to an output directory, naming them so they sort by rank and
carry the key facts in the filename for quick visual review.

Usage:
    python scripts/save_review_thumbs.py --json out/brubeck_final.json \
        --out out/brubeck_thumbs
"""
import argparse
import json
import re
from pathlib import Path

import requests

UA = {"User-Agent": "ApproachNote/1.0 (+support@approachnote.com)"}


def slug(s: str, n: int = 40) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "-", s or "").strip("-")
    return s[:n] or "image"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True, help="Path to an --emit-json file")
    ap.add_argument("--out", required=True, help="Output directory for thumbnails")
    args = ap.parse_args()

    data = json.loads(Path(args.json).read_text())
    images = data.get("images", [])
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    print(f"{data.get('performer_name')}: {len(images)} image(s) -> {out}")
    for i, img in enumerate(images, 1):
        a = img.get("analysis") or {}
        vision = a.get("vision") or {}
        solo = "solo" if vision.get("single_subject") else (
            "group" if vision.get("single_subject") is False else "na")
        passed = "PASS" if a.get("passed_gate") else "GATE"
        score = round(img.get("quality_score") or 0)
        url = img.get("thumbnail_url") or img.get("url")
        name = f"{i:02d}_{passed}_score{score:02d}_{solo}_{slug(img.get('title',''))}.jpg"
        dest = out / name
        try:
            r = requests.get(url, headers=UA, timeout=30)
            r.raise_for_status()
            dest.write_bytes(r.content)
            print(f"  [{i}] {score:>3}  {solo:<5} {passed}  {dest.name}")
        except Exception as e:
            print(f"  [{i}] download failed: {e}  ({url})")


if __name__ == "__main__":
    main()
