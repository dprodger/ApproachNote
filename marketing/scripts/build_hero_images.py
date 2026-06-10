#!/usr/bin/env python3
"""Build the marketing-site hero images from blue-treatment source photos.

For each source photo this:
  1. trims the film-scan frame/rebate with a hand-tuned per-image inset,
  2. center-crops to a square, and
  3. resizes to an identical 1100x1100 px so the hero never shifts the page
     layout when the template rotates between them on reload. The hero box
     renders at most ~524px wide, so 1100px keeps >2x for retina with margin.

Output lands in backend/static/images/hero_NNN.jpg. The crop insets were chosen
by eye per scan (some have thick dark frames, some only a thin bright rebate);
adjust HEROES and re-run to regenerate.

Run with the backend venv (Pillow):
    backend/venv/bin/python marketing/scripts/build_hero_images.py
"""
import os
from PIL import Image

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
SRC_DIR = os.path.join(REPO, "marketing", "photos", "blue-treatment")
OUT_DIR = os.path.join(REPO, "backend", "static", "images")
SIZE = 1100

# hero id -> (source basename, (left, top, right, bottom) frame insets as fractions)
HEROES = {
    "hero_001": ("buck_clayton_full",           (0.040, 0.035, 0.045, 0.030)),
    "hero_002": ("carl_kress_full",             (0.075, 0.060, 0.065, 0.060)),
    "hero_003": ("charlie_parker_max_roach",    (0.060, 0.050, 0.045, 0.055)),
    "hero_004": ("freddie_moore_sidney_bechet", (0.055, 0.045, 0.055, 0.040)),
    "hero_005": ("illinois_jacquet_2",          (0.075, 0.058, 0.070, 0.075)),
    "hero_006": ("sidney_de_paris_full",        (0.040, 0.030, 0.030, 0.040)),
    "hero_007": ("mary_lou_williams",           (0.020, 0.015, 0.020, 0.020)),  # low-res source; upscaled
    "hero_008": ("billie_holiday",              (0.025, 0.025, 0.050, 0.035)),
    "hero_009": ("teddy_kaye_vivien_garry",     (0.040, 0.040, 0.050, 0.050)),
    "hero_010": ("ella_fitz_1",                 (0.048, 0.030, 0.050, 0.040)),
    "hero_011": ("june_christy_full",           (0.030, 0.030, 0.035, 0.035)),
}


def build_one(src_name, insets):
    l, t, r, b = insets
    im = Image.open(os.path.join(SRC_DIR, f"{src_name}.jpg")).convert("RGB")
    w, h = im.size
    im = im.crop((int(w * l), int(h * t), int(w * (1 - r)), int(h * (1 - b))))  # trim frame
    w, h = im.size
    s = min(w, h)                                                               # center square
    im = im.crop(((w - s) // 2, (h - s) // 2, (w - s) // 2 + s, (h - s) // 2 + s))
    return im.resize((SIZE, SIZE), Image.LANCZOS), s


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    for out_name, (src_name, insets) in HEROES.items():
        out, square_px = build_one(src_name, insets)
        dst = os.path.join(OUT_DIR, f"{out_name}.jpg")
        out.save(dst, quality=82, optimize=True, progressive=True)
        flag = "  (UPSCALED)" if square_px < SIZE else ""
        print(f"{out_name}.jpg <- {src_name}  square={square_px}px{flag}  "
              f"{os.path.getsize(dst) // 1024} KB")


if __name__ == "__main__":
    main()
