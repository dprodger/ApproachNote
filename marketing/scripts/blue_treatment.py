#!/usr/bin/env python3
"""Apply the ApproachNote "blue duotone" treatment to black-and-white photos.

The treatment converts a photo to grayscale, stretches its tonal range, and
maps it onto a three-stop gradient anchored on the brand indigo (#363A87) — a
deep near-black indigo in the shadows, the brand hue in the mids, and a bright
tint in the highlights. This is the "vivid" model used for the marketing-site
hero images.

Usage:
    python blue_treatment.py INPUT [INPUT ...] -o OUTPUT_DIR

Each INPUT may be a file or a directory (directories are scanned for .jpg/.png).
Requires Pillow and numpy (the backend venv has both:
`backend/venv/bin/python marketing/scripts/blue_treatment.py ...`).
"""
import argparse
import os
from PIL import Image, ImageOps
import numpy as np

# Gradient stops: (position 0..1, #RRGGBB). Mid stop is the brand indigo.
STOPS = [(0.0, "0A0B33"), (0.5, "3A3FB0"), (1.0, "CFD1FA")]
AUTOCONTRAST_CUTOFF = 1  # percent clipped at each end before the tonal stretch


def _hex(h):
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


def build_lut(stops=STOPS):
    """256-entry RGB lookup table interpolating linearly between the stops."""
    pts = [(p, _hex(c)) for p, c in stops]
    lut = np.zeros((256, 3))
    for i in range(256):
        t = i / 255.0
        for (p0, c0), (p1, c1) in zip(pts, pts[1:]):
            if p0 <= t <= p1:
                f = (t - p0) / (p1 - p0)
                lut[i] = [c0[k] + (c1[k] - c0[k]) * f for k in range(3)]
                break
    return lut.astype(np.uint8)


def apply_blue_treatment(img, lut=None):
    """Return a blue-duotone RGB copy of a PIL image."""
    if lut is None:
        lut = build_lut()
    gray = ImageOps.autocontrast(img.convert("L"), cutoff=AUTOCONTRAST_CUTOFF)
    return Image.fromarray(lut[np.asarray(gray)], "RGB")


def _iter_inputs(inputs):
    exts = {".jpg", ".jpeg", ".png"}
    for path in inputs:
        if os.path.isdir(path):
            for name in sorted(os.listdir(path)):
                if os.path.splitext(name)[1].lower() in exts:
                    yield os.path.join(path, name)
        else:
            yield path


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("inputs", nargs="+", help="image files or directories")
    ap.add_argument("-o", "--output-dir", required=True, help="destination directory")
    ap.add_argument("-q", "--quality", type=int, default=90, help="JPEG quality (default 90)")
    args = ap.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    lut = build_lut()
    for src in _iter_inputs(args.inputs):
        name = os.path.splitext(os.path.basename(src))[0]
        out = apply_blue_treatment(Image.open(src), lut)
        dst = os.path.join(args.output_dir, f"{name}.jpg")
        out.save(dst, quality=args.quality)
        print(f"{name}: {out.size} -> {dst}")


if __name__ == "__main__":
    main()
