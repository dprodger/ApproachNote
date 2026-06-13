#!/usr/bin/env python3
"""Build the Open Graph / social share card for the marketing site.

Facebook, LinkedIn, iMessage, Slack, etc. read <meta property="og:image"> to
build the link-preview card. They want a 1200x630 (1.91:1) raster — NOT an SVG
and NOT a square logo, both of which render as an awkward cropped block.

The card mirrors the site hero (backend/templates/index.html):
  - cream background, brand-blue logo lockup + "Your Pocket Jazz Reference"
    headline in Colt (with "Jazz" in brand blue), and
  - the square blue-duotone hero photo beside the copy, rounded like the site.

Output lands in backend/static/images/og-card.png and is referenced by the
og:image / twitter:image tags in backend/templates/index.html.

Run with the backend venv (Pillow):
    backend/venv/bin/python marketing/scripts/build_og_card.py
"""
import os
from PIL import Image, ImageDraw, ImageFont, ImageFilter

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))

# Blue logo lockup (icon + wordmark). Canonical source:
#   marketing/1-Logos/Blue/PNG @3x/ ; the same art ships as the email logo.
LOGO = os.path.join(REPO, "backend", "static", "images", "email-logo.png")
HERO = os.path.join(REPO, "backend", "static", "images", "hero_006.jpg")
FONT = os.path.join(REPO, "marketing", "fonts", "Colt", "OTF", "Colt-Bold.otf")
OUT = os.path.join(REPO, "backend", "static", "images", "og-card.png")

# Palette — matches backend/static/css/style.css :root.
W, H = 1200, 630
BG = (255, 252, 247)          # --background  #FFFCF7
TEXT = (65, 55, 55)           # --text-primary #413737
BRAND = (54, 58, 135)         # --brand        #363A87

MARGIN = 80                   # outer margin
PHOTO = 340                   # square photo edge — kept small so the headline leads
PHOTO_MARGIN = 70             # photo's right/edge margin
RADIUS = 20                   # photo corner radius (matches .hero-img)
LOGO_W = 330                  # rendered logo width
HEAD_LINES = [("Your Pocket", False), ("Jazz Reference", None)]  # None -> two-tone
HEAD_MAX = 96                 # cap; shrunk to fit the text column
LINE_GAP = 6


def rounded_photo():
    img = Image.open(HERO).convert("RGB")
    scale = max(PHOTO / img.width, PHOTO / img.height)
    img = img.resize((round(img.width * scale), round(img.height * scale)), Image.LANCZOS)
    left = (img.width - PHOTO) // 2
    top = (img.height - PHOTO) // 2
    img = img.crop((left, top, left + PHOTO, top + PHOTO)).convert("RGBA")
    mask = Image.new("L", (PHOTO, PHOTO), 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, PHOTO, PHOTO), RADIUS, fill=255)
    img.putalpha(mask)
    return img


def main():
    card = Image.new("RGB", (W, H), BG)

    photo = rounded_photo()
    px = W - PHOTO_MARGIN - PHOTO
    py = (H - PHOTO) // 2

    # soft blue drop shadow under the photo (echoes --shadow-lg)
    shadow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow)
    sd.rounded_rectangle((px, py + 10, px + PHOTO, py + PHOTO + 10), RADIUS,
                         fill=(54, 58, 135, 60))
    shadow = shadow.filter(ImageFilter.GaussianBlur(18))
    card.paste(shadow, (0, 0), shadow)
    card.paste(photo, (px, py), photo)

    # text column: from MARGIN to a gap before the photo
    col_w = px - 56 - MARGIN
    draw = ImageDraw.Draw(card)

    # fit the headline to the column
    size = HEAD_MAX
    while size > 24:
        f = ImageFont.truetype(FONT, size)
        widest = max(draw.textlength(t, font=f) for t, _ in HEAD_LINES)
        if widest <= col_w:
            break
        size -= 1
    font = ImageFont.truetype(FONT, size)
    asc, desc = font.getmetrics()
    line_h = asc + desc

    # logo
    logo = Image.open(LOGO).convert("RGBA")
    lh = round(logo.height * (LOGO_W / logo.width))
    logo = logo.resize((LOGO_W, lh), Image.LANCZOS)

    # vertically center the whole block (logo + gap + two headline lines)
    gap_logo = 40
    block_h = lh + gap_logo + 2 * line_h + LINE_GAP
    y = (H - block_h) // 2

    card.paste(logo, (MARGIN, y), logo)
    y += lh + gap_logo

    # line 1: solid text
    draw.text((MARGIN, y), HEAD_LINES[0][0], font=font, fill=TEXT)
    y += line_h + LINE_GAP
    # line 2: "Jazz" in brand blue, " Reference" in text color
    draw.text((MARGIN, y), "Jazz", font=font, fill=BRAND)
    jazz_w = draw.textlength("Jazz", font=font)
    draw.text((MARGIN + jazz_w, y), " Reference", font=font, fill=TEXT)

    card.save(OUT, "PNG", optimize=True)
    print(f"wrote {OUT} ({W}x{H}), headline {size}px")


if __name__ == "__main__":
    main()
