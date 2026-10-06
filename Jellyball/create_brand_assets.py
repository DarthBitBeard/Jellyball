"""Regenerate the JellyBall 2.1.3 brand assets ("Crystal Trophy" identity).

Approach: every shipped asset is derived from one committed master source,
``assets/jellyball-logo.jpg`` (the approved crystal-football + crystal
"jellyball" wordmark lockup, 1120px, JPEG q88). Re-running this script
reproduces the committed files byte-for-byte:

    python Jellyball/create_brand_assets.py

The master JPG was produced from Landon-approved reference art
(a 2240px AI-generated crystal lockup) by downscaling to 1120px wide with
LANCZOS and saving at JPEG quality 88. The derivation below is fully
deterministic (fixed crop geometry, MEDIANCUT quantization, fixed ICO
sizes), so the script doubles as the reproducibility proof.

Derived assets:
    jellyball-logo.jpg           1120px master lockup (committed source,
                                 also the shipped logo: README header,
                                 No-Signal placeholder)
    jellyball-icon.png           256px square app/tray icon (ball crop,
                                 256-color quantized PNG)
    jellyball.ico                Windows icon: 16/32/48/128 (ball crop)
    favicon.ico                  browser favicon: 16/32/48 (ball crop)
    favicon-32.png               32px PNG favicon fallback
    favicon-180.png              180px Apple touch icon

The square icon is a content-aware crop of the crystal football out of the
top ~62% of the lockup (the wordmark lives below that line): pixels brighter
than the navy background are boxed, then squared around the box center with
8% padding. The crop box is deterministic for the fixed master image.
"""

from pathlib import Path

from PIL import Image

ROOT = Path(__file__).resolve().parent
ASSETS = ROOT / "assets"
LOGO_MASTER = ASSETS / "jellyball-logo.jpg"

# Fraction of the lockup height that holds the football (wordmark is below).
BALL_REGION_FRACTION = 0.62
CROP_PADDING_FRACTION = 0.08
CONTENT_THRESHOLD = 26

# Windows icon sizes shipped in jellyball.ico (largest-first not required;
# 128px cap keeps the file small while covering high-DPI taskbar use).
ICO_SIZES = [(16, 16), (32, 32), (48, 48), (128, 128)]
FAVICON_ICO_SIZES = [(16, 16), (32, 32), (48, 48)]


def _ensure_assets_dir() -> None:
    ASSETS.mkdir(parents=True, exist_ok=True)


def ball_mark(lockup: Image.Image) -> Image.Image:
    """Square crop of the crystal football out of the lockup.

    The football sits above the wordmark with no gap between them, so the
    crop is sized to the largest square that fits between the image top and
    the wordmark's first content row, centered on the ball. The ball's
    pointed tips may lose a few pixels per side; invisible at icon sizes.
    """
    width, height = lockup.size
    gray = lockup.convert("L")
    corner = gray.getpixel((0, 0))
    mask = gray.point(lambda v: 255 if abs(v - corner) > CONTENT_THRESHOLD else 0)

    region_height = int(height * BALL_REGION_FRACTION)
    ball_bbox = mask.crop((0, 0, width, region_height)).getbbox()
    if not ball_bbox:
        raise RuntimeError("No football found in lockup ball region")
    left, upper, right, lower = ball_bbox

    # First content row below the ball = top of the wordmark.
    wordmark_top = height
    for y in range(lower, height):
        if mask.crop((0, y, width, y + 1)).getbbox():
            wordmark_top = y
            break

    side = min(int(max(right - left, lower - upper) * (1 + CROP_PADDING_FRACTION)), wordmark_top - 4)
    cx = (left + right) // 2
    x0 = min(max(cx - side // 2, 0), width - side)
    y1 = wordmark_top - 4
    y0 = max(y1 - side, 0)
    return lockup.crop((x0, y0, x0 + side, y0 + side))


def create_assets() -> None:
    _ensure_assets_dir()
    lockup = Image.open(LOGO_MASTER).convert("RGB")

    mark = ball_mark(lockup)

    icon = mark.resize((256, 256), Image.Resampling.LANCZOS)
    # 256-color quantization keeps the 256px icon small without visible
    # banding; MEDIANCUT is deterministic for identical input.
    icon_q = icon.quantize(colors=256, method=Image.Quantize.MEDIANCUT)
    icon_q.save(ASSETS / "jellyball-icon.png", optimize=True)

    icon.save(ASSETS / "jellyball.ico", sizes=ICO_SIZES)
    icon.save(ASSETS / "favicon.ico", sizes=FAVICON_ICO_SIZES)

    mark.resize((32, 32), Image.Resampling.LANCZOS).save(ASSETS / "favicon-32.png", optimize=True)
    mark.resize((180, 180), Image.Resampling.LANCZOS).save(ASSETS / "favicon-180.png", optimize=True)


if __name__ == "__main__":
    create_assets()
    print(f"Created JellyBall assets in {ASSETS}")
