"""Regenerate the JellyBall 2.1.3 brand assets ("Crystal Trophy" identity).

Approach: every shipped asset is derived from one committed master source,
``assets/jellyball-lockup-master.png`` (the approved crystal-football +
crystal "jellyball" wordmark lockup, converted from the reference art).
Re-running this script reproduces the committed files byte-for-byte
(PIL PNG output is deterministic for identical input):

    python Jellyball/create_brand_assets.py

Derived assets:
    jellyball-lockup-master.png  2240x1120 master lockup (committed source)
    jellyball-logo.png           the master lockup, shipped as-is
    jellyball-icon.png           256px square app/tray icon (ball crop)
    jellyball.ico                Windows icon, multi-size (ball crop)
    favicon.ico                  browser favicon, multi-size (ball crop)
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
LOCKUP_MASTER = ASSETS / "jellyball-lockup-master.png"

# Approved final lockup art (read-only reference, not committed).
CONCEPT = Path.home() / (
    "workspace/jellyball-review/logo-concepts/"
    "media-generation-jellyball-crystal-lockup-full-0-d6764859-"
    "c39d-42a2-af0a-ce83d582a8a0.webp"
)

# Fraction of the lockup height that holds the football (wordmark is below).
BALL_REGION_FRACTION = 0.62
CROP_PADDING_FRACTION = 0.08
CONTENT_THRESHOLD = 26


def _ensure_assets_dir() -> None:
    ASSETS.mkdir(parents=True, exist_ok=True)


def build_lockup_master() -> Image.Image:
    """Convert the approved reference art to the committed PNG master."""
    return Image.open(CONCEPT).convert("RGB")


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
    lockup = build_lockup_master()
    lockup.save(LOCKUP_MASTER, optimize=True)
    lockup.save(ASSETS / "jellyball-logo.png", optimize=True)

    mark = ball_mark(lockup)

    icon = mark.resize((256, 256), Image.Resampling.LANCZOS)
    icon.save(ASSETS / "jellyball-icon.png", optimize=True)

    ico_sizes = [(16, 16), (24, 24), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]
    icon.save(ASSETS / "jellyball.ico", sizes=ico_sizes)
    icon.save(ASSETS / "favicon.ico", sizes=ico_sizes)

    mark.resize((32, 32), Image.Resampling.LANCZOS).save(ASSETS / "favicon-32.png", optimize=True)
    mark.resize((180, 180), Image.Resampling.LANCZOS).save(ASSETS / "favicon-180.png", optimize=True)


if __name__ == "__main__":
    create_assets()
    print(f"Created JellyBall assets in {ASSETS}")
