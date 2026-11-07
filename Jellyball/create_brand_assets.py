"""Regenerate the JellyBall brand assets (3.1.1 "Crystal Trophy" artwork, with laces).

Approach: every shipped asset is derived from one committed master source,
``assets/jellyball-logo.jpg`` (the approved crystal-football + crystal
"jellyball" wordmark lockup, 1120px, JPEG q88). Re-running this script
reproduces the committed files byte-for-byte:

    python Jellyball/create_brand_assets.py

The master JPG was produced from owner-approved reference art
(a 2880px AI-generated crystal lockup, 3.1.1) by downscaling to 1120px wide with
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

The square icon is the whole crystal football from the top ~62% of the
lockup (the wordmark lives below that line): pixels brighter than the navy
background are boxed and centered on a navy square with 8% padding, so the
ball's tips are never cut. Deterministic for the fixed master image.
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


def _fill_fading(canvas: Image.Image, edge_row: Image.Image, top: int, rows: int, fade_down: bool = True) -> None:
    """Fill canvas rows [top, top+rows) with edge_row stretched vertically,
    fading to the canvas background color away from the image edge."""
    width = canvas.width
    stretched = edge_row.resize((width, rows))
    flat = Image.new("RGB", (width, rows), canvas.getpixel((0, canvas.height - 1)) if fade_down else canvas.getpixel((0, 0)))
    mask = Image.new("L", (1, rows))
    for y in range(rows):
        t = (y + 1) / rows
        mask.putpixel((0, y), int(255 * (1 - t if fade_down else t)))
    canvas.paste(Image.composite(stretched, flat, mask.resize((width, rows))), (0, top))


def ball_mark(lockup: Image.Image) -> Image.Image:
    """Square image of the whole crystal football from the lockup.

    The ball is wider than the space between the image top and the wordmark,
    so a plain square crop would cut its tips off. Instead the ball's box
    (everything brighter than the navy background, above the wordmark) is
    centered in a square with CROP_PADDING_FRACTION padding around its longer
    side; the square is filled with the real pixels around the ball, and only
    the rows above the image top or below the wordmark line get the flat
    background color (far from the glow, so no visible seam).
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

    # First content row below the ball = top of the wordmark; never include it.
    wordmark_top = height
    for y in range(lower, height):
        if mask.crop((0, y, width, y + 1)).getbbox():
            wordmark_top = y
            break
    lower = min(lower, wordmark_top - 1)

    side = int(max(right - left, lower - upper) * (1 + CROP_PADDING_FRACTION))
    cx, cy = (left + right) // 2, (upper + lower) // 2
    # Real pixels around the ball (its glow fades into the background) for
    # the whole square width and every row above the wordmark; only rows the
    # image does not have are filled with the background color.
    x0 = cx - side // 2
    band = lockup.crop((x0, 0, x0 + side, wordmark_top - 1))
    canvas = Image.new("RGB", (side, side), lockup.getpixel((0, 0)))
    # Center the ball, but when the band is tall enough keep the square inside
    # it so no flat strip shows (the ball then sits a little off center).
    offset = side // 2 - cy
    if band.height >= side:
        offset = min(0, max(offset, side - band.height))
    canvas.paste(band, (0, offset))
    # Rows the image does not have: continue the band's edge row and fade it
    # into the background color, which leaves no seam and no streaks.
    if offset > 0:
        _fill_fading(canvas, band.crop((0, 0, side, 1)), 0, offset, fade_down=False)
    bottom = offset + band.height
    if bottom < side:
        _fill_fading(canvas, band.crop((0, band.height - 1, side, band.height)), bottom, side - bottom)
    return canvas


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
