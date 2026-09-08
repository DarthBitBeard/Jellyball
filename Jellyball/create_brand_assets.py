from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageFilter

ROOT = Path(__file__).resolve().parent
ASSETS = ROOT / "assets"
ASSETS.mkdir(parents=True, exist_ok=True)


def font(size: int, bold: bool = False):
    candidates = [
        Path("C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf"),
        Path("C:/Windows/Fonts/segoeuib.ttf" if bold else "C:/Windows/Fonts/segoeui.ttf"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return ImageFont.truetype(str(candidate), size)
    return ImageFont.load_default()


def football_mark(size: int) -> Image.Image:
    image = Image.new("RGBA", (size, size), (8, 12, 34, 0))
    glow = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    glow_draw = ImageDraw.Draw(glow)
    pad = size * 0.10
    glow_draw.ellipse((pad, pad, size - pad, size - pad), fill=(20, 190, 255, 120))
    glow = glow.filter(ImageFilter.GaussianBlur(max(2, int(size * 0.08))))
    image.alpha_composite(glow)

    draw = ImageDraw.Draw(image)
    cx = cy = size / 2
    rx = size * 0.23
    ry = size * 0.40
    points = []
    for index in range(72):
        import math
        angle = 2 * math.pi * index / 72
        points.append((cx + rx * math.cos(angle), cy + ry * math.sin(angle)))
    draw.polygon(points, fill=(25, 52, 93, 255), outline=(105, 132, 172, 255), width=max(2, size // 55))

    # Cyan and magenta football panels.
    draw.arc((cx - rx * 1.02, cy - ry * 0.94, cx + rx * 1.02, cy + ry * 0.94), 205, 330, fill=(35, 224, 236, 255), width=max(4, size // 22))
    draw.arc((cx - rx * 1.02, cy - ry * 0.94, cx + rx * 1.02, cy + ry * 0.94), 25, 150, fill=(204, 40, 235, 255), width=max(4, size // 22))
    draw.arc((cx - rx * 0.82, cy - ry * 0.76, cx + rx * 0.82, cy + ry * 0.76), 198, 315, fill=(17, 145, 242, 255), width=max(3, size // 30))
    draw.arc((cx - rx * 0.82, cy - ry * 0.76, cx + rx * 0.82, cy + ry * 0.76), 18, 135, fill=(170, 33, 207, 255), width=max(3, size // 30))

    # White football laces.
    lace_width = max(3, size // 28)
    draw.line((cx - size * 0.16, cy - size * 0.18, cx + size * 0.16, cy + size * 0.18), fill=(245, 250, 255, 255), width=lace_width)
    for offset in (-0.11, -0.035, 0.04, 0.115):
        x = cx + size * offset
        y = cy + size * offset
        draw.line((x - size * 0.045, y - size * 0.045, x + size * 0.045, y + size * 0.045), fill=(245, 250, 255, 255), width=lace_width)

    return image


def create_assets():
    full = Image.new("RGBA", (768, 768), (8, 9, 43, 255))
    background = Image.new("RGBA", full.size, (0, 0, 0, 0))
    bg_draw = ImageDraw.Draw(background)
    bg_draw.ellipse((-180, 20, 600, 600), fill=(10, 136, 205, 110))
    bg_draw.ellipse((350, 260, 980, 900), fill=(132, 0, 205, 120))
    background = background.filter(ImageFilter.GaussianBlur(80))
    full.alpha_composite(background)

    panel = Image.new("RGBA", (620, 620), (0, 0, 0, 0))
    panel_draw = ImageDraw.Draw(panel)
    panel_draw.rounded_rectangle((8, 8, 612, 612), radius=78, fill=(24, 35, 59, 235), outline=(99, 126, 169, 255), width=7)
    panel_mark = football_mark(430)
    panel.alpha_composite(panel_mark, (95, 45))
    full.alpha_composite(panel, (74, 40))

    draw = ImageDraw.Draw(full)
    title = "JellyBall"
    title_font = font(88, bold=True)
    bbox = draw.textbbox((0, 0), title, font=title_font)
    x = (768 - (bbox[2] - bbox[0])) // 2
    y = 610
    draw.text((x + 2, y + 2), title, font=title_font, fill=(8, 13, 34, 255))
    draw.text((x, y), title, font=title_font, fill=(236, 244, 255, 255))
    draw.text((x, y), "Jelly", font=title_font, fill=(38, 216, 232, 255))
    draw.text((x, y + 3), "Jelly", font=title_font, fill=(208, 44, 229, 210))

    full.save(ASSETS / "jellyball-logo.png", optimize=True)

    icon = Image.new("RGBA", (256, 256), (8, 12, 34, 255))
    icon_draw = ImageDraw.Draw(icon)
    icon_draw.rounded_rectangle((4, 4, 252, 252), radius=42, fill=(24, 35, 59, 255), outline=(99, 126, 169, 255), width=4)
    icon.alpha_composite(football_mark(220), (18, 18))
    icon.save(ASSETS / "jellyball-icon.png", optimize=True)
    icon.save(ASSETS / "jellyball.ico", sizes=[(16, 16), (24, 24), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)])


if __name__ == "__main__":
    create_assets()
    print(f"Created JellyBall assets in {ASSETS}")
