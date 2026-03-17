"""
Test quadrant approach: split page into 4 sections, OCR each separately.
This gives Claude 4x the effective resolution per section.
"""
import os
import fitz
import base64
import json
import anthropic
from PIL import Image
from io import BytesIO

os.environ.setdefault("ANTHROPIC_API_KEY", "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")

pdf_path = r"c:\lake_worth\pdfs\Fort_Worth_Star_Telegram_1914_02_02_2.pdf"

# Render at 300 DPI
doc = fitz.open(pdf_path)
page = doc.load_page(0)
zoom = 300 / 72.0
pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
doc.close()

w, h = img.size
print(f"Full image: {w}x{h}")

# Split into 4 quadrants with 10% overlap
overlap_w = int(w * 0.1)
overlap_h = int(h * 0.1)
mid_w = w // 2
mid_h = h // 2

quadrants = {
    "top-left":     img.crop((0, 0, mid_w + overlap_w, mid_h + overlap_h)),
    "top-right":    img.crop((mid_w - overlap_w, 0, w, mid_h + overlap_h)),
    "bottom-left":  img.crop((0, mid_h - overlap_h, mid_w + overlap_w, h)),
    "bottom-right": img.crop((mid_w - overlap_w, mid_h - overlap_h, w, h)),
}

client = anthropic.Anthropic()

for name, quad in quadrants.items():
    print(f"\n{'='*60}")
    print(f"Quadrant: {name} ({quad.size[0]}x{quad.size[1]})")
    print('='*60)

    buf = BytesIO()
    quad.save(buf, format="JPEG", quality=92)
    b64 = base64.standard_b64encode(buf.getvalue()).decode("ascii")

    resp = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2000,
        messages=[{"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64}},
            {"type": "text", "text": """This is a section of a scanned newspaper page (Fort Worth Star-Telegram or Record-Telegram, circa 1914).

Does this section contain the phrase "Lake Worth" anywhere? Read every word carefully — headlines, article text, ads, classifieds, notices.

If you find "Lake Worth", transcribe the COMPLETE article containing it — every word, every sentence.
If you don't find it, just say "No Lake Worth mention found in this section." """}
        ]}]
    )
    print(resp.content[0].text[:1500])
