"""
Test two-pass Claude approach:
  Pass 1: Quick scan - "where is Lake Worth mentioned on this page?"
  Pass 2: Focused extraction of just that article
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

# Don't resize - send full resolution
buf = BytesIO()
img.save(buf, format="JPEG", quality=90)
img_b64 = base64.standard_b64encode(buf.getvalue()).decode("ascii")
print(f"Image: {img.size[0]}x{img.size[1]}")

client = anthropic.Anthropic()

# PASS 1: Find where Lake Worth is mentioned
print("\n--- PASS 1: Locating Lake Worth ---")
resp = client.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=1000,
    messages=[{"role": "user", "content": [
        {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}},
        {"type": "text", "text": """This is a scanned newspaper page that was found by searching for "Lake Worth".
The phrase "Lake Worth" appears somewhere on this page.

Read the ENTIRE page carefully. Find every mention of "Lake Worth" and for each one, tell me:
1. The approximate location (e.g. "top right column", "middle left")
2. The headline of the article it appears in
3. The sentence containing "Lake Worth"

Be thorough. Check every column, every small item, every ad. The mention may be very small."""}
    ]}]
)
print(resp.content[0].text)

# PASS 2: Extract the full article
print("\n--- PASS 2: Full extraction ---")
resp2 = client.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=4000,
    messages=[{"role": "user", "content": [
        {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}},
        {"type": "text", "text": """This is a scanned newspaper page. Read the ENTIRE page and transcribe EVERY article that mentions "Lake Worth".

For each article, provide:
- HEADLINE
- FULL TEXT (transcribe every word you can read, use [unclear] for words you can't make out)
- Any QUOTES (text in quotation marks with speaker attribution)
- PEOPLE mentioned (names and roles)

Transcribe the complete article text. Do your best - an imperfect transcription is much better than nothing."""}
    ]}]
)
print(resp2.content[0].text)
