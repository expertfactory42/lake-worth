"""
Test EasyOCR on a newspaper page.
"""
import os
import sys
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.stdout.reconfigure(encoding='utf-8')

import fitz
from PIL import Image
import numpy as np
import easyocr

pdf_path = r"c:\lake_worth\pdfs\Fort_Worth_Star_Telegram_1914_02_02_2.pdf"

# Render PDF to image at 300 DPI
doc = fitz.open(pdf_path)
page = doc.load_page(0)
zoom = 300 / 72.0
pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
doc.close()

print(f"Image size: {img.size}")
print("Loading EasyOCR (first run downloads models)...")

reader = easyocr.Reader(['en'], gpu=False)

print("Running OCR...")
img_np = np.array(img)
results = reader.readtext(img_np)

print(f"\nFound {len(results)} text regions")

# Build full text
full_text = "\n".join([r[1] for r in results])

print("\n" + "=" * 60)
print("SAMPLE TEXT (first 2000 chars):")
print("=" * 60)
print(full_text[:2000])

# Check for Lake Worth mentions
print("\n" + "=" * 60)
lower = full_text.lower()
if "lake worth" in lower:
    print("FOUND 'Lake Worth' in OCR text!")
    idx = 0
    while True:
        idx = lower.find("lake worth", idx)
        if idx == -1:
            break
        start = max(0, idx - 100)
        end = min(len(full_text), idx + 100)
        print(f"\n  ...{full_text[start:end]}...")
        idx += 10
else:
    print("'Lake Worth' NOT found in OCR text")
    # Check for partial matches
    for term in ["lake", "worth", "lak", "wort"]:
        count = lower.count(term)
        if count:
            print(f"  Found '{term}' {count} time(s)")
