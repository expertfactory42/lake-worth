"""
Test Surya OCR on a newspaper page.
"""
import fitz
from PIL import Image
from surya.foundation import FoundationPredictor
from surya.recognition import RecognitionPredictor
from surya.detection import DetectionPredictor

# Pick a page we know mentions Lake Worth
pdf_path = r"c:\lake_worth\pdfs\Fort_Worth_Star_Telegram_1914_02_02_2.pdf"

# Render PDF to image at 300 DPI
doc = fitz.open(pdf_path)
page = doc.load_page(0)
zoom = 300 / 72.0
pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
doc.close()

print(f"Image size: {img.size}")
print("Loading Surya models...")

foundation = FoundationPredictor()
det_predictor = DetectionPredictor()
rec_predictor = RecognitionPredictor(foundation)

print("Running OCR...")
predictions = rec_predictor([img], [["en"]], det_predictor=det_predictor)

# Print all recognized text
print("\n" + "=" * 60)
print("FULL OCR TEXT:")
print("=" * 60)

full_text = ""
for pred in predictions:
    for line in pred.text_lines:
        full_text += line.text + "\n"

print(full_text[:3000])

# Check for Lake Worth mentions
print("\n" + "=" * 60)
if "lake worth" in full_text.lower():
    print("FOUND 'Lake Worth' in OCR text!")
    # Show context around each mention
    lower = full_text.lower()
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
