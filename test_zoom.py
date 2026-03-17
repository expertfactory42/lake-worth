"""Quick test: zoom out in the viewer, see how many tiles load and at what resolution."""

import os
import time
import re
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

SAVE_DIR = r"c:\lake_worth\captures"

def setup_driver():
    options = uc.ChromeOptions()
    temp_profile = os.path.join(os.path.dirname(SAVE_DIR), "chrome_temp_profile")
    os.makedirs(temp_profile, exist_ok=True)
    options.add_argument(f"--user-data-dir={temp_profile}")
    driver = uc.Chrome(options=options, version_main=145)
    driver.set_window_size(1920, 1080)
    driver.implicitly_wait(5)
    return driver

def collect_tiles(driver):
    return driver.execute_script("""
        var tiles = [];
        var seen = {};
        var images = document.querySelectorAll('image');
        for (var i = 0; i < images.length; i++) {
            var href = images[i].getAttribute('href') || images[i].getAttribute('xlink:href') || '';
            if (href.indexOf('img.newspapers.com') !== -1 && href.indexOf('iat=') !== -1) {
                if (!seen[href]) {
                    seen[href] = true;
                    tiles.push(href);
                }
            }
        }
        return tiles;
    """)

def analyze_tiles(tiles):
    """Parse tile URLs to understand coverage and resolution."""
    if not tiles:
        return
    min_x, min_y = float('inf'), float('inf')
    max_x, max_y = 0, 0
    widths = set()
    heights = set()
    for url in tiles:
        crop = re.search(r'crop=([^&]+)', url)
        w = re.search(r'[?&]width=(\d+)', url)
        h = re.search(r'[?&]height=(\d+)', url)
        if crop:
            parts = crop.group(1).split(',')
            cx, cy, cw, ch = [int(float(p)) for p in parts]
            min_x = min(min_x, cx)
            min_y = min(min_y, cy)
            max_x = max(max_x, cx + cw)
            max_y = max(max_y, cy + ch)
            widths.add(cw)
            heights.add(ch)
        if w and h:
            render_w, render_h = int(w.group(1)), int(h.group(1))

    print(f"  Coverage: ({min_x},{min_y}) to ({max_x},{max_y}) = {max_x}x{max_y}")
    print(f"  Tile crop sizes: w={sorted(widths)}, h={sorted(heights)}")
    # Show scale from first tile
    sample = tiles[0]
    w_m = re.search(r'[?&]width=(\d+)', sample)
    crop_m = re.search(r'crop=([^&]+)', sample)
    if w_m and crop_m:
        rw = int(w_m.group(1))
        cw = int(float(crop_m.group(1).split(',')[2]))
        print(f"  Scale: render_w={rw}, crop_w={cw}, ratio={rw/cw:.2f}")

# Second page URL (the one that was problematic)
TEST_URL = "https://star-telegram.newspapers.com/image/634018730/?match=1&terms=%22lake%20worth%22"

driver = setup_driver()
try:
    print("Waiting 60s for login...")
    driver.get("https://star-telegram.newspapers.com/")
    time.sleep(60)

    print(f"\nLoading: {TEST_URL}")
    driver.get(TEST_URL)
    time.sleep(10)

    # Check initial tiles
    tiles = collect_tiles(driver)
    print(f"\n=== INITIAL (default zoom) ===")
    print(f"  Tiles: {len(tiles)}")
    analyze_tiles(tiles)

    # Find the viewer SVG
    svg_el = None
    svgs = driver.find_elements(By.TAG_NAME, 'svg')
    for svg in svgs:
        imgs = svg.find_elements(By.TAG_NAME, 'image')
        if len(imgs) > 3:
            svg_el = svg
            break

    if not svg_el:
        svg_el = driver.find_element(By.TAG_NAME, 'body')

    body = driver.find_element(By.TAG_NAME, 'body')

    # Click the actual zoom-out button
    print("\n--- Clicking 'Zoom Out' button ---")
    zoom_out_btn = None
    buttons = driver.find_elements(By.CSS_SELECTOR, "button")
    for btn in buttons:
        try:
            text = btn.text.strip().lower()
            if 'zoom out' in text and btn.is_displayed():
                zoom_out_btn = btn
                break
        except:
            pass

    if zoom_out_btn:
        for i in range(8):
            try:
                zoom_out_btn.click()
                time.sleep(1.5)
                tiles = collect_tiles(driver)
                print(f"  Click {i+1}: {len(tiles)} tiles")
                analyze_tiles(tiles)
            except Exception as e:
                print(f"  Click {i+1}: error - {e}")
                break

        time.sleep(3)
        tiles = collect_tiles(driver)
        print(f"\n=== AFTER ZOOM OUT (button clicks) ===")
        print(f"  Tiles: {len(tiles)}")
        analyze_tiles(tiles)
    else:
        print("  Zoom out button not found!")

    # Also check: zoom IN to see if we get higher res tiles
    print("\n--- Now clicking 'Zoom In' button ---")
    zoom_in_btn = None
    for btn in buttons:
        try:
            text = btn.text.strip().lower()
            if 'zoom in' in text and btn.is_displayed():
                zoom_in_btn = btn
                break
        except:
            pass

    if zoom_in_btn:
        # First reset by clicking zoom in a lot
        for i in range(12):
            try:
                zoom_in_btn.click()
                time.sleep(1.5)
                tiles = collect_tiles(driver)
                print(f"  Zoom in {i+1}: {len(tiles)} tiles")
                if tiles:
                    sample = tiles[0]
                    w_m = re.search(r'[?&]width=(\d+)', sample)
                    crop_m = re.search(r'crop=([^&]+)', sample)
                    if w_m and crop_m:
                        rw = int(w_m.group(1))
                        cw = int(float(crop_m.group(1).split(',')[2]))
                        print(f"    Scale: render={rw}, crop={cw}, ratio={rw/cw:.2f}")
            except Exception as e:
                print(f"  Zoom in {i+1}: error - {e}")
                break

    print(f"\n--- Sample tile URL ---")
    tiles = collect_tiles(driver)
    if tiles:
        print(f"  {tiles[0][:250]}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
finally:
    driver.quit()
