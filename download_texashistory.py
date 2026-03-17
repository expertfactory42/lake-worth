"""
Portal to Texas History - Newspaper Downloader
Downloads full-resolution newspaper page images via IIIF from texashistory.unt.edu.
Uses Selenium for search (CAPTCHA), then downloads images directly via IIIF API.
"""

import os
import time
import re
import requests
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, StaleElementReferenceException
)

# === CONFIGURATION ===
# Search within the Texas Digital Newspaper Program collection
SEARCH_URL = (
    "https://texashistory.unt.edu/search/"
    "?q=%22lake+worth%22"
    "&t=fulltext"
    "&start_date=1909-01-01"
    "&end_date=1925-12-31"
    "&sort=date_a"
    "&fq=dc_type%3Atext_newspaper"
)
SAVE_DIR = r"c:\lake_worth\images"
IIIF_BASE = "https://texashistory.unt.edu/iiif"

WAIT_TIMEOUT = 15
ACTION_DELAY = 2
# Max width for downloaded images (IIIF supports "max" for full resolution)
IMAGE_SIZE = "max"


def setup_driver():
    """Create an undetected Chrome driver."""
    os.makedirs(SAVE_DIR, exist_ok=True)
    options = uc.ChromeOptions()
    temp_profile = os.path.join(os.path.dirname(SAVE_DIR), "chrome_temp_profile_txhist")
    os.makedirs(temp_profile, exist_ok=True)
    options.add_argument(f"--user-data-dir={temp_profile}")
    driver = uc.Chrome(options=options, version_main=145)
    driver.set_window_size(1920, 1080)
    driver.implicitly_wait(5)
    return driver


def extract_ark_id(url):
    """Extract the ARK identifier from a Portal to Texas History URL."""
    # URLs look like: /ark:/67531/metapth1495184/
    match = re.search(r'(ark:/67531/metapth\d+)', url)
    if match:
        return match.group(1)
    return None


def get_manifest(ark_id):
    """Fetch the IIIF manifest for a newspaper issue to get page count and info."""
    manifest_url = f"https://texashistory.unt.edu/{ark_id}/manifest/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        resp = requests.get(manifest_url, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"    Manifest error: {e}")
    return None


def get_page_count_from_manifest(manifest):
    """Get the number of pages from a IIIF manifest."""
    if not manifest:
        return 0
    sequences = manifest.get('sequences', [])
    if sequences:
        canvases = sequences[0].get('canvases', [])
        return len(canvases)
    return 0


def download_page_image(ark_id, page_num, filepath):
    """Download a single newspaper page image via IIIF."""
    # IIIF Image API URL: {base}/{identifier}/full/{size}/0/default.jpg
    url = f"{IIIF_BASE}/{ark_id}/m1/{page_num}/full/{IMAGE_SIZE}/0/default.jpg"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=60)
        if resp.status_code == 200 and len(resp.content) > 5000:
            with open(filepath, 'wb') as f:
                f.write(resp.content)
            size_mb = len(resp.content) / (1024 * 1024)
            return True, size_mb
        else:
            return False, f"HTTP {resp.status_code}, {len(resp.content)} bytes"
    except Exception as e:
        return False, str(e)


def parse_result_info(result_element):
    """Extract title, date, and URL from a search result element."""
    info = {"title": "", "date": "", "url": "", "ark_id": ""}
    try:
        # Find the main link
        links = result_element.find_elements(By.CSS_SELECTOR, "a[href*='ark:']")
        if links:
            info["url"] = links[0].get_attribute("href")
            info["title"] = links[0].text.strip()
            info["ark_id"] = extract_ark_id(info["url"])
    except Exception:
        pass

    # Try to extract date from the text
    try:
        text = result_element.text
        date_match = re.search(
            r'(\w+\s+\d{1,2},?\s+\d{4})', text
        )
        if date_match:
            info["date"] = date_match.group(1)
    except Exception:
        pass

    return info


def collect_search_results(driver):
    """Collect all result items from the current search page."""
    results = []
    try:
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='ark:']"))
        )
        time.sleep(ACTION_DELAY)

        # Find result containers - try various selectors
        containers = driver.find_elements(By.CSS_SELECTOR,
            ".result-item, .search-result, [class*='result'], li[class*='item']")

        if not containers:
            # Fallback: just get all ark links
            links = driver.find_elements(By.CSS_SELECTOR, "a[href*='ark:/67531/metapth']")
            seen = set()
            for link in links:
                try:
                    href = link.get_attribute("href")
                    ark_id = extract_ark_id(href)
                    if ark_id and ark_id not in seen:
                        seen.add(ark_id)
                        results.append({
                            "title": link.text.strip() or "Unknown",
                            "url": href,
                            "ark_id": ark_id,
                            "date": ""
                        })
                except StaleElementReferenceException:
                    continue
        else:
            seen = set()
            for container in containers:
                info = parse_result_info(container)
                if info["ark_id"] and info["ark_id"] not in seen:
                    seen.add(info["ark_id"])
                    results.append(info)

    except TimeoutException:
        print("  No results found on this page.")
    return results


def get_next_page_url(driver):
    """Find and return the URL for the next page of results."""
    try:
        next_links = driver.find_elements(By.CSS_SELECTOR,
            "a[rel='next'], a.next, [class*='next'] a, a[title*='Next']")
        for link in next_links:
            try:
                href = link.get_attribute("href")
                if href and link.is_displayed():
                    return href
            except StaleElementReferenceException:
                continue

        # Fallback: look for "Next" text in links
        links = driver.find_elements(By.PARTIAL_LINK_TEXT, "Next")
        for link in links:
            try:
                href = link.get_attribute("href")
                if href:
                    return href
            except StaleElementReferenceException:
                continue

        # Try pagination with page numbers
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='page=']")
        current_url = driver.current_url
        page_match = re.search(r'page=(\d+)', current_url)
        current_page = int(page_match.group(1)) if page_match else 1
        for link in links:
            try:
                href = link.get_attribute("href")
                pm = re.search(r'page=(\d+)', href)
                if pm and int(pm.group(1)) == current_page + 1:
                    return href
            except StaleElementReferenceException:
                continue

    except Exception:
        pass
    return None


def make_safe_filename(title, date_str):
    """Create a safe filename from title and date."""
    # Clean up the title
    safe = re.sub(r'[<>:"/\\|?*]', '_', title)
    safe = re.sub(r'\s+', '_', safe)
    safe = safe[:120]  # Limit length
    return safe


def process_issue(ark_id, title, date_str, issue_num):
    """Download all pages of a newspaper issue."""
    print(f"\n  [{issue_num}] {title}")
    print(f"    ARK: {ark_id}")

    # Get the IIIF manifest to find page count
    manifest = get_manifest(ark_id)
    page_count = get_page_count_from_manifest(manifest)

    if page_count == 0:
        print(f"    WARNING: Could not determine page count, trying pages 1-20...")
        page_count = 20  # Try up to 20 pages

    print(f"    Pages: {page_count}")

    safe_name = make_safe_filename(title, date_str)
    downloaded = 0

    for page in range(1, page_count + 1):
        filename = f"{safe_name}_p{page:02d}.jpg"
        filepath = os.path.join(SAVE_DIR, filename)

        if os.path.exists(filepath):
            print(f"    Page {page}/{page_count}: already exists, skipping")
            downloaded += 1
            continue

        success, info = download_page_image(ark_id, page, filepath)
        if success:
            print(f"    Page {page}/{page_count}: {filename} ({info:.1f} MB)")
            downloaded += 1
        else:
            if page_count == 20:
                # We were guessing page count, stop at first failure
                print(f"    Page {page}: no more pages")
                break
            else:
                print(f"    Page {page}/{page_count}: FAILED - {info}")

        # Small delay to be polite to the server
        time.sleep(0.5)

    return downloaded


def main():
    print("=" * 60)
    print("Portal to Texas History - Newspaper Downloader")
    print("=" * 60)
    print(f"Search: 'lake worth' in Fort Worth newspapers (1909-1925)")
    print(f"Save directory: {SAVE_DIR}")
    print()

    print("Starting Chrome...")
    driver = setup_driver()

    try:
        print(f"\nLoading search: {SEARCH_URL[:80]}...")
        driver.get(SEARCH_URL)

        # Wait for CAPTCHA or results to load
        print("If you see a CAPTCHA, please complete it in the browser.")
        print("Waiting up to 120 seconds for results to appear...")

        try:
            WebDriverWait(driver, 120).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='ark:']"))
            )
            print("Results loaded!")
        except TimeoutException:
            print("Timed out waiting for results. Please check the browser.")
            return

        # Collect all results across all pages
        all_results = []
        page_num = 0

        while True:
            page_num += 1
            print(f"\n--- Search Results Page {page_num} ---")
            results = collect_search_results(driver)

            if not results:
                print("  No results found.")
                break

            all_results.extend(results)
            print(f"  Found {len(results)} issues (total: {len(all_results)})")

            # Check for next page
            next_url = get_next_page_url(driver)
            if next_url:
                print(f"  Loading next page...")
                driver.get(next_url)
                time.sleep(ACTION_DELAY)
            else:
                print("  No more pages.")
                break

        print(f"\n{'=' * 60}")
        print(f"Total issues found: {len(all_results)}")
        print(f"{'=' * 60}")

        if not all_results:
            print("No results to download.")
            return

        # Close the browser - we don't need it for IIIF downloads
        print("\nClosing browser (IIIF downloads don't need it)...")
        driver.quit()
        driver = None

        # Download all pages for each issue
        total_pages = 0
        total_issues = 0

        for i, result in enumerate(all_results, 1):
            ark_id = result["ark_id"]
            if not ark_id:
                print(f"\n  [{i}] Skipping - no ARK ID: {result['title']}")
                continue

            pages = process_issue(ark_id, result["title"], result["date"], i)
            total_pages += pages
            total_issues += 1

        print(f"\n{'=' * 60}")
        print(f"DONE!")
        print(f"  Issues processed: {total_issues}")
        print(f"  Pages downloaded: {total_pages}")
        print(f"  Files saved to: {SAVE_DIR}")
        print(f"{'=' * 60}")

    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == "__main__":
    main()
