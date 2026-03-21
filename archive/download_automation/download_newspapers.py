"""
Newspapers.com PDF Downloader
Automates downloading newspaper pages as PDFs via the Print/Save button.
"""

import os
import time
import re
import glob
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, StaleElementReferenceException
)

# === CONFIGURATION ===
SAVE_DIR = r"c:\lake_worth\pdfs"
DOWNLOAD_DIR = r"c:\lake_worth\downloads_temp"
URL_MAP_FILE = r"c:\lake_worth\pdf_urls.csv"
DATE_END = "1925-12-31"

WAIT_TIMEOUT = 15
ACTION_DELAY = 2


def get_start_date():
    """Determine start date from the last downloaded PDF filename."""
    pdfs = glob.glob(os.path.join(SAVE_DIR, "*.pdf"))
    latest_date = "1914-04-23"  # default
    for pdf in pdfs:
        basename = os.path.basename(pdf)
        m = re.search(r'(\d{4})_(\d{2})_(\d{2})_\d+\.pdf$', basename)
        if m:
            date_str = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            if date_str > latest_date:
                latest_date = date_str
    return latest_date


def build_search_url(start_date):
    return (
        "https://star-telegram.newspapers.com/search/results/"
        f"?date-end={DATE_END}&date-start={start_date}"
        "&keyword=%22lake+worth%22"
        "&sort=paper-date-asc"
    )


def setup_driver():
    """Create an undetected Chrome driver with download directory set."""
    os.makedirs(SAVE_DIR, exist_ok=True)
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    options = uc.ChromeOptions()
    temp_profile = os.path.join(os.path.dirname(SAVE_DIR), "chrome_temp_profile")
    os.makedirs(temp_profile, exist_ok=True)
    options.add_argument(f"--user-data-dir={temp_profile}")

    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
    }
    options.add_experimental_option("prefs", prefs)

    driver = uc.Chrome(options=options, version_main=145)
    driver.set_window_size(1920, 1080)
    driver.implicitly_wait(5)
    return driver


def parse_filename_from_title(title, page_url):
    """Parse page title into a filename."""
    month_map = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }

    paper_name = "Fort_Worth_Star_Telegram"
    m = re.search(r'(Fort Worth (?:Star-Telegram|Record-Telegram|Record Telegram|Star Telegram|Record))',
                  title, re.IGNORECASE)
    if m:
        paper_name = re.sub(r'[^a-zA-Z0-9]+', '_', m.group(1)).strip('_')

    date_match = re.search(r'(\w{3,9})\s+(\d{1,2}),?\s+(\d{4})', title)
    page_match = re.search(r'page\s*(\d+)', title, re.IGNORECASE)

    if date_match:
        month_str = date_match.group(1)[:3].lower()
        month = month_map.get(month_str, "00")
        day = date_match.group(2).zfill(2)
        year = date_match.group(3)
        page_num = page_match.group(1) if page_match else "0"
        return f"{paper_name}_{year}_{month}_{day}_{page_num}.pdf"

    img_match = re.search(r'/image/(\d+)', page_url)
    img_id = img_match.group(1) if img_match else str(int(time.time()))
    return f"{paper_name}_{img_id}.pdf"


def get_existing_downloads():
    """Get set of files currently in the download directory."""
    return set(glob.glob(os.path.join(DOWNLOAD_DIR, "*")))


def wait_for_new_download(before_files, timeout=30):
    """Wait for a new file to appear in the download directory."""
    for _ in range(timeout * 2):
        current = set(glob.glob(os.path.join(DOWNLOAD_DIR, "*")))
        new_files = current - before_files
        complete = [f for f in new_files
                    if not f.endswith('.crdownload') and not f.endswith('.tmp')]
        if complete:
            return complete[0]
        time.sleep(0.5)
    return None


def find_and_click(driver, search_texts, tag_selector="button, a, label, span, div"):
    """Find the most specific visible element containing any search text and click it."""
    candidates = []
    elements = driver.find_elements(By.CSS_SELECTOR, tag_selector)
    for el in elements:
        try:
            text = el.text.strip().lower()
            if any(s in text for s in search_texts) and el.is_displayed():
                tag = el.tag_name
                candidates.append((len(text), tag, text, el))
        except:
            pass
    # Sort by: 1) text length (shortest = most specific), 2) prefer clickable tags (a, button)
    tag_priority = {'a': 0, 'button': 1, 'label': 2, 'span': 3, 'div': 4, 'li': 5, 'p': 6}
    candidates.sort(key=lambda x: (x[0], tag_priority.get(x[1], 9)))
    if candidates:
        print(f"    find_and_click candidates for {search_texts}:")
        for text_len, tag, text, el in candidates[:5]:
            print(f"      {tag}: '{text[:60]}' (len={text_len})")
    for text_len, tag, text, el in candidates:
        try:
            driver.execute_script("arguments[0].click();", el)
            print(f"    Clicked: {tag} '{text[:60]}'")
            return True
        except:
            continue
    print(f"    find_and_click: NO candidates found for {search_texts}")
    return False


def click_download_icon(driver):
    """Step 1: Click the download icon button (down arrow) in the toolbar.

    See DOWNLOAD_FLOW.md for reference.
    Toolbar: [back] [search] [badge?] [Clip] [Download ↓] [Share] [Save to Ancestry]
    The download button is AFTER Clip.
    """
    all_buttons = driver.find_elements(By.CSS_SELECTOR, "button")
    clip_idx = -1
    for idx, btn in enumerate(all_buttons):
        try:
            text = btn.text.strip().lower()
            if 'clip' in text and btn.is_displayed():
                clip_idx = idx
                break
        except:
            pass

    if clip_idx < 0:
        print("    Could not find Clip button")
        return False

    # The download icon is the first icon-only button after Clip
    for check_idx in range(clip_idx + 1, min(clip_idx + 4, len(all_buttons))):
        btn = all_buttons[check_idx]
        try:
            if not btn.is_displayed():
                continue
            text = btn.text.strip().lower()
            # Skip labeled buttons like "Save to Ancestry"
            if 'save' in text or 'ancestry' in text or 'clip' in text:
                continue
            btn_text = text if text else '(icon-only)'
            driver.execute_script("arguments[0].click();", btn)
            print(f"    Clicked download button: '{btn_text}'")
            time.sleep(2)
            return True
        except:
            pass

    print("    Could not find download icon after Clip")
    return False


def click_entire_page(driver):
    """Step 2: Click the 'Entire Page' card in the Print or Download panel.

    The panel shows two cards: 'Entire Page' (left) and 'Select portion of page' (right).
    Each card is a clickable container with a thumbnail image and text label.
    """
    # Find ALL elements containing "entire page" text, pick the smallest/most specific one
    candidates = []
    elements = driver.find_elements(By.CSS_SELECTOR, "button, a, div, label, span, li, p")
    for el in elements:
        try:
            text = el.text.strip().lower()
            if 'entire page' in text and el.is_displayed():
                # Score by text length — shorter text = more specific element
                candidates.append((len(text), el))
        except:
            pass

    # Sort by text length (shortest first = most specific match)
    candidates.sort(key=lambda x: x[0])

    for text_len, el in candidates:
        try:
            driver.execute_script("arguments[0].click();", el)
            time.sleep(1)
            print("    Clicked 'Entire Page'")
            return True
        except:
            continue

    print("    Could not find 'Entire Page' option")
    return False


def download_page_as_pdf(driver, attempt_num=0):
    """Click through the download flow: download icon → Entire Page → Save as PDF.

    See DOWNLOAD_FLOW.md for the proven UI flow reference.
    """

    # Step 1: Click the download icon (after Clip button)
    if not click_download_icon(driver):
        if attempt_num == 0:
            debug_path = os.path.join(SAVE_DIR, "_debug_fail.png")
            driver.save_screenshot(debug_path)
            print(f"    Debug screenshot saved: {debug_path}")
        return False

    # Step 2: Click "Entire Page" card
    if not click_entire_page(driver):
        if attempt_num == 0:
            debug_path = os.path.join(SAVE_DIR, "_debug_fail.png")
            driver.save_screenshot(debug_path)
            print(f"    Debug screenshot saved: {debug_path}")
        return False

    time.sleep(2)

    # Step 3: Click "Save as PDF*" button
    # Panel shows: [Print] [Save as JPG] [Save as PDF*]
    if not find_and_click(driver, ['save as pdf']):
        print("    Could not find 'Save as PDF' button")
        debug_path = os.path.join(SAVE_DIR, "_debug_step3.png")
        driver.save_screenshot(debug_path)
        print(f"    Debug screenshot saved: {debug_path}")
        return False
    print("    Clicked 'Save as PDF'")

    return True


def collect_search_results(driver):
    """Collect result links from the current search results page."""
    results = []
    try:
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/image/']"))
        )
        time.sleep(ACTION_DELAY)
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/image/']")
        seen_urls = set()
        for link in links:
            try:
                href = link.get_attribute("href")
                if href and href not in seen_urls and "/image/" in href:
                    seen_urls.add(href)
                    results.append({"url": href})
            except StaleElementReferenceException:
                continue
    except TimeoutException:
        print("  No results found.")
    return results


def click_show_more(driver):
    """Click 'Show More Results' button."""
    try:
        for tag in ["button", "a", "[role='button']"]:
            elements = driver.find_elements(By.CSS_SELECTOR, tag)
            for el in elements:
                try:
                    text = el.text.strip().lower()
                    if "show more" in text or "load more" in text:
                        if el.is_displayed() and el.is_enabled():
                            driver.execute_script(
                                "arguments[0].scrollIntoView({block: 'center'});", el)
                            time.sleep(1)
                            el.click()
                            print("    Clicked 'Show More Results'")
                            time.sleep(ACTION_DELAY * 2)
                            return True
                except StaleElementReferenceException:
                    continue
    except Exception:
        pass
    return False


def run_batch(driver, seen_urls, success_count, fail_count, batch_start, search_url=None):
    """Run download batches."""
    batch_num = batch_start
    consecutive_fails = 0

    while True:
        batch_num += 1
        print(f"\n--- Batch {batch_num} ---")

        results = collect_search_results(driver)
        new_results = [r for r in results if r["url"] not in seen_urls]

        if not new_results:
            print("No new results.")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(ACTION_DELAY)
            if click_show_more(driver):
                continue
            else:
                print("No more results.")
                return success_count, fail_count, batch_num

        print(f"  Processing {len(new_results)} results")
        for r in new_results:
            seen_urls.add(r["url"])

        for i, result in enumerate(new_results, 1):
            overall = success_count + fail_count + 1
            print(f"\n[Batch {batch_num}, {i}/{len(new_results)}] (Overall #{overall})")

            # Navigate to the page
            driver.get(result["url"])
            time.sleep(5)
            title = driver.title or ""
            filename = parse_filename_from_title(title, result["url"])
            filepath = os.path.join(SAVE_DIR, filename)

            if os.path.exists(filepath):
                print(f"  Already exists: {filename}")
                success_count += 1
                consecutive_fails = 0
                continue

            print(f"  Downloading: {filename}")

            # Try up to 2 times
            downloaded = False
            for attempt in range(2):
                if attempt > 0:
                    print(f"    Retry {attempt}...")
                    driver.get(result["url"])
                    time.sleep(5)

                before_files = get_existing_downloads()

                if not download_page_as_pdf(driver):
                    continue

                print("    Waiting for download...")
                downloaded_file = wait_for_new_download(before_files, timeout=30)

                if downloaded_file:
                    try:
                        os.rename(downloaded_file, filepath)
                        size_kb = os.path.getsize(filepath) / 1024
                        print(f"    Saved: {filename} ({size_kb:.0f} KB)")
                        downloaded = True
                        break
                    except Exception as e:
                        print(f"    Error moving file: {e}")

            if downloaded:
                # Save URL mapping
                with open(URL_MAP_FILE, "a", encoding="utf-8") as uf:
                    uf.write(f"{filename},{result['url']}\n")
                success_count += 1
                consecutive_fails = 0
            else:
                fail_count += 1
                consecutive_fails += 1
                print(f"  FAILED after retries: {filename}")
                if consecutive_fails >= 3:
                    print(f"\n  3 consecutive failures. Stopping.")
                    return success_count, fail_count, batch_num

            time.sleep(ACTION_DELAY)

        # Return to search and load more
        print(f"\n  Returning to search results...")
        driver.get(search_url)
        time.sleep(ACTION_DELAY * 2)

        print(f"  Loading more results...")
        while True:
            current_results = collect_search_results(driver)
            unseen = [r for r in current_results if r["url"] not in seen_urls]
            if unseen:
                loaded = len(driver.find_elements(By.CSS_SELECTOR, "a[href*='/image/']"))
                print(f"  Found {len(unseen)} unseen results after loading {loaded}")
                break
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(ACTION_DELAY)
            if not click_show_more(driver):
                print(f"  No more results.")
                unseen = []
                break
            time.sleep(ACTION_DELAY * 2)

        if not unseen:
            return success_count, fail_count, batch_num

    return success_count, fail_count, batch_num


def main():
    start_date = get_start_date()
    search_url = build_search_url(start_date)

    print("=" * 60)
    print("Newspapers.com PDF Downloader")
    print("=" * 60)
    print(f"Save directory: {SAVE_DIR}")
    print(f"Starting from: {start_date}")
    print()

    success_count = 0
    fail_count = 0
    seen_urls = set()
    batch_num = 0
    first_run = True
    max_restarts = 20

    for restart in range(max_restarts):
        print(f"\n{'Starting' if first_run else 'Restarting'} Chrome (attempt {restart + 1})...")
        driver = None
        try:
            driver = setup_driver()

            if first_run:
                print("\nOpening Newspapers.com...")
                driver.get("https://star-telegram.newspapers.com/")
                print("Waiting 5 seconds for login...")
                time.sleep(5)
                first_run = False
            else:
                print("  Resuming...")
                time.sleep(ACTION_DELAY)

            print(f"\nLoading search results...")
            driver.get(search_url)
            time.sleep(ACTION_DELAY * 2)

            success_count, fail_count, batch_num = run_batch(
                driver, seen_urls, success_count, fail_count, batch_num, search_url
            )
            break

        except KeyboardInterrupt:
            print("\n\nInterrupted.")
            break
        except Exception as e:
            print(f"\n  Crashed: {e}")
            print(f"  Downloaded so far: {success_count}")
            print(f"  Restarting in 5 seconds...")
            time.sleep(5)
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    print(f"\n{'=' * 60}")
    print(f"DONE!")
    print(f"  Downloaded: {success_count}")
    print(f"  Failed: {fail_count}")
    print(f"  Files saved to: {SAVE_DIR}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
