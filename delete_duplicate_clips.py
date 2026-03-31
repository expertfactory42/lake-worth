"""
Delete duplicate clips from newspapers.com clippings library.

Keeps clips referenced in our processed_pdfs table.
Deletes all others (duplicates from re-clip logic).

Flow per clip:
  1. Find clip card not in our keep list
  2. Click "Edit Clipping" button (aria-label)
  3. Click "Delete" span in the edit modal footer
  4. Click second "Delete" button in the confirmation dialog
  5. Repeat
"""

import os
import re
import time
import sqlite3
import logging
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

CLIPPINGS_URL = "https://star-telegram.newspapers.com/clippings/my-clippings/"
DB_PATH = r"c:\lake_worth\lake_worth.db"
CHROME_PROFILE = r"c:\lake_worth\chrome_temp_profile_deleter"


def get_keep_ids():
    """Load clip article IDs we want to keep from the database."""
    conn = sqlite3.connect(DB_PATH)
    keep_ids = set()

    # From processed_pdfs clip_urls (automated clips we're using)
    rows = conn.execute(
        "SELECT clip_url FROM processed_pdfs WHERE clip_url IS NOT NULL AND clip_url != ''"
    ).fetchall()
    for row in rows:
        m = re.search(r'/article/[^/]+/(\d+)', row[0])
        if m:
            keep_ids.add(m.group(1))

    # From articles clip_id (hand-clipped articles)
    rows2 = conn.execute(
        "SELECT DISTINCT clip_id FROM articles WHERE clip_id IS NOT NULL AND clip_id != ''"
    ).fetchall()
    for row in rows2:
        keep_ids.add(str(row[0]))

    conn.close()
    log.info(f"Loaded {len(keep_ids)} clip IDs to keep (automated + hand-clipped)")
    return keep_ids


def setup_driver():
    options = uc.ChromeOptions()
    os.makedirs(CHROME_PROFILE, exist_ok=True)
    options.add_argument(f"--user-data-dir={CHROME_PROFILE}")
    driver = uc.Chrome(options=options, version_main=145)
    driver.set_window_size(1920, 1080)
    return driver


def click_show_more(driver):
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
                            time.sleep(3)
                            return True
                except StaleElementReferenceException:
                    continue
    except Exception:
        pass
    return False


def get_cards(driver):
    """Get all clip cards with their article IDs and edit buttons."""
    # Wait up to 10s for cards to appear
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[class*=ClippingCard]"))
        )
    except Exception:
        pass
    cards = driver.find_elements(By.CSS_SELECTOR, "[class*=ClippingCard]")
    log.info(f"  Raw ClippingCard elements: {len(cards)}")
    results = []
    for i, card in enumerate(cards):
        try:
            link = card.find_element(By.CSS_SELECTOR, "a[href*='/article/']")
            href = link.get_attribute("href") or ""
            m = re.search(r'/article/[^/]+/(\d+)', href)
            if m:
                article_id = m.group(1)
                edit_btn = card.find_element(By.CSS_SELECTOR, "button[aria-label='Edit Clipping']")
                results.append({"id": article_id, "href": href, "edit_btn": edit_btn})
            else:
                log.debug(f"  Card {i}: no article ID in href: {href[:80]}")
        except Exception as e:
            log.warning(f"  Card {i}: parse error: {e}")
            continue
    log.info(f"  Parsed {len(results)} cards with IDs")
    if results:
        log.info(f"  First 5 IDs: {[r['id'] for r in results[:5]]}")
    return results


def delete_clip(driver, edit_btn):
    """Delete a clip: click edit, click delete, confirm delete."""
    try:
        # Step 1: Scroll to and click edit button
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", edit_btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", edit_btn)
        log.info("  Clicked Edit")

        # Step 2: Wait for edit modal, find Delete
        clicked = False
        for attempt in range(5):
            time.sleep(1)
            # Try span first
            for span in driver.find_elements(By.XPATH, "//span[text()='Delete']"):
                if span.is_displayed():
                    driver.execute_script("arguments[0].click();", span)
                    clicked = True
                    log.info("  Clicked Delete span in modal")
                    break
            if clicked:
                break
            # Try button
            for btn in driver.find_elements(By.XPATH, "//button[contains(text(), 'Delete')]"):
                if btn.is_displayed():
                    driver.execute_script("arguments[0].click();", btn)
                    clicked = True
                    log.info("  Clicked Delete button in modal")
                    break
            if clicked:
                break
            log.info(f"  Waiting for Delete in modal (attempt {attempt+1})...")

        if not clicked:
            log.warning("  Could not find Delete in edit modal after 5 attempts")
            return False

        # Step 3: Wait for confirmation dialog, click confirm Delete
        confirmed = False
        for attempt in range(5):
            time.sleep(1)
            delete_btns = driver.find_elements(By.XPATH, "//button[contains(text(), 'Delete')]")
            visible_deletes = [b for b in delete_btns if b.is_displayed()]
            log.info(f"  Confirmation: {len(visible_deletes)} visible Delete buttons")
            if len(visible_deletes) >= 2:
                driver.execute_script("arguments[0].click();", visible_deletes[-1])
                confirmed = True
                log.info("  Clicked confirm Delete")
                break
            elif len(visible_deletes) == 1:
                driver.execute_script("arguments[0].click();", visible_deletes[0])
                confirmed = True
                log.info("  Clicked confirm Delete (single button)")
                break
            log.info(f"  Waiting for confirm dialog (attempt {attempt+1})...")

        if not confirmed:
            log.warning("  Could not find confirm Delete after 5 attempts")
            return False

        time.sleep(1)
        return True

    except Exception as e:
        log.warning(f"  Delete error: {e}")
        return False


def main():
    keep_ids = get_keep_ids()
    driver = setup_driver()
    log.info("Logged in. Loading clippings page...")
    driver.get(CLIPPINGS_URL)
    time.sleep(5)

    deleted = 0
    kept = 0
    errors = 0
    attempted_ids = set()  # track IDs we've already tried to delete

    while True:
        cards = get_cards(driver)
        log.info(f"Found {len(cards)} clip cards on page")
        if not cards:
            if click_show_more(driver):
                continue
            log.info("No cards and no Show More. Done.")
            break

        dupes_on_page = [c for c in cards if c["id"] not in keep_ids and c["id"] not in attempted_ids]
        kept = len(cards) - len(dupes_on_page)
        log.info(f"  Keepers: {kept}, Dupes to delete: {len(dupes_on_page)}")

        if not dupes_on_page:
            # All visible cards are keepers — load more
            log.info(f"Page done. Loading more...")
            if click_show_more(driver):
                time.sleep(2)
            else:
                log.info("No more Show More. Done.")
                break
            continue

        # Delete first dupe, then re-scan DOM for fresh references
        card = dupes_on_page[0]
        log.info(f"Deleting: {card['href'][:70]} (id={card['id']})")
        if delete_clip(driver, card["edit_btn"]):
            deleted += 1
            attempted_ids.add(card["id"])
            log.info(f"  Deleted #{deleted}")
            time.sleep(2)  # let DOM settle after delete
        else:
            errors += 1
            attempted_ids.add(card["id"])
            log.warning(f"  Failed ({errors} errors)")
            # Dismiss modals only on failure
            try:
                from selenium.webdriver.common.keys import Keys
                driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
                time.sleep(1)
                driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
                time.sleep(1)
            except Exception:
                pass

    log.info("=" * 60)
    log.info(f"DONE: Deleted {deleted}, Kept {kept}, Errors {errors}")
    log.info("=" * 60)

    driver.quit()


if __name__ == "__main__":
    main()
