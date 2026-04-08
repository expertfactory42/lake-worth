"""One-shot test runner: clip a single URL to exercise the picture-page
vision-check code path. Consumes 1 clip from the active account.

Usage:
    python run_one_url.py <url>
    python run_one_url.py           # uses the hard-coded default URL below
"""

import sys
import traceback

DEFAULT_URL = (
    "https://star-telegram.newspapers.com/image/634438120/"
    "?match=1&terms=%22lake%20worth%22"
)


def main():
    url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL
    print(f"[run_one_url] Target URL: {url}")

    try:
        import clip_and_extract as ce
    except Exception as e:
        print(f"[run_one_url] FAILED to import clip_and_extract: {e}")
        traceback.print_exc()
        return 2

    conn = None
    driver = None
    try:
        try:
            conn = ce.get_db()
            print("[run_one_url] DB opened.")
        except Exception as e:
            print(f"[run_one_url] FAILED to open DB: {e}")
            traceback.print_exc()
            return 2

        try:
            clipped_ids = ce.build_clipped_image_ids(conn)
            print(f"[run_one_url] Loaded {len(clipped_ids)} already-clipped image ids.")
        except Exception as e:
            print(f"[run_one_url] WARN: build_clipped_image_ids failed: {e}")
            clipped_ids = set()

        try:
            driver = ce.resilient_setup_driver()
        except Exception as e:
            print(f"[run_one_url] FAILED during resilient_setup_driver: {e}")
            traceback.print_exc()
            return 2
        if driver is None:
            print("[run_one_url] resilient_setup_driver returned None — aborting.")
            return 2
        print("[run_one_url] Browser session ready.")

        try:
            result = ce.clip_page(
                driver,
                url,
                conn,
                clipped_image_ids=clipped_ids,
                done_filenames=set(),
            )
        except Exception as e:
            print(f"[run_one_url] EXCEPTION inside clip_page: {e}")
            traceback.print_exc()
            return 3

        print(f"[run_one_url] clip_page returned: {result!r}")
        if isinstance(result, dict):
            print(
                f"[run_one_url] SUCCESS: pdf={result.get('pdf_filename')} "
                f"ocr_len={result.get('ocr_len')} "
                f"articles={result.get('articles')} "
                f"date={result.get('date')}"
            )
            return 0
        if result == "skipped":
            print("[run_one_url] Page was already clipped (skipped).")
            return 0
        if result == "stop":
            print("[run_one_url] clip_page signalled 'stop' (cloudflare/block).")
            return 4
        if result == "throttled":
            print("[run_one_url] clip_page signalled 'throttled'.")
            return 5
        if result is None:
            print("[run_one_url] clip_page returned None (retry-later).")
            return 6
        if isinstance(result, list):
            print(
                "[run_one_url] clip_page returned an empty list "
                "(mostly-images path: vision said CONSISTENT, short OCR saved)."
            )
            return 0
        print("[run_one_url] Unexpected return value.")
        return 7

    finally:
        if driver is not None:
            try:
                driver.quit()
                print("[run_one_url] Driver quit.")
            except Exception as e:
                print(f"[run_one_url] WARN: driver.quit failed: {e}")
        if conn is not None:
            try:
                conn.close()
                print("[run_one_url] DB closed.")
            except Exception as e:
                print(f"[run_one_url] WARN: conn.close failed: {e}")


if __name__ == "__main__":
    sys.exit(main())
