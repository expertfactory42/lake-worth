"""
Scheduled downloader: runs download_newspapers.py every hour.
Downloads up to 60 files per run, then waits for the rate limit to reset.
Keeps the computer active between runs.
"""

import subprocess
import time
import ctypes
import sys
import os
from datetime import datetime

MAX_PER_RUN = 60
WAIT_MINUTES = 60

# Prevent Windows from sleeping
ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
ES_DISPLAY_REQUIRED = 0x00000002

def keep_awake():
    ctypes.windll.kernel32.SetThreadExecutionState(
        ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED
    )
    print("  System sleep disabled.")

def allow_sleep():
    ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)


def cleanup_chrome():
    """Kill any leftover chromedriver/zombie Chrome processes from failed runs."""
    for proc_name in ["chromedriver.exe"]:
        os.system(f"taskkill /F /IM {proc_name} >nul 2>&1")


def count_pdfs():
    import glob
    return len(glob.glob(r"c:\lake_worth\pdfs\*.pdf"))


def run_download():
    """Run the download script, kill it after MAX_PER_RUN successful downloads."""
    start_count = count_pdfs()
    print(f"  Starting PDF count: {start_count}")

    proc = subprocess.Popen(
        [sys.executable, "-u", "download_newspapers.py"],
        cwd=r"c:\lake_worth",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    downloaded = 0
    try:
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                print(f"    {line}")
            if "Saved:" in line:
                downloaded += 1
                if downloaded >= MAX_PER_RUN:
                    print(f"\n  Reached {MAX_PER_RUN} downloads. Stopping.")
                    proc.kill()
                    break
            if "DONE!" in line or "No more results" in line:
                break
    except Exception as e:
        print(f"  Error reading output: {e}")
    finally:
        proc.kill()
        proc.wait()
        cleanup_chrome()

    end_count = count_pdfs()
    new_files = end_count - start_count
    print(f"  New files this run: {new_files} (total: {end_count})")
    return new_files


def main():
    print("=" * 60)
    print("Lake Worth Download Scheduler")
    print(f"  Downloads up to {MAX_PER_RUN} files per run")
    print(f"  Waits {WAIT_MINUTES} minutes between runs")
    print("  Ctrl+C to stop")
    print("=" * 60)

    keep_awake()
    run_num = 0

    try:
        while True:
            run_num += 1
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n--- Run #{run_num} at {now} ---")

            new = run_download()

            if new == 0:
                print("  No new files downloaded. May have hit the end or rate limit.")

            next_run = datetime.now().strftime("%H:%M:%S")
            print(f"\n  Waiting {WAIT_MINUTES} minutes until next run...")
            for remaining in range(WAIT_MINUTES, 0, -1):
                mins = remaining
                if mins % 10 == 0 or mins <= 5:
                    print(f"    {mins} minutes remaining...")
                time.sleep(60)

    except KeyboardInterrupt:
        print("\n\nScheduler stopped by user.")
    finally:
        allow_sleep()
        print("System sleep re-enabled.")
        total = count_pdfs()
        print(f"Total PDFs: {total}")


if __name__ == "__main__":
    main()
