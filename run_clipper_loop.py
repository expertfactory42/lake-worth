"""Auto-restart wrapper for clip_and_extract.py. Restarts on crash."""
import subprocess
import sys
import time
import os

consecutive_fails = 0

while True:
    print(f"\n{'='*60}")
    print(f"Starting clipper...")
    print(f"{'='*60}")

    start = time.time()
    result = subprocess.run(
        [sys.executable, "clip_and_extract.py"],
        cwd=r"c:\lake_worth",
    )
    elapsed = time.time() - start

    print(f"\nClipper exited with code {result.returncode} after {elapsed:.0f}s")

    # Kill stale Chrome
    os.system("taskkill /F /IM chrome.exe >nul 2>&1")
    os.system("taskkill /F /IM chromedriver.exe >nul 2>&1")

    # Track consecutive quick failures (< 60s = probably Chrome can't start)
    if elapsed < 60:
        consecutive_fails += 1
        if consecutive_fails >= 3:
            wait = min(300, 30 * consecutive_fails)
            print(f"  {consecutive_fails} consecutive quick failures. Waiting {wait}s...")
            time.sleep(wait)
        else:
            time.sleep(15)
    else:
        consecutive_fails = 0
        print("Restarting in 10 seconds...")
        time.sleep(10)
