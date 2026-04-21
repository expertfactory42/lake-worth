"""
Standalone test: login to newspapers.com with Claude vision-based Turnstile click.
Three-tier system: fast path (cached), diagnostic (re-find), full calibration (two-point).
"""
import os
import sys
import time
import base64
import json
import random
import re
import sqlite3
from datetime import datetime

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

import anthropic
import pyautogui
from PIL import Image, ImageDraw
from seleniumbase import Driver
from selenium.webdriver.common.by import By

LOG_DIR = r"c:\lake_worth\collector_logs"
CALIB_FILE = r"c:\lake_worth_runtime\turnstile_calibration.json"
PROFILE = r"c:\lake_worth_runtime\chrome_temp_profile_test_turnstile"
JITTER_RANGE = 2  # pixels — keep small, checkbox is only ~18px wide

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(os.path.dirname(CALIB_FILE), exist_ok=True)


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def get_account():
    conn = sqlite3.connect(r"c:\lake_worth\lake_worth.db")
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT email, password FROM accounts WHERE active=1 LIMIT 1").fetchone()
    conn.close()
    return row["email"], row["password"]


# ── Claude API ──────────────────────────────────────────────────────────────

def ask_claude(image_path, prompt):
    """Send an image to Claude and get a text response."""
    with open(image_path, "rb") as f:
        img_b64 = base64.standard_b64encode(f.read()).decode("utf-8")
    client = anthropic.Anthropic()
    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": {
                    "type": "base64", "media_type": "image/png",
                    "data": img_b64}},
                {"type": "text", "text": prompt}
            ]
        }]
    )
    return resp.content[0].text.strip()


# ── Screenshot / marker helpers ─────────────────────────────────────────────

def screenshot_with_marker(save_path, x, y):
    """Take screenshot and draw a red crosshair at pixel position (x, y)."""
    img = pyautogui.screenshot()
    draw = ImageDraw.Draw(img)
    r = 14
    draw.ellipse([x - r, y - r, x + r, y + r], fill="red", outline="yellow", width=3)
    draw.line([x - 30, y, x + 30, y], fill="red", width=3)
    draw.line([x, y - 30, x, y + 30], fill="red", width=3)
    img.save(save_path)
    return save_path


def parse_coords(text, label):
    """Parse 'LABEL x,y' or just 'x, y' from Claude's response."""
    m = re.search(rf'{label}\s+(\d+)\s*,\s*(\d+)', text, re.IGNORECASE)
    if m:
        return int(m.group(1)), int(m.group(2))
    # Fallback: any x,y pair
    m = re.search(r'(\d+)\s*,\s*(\d+)', text)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None


# ── Calibration persistence ─────────────────────────────────────────────────

def load_calibration():
    """Load calibration from disk. Returns dict or None."""
    if not os.path.exists(CALIB_FILE):
        return None
    try:
        with open(CALIB_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return None


def save_calibration(data):
    """Save calibration dict to disk atomically."""
    tmp = CALIB_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, CALIB_FILE)
    log(f"Calibration saved: scale=({data.get('scale_x', '?')},{data.get('scale_y', '?')}) "
        f"pos=({data.get('last_click_x', '?')},{data.get('last_click_y', '?')})")


def calibration_is_valid(calib):
    """Check if cached calibration is usable for Tier 1."""
    if not calib:
        return False
    # Screen resolution must match
    current_res = list(pyautogui.size())
    if calib.get("screen_resolution") != current_res:
        log(f"  Resolution mismatch: {calib.get('screen_resolution')} vs {current_res}")
        return False
    # Must have a successful position
    if not calib.get("last_click_x") or not calib.get("last_success_time"):
        return False
    # Not too many consecutive failures
    if calib.get("fail_count", 0) >= 3:
        log(f"  Too many consecutive failures: {calib['fail_count']}")
        return False
    # Calibration not older than 7 days
    cal_time = calib.get("calibration_time")
    if cal_time:
        try:
            age = datetime.now() - datetime.fromisoformat(cal_time)
            if age.days > 7:
                log(f"  Calibration too old: {age.days} days")
                return False
        except Exception:
            pass
    return True


# ── Coordinate helpers ──────────────────────────────────────────────────────

def add_jitter(x, y, max_px=JITTER_RANGE):
    """Add random offset distributed over the checkbox square (rectangular, not circular)."""
    # Checkbox square is ~18x18 pixels. Jitter independently on each axis
    # so the distribution covers the full rectangle, not a circle.
    jx = random.randint(-max_px, max_px)
    jy = random.randint(-max_px, max_px)
    return x + jx, y + jy


def apply_calibration(claude_x, claude_y, calib):
    """Convert Claude's coordinates to actual screen pixels."""
    sx = calib.get("scale_x", 1.0)
    sy = calib.get("scale_y", 1.0)
    ox = calib.get("offset_x", 0)
    oy = calib.get("offset_y", 0)
    return int(claude_x * sx + ox), int(claude_y * sy + oy)


# ── Claude vision steps ─────────────────────────────────────────────────────

def find_checkbox_claude(ts):
    """Take clean screenshot, ask Claude for checkbox position. Returns (claude_x, claude_y) or None."""
    shot = os.path.join(LOG_DIR, f"ts_find_{ts}.png")
    pyautogui.screenshot().save(shot)
    answer = ask_claude(shot, (
        "This is a full-screen screenshot. "
        "Is there a Cloudflare 'Verify you are human' checkbox visible? "
        "If yes, respond with ONLY the pixel coordinates of the CENTER of the "
        "checkbox square (the small clickable square, not the text) in the format: "
        "CHECKBOX x,y\n"
        "If there is no such checkbox, respond with: NONE"
    ))
    log(f"  Find checkbox: {answer}")
    coords = parse_coords(answer, "CHECKBOX")
    if not coords:
        coords = parse_coords(answer, "CLICK")
    return coords


def probe_marker(actual_x, actual_y, ts, label="probe"):
    """Draw marker at (actual_x, actual_y), ask Claude where it sees it. Returns (claude_x, claude_y) or None."""
    shot = os.path.join(LOG_DIR, f"ts_{label}_{ts}.png")
    screenshot_with_marker(shot, actual_x, actual_y)
    answer = ask_claude(shot, (
        "This screenshot has a RED CIRCLE WITH CROSSHAIR drawn on it.\n"
        "Tell me the pixel coordinates of the center of the RED MARKER: MARKER x,y\n"
        "Respond with ONLY that one line."
    ))
    log(f"  Probe {label} at actual ({actual_x},{actual_y}): Claude sees {answer}")
    return parse_coords(answer, "MARKER")


def two_point_calibrate(ts, claude_checkbox):
    """Calibrate by sweeping from top-left corner to near the checkbox.
    claude_checkbox: (x, y) in Claude's coordinate space where it thinks the checkbox is.
    Returns dict or None."""
    # Point A: upper-left of the page content area (on white background, not browser chrome)
    probe_a = (200, 250)
    # Point B: near the checkbox area — use Claude's estimate as the destination
    # (we move to Claude's raw coords, which will be wrong, but that's the point —
    #  we measure where Claude sees the marker to derive the scale)
    probe_b = (claude_checkbox[0], claude_checkbox[1])

    log(f"  Calibration sweep: corner ({probe_a[0]},{probe_a[1]}) -> near checkbox ({probe_b[0]},{probe_b[1]})")

    # Probe A: draw marker at corner
    pyautogui.moveTo(probe_a[0], probe_a[1], duration=0.3)
    time.sleep(0.3)
    claude_a = probe_marker(probe_a[0], probe_a[1], ts, "calib_corner")
    if not claude_a:
        log("  Calibration: couldn't read corner probe")
        return None

    # Probe B: sweep to checkbox area, draw marker
    pyautogui.moveTo(probe_b[0], probe_b[1], duration=0.5)
    time.sleep(0.3)
    claude_b = probe_marker(probe_b[0], probe_b[1], ts, "calib_target")
    if not claude_b:
        log("  Calibration: couldn't read target probe")
        return None

    # Solve affine: actual = claude * scale + offset
    dx_actual = probe_b[0] - probe_a[0]
    dy_actual = probe_b[1] - probe_a[1]
    dx_claude = claude_b[0] - claude_a[0]
    dy_claude = claude_b[1] - claude_a[1]

    if abs(dx_claude) < 10 or abs(dy_claude) < 10:
        log("  Calibration: probes too close in Claude-space")
        return None

    scale_x = dx_actual / dx_claude
    scale_y = dy_actual / dy_claude
    offset_x = probe_a[0] - claude_a[0] * scale_x
    offset_y = probe_a[1] - claude_a[1] * scale_y

    log(f"  Calibration: scale=({scale_x:.4f},{scale_y:.4f}) offset=({offset_x:.1f},{offset_y:.1f})")

    return {
        "scale_x": round(scale_x, 4),
        "scale_y": round(scale_y, 4),
        "offset_x": round(offset_x, 1),
        "offset_y": round(offset_y, 1),
        "probe_points": [
            {"actual": list(probe_a), "claude_saw": list(claude_a)},
            {"actual": list(probe_b), "claude_saw": list(claude_b)},
        ],
    }


def verify_on_target(target_x, target_y, calib, ts):
    """Draw marker at target, ask Claude if it's on the checkbox. Returns (adj_x, adj_y, verified)."""
    shot = os.path.join(LOG_DIR, f"ts_verify_{ts}.png")
    screenshot_with_marker(shot, target_x, target_y)
    answer = ask_claude(shot, (
        "This screenshot has a RED CIRCLE WITH CROSSHAIR marker.\n"
        "Is the red marker on top of the Cloudflare checkbox square?\n"
        "Respond ONLY: ON_TARGET or OFF_TARGET"
    ))
    log(f"  Verify target ({target_x},{target_y}): {answer}")

    if "ON" in answer.upper() and "OFF" not in answer.upper():
        return target_x, target_y, True

    # Off target — ask for both positions and do differential correction
    recal = ask_claude(shot, (
        "RED MARKER and CHECKBOX positions in this screenshot:\n"
        "MARKER x,y\nCHECKBOX x,y\n"
        "Respond with exactly those two lines."
    ))
    log(f"  Differential correction: {recal}")
    marker = parse_coords(recal, "MARKER")
    checkbox = parse_coords(recal, "CHECKBOX")
    if marker and checkbox and calib:
        sx = calib.get("scale_x", 1.0)
        sy = calib.get("scale_y", 1.0)
        diff_x = int((checkbox[0] - marker[0]) * sx)
        diff_y = int((checkbox[1] - marker[1]) * sy)
        adj_x = target_x + diff_x
        adj_y = target_y + diff_y
        log(f"  Adjusted: ({target_x},{target_y}) + ({diff_x},{diff_y}) = ({adj_x},{adj_y})")
        return adj_x, adj_y, False

    return target_x, target_y, False


# ── Click mechanics ─────────────────────────────────────────────────────────

def do_cdp_disconnect_click_reconnect(driver, x, y):
    """Disconnect CDP, wait, click with jitter, reconnect."""
    log("Disconnecting CDP...")
    try:
        driver.disconnect()
    except Exception as e:
        log(f"Disconnect: {e}")

    time.sleep(random.uniform(3.0, 5.0))

    # First click — randomize everything: move speed, pre-click pause, post-click wait
    jx, jy = add_jitter(x, y)
    pyautogui.moveTo(jx, jy, duration=random.uniform(0.3, 0.9))
    time.sleep(random.uniform(0.3, 1.2))
    pyautogui.click()
    log(f"Click 1 at ({jx},{jy})")
    time.sleep(random.uniform(2.0, 4.0))

    # Second click with different jitter and timing
    jx2, jy2 = add_jitter(x, y, max_px=2)
    pyautogui.moveTo(jx2, jy2, duration=random.uniform(0.1, 0.4))
    time.sleep(random.uniform(0.2, 0.7))
    pyautogui.click()
    log(f"Click 2 at ({jx2},{jy2})")
    time.sleep(random.uniform(3.0, 5.0))

    log("Reconnecting...")
    try:
        driver.reconnect(timeout=8)
    except Exception as e:
        log(f"Reconnect: {e}")
    time.sleep(2)


def check_click_success(ts):
    """Screenshot and ask Claude if checkbox is checked."""
    shot = os.path.join(LOG_DIR, f"ts_result_{ts}.png")
    try:
        pyautogui.screenshot().save(shot)
        result = ask_claude(shot, (
            "Is the Cloudflare 'Verify you are human' checkbox now CHECKED "
            "(has a checkmark/tick)? Respond ONLY: CHECKED or UNCHECKED"
        ))
        log(f"  Checkbox status: {result}")
        upper = result.upper()
        return "CHECKED" in upper and "UNCHECKED" not in upper
    except Exception as e:
        log(f"  Result check error: {e}")
        return False


# ── Three-tier click system ─────────────────────────────────────────────────

def click_turnstile(driver):
    """Main entry point: tries Tier 1 (fast), then Tier 2 (diagnostic), then Tier 3 (full calibration)."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    calib = load_calibration()

    # ── Tier 1: Fast path — use last successful position ────────────────
    if calib and calibration_is_valid(calib) and calib.get("last_click_x"):
        log(f"Tier 1 (fast): using cached position ({calib['last_click_x']},{calib['last_click_y']})")
        do_cdp_disconnect_click_reconnect(driver, calib["last_click_x"], calib["last_click_y"])
        ts_check = datetime.now().strftime("%Y%m%d_%H%M%S")
        if check_click_success(ts_check):
            calib["last_success_time"] = datetime.now().isoformat()
            calib["success_count"] = calib.get("success_count", 0) + 1
            calib["fail_count"] = 0
            save_calibration(calib)
            log("Tier 1 SUCCESS")
            return True
        log("Tier 1 failed, trying Tier 2...")
        calib["fail_count"] = calib.get("fail_count", 0) + 1
        save_calibration(calib)

    # ── Tier 2: Diagnostic — re-find checkbox with existing scale ───────
    if calib and calib.get("scale_x"):
        log(f"Tier 2 (diagnostic): re-finding checkbox with scale ({calib['scale_x']},{calib['scale_y']})")
        # Reload page since Tier 1 failure may have changed Turnstile state
        log("  Reloading page for fresh Turnstile...")
        try:
            driver.refresh()
            time.sleep(5)
        except Exception:
            pass
        ts2 = datetime.now().strftime("%Y%m%d_%H%M%S")
        checkbox = find_checkbox_claude(ts2)
        if checkbox:
            cx, cy = checkbox
            tx, ty = apply_calibration(cx, cy, calib)
            log(f"  Claude=({cx},{cy}) -> screen=({tx},{ty})")
            do_cdp_disconnect_click_reconnect(driver, tx, ty)
            ts_check = datetime.now().strftime("%Y%m%d_%H%M%S")
            if check_click_success(ts_check):
                calib["last_click_x"] = tx
                calib["last_click_y"] = ty
                calib["last_success_time"] = datetime.now().isoformat()
                calib["success_count"] = calib.get("success_count", 0) + 1
                calib["fail_count"] = 0
                save_calibration(calib)
                log("Tier 2 SUCCESS")
                return True
        log("Tier 2 failed, trying Tier 3...")

    # ── Tier 3: Full calibration — two-point scale experiment ───────────
    log("Tier 3 (full calibration): two-point scale experiment")
    ts3 = datetime.now().strftime("%Y%m%d_%H%M%S")

    checkbox = find_checkbox_claude(ts3)
    if not checkbox:
        log("No checkbox found by Claude.")
        return False
    cx, cy = checkbox

    cal_data = two_point_calibrate(ts3, (cx, cy))
    if not cal_data:
        log("Calibration failed, using raw coordinates")
        tx, ty = cx, cy
    else:
        tx, ty = apply_calibration(cx, cy, cal_data)
        log(f"  Calibrated: Claude=({cx},{cy}) -> screen=({tx},{ty})")

        # Take a verification screenshot for logging (but don't adjust — trust calibration)
        shot_v = os.path.join(LOG_DIR, f"ts_verify_{ts3}.png")
        screenshot_with_marker(shot_v, tx, ty)
        log(f"  Verification screenshot: {shot_v}")

    do_cdp_disconnect_click_reconnect(driver, tx, ty)
    ts_check = datetime.now().strftime("%Y%m%d_%H%M%S")
    success = check_click_success(ts_check)

    # Save calibration
    new_calib = {
        "scale_x": cal_data.get("scale_x", 1.0) if cal_data else 1.0,
        "scale_y": cal_data.get("scale_y", 1.0) if cal_data else 1.0,
        "offset_x": cal_data.get("offset_x", 0) if cal_data else 0,
        "offset_y": cal_data.get("offset_y", 0) if cal_data else 0,
        "probe_points": cal_data.get("probe_points", []) if cal_data else [],
        "last_click_x": tx,
        "last_click_y": ty,
        "calibration_time": datetime.now().isoformat(),
        "last_success_time": datetime.now().isoformat() if success else None,
        "success_count": 1 if success else 0,
        "fail_count": 0 if success else 1,
        "screen_resolution": list(pyautogui.size()),
    }
    save_calibration(new_calib)

    if success:
        log("Tier 3 SUCCESS")
    else:
        log("Tier 3 FAILED — all tiers exhausted")
    return success


# ── Main ────────────────────────────────────────────────────────────────────

def check_dpi():
    """Log actual DPI settings for diagnostics."""
    try:
        import ctypes
        # Make process DPI aware to get real values
        try:
            ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except Exception:
            pass
        # Get actual screen size
        user32 = ctypes.windll.user32
        phys_w = user32.GetSystemMetrics(0)
        phys_h = user32.GetSystemMetrics(1)
        # Get DPI
        try:
            dpi = user32.GetDpiForSystem()
        except Exception:
            dpi = 96
        scale = dpi / 96.0
        # Compare with pyautogui
        pa_w, pa_h = pyautogui.size()
        img = pyautogui.screenshot()
        ss_w, ss_h = img.size
        log(f"DPI check: physical={phys_w}x{phys_h}, pyautogui={pa_w}x{pa_h}, "
            f"screenshot={ss_w}x{ss_h}, dpi={dpi}, scale={scale:.2f}")
    except Exception as e:
        log(f"DPI check failed: {e}")


def main():
    check_dpi()
    email, password = get_account()
    log(f"Using account: {email}")

    os.makedirs(PROFILE, exist_ok=True)

    log("Launching browser...")
    driver = Driver(
        uc=True, headed=True,
        user_data_dir=PROFILE,
        chromium_arg="--disable-session-crashed-bubble",
    )
    driver.set_window_size(1920, 1080)
    try:
        driver.maximize_window()
    except Exception:
        pass
    driver.implicitly_wait(5)

    try:
        log("Navigating to sign-in...")
        for nav_attempt in range(3):
            try:
                driver.uc_open_with_reconnect("https://www.newspapers.com/signin/", 8)
                break
            except Exception as e:
                log(f"Navigation attempt {nav_attempt + 1} failed: {e}")
                if nav_attempt < 2:
                    try:
                        driver.reconnect(timeout=8)
                    except Exception:
                        pass
                    time.sleep(3)
        time.sleep(5)

        # Check for "trouble verifying" page — may need a refresh
        diag = os.path.join(LOG_DIR, f"ts_page_loaded_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        pyautogui.screenshot().save(diag)
        log(f"Page screenshot: {diag}")
        try:
            page_text = driver.execute_script("return document.body.innerText || ''")
            if "trouble verifying" in page_text.lower():
                log("'Trouble verifying' detected — refreshing...")
                driver.refresh()
                time.sleep(5)
        except Exception:
            pass

        # Fill email
        log("Filling email...")
        email_field = None
        for sel in ["input[name='email']", "input[id='email']",
                     "input[type='email']", "input[type='text']"]:
            fields = driver.find_elements(By.CSS_SELECTOR, sel)
            for f in fields:
                if f.is_displayed():
                    email_field = f
                    break
            if email_field:
                break
        if not email_field:
            log("ERROR: No email field found")
            return False
        email_field.clear()
        email_field.send_keys(email)

        # Fill password
        log("Filling password...")
        pw_field = None
        for sel in ["input[type='password']", "input[name='password']"]:
            fields = driver.find_elements(By.CSS_SELECTOR, sel)
            for f in fields:
                if f.is_displayed():
                    pw_field = f
                    break
            if pw_field:
                break
        if not pw_field:
            log("ERROR: No password field found")
            return False
        pw_field.clear()
        pw_field.send_keys(password)

        # Bring browser to foreground for pyautogui
        try:
            import ctypes
            title = driver.title
            user32 = ctypes.windll.user32
            EnumWindows = user32.EnumWindows
            WNDENUMPROC = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_int))
            GetWindowTextW = user32.GetWindowTextW
            GetWindowTextLengthW = user32.GetWindowTextLengthW
            target_hwnd = [None]
            def _find_cb(hwnd, lParam):
                length = GetWindowTextLengthW(hwnd)
                if length > 0:
                    buf = ctypes.create_unicode_buffer(length + 1)
                    GetWindowTextW(hwnd, buf, length + 1)
                    if title and title[:30] in buf.value:
                        target_hwnd[0] = hwnd
                        return False
                return True
            EnumWindows(WNDENUMPROC(_find_cb), 0)
            if target_hwnd[0]:
                user32.ShowWindow(target_hwnd[0], 5)  # SW_SHOW (keep maximized)
                user32.SetForegroundWindow(target_hwnd[0])
                log(f"Browser activated (hwnd={target_hwnd[0]})")
                time.sleep(0.5)
        except Exception as e:
            log(f"Window activation: {e}")

        # Wait for Turnstile to render
        time.sleep(3)

        # Click the Turnstile checkbox
        log("=== TURNSTILE CLICK ===")
        clicked = click_turnstile(driver)
        if not clicked:
            log("Turnstile click FAILED - trying to submit anyway")

        # Click sign-in button
        log("Clicking sign-in button...")
        btn_clicked = False
        for sel in ["button[type='submit']", "input[type='submit']"]:
            try:
                btn = driver.find_element(By.CSS_SELECTOR, sel)
                if btn.is_displayed():
                    btn.click()
                    btn_clicked = True
                    break
            except Exception:
                pass
        if not btn_clicked:
            try:
                buttons = driver.find_elements(By.CSS_SELECTOR, "button")
                for b in buttons:
                    if "sign in" in (b.text or "").lower():
                        b.click()
                        btn_clicked = True
                        break
            except Exception:
                pass

        if not btn_clicked:
            log("ERROR: Could not find sign-in button")
            return False

        time.sleep(5)

        # Check result
        current_url = driver.current_url.lower()
        if "signin" in current_url:
            log(f"LOGIN FAILED - still on sign-in page: {current_url}")
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            driver.save_screenshot(os.path.join(LOG_DIR, f"test_login_failed_{ts}.png"))
            return False
        else:
            log(f"LOGIN SUCCESS! URL: {current_url}")
            return True

    finally:
        log("Closing browser...")
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
