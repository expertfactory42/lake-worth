# Newspapers.com PDF Download Flow

## Toolbar Layout (left to right)
1. Back arrow (with dropdown chevron)
2. Search box ("lake worth") + X button + "1 of N matches"
3. Blue circle badge button (e.g. "9") — SOMETIMES NOT PRESENT
4. Scissors icon + "Clip" button
5. **Download button** (down arrow icon ↓) — THIS IS THE TARGET
6. Share icon (square with up arrow)
7. "Save to Ancestry" green button

## Download Steps (proven working)

### Step 1: Click the download button
- It is the button with a down-arrow icon, located AFTER "Clip" and BEFORE the share icon
- It is an icon-only button (no visible text label)
- Clicking it opens a panel on the right side titled "Print or Download"

### Step 2: Click "Entire Page"
- The panel shows two card options side by side:
  - LEFT card: thumbnail + "Entire Page" text below
  - RIGHT card: thumbnail + "Select portion of page" text below
- Click the LEFT card ("Entire Page")
- The text "Entire Page" is visible below the thumbnail

### Step 3: Click "Save as PDF*"
- After clicking "Entire Page", the panel changes to "Print or Download Entire Page"
- Three buttons appear: "Print", "Save as JPG", "Save as PDF*"
- Click the "Save as PDF*" button (bottom, has a download icon)
- Below it: "* PDF format includes source information"
- Clicking it triggers the PDF file download

## Key Notes
- The download button is AFTER Clip, not before it
- The "Print or Download" panel appears on the RIGHT side of the page
- "Entire Page" is a card with a thumbnail image — click the card or text
- Use JavaScript clicks (execute_script) for reliability
- The blue badge button (e.g. "9") before Clip is NOT the download button
- When searching for elements by text, ALWAYS pick the most specific (shortest text) match
  to avoid clicking a large parent container instead of the actual button
- The "Save as PDF*" button text includes an asterisk
