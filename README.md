# msfirm-bids-scraper

A simple Python scraper that converts the **Millsap & Singer – Bids Online** PDF into structured JSON.

Source PDF: https://www.msfirm.com/bids/bidsonline.pdf

## What this tool does
- Downloads the PDF directly from the URL
- Uses `tabula-py` (the Tabula wrapper) to extract table-like data
- Normalizes/cleans column names and values
- Writes a single `bids.json` file with one object per row (sale entry)

> **Note:** `tabula-py` requires Java to be installed. See below.

---

## Quick start

### 1) Install prerequisites
- Python 3.9+
- Java 8+ (for Tabula)

Check Java:
```bash
java -version
```

If you don’t have Java, install it (e.g., from Adoptium or your package manager).

### 2) Create and activate a virtual environment
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
```

### 3) Install dependencies
```bash
pip install -r requirements.txt
```

### 4) Run the scraper
```bash
python scraper.py
```

This will create a file named `bids.json` in the project root.  
You can change the output path with `OUT_PATH`, and/or the PDF URL with `BIDSONLINE_URL`:

```bash
# Example: write to data/bids.json
mkdir -p data
OUT_PATH=data/bids.json python scraper.py

# Example: custom URL (if the site changes the location)
BIDSONLINE_URL=https://www.msfirm.com/bids/bidsonline.pdf python scraper.py
```

### 5) (Optional) Commit to GitHub
```bash
git init
git add .
git commit -m "Initial commit: PDF → JSON scraper for msfirm bids"
git branch -M main
git remote add origin https://github.com/<your-username>/msfirm-bids-scraper.git
git push -u origin main
```

---

## Output shape (example)

```json
[
  {
    "sale_date": "7/15/2025",
    "sale_time": "2:00 PM",
    "continued_date": null,
    "continued_time": null,
    "case_number": null,
    "county": "St. Louis County",
    "property_address": null,
    "ms_file": "225571.071525.453192",
    "bid": "146881.95",
    "auction_vendor": null
  }
]
```

> Real output will vary depending on how Tabula detects the table structure per page. The script already tries to normalize and clean what it can.

---

## Troubleshooting

- **Java not found**: Install Java, then re-run `python scraper.py`.
- **No rows found / empty JSON**: PDF layouts change sometimes. Try changing Tabula’s strategy:
  - In `scraper.py`, switch `stream=True` to `lattice=True` (needs vector lines in the PDF).
  - If still no luck, consider using `camelot-py` as an alternative (requires Ghostscript).
- **Messy columns**: The script includes heuristics to map weird column headers (e.g., “MS File #”, “Case #”, “Auction Vendor”) into consistent fields.

---

## License
MIT
