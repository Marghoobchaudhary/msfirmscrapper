import os
import re
import json
import pandas as pd
import tabula  # requires Java (JDK)

# --------- Config ---------
PDF_URL = os.environ.get("BIDSONLINE_URL", "https://www.msfirm.com/bids/bidsonline.pdf")
OUT_PATH = os.environ.get("OUT_PATH", "bids.json")

# Keep ONLY these counties (canonical, all lowercase)
ALLOWED_CANONICAL = {
    "st. louis county",
    "st. charles county",
    "st. louis city",
    "jefferson county",
    "jackson county",
    "jackson county (kansas city)",
    "jackson county (independent)",
}

# Pretty display for final JSON
PRETTY_COUNTY = {
    "st. louis county": "St. Louis County",
    "st. charles county": "St. Charles County",
    "st. louis city": "St. Louis City",
    "jefferson county": "Jefferson County",
    "jackson county": "Jackson County",
    "jackson county (kansas city)": "Jackson County (Kansas City)",
    "jackson county (independent)": "Jackson County (independent)",
}

# --------- Helpers ---------
def _clean_ws(s: str) -> str:
    """Normalize weird whitespace like non-breaking spaces, CR/LF, double spaces."""
    if s is None:
        return ""
    s = str(s).replace("\u00A0", " ").replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def canonicalize_county(raw: str):
    """
    Normalize county strings so variants like:
    'Jackson County  (Kansas City)', 'Jackson County\n(independent)'
    match our allowed set.
    """
    if not raw:
        return None
    s = _clean_ws(raw).lower()
    # unify 'st louis' -> 'st. louis' and 'st charles' -> 'st. charles'
    s = s.replace("st louis", "st. louis").replace("st charles", "st. charles")
    # normalize parentheses spacing
    s = s.replace(" (", "(").replace("( ", "(").replace(") ", ")")
    s = _clean_ws(s)
    # normalize jackson county flavors
    if s.startswith("jackson county"):
        if "(kansas city)" in s:
            s = "jackson county (kansas city)"
        elif "(independent)" in s:
            s = "jackson county (independent)"
        else:
            s = "jackson county"
    return s

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map many possible header variants to a consistent schema."""
    clean_cols = []
    for c in df.columns:
        cl = str(c).strip().lower().replace("\n", " ").replace("\r", " ")
        cl = re.sub(r"\s+", " ", cl)
        if "sale" in cl and "date" in cl:
            clean_cols.append("sale_datetime")
        elif "continued" in cl:
            clean_cols.append("continued_datetime")
        elif cl.startswith("case") or "case #" in cl or "case#" in cl:
            clean_cols.append("case_number")
        elif "county" in cl:
            clean_cols.append("county")
        elif "property address" in cl:
            clean_cols.append("property_address")
        elif "ms file" in cl:
            clean_cols.append("ms_file")
        elif "bid" in cl:
            clean_cols.append("bid")
        elif "auction" in cl and "vendor" in cl:
            clean_cols.append("auction_vendor")
        elif cl == "auction":
            clean_cols.append("auction")
        elif cl == "vendor":
            clean_cols.append("vendor")
        else:
            clean_cols.append(cl)
    df.columns = clean_cols
    return df

def split_datetime(val):
    """Return (date, time) from 'MM/DD/YYYY HH:MM AM/PM'; tolerate partials."""
    if not isinstance(val, str):
        return "", ""
    val = _clean_ws(val)
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s?(?:AM|PM)?)", val, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2).upper().replace("  ", " ")
    # sometimes only date appears
    m2 = re.search(r"\d{1,2}/\d{1,2}/\d{4}", val)
    if m2:
        return m2.group(0), ""
    return "", ""

def parse_bid(val):
    """Normalize currency strings like '$146,881.95' -> '146881.95'."""
    if val is None:
        return ""
    s = str(val).strip()
    s = s.replace("$", "").replace(",", " ")
    s = re.sub(r"\s+", "", s)
    if not re.search(r"\d", s):
        return ""
    return s

def parse_address(addr: str):
    """
    Best-effort split of a full address into (PropAddress, PropCity, PropZip).
    We expect formats like: '123 Main St, Springfield, MO 63101'
    """
    addr = _clean_ws(addr)
    if not addr:
        return "", "", ""
    # Try to capture zip (last 5 digits)
    zip_match = re.search(r"(\d{5})(?:-\d{4})?$", addr)
    prop_zip = zip_match.group(1) if zip_match else ""
    # Remove state + zip if present
    without_state_zip = re.sub(r",?\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?$", "", addr)
    # Split remaining by comma
    parts = [p.strip() for p in without_state_zip.split(",") if p.strip()]
    if len(parts) >= 2:
        prop_address = ", ".join(parts[:-1])
        prop_city = parts[-1]
    else:
        prop_address = parts[0] if parts else addr
        prop_city = ""
    return prop_address, prop_city, prop_zip

# --------- Main scrape ---------
def scrape():
    # Extract table-like data from all pages
    tables = tabula.read_pdf(PDF_URL, pages="all", multiple_tables=True, stream=True, guess=True)
    tables = [t for t in tables if t is not None and len(t) > 0]

    all_records = []
    for t in tables:
        t = t.dropna(how="all")
        t = standardize_columns(t)

        # Some PDFs repeat headers as the first row; try to detect & skip
        if len(t) and any("sale" in str(x).lower() for x in t.iloc[0].tolist()):
            t = t.iloc[1:].reset_index(drop=True)

        for _, row in t.iterrows():
            sale_date, sale_time = split_datetime(row.get("sale_datetime"))
            cont_date, cont_time = split_datetime(row.get("continued_datetime"))

            county_raw = "" if pd.isna(row.get("county")) else _clean_ws(row.get("county"))
            ccanon = canonicalize_county(county_raw)

            # Keep only requested counties
            if ccanon not in ALLOWED_CANONICAL:
                continue

            # Address split
            addr_raw = "" if pd.isna(row.get("property_address")) else _clean_ws(row.get("property_address"))
            prop_address, prop_city, prop_zip = parse_address(addr_raw)

            # Build output in your requested structure with empty strings for missing data
            rec = {
                "Trustee": "MS Firm",
                "Sale_date": sale_date or "",
                "Sale_time": sale_time or "",
                "FileNo": "" if pd.isna(row.get("ms_file")) else _clean_ws(row.get("ms_file")),
                "PropAddress": prop_address,
                "PropCity": prop_city,
                "PropZip": prop_zip,
                "County": PRETTY_COUNTY.get(ccanon, county_raw),
                "OpeningBid": parse_bid(row.get("bid")),
                "vendor": "" if pd.isna(row.get("auction_vendor")) else _clean_ws(row.get("auction_vendor")),
                "status- DROP DOWN": "",
                "Foreclosure Status": "",
            }

            # Add continued_date ONLY if present (omit when empty)
            if cont_date:
                rec["continued_date"] = cont_date
            # (You asked specifically for continued_date; if you also want continued_time, add similarly.)

            all_records.append(rec)

    # Final light cleanup: (no nulls by construction)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2, ensure_ascii=False)

    print(f"Wrote {len(all_records)} filtered records to {OUT_PATH}")

if __name__ == "__main__":
    scrape()
