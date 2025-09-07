import os
import re
import json
import pandas as pd
import tabula  # requires Java (JDK)

# --------- Config ---------
PDF_URL = os.environ.get("BIDSONLINE_URL", "https://www.msfirm.com/bids/bidsonline.pdf")
OUT_PATH = os.environ.get("OUT_PATH", "bids.json")

# Counties you want to keep (canonical, all lowercase)
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
    s = s.replace("st louis", "st. louis")
    s = s.replace("st charles", "st. charles")

    # normalize parentheses spacing
    s = s.replace(" (", "(").replace("( ", "(").replace(") ", ")")
    s = _clean_ws(s)

    # normalize the jackson county flavors
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
    """Return (date, time) from a 'MM/DD/YYYY HH:MM AM/PM' string; tolerate partials."""
    if not isinstance(val, str):
        return None, None
    val = _clean_ws(val)
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s?(?:AM|PM)?)", val, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2).upper().replace("  ", " ")
    # sometimes only date appears
    m2 = re.search(r"\d{1,2}/\d{1,2}/\d{4}", val)
    if m2:
        return m2.group(0), None
    return None, None


def parse_bid(val):
    """Normalize currency strings like '$146,881.95' -> '146881.95'."""
    if val is None:
        return None
    s = str(val).strip()
    s = s.replace("$", "").replace(",", " ")
    s = re.sub(r"\s+", "", s)
    if not re.search(r"\d", s):
        return None
    return s


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

            rec = {
                "sale_date": sale_date,
                "sale_time": sale_time,
                "continued_date": cont_date,
                "continued_time": cont_time,
                "case_number": None if pd.isna(row.get("case_number")) else _clean_ws(row.get("case_number")),
                "county": None if pd.isna(row.get("county")) else _clean_ws(row.get("county")),
                "property_address": None if pd.isna(row.get("property_address")) else _clean_ws(row.get("property_address")),
                "ms_file": None if pd.isna(row.get("ms_file")) else _clean_ws(row.get("ms_file")),
                "bid": parse_bid(row.get("bid")),
                "auction_vendor": None if pd.isna(row.get("auction_vendor")) else _clean_ws(row.get("auction_vendor")),
            }

            # Merge separate auction/vendor columns if present
            if "auction" in t.columns or "vendor" in t.columns:
                a = "" if "auction" not in t.columns or pd.isna(row.get("auction")) else _clean_ws(row.get("auction"))
                v = "" if "vendor" not in t.columns or pd.isna(row.get("vendor")) else _clean_ws(row.get("vendor"))
                if (a or v) and not rec.get("auction_vendor"):
                    rec["auction_vendor"] = " ".join([a, v]).strip()

            # Keep rows that have at least some signal (avoid footers)
            if any(rec.get(k) for k in ("county", "ms_file", "case_number", "property_address")):
                # ---- County filter here ----
                ccanon = canonicalize_county(rec.get("county"))
                if ccanon in ALLOWED_CANONICAL:
                    rec["county"] = PRETTY_COUNTY.get(ccanon, rec.get("county"))
                    all_records.append(rec)

    # Final light cleanup: remove obvious headers/footers that slipped through
    cleaned = []
    for r in all_records:
        c = (r.get("county") or "").lower()
        if "millsap & singer" in c or "sale date" in c:
            continue
        cleaned.append(r)

    # Write filtered JSON
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=2, ensure_ascii=False)

    print(f"Wrote {len(cleaned)} filtered records to {OUT_PATH}")


if __name__ == "__main__":
    scrape()
