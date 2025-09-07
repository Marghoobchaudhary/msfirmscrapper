import os
import re
import json
import pandas as pd
import tabula

PDF_URL = os.environ.get("BIDSONLINE_URL", "https://www.msfirm.com/bids/bidsonline.pdf")
OUT_PATH = os.environ.get("OUT_PATH", "bids.json")

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map many possible header variants to a consistent schema."""
    clean = []
    for c in df.columns:
        cl = str(c).strip().lower().replace("\n", " ").replace("\r", " ")
        cl = re.sub(r"\s+", " ", cl)
        if "sale" in cl and "date" in cl:
            clean.append("sale_datetime")
        elif "continued" in cl:
            clean.append("continued_datetime")
        elif cl.startswith("case") or "case #" in cl or "case#" in cl:
            clean.append("case_number")
        elif "county" in cl:
            clean.append("county")
        elif "property address" in cl:
            clean.append("property_address")
        elif "ms file" in cl:
            clean.append("ms_file")
        elif cl == "bid" or "bid" in cl:
            clean.append("bid")
        elif "auction" in cl and "vendor" in cl:
            clean.append("auction_vendor")
        elif cl == "auction":
            clean.append("auction")
        elif cl == "vendor":
            clean.append("vendor")
        else:
            clean.append(cl)
    df.columns = clean
    return df

def split_datetime(val):
    if not isinstance(val, str):
        return None, None
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s?(AM|PM)?)", val, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2).upper().replace("  ", " ")
    return val, None  # sometimes Tabula gives only date or only time here

def parse_bid(val):
    if val is None:
        return None
    s = str(val)
    # Remove currency and commas, compress spaces
    s = s.replace("$", "").replace(",", " ")
    s = re.sub(r"\s+", "", s)
    # Keep digits and dot
    if not re.search(r"\d", s):
        return None
    return s

def scrape():
    # Extract table-like data from all pages
    tables = tabula.read_pdf(PDF_URL, pages="all", multiple_tables=True, stream=True, guess=True)
    tables = [t for t in tables if t is not None and len(t) > 0]

    all_records = []
    for t in tables:
        # Drop fully-empty rows
        t = t.dropna(how="all")
        t = standardize_columns(t)

        # Some PDFs repeat headers as the first row; try to detect & skip
        if len(t) and any("sale" in str(x).lower() for x in t.iloc[0].tolist()):
            # Heuristic: if the first row looks like headers, remove it
            t = t.iloc[1:].reset_index(drop=True)

        for _, row in t.iterrows():
            # Build a robust record with graceful fallbacks
            sale_date, sale_time = split_datetime(row.get("sale_datetime"))
            cont_date, cont_time = split_datetime(row.get("continued_datetime"))

            rec = {
                "sale_date": sale_date,
                "sale_time": sale_time,
                "continued_date": cont_date,
                "continued_time": cont_time,
                "case_number": None if pd.isna(row.get("case_number")) else str(row.get("case_number")).strip(),
                "county": None if pd.isna(row.get("county")) else str(row.get("county")).strip(),
                "property_address": None if pd.isna(row.get("property_address")) else str(row.get("property_address")).strip(),
                "ms_file": None if pd.isna(row.get("ms_file")) else str(row.get("ms_file")).strip(),
                "bid": parse_bid(row.get("bid")),
                "auction_vendor": None if pd.isna(row.get("auction_vendor")) else str(row.get("auction_vendor")).strip(),
            }

            # Merge separate auction/vendor columns if present
            if "auction" in t.columns or "vendor" in t.columns:
                a = "" if "auction" not in t.columns or pd.isna(row.get("auction")) else str(row.get("auction")).strip()
                v = "" if "vendor" not in t.columns or pd.isna(row.get("vendor")) else str(row.get("vendor")).strip()
                if (a or v) and not rec.get("auction_vendor"):
                    rec["auction_vendor"] = " ".join([a, v]).strip()

            # Keep rows that have at least a county or ms_file or case_number (avoid footers)
            if any(rec.get(k) for k in ("county", "ms_file", "case_number", "property_address")):
                all_records.append(rec)

    # Final light cleanup: remove obvious headers/footers that slipped through
    cleaned = []
    for r in all_records:
        if r.get("county") and ("Millsap & Singer" in r["county"] or "Sale Date" in r["county"]):
            continue
        cleaned.append(r)

    os.makedirs(os.path.dirname(OUT_PATH) or ".", exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=2, ensure_ascii=False)

    print(f"Wrote {len(cleaned)} records to {OUT_PATH}")

if __name__ == "__main__":
    scrape()
