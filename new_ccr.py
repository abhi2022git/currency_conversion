# xrates_current_month.py
# Build a month block (e.g., "2025-October") using ONLY x-rates.com.
# "conversion" = USD per 1 unit of the listed currency.
# Extras:
#  - If PAB (Panama) isn't present in x-rates, default to 1.0
#  - Append/merge into existing CSV/XLSX using unique key "conversion year-month-currency"

import re
from pathlib import Path
from datetime import date, datetime
from calendar import monthrange

import requests
import pandas as pd

# ---------- CONFIG ----------
BASE_CCY = "USD"
CURRENCIES = ["USD","ARS","AUD","BRL","CAD","CLP","CNY","AED","EUR","MXN","NZD","PAB","GBP"]

COUNTRY_MAP = {
    "USD": ("UNITED STATES","USA"),
    "ARS": ("ARGENTINA","ARG"),
    "AUD": ("AUSTRALIA","AUS"),
    "BRL": ("BRASIL","BRA"),
    "CAD": ("CANADA","CAN"),
    "CLP": ("CHILE","CHL"),
    "CNY": ("CHINA","CHN"),
    "AED": ("DUBAI","UAE"),
    "EUR": ("EUROPE","EUR"),
    "MXN": ("MEXICO","MEX"),
    "NZD": ("NEW ZEALAND","NZ"),
    "PAB": ("PANAMA","PAN"),
    "GBP": ("UNITED KINGDOM","UK"),
}

# x-rates currency-name → ISO code (include common variants)
XRATES_NAME_TO_ISO = {
    "US Dollar": "USD", "U.S. Dollar": "USD",
    "Argentine Peso": "ARS",
    "Australian Dollar": "AUD",
    "Brazilian Real": "BRL",
    "Canadian Dollar": "CAD",
    "Chilean Peso": "CLP",
    "Chinese Yuan": "CNY", "Chinese Yuan Renminbi": "CNY",
    "UAE Dirham": "AED", "Emirati Dirham": "AED",
    "Euro": "EUR",
    "Mexican Peso": "MXN",
    "New Zealand Dollar": "NZD",
    "Panamanian Balboa": "PAB",
    "British Pound": "GBP", "British Pound Sterling": "GBP",
}

OUT_CSV  = "currency_conversions_current_month.csv"
OUT_XLSX = "currency_conversions_current_month.xlsx"
TIMEOUT  = 25

# ---------- HELPERS ----------
def ym_label(y: int, m: int) -> str:
    return datetime(y, m, 1).strftime("%Y-%B")

def fetch_xrates_usd_to_quotes() -> dict:
    """
    Scrape https://www.x-rates.com/table/?from=USD&amount=1
    Returns {ISO: units_of_that_currency_per_1_USD}
    """
    url = "https://www.x-rates.com/table/?from=USD&amount=1"
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) xrates-script/1.1",
        "Accept-Language": "en-US,en;q=0.9",
    })
    r = s.get(url, timeout=TIMEOUT)
    r.raise_for_status()

    tables = pd.read_html(r.text)
    if not tables:
        raise ValueError("x-rates: no tables found")

    rates = {}
    for df in tables:
        df.columns = [str(c).strip() for c in df.columns]
        name_col, rate_col = None, None
        for c in df.columns:
            if re.search(r"(currency|name)", c, re.I): name_col = c
            if re.search(r"(US\s*Dollar|USD|1\.00|rate)", c, re.I) and c != name_col: rate_col = c
        if name_col is None or rate_col is None:
            if len(df.columns) >= 2:
                name_col, rate_col = df.columns[0], df.columns[1]
            else:
                continue

        for _, row in df.iterrows():
            name = str(row[name_col]).strip()
            iso = XRATES_NAME_TO_ISO.get(name)
            if not iso:
                continue
            try:
                value = float(str(row[rate_col]).replace(",", ""))
            except Exception:
                continue
            rates[iso] = value  # 1 USD = value units of ISO

    # Ensure USD is present
    rates["USD"] = 1.0
    # Ensure PAB default (1 USD = 1 PAB); if x-rates didn't include it, add it
    rates.setdefault("PAB", 1.0)

    # Sanity check
    if len(rates) <= 1:
        raise ValueError("x-rates: parsed but no usable rates")

    return rates  # USD->C

def build_rows_for_current_month(usd_to_c: dict):
    """
    Convert USD->C to USD per 1 currency (invert), and build rows
    for a single block: current year & month (e.g., 2025-October).
    """
    today = date.today()
    label = ym_label(today.year, today.month)   # e.g., "2025-October"
    year, month_name = label.split("-")

    rows = []
    for ccy in CURRENCIES:
        country, ccode = COUNTRY_MAP.get(ccy, ("", ""))
        if ccy == "USD":
            conv = 1.0
        else:
            q = usd_to_c.get(ccy)  # how many units of CCY per 1 USD
            conv = (1.0 / q) if q else None  # USD per 1 CCY
        rows.append({
            "conversion year-month": label,
            "conversion year-month-currency": f"{label}-{ccy}",
            "conversion year-month-country": f"{label}-{country}" if country else f"{label}-",
            "currency-year-month": f"{ccy}-{label}",
            "conversion year": year,
            "conversion month": month_name,
            "country": country,
            "country code": ccode,
            "currency": ccy,
            "conversion": conv,
        })
    return rows

def merge_and_write(df_new: pd.DataFrame, csv_path: Path, xlsx_path: Path):
    """
    Append only truly new keys into existing CSV/XLSX files.
    Key = 'conversion year-month-currency'.
    Existing rows are kept as-is (no overwrite).
    """
    key = "conversion year-month-currency"

    # Start from df_new
    df_out = df_new.copy()

    # If CSV exists, merge existing
    if csv_path.exists():
        try:
            df_old_csv = pd.read_csv(csv_path)
            if key in df_old_csv.columns:
                df_out = pd.concat([df_old_csv, df_new], ignore_index=True)
                # drop duplicates by key, keep the first (the old row wins)
                df_out = df_out.drop_duplicates(subset=[key], keep="first")
        except Exception:
            pass

    # Write CSV (full merged content)
    df_out.to_csv(csv_path, index=False)

    # If XLSX exists, do the same merge logic based on what's on disk
    if xlsx_path.exists():
        try:
            df_old_xlsx = pd.read_excel(xlsx_path)
            if key in df_old_xlsx.columns:
                df_out_xlsx = pd.concat([df_old_xlsx, df_new], ignore_index=True)
                df_out_xlsx = df_out_xlsx.drop_duplicates(subset=[key], keep="first")
            else:
                df_out_xlsx = df_out
        except Exception:
            df_out_xlsx = df_out
    else:
        df_out_xlsx = df_out

    # Write XLSX
    try:
        df_out_xlsx.to_excel(xlsx_path, index=False)
    except PermissionError:
        # If the Excel file is open, write a timestamped copy
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        alt = xlsx_path.with_name(f"{xlsx_path.stem}_{ts}{xlsx_path.suffix}")
        df_out_xlsx.to_excel(alt, index=False)
        print(f"Excel was open. Wrote to: {alt}")

def main():
    usd_to_c = fetch_xrates_usd_to_quotes()
    rows = build_rows_for_current_month(usd_to_c)

    order_cols = [
        "conversion year-month","conversion year-month-currency","conversion year-month-country",
        "currency-year-month","conversion year","conversion month",
        "country","country code","currency","conversion"
    ]
    df_new = pd.DataFrame(rows)[order_cols]

    csv_path  = Path(OUT_CSV).resolve()
    xlsx_path = Path(OUT_XLSX).resolve()

    merge_and_write(df_new, csv_path, xlsx_path)

    print(f"✅ Built & merged current month block from x-rates: {df_new['conversion year-month'].iloc[0]}")
    print(f" - CSV : {csv_path}")
    print(f" - XLSX: {xlsx_path}")

if __name__ == "__main__":
    main()
