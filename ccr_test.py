import os
import sys
import subprocess
from io import StringIO
import shutil
import logging
import time
import random
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import date, datetime, timedelta
import requests
import pandas as pd

# Ensure UTF-8 console I/O (Windows-safe)
try:
    sys.stdin.reconfigure(encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# ---------- FOLDER CONFIG ----------
download_dir = Path(r"C:\Working\Abhishek_Testing\ccr_files")
archive_dir  = download_dir / "ARCHIVE"
log_file     = download_dir / "ccr_run.log"
download_dir.mkdir(parents=True, exist_ok=True)
archive_dir.mkdir(parents=True, exist_ok=True)
db_push_path = Path(r"C:\Working\Abhishek_Testing\db_push.py")

# ---------- LOGGING ----------
logger = logging.getLogger("ccr")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=5, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

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

# x-rates name→ISO map (for scraper)
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

OUT_CSV  = "currency_exchange_rate.csv"
OUT_XLSX = "currency_exchange_rate.xlsx"
TIMEOUT  = 25

# --- Historical backfill control ---
BACKFILL_START_DATE = "2023-01-01" 

# ---------- HELPERS ----------
def ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def archive_file(path: Path) -> Path | None:
    """Move existing file to ARCHIVE with timestamp."""
    if not path.exists():
        return None
    stamped = archive_dir / f"{path.stem}_{ts()}{path.suffix}"
    try:
        shutil.move(str(path), str(stamped))
        logger.info(f"📦 Archived: {path.name} -> {stamped.name}")
        return stamped
    except PermissionError:
        shutil.copy2(str(path), str(stamped))
        logger.warning(f"📄 File locked; copied to archive instead: {stamped.name}")
        return stamped
    except Exception as e:
        logger.error(f"Archive failed for {path.name}: {e}")
        return None

# ---------- HTTP SESSION & RETRIES ----------
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Maxima-CCR/1.2 (+analytics)",
    "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
})

def _retry_get(url, params=None, timeout=TIMEOUT, attempts=3, expect_json=True):
    for i in range(attempts):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            if expect_json:
                ct = r.headers.get("Content-Type", "")
                if "json" not in ct.lower():
                    raise ValueError(f"Non-JSON response (Content-Type={ct})")
                return r.json()
            else:
                return r.text
        except Exception as e:
            wait = (2 ** i) + random.random()
            logger.warning(f"GET failed ({i+1}/{attempts}) {url}: {e} ⇒ retry in {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError(f"All retries failed for {url}")

# ---------- RATE PROVIDERS ----------
def _from_xrates(as_of: date) -> dict:
    """https://www.x-rates.com/historical/?from=USD&amount=1&date=YYYY-MM-DD -- If you want to mannually check."""
    
    url = "https://www.x-rates.com/historical/"
    params = {"from": "USD", "amount": "1", "date": as_of.isoformat()}
    html = _retry_get(url, params, expect_json=False)
    tables = pd.read_html(StringIO(html))
    rates = {}
    for df in tables:
        cols = [str(c).strip() for c in df.columns]
        if len(cols) < 2:
            continue
        name_col, rate_col = cols[0], cols[1]
        try:
            for _, row in df.iterrows():
                name = str(row[name_col]).strip()
                iso = XRATES_NAME_TO_ISO.get(name)
                if not iso:
                    continue
                try:
                    val = float(str(row[rate_col]).replace(",", ""))
                except Exception:
                    continue
                rates[iso] = val  # 1 USD = val units of ISO
        except Exception:
            continue
    return rates

def _from_exchangerate_host(as_of: date, needed: set[str]) -> dict:
    url = f"https://api.exchangerate.host/{as_of.isoformat()}"
    params = {"base": "USD", "symbols": ",".join(sorted(needed))}
    data = _retry_get(url, params, expect_json=True)
    return data.get("rates") or {}

def _from_frankfurter(as_of: date, needed: set[str]) -> dict:
    url = f"https://api.frankfurter.app/{as_of.isoformat()}"
    params = {"from": "USD", "to": ",".join(sorted(needed))}
    data = _retry_get(url, params, expect_json=True)
    return data.get("rates") or {}

def fetch_usd_quotes_for_date(as_of):

    # try x-rates first
    xr = _from_xrates(as_of)
    required = set(CURRENCIES) - {"USD"}  # we inject USD separately
    xr_has_all = required.issubset(set(k for k in xr if xr[k] is not None))

    if xr_has_all:
        rates = {k: float(v) for k, v in xr.items() if v is not None}
        logger.info(f"🌐 Using x-rates ONLY for {as_of.isoformat()} (complete coverage).")
    else:
        rates = {k: float(v) for k, v in xr.items() if v is not None}
        missing = required - set(rates.keys())
        if missing:
            logger.info(f"x-rates missing {sorted(missing)} for {as_of.isoformat()} → trying exchangerate.host")
            eh = _from_exchangerate_host(as_of, missing)
            for k, v in (eh or {}).items():
                if v is not None and k not in rates:
                    rates[k] = float(v)
            missing = required - set(rates.keys())
        if missing:
            logger.info(f"exchangerate.host still missing {sorted(missing)} → trying Frankfurter")
            fk = _from_frankfurter(as_of, missing)
            for k, v in (fk or {}).items():
                if v is not None and k not in rates:
                    rates[k] = float(v)

    # Ensure USD & PAB
    rates["USD"] = 1.0
    rates.setdefault("PAB", 1.0)
    return rates

# ---------- BUILDERS ----------
def build_rows_for_date(as_of: date, usd_to: dict) -> list[dict]:
    rows = []
    for ccy in CURRENCIES:
        country, ccode = COUNTRY_MAP.get(ccy, ("", ""))

        # Force Panama (PAB) to 1.0 regardless of source availability
        if ccy == "PAB":
            conv = 1.00
        else:
            conv = round(1 / float(usd_to[ccy]), 6) if ccy in usd_to and usd_to[ccy] is not None else None

        rows.append({
            "conversion_date": as_of.isoformat(),
            "conversion_year": as_of.year,
            "conversion_month": as_of.strftime("%B"),
            "country": country,
            "country_code": ccode,
            "currency": ccy,
            "conversion_rate": conv,
        })
    return rows


def collect_for_dates(dates):

    all_rows = []
    for d in dates:
        usd_to = fetch_usd_quotes_for_date(d)
        rows = build_rows_for_date(d, usd_to)
        all_rows.extend(rows)
        logger.info(f"✅ Built {len(rows)} rows for {d.isoformat()}")

    cols = ["log_date","conversion_year","conversion_month","country","country_code","currency","conversion_rate"]
    df = pd.DataFrame(all_rows)[cols]

    # Fill gaps: sort, then bfill+ffill per currency (weekends / provider gaps)
    df["log_date"] = pd.to_datetime(df["log_date"])
    df = df.sort_values(["currency", "log_date"])
    df["conversion_rate"] = (
        df.groupby("currency", group_keys=False)["conversion_rate"]
          .apply(lambda s: s.bfill().ffill())
    )
    df["log_date"] = df["log_date"].dt.date.astype("string")
    return df

# ---------- WRITERS ----------
def write_append_dedupe(df_new: pd.DataFrame, csv_path: Path, xlsx_path: Path):
    """Append, de-dupe on (log_date, currency), then write CSV/XLSX (with archiving)."""
    if csv_path.exists():
        try:
            df_old = pd.read_csv(csv_path, dtype={"log_date": str})
        except Exception:
            df_old = pd.DataFrame(columns=df_new.columns)
    else:
        df_old = pd.DataFrame(columns=df_new.columns)

    df_all = pd.concat([df_old, df_new], ignore_index=True)

    df_all["log_date"] = pd.to_datetime(df_all["log_date"], errors="coerce")
    df_all["conversion_year"] = pd.to_numeric(df_all["conversion_year"], errors="coerce").astype("Int64")

    df_all = (
        df_all.drop_duplicates(subset=["log_date","currency"], keep="last")
              .sort_values(["log_date","currency"])
    )

    df_all["log_date"] = df_all["log_date"].dt.date.astype("string")
    df_all["conversion_month"] = df_all["conversion_month"].astype(str)

    if csv_path.exists():
        archive_file(csv_path)
    if xlsx_path.exists():
        archive_file(xlsx_path)

    df_all.to_csv(csv_path, index=False)
    logger.info(f"📝 Wrote CSV: {csv_path.name} (rows={len(df_all)})")

    try:
        df_all.to_excel(xlsx_path, index=False)
        logger.info(f"📊 Wrote XLSX: {xlsx_path.name} (rows={len(df_all)})")
    except PermissionError:
        alt = xlsx_path.with_name(f"{xlsx_path.stem}_{ts()}{xlsx_path.suffix}")
        df_all.to_excel(alt, index=False)
        logger.warning(f"XLSX in use; wrote to: {alt.name}")

# ---------- MAIN ----------
def daterange(start: date, end_inclusive: date):
    d = start
    while d <= end_inclusive:
        yield d
        d += timedelta(days=1)

def main():
    start_ts = datetime.now()
    logger.info("=== CCR RUN STARTED ===")

    today = date.today()

    # Build the list of dates to fetch
    if BACKFILL_START_DATE:
        try:
            start_dt = datetime.strptime(BACKFILL_START_DATE, "%Y-%m-%d").date()
            if start_dt > today:
                start_dt = today
            dates = list(daterange(start_dt, today))
            logger.info(f"🔁 Backfill enabled: {start_dt} -> {today} ({len(dates)} days)")
        except Exception as e:
            logger.error(f"Invalid BACKFILL_START_DATE: {e}")
            dates = [today]
    else:
        dates = [today]

    try:
        df_new = collect_for_dates(dates)
    except Exception as e:
        logger.error(f"Data collection failed: {e}")
        logger.info("=== CCR RUN ENDED (FAILED) ===")
        return

    csv_path  = download_dir / OUT_CSV
    xlsx_path = download_dir / OUT_XLSX

    try:
        write_append_dedupe(df_new, csv_path, xlsx_path)
    except Exception as e:
        logger.error(f"File write failed: {e}")

    logger.info(f"✅ Built daily block(s) through {today.isoformat()}")
    logger.info(f"📁 CSV : {csv_path}")
    logger.info(f"📁 XLSX: {xlsx_path}")
    logger.info(f"=== CCR RUN ENDED (OK) in {(datetime.now()-start_ts).seconds}s ===")

    # Optional DB push
    try:
        #subprocess.run([sys.executable, db_push_path], check=True)
        logger.info("✅ DB PUSH completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"DB PUSH failed: {e}")

if __name__ == "__main__":
    main()
