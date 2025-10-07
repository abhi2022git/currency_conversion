# 💱 Currency Conversion Scraper (X-Rates)

This Python script automatically fetches **live currency conversion rates** from [x-rates.com](https://www.x-rates.com/table/?from=USD&amount=1) and stores them in **CSV** and **Excel** files.  
It builds a structured table for the **current month**, converts rates to **USD per 1 unit of each currency**, and intelligently **appends** new data without duplicating older records.

---

## 🚀 Features
- Scrapes real-time exchange rates directly from **x-rates.com**.
- Supports multiple currencies:
- Automatically sets:
- **USD = 1.0**
- **PAB (Panama)** = 1.0 (default if missing)
- Builds monthly records labeled as `"YYYY-Month"` (e.g., `2025-October`).
- Saves results to:
- `currency_conversions_current_month.csv`
- `currency_conversions_current_month.xlsx`
- If those files already exist → appends only **new month–currency** rows (no duplicates).

---

## 🧩 File Structure
| Column Name | Description |
|--------------|--------------|
| conversion year-month | e.g., `2025-October` |
| conversion year-month-currency | unique key like `2025-October-USD` |
| conversion year-month-country | e.g., `2025-October-UNITED STATES` |
| currency-year-month | `USD-2025-October` |
| conversion year | `2025` |
| conversion month | `October` |
| country | country name |
| country code | 3-letter code |
| currency | ISO currency code |
| conversion | USD per 1 unit of that currency |

---

## ⚙️ Requirements
Install the following Python packages:
```bash
pip install requests pandas openpyxl
