# 🏠 Mecklenburg County — Motivated Seller Lead Scraper

Automated daily scraper for **tax-delinquent property leads** in Mecklenburg County, NC.
Scores every lead 0–100, publishes a live dashboard to GitHub Pages, and exports a GHL-ready CSV.

---

## 📁 File Structure

```
.
├── scraper/
│   ├── fetch.py            ← Main scraper (run this)
│   └── requirements.txt
├── dashboard/
│   ├── index.html          ← Auto-generated live dashboard (GitHub Pages)
│   └── records.json        ← Latest structured records (served to dashboard)
├── data/
│   ├── records.json        ← Duplicate of dashboard/records.json
│   └── ghl_export.csv      ← GoHighLevel-ready CSV export
└── .github/
    └── workflows/
        └── scrape.yml      ← Daily 7 AM UTC cron + manual trigger
```

---

## 🚀 Quick Start (local)

```bash
# 1. Clone
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO

# 2. Install dependencies
pip install -r scraper/requirements.txt
python -m playwright install --with-deps chromium

# 3. Run
python scraper/fetch.py
```

Outputs:
- `dashboard/records.json`
- `data/records.json`
- `data/ghl_export.csv`
- `dashboard/index.html`

---

## 📊 Lead Sources

| Source | Category | URL |
|--------|----------|-----|
| Tax Delinquent PDF | `TAXDEL` | https://mecknc.widen.net/s/tjgf7bcwrj/ind_taxbills_advertisement |

### Future category stubs (dashboard-ready)

| Code | Label |
|------|-------|
| `LP` | Lis Pendens |
| `NOFC` | Notice of Foreclosure |
| `TAXDEED` | Tax Deed |
| `JUD` / `CCJ` / `DRJUD` | Judgment types |
| `LNCORPTX` / `LNIRS` / `LNFED` | Tax / federal liens |
| `LN` / `LNMECH` / `LNHOA` | Lien types |
| `MEDLN` | Medicaid Lien |
| `PRO` | Probate Documents |
| `NOC` | Notice of Commencement |
| `RELLP` | Release Lis Pendens |

---

## 🎯 Seller Score (0–100)

| Condition | Points |
|-----------|--------|
| Base score | +30 |
| Each motivation flag | +10 |
| Lis Pendens **AND** Pre-foreclosure (combo) | +20 extra |
| Amount > $100,000 | +15 |
| Amount > $50,000 | +10 |
| New this week | +5 |
| Has property address | +5 |

**Flags:** `Tax lien`, `Lis pendens`, `Pre-foreclosure`, `Judgment lien`, `Mechanic lien`, `Probate / estate`, `LLC / corp owner`, `New this week`

---

## 📤 GHL Export Columns

`First Name`, `Last Name`, `Mailing Address`, `Mailing City`, `Mailing State`, `Mailing Zip`,
`Property Address`, `Property City`, `Property State`, `Property Zip`,
`Lead Type`, `Document Type`, `Date Filed`, `Document Number`, `Amount/Debt Owed`,
`Seller Score`, `Motivated Seller Flags`, `Source`, `Public Records URL`

---

## ⚙️ GitHub Actions Setup

1. Go to **Settings → Pages → Source** → set to **GitHub Actions**
2. Enable **Actions** on your repo (already enabled by default)
3. The workflow runs automatically every day at **7:00 AM UTC**
4. Manually trigger from the **Actions** tab → `Scrape Motivated Seller Leads` → `Run workflow`

---

## 🔧 JSON Record Schema

```jsonc
{
  "doc_num":      "",          // Document number (future clerk portal)
  "doc_type":     "Tax Delinquent Advertisement",
  "filed":        "2025-01-15",
  "cat":          "TAXDEL",
  "cat_label":    "Tax Delinquent",
  "owner":        "Smith John A",
  "grantee":      "",
  "amount":       12345.67,
  "amount_raw":   "$12,345.67",
  "legal":        "",
  "prop_address": "123 Main St",
  "prop_city":    "Charlotte",
  "prop_state":   "NC",
  "prop_zip":     "28202",
  "mail_address": "123 Main St",
  "mail_city":    "Charlotte",
  "mail_state":   "NC",
  "mail_zip":     "28202",
  "clerk_url":    "https://mecknc.widen.net/...",
  "flags":        ["Tax lien", "LLC / corp owner"],
  "score":        55,
  "source":       "Mecklenburg County Tax Delinquent PDF"
}
```
