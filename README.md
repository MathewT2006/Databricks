# Databricks — salescatalog Project

A Databricks workspace project following the **medallion architecture** (Bronze → Silver → Gold) for sales and employee data, using Unity Catalog (`salescatalog`).

---

## Workspace Setup

| Item | Detail |
|------|--------|
| **Workspace URL** | `https://dbc-1954d4ea-ed6a.cloud.databricks.com` |
| **Catalog** | `salescatalog` |
| **Schemas** | `bronze`, `silver`, `gold`, `default` |
| **SQL Warehouse** | Serverless Starter Warehouse (`9d262c42f40e12bc`) |

---

## Medallion Architecture

```
Volume CSV / Manual INSERT
        │
        ▼
  ┌─────────────┐
  │   BRONZE    │  Raw ingestion — no transformations, original types
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │   SILVER    │  Typed, cleaned, enriched (derived columns, salary bands, tenure)
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │    GOLD     │  Aggregated, business-ready tables for analytics/reporting
  └─────────────┘
```

---

## Tables

### Bronze

| Table | Description | Rows |
|-------|-------------|------|
| `bronze.sales_sample_data` | Raw sales orders (order_id, date, customer, product, qty, price) | 5 |
| `bronze.employees_raw` | Raw employee records inserted manually | 15 |
| `bronze.employees_csv_raw` | Raw CSV ingest from Volume — all fields as strings | from CSV |

### Silver

| Table | Description |
|-------|-------------|
| `silver.employees` | Cleaned from `employees_raw`: typed salary, parsed hire_date, full_name, tenure_days, tenure_years, salary_band, `_updated_at` |
| `silver.employees_csv` | Cleaned from `employees_csv_raw`: adds city, state, region, same enrichments as above |

### Gold

| Table | Description |
|-------|-------------|
| `gold.employee_dept_summary` | Per-department: headcount, active/inactive count, avg/min/max salary, avg tenure, team lead count |
| `gold.employee_salary_bands` | Salary band × department: employee count, avg salary, avg tenure (active only) |
| `gold.employees_from_csv` | CSV pipeline: department × region aggregation (headcount, salary stats, tenure, unique cities) |
| `gold.employees_salary_bands_csv` | CSV pipeline: salary band × department breakdown |

---

## Salary Bands

| Band | Salary Range |
|------|-------------|
| Band 5 - Executive | ≥ $130,000 |
| Band 4 - Senior Lead | $110,000 – $129,999 |
| Band 3 - Lead | $90,000 – $109,999 |
| Band 2 - Mid | $70,000 – $89,999 |
| Band 1 - Junior | < $70,000 |

---

## Notebooks

### `SalesExploration` (SQL)
Exploratory SQL notebook for querying the `salescatalog` tables.

### `SalesETL` (Python)
Full medallion ETL pipeline that reads employee data from a Unity Catalog Volume and writes through Bronze → Silver → Gold.

**Source:** `/Volumes/salescatalog/default/myvolume/employees.csv`

**CSV columns:** `employee_id`, `first_name`, `last_name`, `email`, `department`, `job_title`, `salary`, `hire_date`, `city`, `state`, `status`

**Pipeline steps:**

| Step | Action |
|------|--------|
| **Bronze** | Read CSV raw (all strings), add `_source_file` + `_loaded_at`, write `bronze.employees_csv_raw` |
| **Silver** | Cast types, parse dates, compute `full_name`, `tenure_years`, `salary_band`, `region`, write `silver.employees_csv` |
| **Gold 1** | Group by department × region — headcount, salary stats, tenure, unique cities → `gold.employees_from_csv` |
| **Gold 2** | Group by salary band × department (active only) → `gold.employees_salary_bands_csv` |
| **Validation** | Print row counts for all 4 tables |

---

## Volume

| Path | Contents |
|------|----------|
| `/Volumes/salescatalog/default/myvolume/employees.csv` | 15+ employee records with city/state/region data |

---

## Department Summary (from `gold.employee_dept_summary`)

| Department | Headcount | Avg Salary |
|------------|-----------|------------|
| Engineering | 5 | $106,600 |
| Sales | 3 | $90,667 |
| Marketing | 3 | $81,333 |
| HR | 2 | $88,500 |
| Finance | 2 | $107,500 |
