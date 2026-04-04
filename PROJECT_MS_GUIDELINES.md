# Microsoft Azure Certifications Project Guidelines

## Overview

This project generates automated daily rankings of **Microsoft (Azure/M365/Security) Certifications** leaders across different regions worldwide. It fetches certification data from the Credly API strictly targeting authentic Microsoft credentials, bypassing closed directory restrictions by cleverly injecting known users and parsing public member lists, and produces ranking markdown files with **Top 10 positions** for professionals, **Top 5 companies**, and **Top 5 countries** worldwide.

## Architecture

- **`fetch_ms_country.py`** — Fetches certifications for a single standard country from Credly API. Applies strict Microsoft validations using Credly's internal IDs.
- **`fetch_large_ms_country.py`** — Specialized multi-threaded parallel fetcher for large countries (Brazil, India, USA, UK, etc.) scaling up to 100 pages of results per country to handle high volume.
- **`fetch_all_ms_countries.py`** — Orchestrator that spawns separate asynchronous Python processes for all 198 countries, respecting a virtual environment dependency model.
- **`generate_ms_rankings.py`** — Reads generated Microsoft CSV files, applies geographic aggregation, and outputs exactly 6 markdown files (`MS_TOP10_WORLD.md` + 5 Continents).
- **` conocida/known_missing_users.json`** — An exclusive fallback mechanism that allows injecting known Microsoft leaders (like `diego-giglioli`) who would otherwise be invisible to the general directory proxy scanner because they lack a GitHub badge.
- **`datasource_ms/`** — Dedicated database directory storing one CSV per country containing curated Microsoft certification datasets.

## Code Style

- Python 3.11+, using standard libraries like `csv`, `json`, `os`, and utilizing `requests` for the Credly REST API.
- Leverages `concurrent.futures.ThreadPoolExecutor` for parallelism in large countries to speed up fetch operations by 10x.
- Utilizes the `subprocess` module strictly bound to the correct virtual environment `python3` binary format (`sys.executable`).

## Conventions

- Country files follow the format `ms-certs-<country-name>.csv`.
- The core validation mechanism requires `is_ms` to strict-match the official Microsoft Organization ID on Credly: `1392f199-abe0-4698-92b5-834610af6baf` or precisely literal `"Microsoft"` strings inside the metadata label, avoiding loosely related partner groups.
- Expired certifications rely on parsing `expires_at_date`. The function evaluates expiration against `datetime.now()` to discard any legacy or timed-out badges automatically.
- Spoofed/injected users via the JSON logic override are forced into the candidate queue for validation to guarantee accurate representation of top players even if they are cloaked in the standard search filters.
- Company/Partner Names parsing extracts the "Company Data" dynamically.

## Ranking Rules

- **Strict Mode Operations**: The Microsoft ranking runs in "strict mode," meaning we focus exclusively on current, active, non-expired credentials.
- **Position-based Ties**: Ties present the users grouped under the same rank slot to prevent penalizing shared certification thresholds.
- **Companies**: Ranks the top 5 companies globally based on total aggregated certificate volume per candidate declaration.

## Build and Execute

```bash
# Prepare python environment
python3 -m venv venv
source venv/bin/activate
pip install requests

# Fetch single standard country
python3 fetch_ms_country.py "South Africa"

# Fetch large parallel country mapping
python3 fetch_large_ms_country.py "Brazil"

# Run global orchestration (may take a long loop of async fetches)
python3 fetch_all_ms_countries.py

# Generate Top 10 Markdowns exclusively for Microsoft
python3 generate_ms_rankings.py
```

## CI/CD Pipeline

- Handled automatically by `.github/workflows/generate-ms-rankings.yml`.
- Executed on a daily cron schedule to update daily badge counts or rank changes.
- Commits exclusively matching `MS_TOP10` maps and `datasource_ms` structures automatically.
