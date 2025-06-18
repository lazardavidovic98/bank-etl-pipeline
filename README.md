# Bank Database Project 

## Overview
Automated ETL pipeline with normalization and analysis for bank transaction data using Python, Pandas and Microsoft SQL Server.

## Technologies
- Python 3.9
- pandas, pyodbc
- Microsoft SQL Server (SSMS)
- SQL queries and normalization (1NF, 2NF, etc.)

## Features
- Authenticated JSON data retrieval (Bearer token)
- CSV/JSON parsing and error logging
- SQL Server integration via ODBC
- Full normalization with helper tables
- Datamart report with city-based aggregates

## Project Structure
- `main.py` – main script
- `config.json` – connection info
- `SQLQuery.sql` – supporting SQL scripts
- `dataset.csv` / `dataset.json`

## Full documentation
See [Documentation.md](docs/Documentation.md) for detailed steps and phase breakdown.

