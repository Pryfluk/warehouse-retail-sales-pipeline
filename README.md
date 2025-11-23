# AIE321 Mini Project: Warehouse and Retail Sales Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

### Project Overview
This project is a data pipeline for processing sales data from warehouses and retail stores. The pipeline performs three main tasks:

1. **Ingest**: Load raw CSV sales data into a PostgreSQL database.  
2. **Transform**: Aggregate and summarize sales data per month (already pre-processed).  
3. **Publish**: Export the monthly sales summary to Google Sheets for reporting and sharing.  

### Technologies Used
- Python 3.11  
- pandas, SQLAlchemy, psycopg2, gspread, gspread-dataframe  
- PostgreSQL (Docker)  
- Docker & Docker Compose  

### How it Works
- The pipeline can run locally or inside Docker.  
- Environment detection automatically sets the database host to `localhost` (local) or `postgres_db` (Docker).  
- Place your Google Sheets service account JSON file in the `data/` folder.  

### How to Run

1. Clone the repository:  
```bash
git clone <repo-url>
cd <repo-folder>

Build Docker images and start services:

docker-compose up --build

The pipeline runs automatically:

ingest.py → loads CSV into PostgreSQL

transform.py → prepares the monthly summary (already done)

publish.py → uploads summary to Google Sheets

/app
  |- data/                # CSV and service account files
  |- ingest.py
  |- transform.py
  |- publish.py
  |- run_pipeline.py      # optional combined runner
  |- requirements.txt
  |- Dockerfile
  |- docker-compose.yml
.gitignore
README.md

Notes

Do not commit sensitive files like data/*.json or CSV data.

Make sure Docker is installed and running.

