# S&P 500 Intraday Data Pipeline

An automated ETL pipeline that extracts, transforms, and loads real-time intraday data for the S&P 500 using Apache Airflow running in Docker. The pipeline fetches 1-minute interval stock data from Yahoo Finance, processes it for analytics, and loads it into Amazon S3 and Snowflake for storage and visualization. 📈❄️

---

## Table of Contents

- [Project Overview](#project-overview)
- [Technologies Used](#technologies-used)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Docker Setup](#docker-setup)
- [Airflow Setup](#airflow-setup)
- [Usage](#usage)
- [Automation](#automation)
- [Monitoring](#monitoring)
- [Data Outputs](#data-outputs)
- [Contributing](#contributing)
- [License](#license)

---

### Project Overview

This project uses Apache Airflow to orchestrate an ETL pipeline for real-time S&P 500 data.

Workflow Summary:
- Extract: Pulls 1-minute interval data for the top 10 S&P 500 tickers using yfinance.
- Transform: Adds minute returns, trading hours, and other metrics.
- Load: Uploads cleaned data to Amazon S3 and Snowflake for long-term storage and analysis.

All tasks run in Dockerized Airflow containers, ensuring a reproducible and isolated environment. 🧩

---

### Technologies Used

| Tool | Purpose |
|------|----------|
| Apache Airflow | Workflow orchestration and scheduling |
| Docker | Containerization and environment management |
| yfinance | Fetching real-time market data |
| pandas | Data cleaning and transformation |
| Amazon S3 | Cloud data storage |
| Snowflake | Data warehousing and analytics |
| Python 3.x | Primary programming language |

---

### Prerequisites

- Docker & Docker Compose  
- Python 3.x  
- AWS Account (S3 credentials for storage)  
- Snowflake Account (database and warehouse access)  
- Git (for cloning the repository)

---

### Setup

#### Docker Setup

1. Clone the repository:
   ```
   git clone https://github.com/syedshaharham-2006/S-P500_PIPELINE
   cd <repository_directory>
   ```

2. Build and start services:
   ```
   sudo docker-compose up --build -d
   ```

3. Access Airflow UI:  
   [http://localhost:8080](http://localhost:8080/)

   Login Credentials:  
   - Username: airflow  
   - Password: airflow

---

#### Airflow Setup

1. Initialize Airflow database:
   ```
   sudo docker-compose run --rm airflow-init
   ```

2. Configure connections in Airflow UI:
   - AWS S3 Connection: Conn ID `aws_default` — Access Key & Secret Key  
   - Snowflake Connection: Conn ID `snowflake_default` — Account, User, Password, Database, Schema, Warehouse

3. Schedule the ETL DAG:  
   Runs automatically based on your configured schedule (`@daily` or custom).

---

### Usage

- The pipeline executes automatically on your defined schedule.  
- All tasks (Extract → Transform → Load) are traceable in the Airflow UI.

---

### Automation

- Pipeline runs are fully automated via Airflow scheduling.  
- Manual DAG execution available in the Airflow UI.

---

### Monitoring

Use the Airflow UI ([http://localhost:8080](http://localhost:8080/)) to:
- Track task success or failure ✅❌  
- View real-time logs 📝  
- Trigger manual DAG runs 🔄  

---

### Data Outputs

#### Amazon S3

- Data stored at:
  ```
  s3://your-bucket/sp500_intraday/{trading_date}.csv
  ```
- Partitioned by trading date for efficient querying.

#### Snowflake

- Data loaded into:
  ```
  DATABASE.SP500_SCHEMA.INTRADAY_DATA
  ```
- Ready for BI tools (Tableau, Power BI) and SQL analytics.

---

### Contributing

Contributions are welcome.  
Please open an issue or submit a pull request with improvements or bug fixes.

---

### License

See the [LICENSE](./LICENSE) file for details.
