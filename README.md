# 🌍 S&P 500 Real-Time Data Pipeline

An automated ETL pipeline designed to pull, process, and store real-time intraday data for the S&P 500 index. This pipeline leverages **Apache Airflow** running in **Docker** to fetch 1-minute interval stock data from **Yahoo Finance**, processes it for analysis, and stores it in **Amazon S3** and **Snowflake** for further visualization and exploration. 📊❄️

---

## 📖 Table of Contents

- [Project Overview](#project-overview)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
  - [Docker Setup](#docker-setup)
  - [Airflow Setup](#airflow-setup)
- [How to Use](#how-to-use)
  - [Automation](#automation)
  - [Monitoring](#monitoring)
  - [Data Outputs](#data-outputs)
- [Contributing](#contributing)
- [License](#license)

---

## 🚀 Project Overview

This project orchestrates an **ETL (Extract, Transform, Load)** pipeline using **Apache Airflow** for pulling, transforming, and loading **S&P 500 intraday stock data**.

### Workflow Breakdown:

- **Extract**: Retrieves 1-minute interval data for the S&P 500 stocks from [Yahoo Finance](https://www.yfinance.com/) using the `yfinance` Python library.
- **Transform**: Cleanses and processes the data, adding essential metrics like **minute returns** and **trading hours** to enhance the data for further analysis.
- **Load**: Pushes the transformed data into **Amazon S3** (for cloud storage) and **Snowflake** (for data warehousing and analytics) for efficient querying and reporting.
- **Execution**: All tasks are executed within **Dockerized Airflow containers**, ensuring a **reproducible**, **scalable**, and **isolated environment** for processing.

### Key Features:

- **Real-Time Data Collection**: Fetches minute-by-minute data for accurate stock analysis.
- **Automated Pipeline**: Fully automated ETL process using Apache Airflow.
- **Cloud Integration**: Utilizes Amazon S3 for data storage and Snowflake for warehousing and advanced analytics.

---

## 🧰 Tech Stack

### Tools & Technologies Used:

- **🐳 Apache Airflow**: Workflow orchestration and scheduling for managing the ETL pipeline.
- **🐋 Docker / Docker Compose**: Containerization and environment management to ensure a clean and consistent development and production environment.
- **📊 yfinance**: Library for fetching real-time stock market data from Yahoo Finance.
- **🧹 pandas**: Data manipulation and transformation for data cleaning and feature engineering.
- **☁️ Amazon S3**: Cloud storage for storing the processed data.
- **❄️ Snowflake**: Data warehousing platform for querying and visualizing large datasets.
- **🐍 Python 3.x**: Primary programming language for writing the ETL scripts and handling the data processing logic.

---

## 🔧 Prerequisites

Before getting started with this project, ensure you have the following installed:

- **Docker**: [Docker Installation Guide](https://docs.docker.com/get-docker/)
- **Docker Compose**: [Docker Compose Installation Guide](https://docs.docker.com/compose/install/)
- **Python 3.x**: [Download Python](https://www.python.org/downloads/)
- **AWS Account**: To use **Amazon S3** and **Snowflake** for data storage and warehousing.

Additionally, you'll need the following Python packages:

```bash
pip install apache-airflow
pip install yfinance
pip install pandas
pip install boto3
pip install snowflake-connector-python
