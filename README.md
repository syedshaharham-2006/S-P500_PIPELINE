## 🌍 S&P 500 Real-Time Data Pipeline
An automated ETL pipeline designed to pull, process, and store real-time intraday data for the S&P 500 index. This pipeline leverages Apache Airflow running in Docker to fetch 1-minute interval stock data from Yahoo Finance, processes it for analysis, and stores it in Amazon S3 and Snowflake for further visualization and exploration. 📊❄️

📖 Table of Contents

• Project Overview
• Tech Stack
• Prerequisites

# Setup

🐳 Docker Setup
⚙️ Airflow Setup

How to Use

🔁 Automation
📊 Monitoring
💾 Data Outputs
💡 Contributing
📜 License

# 🚀 Project Overview

This project orchestrates an ETL pipeline using Apache Airflow for pulling, transforming, and loading S&P 500 intraday data.
Workflow Breakdown:

 • Extract: Retrieves 1-minute interval data for the top 10 S&P 500 stocks via yfinance.
 • Transform: Adds new metrics like minute returns and trading hours for analysis.
 • Load: Pushes the processed data into Amazon S3 and Snowflake for future analysis and reporting.
 •  All tasks are executed within Dockerized Airflow containers, ensuring a reproducible and isolated environment. 🧩

# 🧰 Tech Stack

Tools

🐳 Apache Airflow	Workflow orchestration & scheduling
🐋 Docker / Docker Compose	Containerization & environment management
📊 yfinance	Fetching real-time stock market data
🧹 pandas	Data cleaning & transformation
☁️ Amazon S3	Cloud storage for data storage
❄️ Snowflake	Data warehousing and analytics
🐍 Python 3.x	Primary programming language
🔧 Prerequisites

Before setting up, ensure you have the following:
✅ Docker & Docker Compose - Install Docker
✅ Python 3.x
✅ AWS Account - S3 credentials for storage
✅ Snowflake Account - Database access credentials
✅ Git - To clone the repository

