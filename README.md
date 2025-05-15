# 📦 E-commerce Sales Dashboard

This project processes e-commerce sales data and visualizes key insights using Power BI. It uses a classic ETL pipeline: data is downloaded from Kaggle, transformed, enriched with postal codes via API, loaded into a PostgreSQL database, and visualized in an interactive dashboard.

---

## 📁 Project Structure
'''
Sales/
- ├── .git
- ├── .idea
- ├── .venv
- ├── data/
- │ └── train.csv # Raw Kaggle dataset
- ├── src/
- │ ├── extract.py # Downloads CSV, enriches with postal codes
- │ ├── transform.py # Cleans & preprocesses data
- │ ├── load_and_visual.py # Creates tables and loads data into PostgreSQL
- │ └── main.py # Pipeline entry point
- ├── sales.pbix # Power BI Dashboard
- └── README.md

---

## 🛠️ Technologies

- Python (pandas, requests, psycopg2, kagglehub)
- PostgreSQL
- Power BI
- REST API for postal code enrichment "https://api.zippopotam"

---

## 📊 Dashboard Overview

The dashboard contains the following visualizations:

- **Total Sales** — overall revenue  
- **Unique Customers** — total number of buyers  
- **Average Order Value**  
- **Orders Over Time** — by month, quarter, year  
- **Top Products & Categories**  
- **Sales by Customer**  
- **Breakdowns by Sub-category and Region**

---

## ▶️ How to Run the Project
This project implements a basic ETL pipeline using Python 3.13. It automatically downloads data from Kaggle, transforms it using pandas, and loads it into a PostgreSQL database. No manual data download or Kaggle API token is required.

---

### 🔧 1. Requirements
Install the required Python packages using pip: pip install -r requirements.txt

If you don’t have a requirements.txt, you can install manually: pip install pandas requests psycopg2-binary kagglehub python-dotenv

---

### 🧪 2. PostgreSQL Setup
Make sure you have a PostgreSQL database already created. This script does not create the database, only tables inside it.

Create a .env file in the project root directory with the following content:

 - DB_NAME=your_database
 - DB_USER=your_user
 - DB_PASSWORD=your_password
 - DB_HOST=your_host
 - DB_PORT=5432

---

### 🚀 4. To run the Project just run the main script: python main.py
This will:

 - Automatically download the dataset from Kaggle and save it locally
 - Clean and transform the raw data
 - Connect to your PostgreSQL database
 - Create tables (if not present) and insert the data
 - All stages (Extract, Transform, Load) are logged in project.log.

---

### 📦 5. Output

Raw data is saved to YourPath directory (you can change the path in main.py)
Transformed data is inserted into the specified PostgreSQL database

---

### 📝 Notes

 - No Kaggle API token is needed — data is pulled anonymously via kagglehub

 - Logs are saved to project.log

 - You can change paths, DB settings, or transformation logic freely



