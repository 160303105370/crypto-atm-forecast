# 💰 Cryptocurrency ATM Transaction Forecasting (2025)

### End-to-End Automated Data Pipeline using AWS S3 + Redshift + Power BI

This project demonstrates a complete cloud-based forecasting system that predicts **daily ATM cash demand** for cryptocurrency kiosks.  
It integrates **data engineering, machine learning, and business intelligence** into one cohesive workflow.

---

## 📊 Project Overview

### 🧩 Goal
To forecast daily cash withdrawals across crypto ATMs and help operations teams optimize cash replenishment and avoid idle reserves.

### 🧠 Business Value
- **Reduced stockouts & idle cash:** Predictive replenishment allows smarter cash logistics.
- **Faster decision-making:** Automated daily pipeline reduces manual reporting from 3 hrs → 20 min.
- **Real-time insights:** Executives can track transaction trends and crypto price correlations via Power BI.

---

## 🏗️ Architecture

[Python ETL] → [AWS S3] → [AWS Redshift Serverless] → [Power BI Dashboard]

### 🔹 Flow Explanation
1. **Data Extraction:**  
   Kaggle crypto & ATM transaction datasets combined using Python (Pandas, NumPy).  
2. **Transformation:**  
   Merged on `date`, engineered features (rolling averages, lags, weekday flags).  
3. **Load (ETL):**  
   Processed files uploaded to **S3**, then ingested to **Redshift** via automated script (`daily_load.py`).  
4. **Visualization:**  
   Power BI dashboard connects live to Redshift for real-time analysis.

---


## ⚙️ Tech Stack

| Layer | Tools / Services |
|-------|------------------|
| Data Source | Kaggle – BTC & ATM datasets |
| ETL & Modeling | Python, Pandas, NumPy, Scikit-learn |
| Storage | AWS S3 (raw + processed data) |
| Warehouse | AWS Redshift Serverless |
| BI / Visualization | Power BI |
| Automation | Windows Task Scheduler (demo) |

---


## 🧮 Modeling Summary

**| Model | RMSE | MAPE | Accuracy |**
|--------|------|------|----------|--------|
| **Random Forest Regressor** | 3,414 | 6.49 % | Best performer
| **XGBoost** | 3,733 | 7.01 % | Slightly higher RMSE

**Key Predictors:**  
- Yesterday’s withdrawals (`wd_lag_1`)  
- Transaction counts (`income_txn_count`, `outcome_txn_count`)  
- BTC daily % change (minor impact)

---


## 📈 Dashboard Highlights (Power BI)

| KPI | Description |
|------|-------------|
| 💵 **Total Withdrawals (USD)** | Cumulative amount dispensed |
| 📊 **% Growth (Day-over-Day)** | Daily cash demand change |
| 📉 **7-Day Rolling Average** | Smooth trend visualization |
| 🔁 **ATM Transaction Mix** | Income vs Outcome transaction composition |
| 📆 **Weekend Filter** | Toggle to study behavioral shifts |


---

## 🔄 Pipeline Automation (ETL)

The ETL script [`etl/daily_load.py`](etl/daily_load.py):
1. Extracts one day of data from the processed CSV.  
2. Uploads it to S3 (`processed/loads/atm_forecast_<date>.csv`).  
3. Runs a `COPY` command to load it into Redshift staging and append to `atm_daily`.  

This simulates a **daily feed** scenario in production.

---


## 🧾 Data Sources

**ATM Transactions Dataset (Kaggle)**  
🔗 [https://www.kaggle.com/datasets/](https://www.kaggle.com/datasets)  
Search for: `ATM Transaction Dataset` or `Bank ATM Cash Forecasting Dataset`  

**Bitcoin Price History (Kaggle)**  
🔗 [https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data](https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data)



### How to Reproduce
1. Download the above datasets.  
2. Place them under:

data/raw/atm_raw.csv
data/raw/btc_raw.csv

3. Run `notebooks/modeling_notebook.ipynb` to generate `atm_crypto_merged_daily.csv` inside `data/processed/`.  
4. Execute the ETL:
```bash
python etl/daily_load.py

