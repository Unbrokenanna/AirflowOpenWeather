# 🌦️ Airflow OpenWeather Pipeline

## 📌 Overview
Simple data pipeline built with Apache Airflow that:
- extracts weather data from OpenWeather API  
- processes key metrics  
- loads results into SQLite  

Supports multiple cities and historical runs via Airflow **catchup**.

---
## ⚙️ Tech Stack
- Python  
- Apache Airflow  
- SQLite  
- OpenWeather API  

---

## 🔑 Configuration
###   Airflow Variable
    Create variable in Airflow UI:
        Key: WEATHER_API_KEY
        Value: <your_api_key>
        
###   Airflow Connections
      1. SQLite connection
            Conn ID: weather_conn
            Conn Type: sqlite
            Host: /path/to/weather.db
      2. HTTP connection
            Conn ID: weather_conn_http
            Conn Type: http
            Host: https://api.openweathermap.org


## 🏗️ Pipeline
create_table → extract_* → process_* → inject_*

- **extract** → API call (per city)  
- **process** → parse JSON  
- **inject** → store in DB  

Runs in parallel for:
- Lviv  
- Kyiv  
- Odesa  
- Dnipro  

---
