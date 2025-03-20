# BDA2 - Spark SQL Exercises  

This repository contains **Big Data Analytics (BDA2) exercises** focused on **Spark SQL**, designed to process and analyze large-scale **temperature and precipitation datasets**. The exercises aim to replicate **BDA1** but using **Spark SQL's built-in API functions** for querying and aggregation.

## Overview  

- **Spark SQL** is used for querying structured data efficiently.
- **CSV files** are processed using **Spark’s map function** for initial transformations.
- Queries are implemented using **built-in API functions**, avoiding direct SQL queries.
- The dataset consists of **temperature and precipitation readings (1950-2014)**.

## Exercises & Tasks 

### 1. Finding Extreme Temperatures (1950-2014)  
- **Goal:** Identify the lowest and highest recorded temperatures per year.  
- **Dataset:** `temperature-readings.csv`  
- **Output:**  
  - `year, station with max temp, maxValue ORDER BY maxValue DESC`  
  - `year, station with min temp, minValue ORDER BY minValue DESC`  

### 2. Counting High-Temperature Readings per Month  
- **Goal:** Count occurrences of temperature readings above 10°C per month.  
- **Dataset:** `temperature-readings.csv`  
- **Output:**  
  - `year, month, count ORDER BY count DESC`  
  - `year, month, distinct count (per station) ORDER BY count DESC`  

### 3. Average Monthly Temperature per Station (1960-2014) 
- **Goal:** Compute the **average monthly temperature** per weather station.  
- **Dataset:** `temperature-readings.csv`  
- **Output:**  
  - `year, month, station, avgMonthlyTemperature ORDER BY avgMonthlyTemperature DESC`  

### 4. Identifying Stations with Extreme Conditions  
- **Goal:** List stations where:  
  - Maximum temperature is between 25°C and 30°C.  
  - Maximum daily precipitation is between 100mm and 200mm.  
- **Datasets:** `temperature-readings.csv`, `precipitation-readings.csv`  
- **Output:**  
  - `station, maxTemp, maxDailyPrecipitation ORDER BY station DESC`  

### 5. Average Monthly Precipitation in Östergötland (1993-2016)  
- **Goal:** Compute average monthly precipitation for the Östergötland region.  
- **Datasets:** `precipitation-readings.csv`, `stations-Ostergotland.csv`  
- **Output:**  
  - `year, month, avgMonthlyPrecipitation ORDER BY year DESC, month DESC`  

