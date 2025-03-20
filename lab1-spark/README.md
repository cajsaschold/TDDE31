# Spark Exercises

This repository contains a series of **Big Data Analytics (BDA1)** exercises focused on **Apache Spark**. The tasks involve processing large-scale temperature and precipitation datasets to extract insights such as temperature trends, station-based averages, and precipitation levels. The exercises are designed to be executed using Spark (PySpark or Scala).

## **Exercises & Tasks**

### 1. Finding Extreme Temperatures (1950-2014)
- **Goal:** Identify the lowest and highest recorded temperatures for each year.
- **Dataset:** `temperature-readings.csv`
- **Output:** List of years with corresponding extreme temperatures, sorted in descending order by maximum temperature.
- **Key Consideration:** Filtering before applying reduce operations optimizes performance.

### 2. Counting High-Temperature Readings per Month
- **Goal:** Count the number of temperature readings exceeding **10°C** per month.
- **Dataset:** `temperature-readings.csv`
- **Variations:**
  - **All readings** above 10°C.
  - **Distinct readings** per station (each station contributes at most once per month).
- **Output:** Year, month, count of occurrences.

### 3. Average Monthly Temperature per Station (1960-2014)
- **Goal:** Compute the average monthly temperature for each weather station.
- **Dataset:** `temperature-readings.csv`
- **Output:** Year, month, station number, and average monthly temperature.
- **Key Consideration:** Not all stations have continuous readings for every month.

### 4. Identifying Stations with Extreme Conditions
- **Goal:** List stations where:
  - Maximum temperature is between 25°C and 30°C.
  - Maximum daily precipitation is between 100mm and 200mm.
- **Datasets:** `temperature-readings.csv`, `precipitation-readings.csv`
- **Output:** Station number, maximum temperature, and maximum precipitation.

### 5. Average Monthly Precipitation in Östergötland (1993-2016)
- **Goal:** Compute the average monthly precipitation for the Östergötland region.
- **Datasets:** `precipitation-readings.csv`, `stations-Ostergotland.csv`
- **Process:**
  - Compute total monthly precipitation per station.
  - Calculate monthly averages across all stations.
- **Output:** Year, month, and average monthly precipitation.

