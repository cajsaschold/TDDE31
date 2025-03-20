# Machine Learning for Big Data

This repository contains the implementation of **kernel-based temperature prediction** using **PySpark**. The goal is to predict **hourly temperatures** for a specific date and location in Sweden using **Gaussian kernel models** applied to historical weather data.

## Overview  
- Implements a kernel model in Spark (PySpark) to predict temperatures.  
- Uses data from temperature-readings.csv and stations.csv.  
- Avoids Spark SQL and follows RDD-based operations for distributed computing.

## Methodology 
The temperature forecast is computed using a Gaussian kernel-based approach The kernel function is designed as follows:  
1. **Spatial Kernel** - Weighs stations based on their geographical distance from the target location.  
2. **Temporal Kernel** - Weighs temperature records based on their date proximity to the forecast date.  
3. **Hourly Kernel** - Weighs temperatures based on their hourly difference from the target time.  

### The model is implemented using two variations  
- **Additive Kernel:** The sum of the three kernels.  
- **Multiplicative Kernel:** The product of the three kernels.  

### ** Key Considerations**  
- **Choosing Kernel Widths:** The smoothing coefficients for each kernel (`h_distance`, `h_date`, `h_time`) are optimized to ensure closer points have higher weights.  
- **Performance Optimizations:**  
  - Filtering out temperature readings after the forecast date.  
  - Using `rdd.cache()` to store frequently accessed data.  
  - Broadcasting small datasets instead of joining RDDs.  

