# 🚀 Phase 3 – Production-Ready ETL Pipeline using PySpark

---

## 🔹 Objective

The objective of this phase is to design and implement a **production-grade ETL (Extract → Transform → Load) pipeline** using PySpark.
This phase emphasizes **data quality, transformation logic, and scalable pipeline design**, aligning with real-world data engineering practices.

---

## 🔹 Problem Statement

The project involves processing raw datasets:

* `customers.csv`
* `sales.csv`

These datasets simulate real-world enterprise data challenges, including:

* Missing and null values
* Inconsistent and invalid records
* Incorrect data types (e.g., numeric values stored as strings)

### 🎯 Goals

* Ingest raw data efficiently
* Perform data cleaning and validation
* Apply business transformations
* Generate analytical insights
* Build a reusable ETL pipeline

---

## 🔹 Architecture Overview

The pipeline follows a structured ETL workflow:

```
Raw Data → Data Cleaning → Data Validation → Transformation → Aggregation → Output
```

---

## 🔹 Implementation Approach

### 🔸 1. Data Ingestion (Extract)

* Loaded datasets using PySpark DataFrame API
* Enabled schema inference and header parsing
* Ensured compatibility with structured processing

---

### 🔸 2. Data Cleaning & Validation (Transform)

* Identified missing values using null checks
* Applied `dropna()` for critical fields
* Filtered invalid records (e.g., negative quantity, zero revenue)
* Standardized schema:

  * Converted `customer_id` → Integer
  * Converted `quantity` → Integer
  * Converted `total_amount` → Double

---

### 🔸 3. Data Integration

* Joined `sales` and `customers` datasets using `customer_id`
* Ensured referential integrity before aggregation

---

### 🔸 4. Business Transformations

The following analytical transformations were implemented:

* **Daily Sales Aggregation**
* **City-wise Revenue Analysis**
* **Repeat Customer Identification** (>2 orders)
* **Top Customer per City** using window functions
* **Final Reporting Table** with customer-level KPIs

---

### 🔸 5. Data Output (Load)

* Stored final results in **Parquet format** for optimized performance
* Used `/tmp/` path to ensure compatibility with execution environments

---

## 🔹 Key PySpark Operations

| Category            | Operations                      |
| ------------------- | ------------------------------- |
| Data Ingestion      | `read()`                        |
| Data Cleaning       | `dropna()`, `filter()`          |
| Data Transformation | `withColumn()`, `cast()`        |
| Data Integration    | `join()`                        |
| Aggregation         | `groupBy()`, `sum()`, `count()` |
| Advanced Analytics  | `Window`, `row_number()`        |
| Output              | `write().parquet()`             |

---

## 🔹 Results & Deliverables

The pipeline successfully generated:

* 📊 **Customer-level reporting table** (total spend, order count)
* 🏙️ **City-wise revenue insights**
* 🔁 **Repeat customer segmentation**
* 🏆 **Top-performing customers per city**
* 📅 **Daily revenue trends**

---

## 🔹 Data Engineering Best Practices Applied

* ✔ Schema enforcement before transformations
* ✔ Data validation prior to joins
* ✔ Logical sequencing of ETL steps
* ✔ Reusable and modular pipeline design
* ✔ Use of columnar storage (Parquet) for performance optimization

---

## 🔹 Challenges & Solutions

| Challenge               | Solution                                  |
| ----------------------- | ----------------------------------------- |
| Null and missing values | Applied targeted `dropna()` strategy      |
| Incorrect data types    | Used explicit casting with `withColumn()` |
| Data inconsistency      | Implemented filtering rules               |
| Ranking logic           | Used window functions (`row_number()`)    |

---

## 🔹 Key Learnings

* Real-world datasets require **extensive preprocessing**
* Schema validation is critical for accurate analytics
* PySpark enables scalable transformation pipelines
* Translating SQL logic into PySpark builds strong engineering intuition
* Designing pipelines is more important than writing isolated queries

---

## 🔹 Conclusion

This phase demonstrates the ability to build a **scalable, reliable, and production-oriented ETL pipeline** using PySpark.
The implementation reflects **industry-standard data engineering practices**, including data validation, transformation logic, and optimized storage.

This project showcases readiness to work on **real-world data engineering workflows in enterprise environments**.

---