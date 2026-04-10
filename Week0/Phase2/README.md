# Phase 2 – Customer Sales Analysis using PySpark

## � What I Learned

* Joining multiple DataFrames using `join()`
* Performing aggregations:

  * `sum()`
  * `count()`
  * `avg()`
* Handling missing data using `left join`
* Casting data types for accurate calculations
* Grouping data with multiple columns

---

## 🛠️ What I Practiced

* Calculating total order amount per customer
* Identifying top customers by total spend
* Finding customers with no orders
* Computing city-wise revenue
* Calculating average order value per customer
* Filtering customers with multiple orders
* Sorting data based on aggregated values

---

## ⚠️ Challenges Faced

* CSV data loaded as string → needed type casting (`cast()`)
* Handling null values after `left join`
* Ensuring correct grouping when using multiple columns
* Avoiding common PySpark errors (e.g., typo in `alias`)

---

## 🎯 Outcome

* Able to perform real-world business analysis using PySpark
* Gained confidence in joins and aggregations
* Improved understanding of DataFrame transformations
* Built a mini analytical pipeline for customer sales insights