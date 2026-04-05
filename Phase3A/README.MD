🚀 Phase 3A – Data Quality & Cleaning using PySpark

--------------------------------------------------

🔹 Objective

The objective of this phase is to work with intentionally messy data and implement robust data cleaning techniques using PySpark.

This phase focuses on understanding the importance of data quality in real-world data engineering pipelines and ensuring that downstream processing is accurate and reliable.

--------------------------------------------------

🔹 Problem Statement

The dataset simulates real-world data inconsistencies:

data = [
(1, "Ravi", "Hyderabad", 25),
(2, None, "Chennai", 32),
(None, "Arun", "Hyderabad", 28),
(4, "Meena", None, 30),
(4, "Meena", None, 30),
(5, "John", "Bangalore", -5)
]

⚠️ Identified Issues:

- Missing values (name, city, customer_id)
- Duplicate records
- Invalid data (negative age)
- Primary key issues (customer_id)

--------------------------------------------------

🔹 Architecture Overview

Raw Data → Data Profiling → Data Cleaning → Validation → Aggregation

--------------------------------------------------

🔹 Implementation Approach

1. Data Ingestion

- Created DataFrame using PySpark
- Defined schema with column names
- Loaded dataset into Spark

2. Data Profiling

- Checked schema using printSchema()
- Identified null values
- Counted total rows before cleaning

3. Data Cleaning

- Removed rows with null customer_id
- Filled missing name and city with "Unknown"
- Removed duplicate records
- Filtered invalid age values (age > 0)

4. Data Validation

- Compared row count before and after cleaning
- Verified cleaned dataset integrity

5. Aggregation

- Grouped data by city
- Counted customers per city

--------------------------------------------------

🔹 Key PySpark Operations

- createDataFrame()
- printSchema()
- count()
- dropna()
- fillna()
- dropDuplicates()
- filter()
- groupBy()
- count()

--------------------------------------------------

🔹 Results & Deliverables

- Cleaned and validated dataset
- Removed duplicates and invalid records
- Generated customer distribution per city
- Prepared data for further processing

--------------------------------------------------

🔹 Data Engineering Best Practices Applied

- Enforced primary key constraints
- Handled missing values using imputation
- Applied validation rules
- Ensured proper ETL flow
- Performed post-cleaning validation

--------------------------------------------------

🔹 Challenges & Solutions

Missing values → Used fillna()
Null primary keys → Used dropna()
Duplicate records → Used dropDuplicates()
Invalid age values → Applied filter (age > 0)
Environment issues → Simplified logic to avoid errors

--------------------------------------------------

🔹 Key Learnings

- Real-world data is messy
- Data cleaning is mandatory before analysis
- Invalid data leads to incorrect results
- Validation ensures data reliability
- Simpler solutions are often more robust

--------------------------------------------------

🔹 Reflection

What happens if cleaning is skipped?

- Incorrect insights
- Faulty aggregations
- Poor business decisions

Which issue impacted results most?

- Duplicate records and invalid age values

How would this affect business decisions?

- Misleading analytics can cause wrong strategic decisions

Data Cleaning Checklist:

- Remove null primary keys
- Handle missing values
- Remove duplicates
- Validate data ranges
- Verify schema
- Validate after cleaning

--------------------------------------------------

🔹 Conclusion

This phase demonstrates the importance of data cleaning in building reliable data pipelines.

It showcases the ability to transform raw, inconsistent data into structured, usable datasets using PySpark, which is essential for real-world data engineering tasks.

--------------------------------------------------