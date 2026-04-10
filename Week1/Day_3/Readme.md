Phase – SQL Conditional Logic & Window Functions
🔹 Objective

In this phase, the goal is to understand and apply SQL conditional logic (CASE WHEN) and window functions to perform advanced data analysis.

This includes:

Writing conditional transformations
Categorizing data based on business rules
Performing ranking and ordering using window functions
🔹 Problem Summary

We worked on multiple datasets such as:

Employee dataset
CustomerUsage dataset
Orders dataset

The tasks involved:

Applying CASE WHEN for business logic
Solving nested conditional problems
Using window functions like ROW_NUMBER(), RANK(), and DENSE_RANK()
Performing partition-based analysis
🔹 Approach
Analyzed datasets and identified required transformations
Applied CASE WHEN for conditional logic
Used nested CASE WHEN for multi-level conditions
Implemented window functions for ranking and comparisons
Used PARTITION BY and ORDER BY for grouped analysis
Verified outputs using sample data
🔹 Key Transformations Used
🔸 CASE WHEN
Used to apply conditional logic
Helps in categorizing data

Example:

CASE 
    WHEN salary > 80000 THEN 'High'
    ELSE 'Low'
END
🔸 Nested CASE WHEN
Used when multiple conditions depend on each other
Helps handle complex business rules
🔸 Window Functions
ROW_NUMBER() → Assigns unique row numbers
RANK() → Assigns rank with gaps
DENSE_RANK() → Assigns rank without gaps
🔸 Window Clauses
PARTITION BY → Divides data into groups
ORDER BY → Defines sorting within groups
🔹 Output / Results

The following outputs were generated:

Employee ranking based on salary
Department-wise ranking
Customer categorization based on usage
Bonus and salary calculations
Identification of high-value and low-value users
🔹 Data Engineering Considerations
Ensured correct order of conditions in CASE WHEN
Avoided overlapping conditions
Used PARTITION BY correctly to prevent incorrect grouping
Explicitly defined sorting (ASC/DESC)
Verified ranking outputs for correctness
🔹 Challenges Faced
Understanding when to use nested CASE
Differentiating between RANK() and DENSE_RANK()
Writing correct logical conditions
Handling multiple conditions in a single query
🔹 Learnings
Strong understanding of conditional logic in SQL
Ability to implement real-world business rules
Clear knowledge of window functions
Understanding of grouping vs partitioning
Improved problem-solving skills in SQL
