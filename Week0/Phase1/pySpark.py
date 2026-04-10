from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Simple Example").getOrCreate()

data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, "Sita", "Chennai", 32),
    (3, "Arun", "Hyderabad", 28)
]

columns = ["id", "name", "city", "age"]

customers = spark.createDataFrame(data, columns)

print("All Customers")
customers.show()

print("Customers from Chennai")
customers.filter(customers.city == "Chennai").show()

print("Customers Age > 25")
customers.filter(customers.age > 25).show()

print("Name and City")
customers.select("name", "city").show()

print("Customers Count by City")
customers.groupBy("city").count().show()