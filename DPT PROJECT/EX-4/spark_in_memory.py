# spark_in_memory.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min
import time

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("InMemoryProcessing") \
    .master("local[*]") \
    .getOrCreate()

# 2. Load dataset
df = spark.read.csv("C:/Users/Manju/DPT PROJECT/EX-4/sensor_data.csv", header=True, inferSchema=True)

# 3. Cache DataFrame (store in memory)
df.cache()

# 4. Show schema and few records
df.printSchema()
df.show(5)

# 5. Perform basic analytics
start = time.time()
avg_temp = df.agg(avg(col("temperature"))).collect()[0][0]
max_temp = df.agg(max(col("temperature"))).collect()[0][0]
min_temp = df.agg(min(col("temperature"))).collect()[0][0]
end = time.time()

print(f"\nAverage Temperature: {avg_temp:.2f}")
print(f"Max Temperature: {max_temp:.2f}")
print(f"Min Temperature: {min_temp:.2f}")
print(f"Processing Time with Cache: {end - start:.4f} seconds")

spark.stop()
