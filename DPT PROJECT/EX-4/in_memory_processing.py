import pandas as pd
import numpy as np
import time

print("🚀 In-Memory Data Processing using Pandas\n")

# Load CSV into in-memory DataFrame
df = pd.read_csv("C:/Users/Manju/DPT PROJECT/EX-4/sensor_data.csv")
print("✅ Dataset loaded successfully!\n")

# Show dataset info
print(df.head(), "\n")
print("Schema:")
print(df.info(), "\n")

# Example 1: Basic Statistics
print("📊 Basic Statistics:")
print(df.describe(), "\n")

# Example 2: Calculate average temperature and humidity
avg_temp = df["temperature"].mean()
avg_hum = df["humidity"].mean()
print(f"🌡️ Average Temperature: {avg_temp:.2f} °C")
print(f"💧 Average Humidity: {avg_hum:.2f} %\n")

# Example 3: Feature Engineering — Temperature Category
df["temp_category"] = np.where(df["temperature"] > 35, "High", "Normal")
print("✅ Added 'temp_category' column.\n")
print(df.head(), "\n")

# Example 4: Demonstrate in-memory speed
start = time.time()
for _ in range(1000):
    _ = df["temperature"].mean() + df["humidity"].mean()
end = time.time()

print(f"⚡ In-Memory computation time for 1000 iterations: {end - start:.4f} seconds")

print("\n✅ In-memory data processing completed successfully!")
