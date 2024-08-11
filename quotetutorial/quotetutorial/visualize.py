from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, regexp_extract, explode
import matplotlib.pyplot as plt
from pymongo import MongoClient
import pandas as pd
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk
from tkinter import ttk

# Helper function to convert ObjectId to string
def convert_object_id(data):
    for record in data:
        if '_id' in record:
            record['_id'] = str(record['_id'])
    return data

# Initialize a SparkSession with increased executor memory
spark = SparkSession.builder \
    .appName("Job market analysis from MongoDB") \
    .config("spark.executor.memory", "4g") \
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/job_database.jobs") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/job_database.jobs") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .getOrCreate()

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client['job_database']
collection = db['jobs']

# Fetch data from MongoDB and convert ObjectId to string
data = list(collection.find())
data = convert_object_id(data)

# Convert MongoDB collection to DataFrame
df = spark.createDataFrame(pd.DataFrame(data))

# Clean the DataFrame: remove rows with missing values in essential columns
df_clean = df.dropna(subset=["job_name", "company", "address", "salary"])

# Create main Tkinter window
root = tk.Tk()
root.title("Job Market Analysis")
root.geometry("1600x1200")

# Create a canvas and scrollbar
canvas = tk.Canvas(root)
scrollbar = ttk.Scrollbar(root, orient="vertical", command=canvas.yview)
scrollable_frame = ttk.Frame(canvas)

scrollable_frame.bind(
    "<Configure>",
    lambda e: canvas.configure(
        scrollregion=canvas.bbox("all")
    )
)

canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
canvas.configure(yscrollcommand=scrollbar.set)

# Create and add plots to the scrollable frame
figures = []

# Visualize jobs distribution by location
location_counts = df_clean.groupBy("address").count().orderBy("count", ascending=False)
fig, ax = plt.subplots()
location_counts.toPandas().plot(kind="bar", x="address", y="count", legend=None, ax=ax)
ax.set_title("Job Distribution by Location")
ax.set_xlabel("Location")
ax.set_ylabel("Number of Jobs")
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
figures.append(fig)

# Analyze and plot most common benefits
benefits_counts = df_clean.select(explode("benefits").alias("benefit")).groupBy("benefit").count().orderBy("count", ascending=False)
fig, ax = plt.subplots()
benefits_counts.limit(10).toPandas().plot(kind="bar", x="benefit", y="count", legend=None, ax=ax)
ax.set_title("Most Common Benefits")
ax.set_xlabel("Benefit")
ax.set_ylabel("Count")
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
figures.append(fig)

# Analyze and plot distribution of job listings by company
company_counts = df_clean.groupBy("company").count().orderBy("count", ascending=False)
top_n = 10  # Number of top companies to display
top_companies = company_counts.limit(top_n).toPandas()
fig, ax = plt.subplots()
ax.bar(top_companies["company"], top_companies["count"])
ax.set_title(f"Top {top_n} Companies with Most Job Listings")
ax.set_xlabel("Company")
ax.set_ylabel("Number of Job Listings")
ax.set_xticklabels(top_companies["company"], rotation=45, ha='right')
figures.append(fig)

# Extract numerical values from salary ranges using regular expressions
df_clean = df_clean.withColumn("salary_min_extracted", regexp_extract(col("salary"), r"Lương: (\d+) Tr", 1))
df_clean = df_clean.withColumn("salary_max_extracted", regexp_extract(col("salary"), r"- (\d+) Tr", 1))
df_clean = df_clean.withColumn("salary_min", expr("CASE WHEN salary_min_extracted <> '' THEN CAST(salary_min_extracted AS DOUBLE) ELSE NULL END"))
df_clean = df_clean.withColumn("salary_max", expr("CASE WHEN salary_max_extracted <> '' THEN CAST(salary_max_extracted AS DOUBLE) ELSE NULL END"))

# Calculate average salary by address
avg_salary_by_address = df_clean.groupBy("address") \
    .agg({"salary_min": "avg", "salary_max": "avg"}) \
    .withColumnRenamed("avg(salary_min)", "avg_salary_min") \
    .withColumnRenamed("avg(salary_max)", "avg_salary_max")
fig, ax = plt.subplots()
avg_salary_by_address.toPandas().plot(kind="bar", x="address", y=["avg_salary_min", "avg_salary_max"], ax=ax)
ax.set_title("Average Salary by Address")
ax.set_xlabel("Address")
ax.set_ylabel("Average Salary (million VND)")
ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')
figures.append(fig)

# Add figures to the scrollable frame in the specified layout
rows, cols = 2, 2  # 2 rows and 2 columns
for i, fig in enumerate(figures):
    canvas_fig = FigureCanvasTkAgg(fig, master=scrollable_frame)
    canvas_fig.get_tk_widget().grid(row=i//cols, column=i%cols, padx=10, pady=10)

# Pack canvas and scrollbar
canvas.pack(side="left", fill="both", expand=True)
scrollbar.pack(side="right", fill="y")

# Run the Tkinter main loop
root.mainloop()

# Stop the Spark session
spark.stop()
