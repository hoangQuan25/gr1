from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, regexp_extract, explode
import matplotlib.pyplot as plt

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Job market analysis") \
    .getOrCreate()

try:
    # Read the JSON file into a DataFrame
    df = spark.read.json("C:/programming/PyCharm/ScrapyDemo/quotetutorial/quotetutorial/data/items.json")

    # Clean the DataFrame: remove rows with missing values in essential columns
    df_clean = df.dropna(subset=["job_name", "company", "address", "salary"])

    # Visualize jobs distribution by location
    location_counts = df_clean.groupBy("address").count().orderBy("count", ascending=False)
    location_counts.show()
    location_counts.toPandas().plot(kind="bar", x="address", y="count", legend=None)
    plt.title("Job Distribution by Location")
    plt.xlabel("Location")
    plt.ylabel("Number of Jobs")
    plt.show()

    # Analyze and plot distribution of job listings by company
    company_counts = df_clean.groupBy("company").count().orderBy("count", ascending=False)
    top_n = 10  # Number of top companies to display
    top_companies = company_counts.limit(top_n).toPandas()
    plt.figure(figsize=(10, 6))
    plt.bar(top_companies["company"], top_companies["count"])
    plt.title(f"Top {top_n} Companies with Most Job Listings")
    plt.xlabel("Company")
    plt.ylabel("Number of Job Listings")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

    # Analyze and plot most common benefits
    benefits_counts = df_clean.select(explode("benefits").alias("benefit")).groupBy("benefit").count().orderBy("count", ascending=False)
    benefits_counts.show()
    benefits_counts.limit(10).toPandas().plot(kind="bar", x="benefit", y="count", legend=None)
    plt.title("Most Common Benefits")
    plt.xlabel("Benefit")
    plt.ylabel("Count")
    plt.show()

    # Extract numerical values from salary ranges using regular expressions
    df_clean = df_clean.withColumn("salary_min_extracted", regexp_extract(col("salary"), r"Lương: (\d+) Tr", 1))
    df_clean = df_clean.withColumn("salary_max_extracted", regexp_extract(col("salary"), r"- (\d+) Tr", 1))
    df_clean = df_clean.withColumn("salary_min", expr("CASE WHEN salary_min_extracted <> '' THEN CAST(salary_min_extracted AS DOUBLE) ELSE NULL END"))
    df_clean = df_clean.withColumn("salary_max", expr("CASE WHEN salary_max_extracted <> '' THEN CAST(salary_max_extracted AS DOUBLE) ELSE NULL END"))
    print("Rows with successfully extracted salaries:")
    df_clean.select("salary", "salary_min", "salary_max").show(truncate=False)

    # Calculate average salary by address
    avg_salary_by_address = df_clean.groupBy("address") \
                                    .agg({"salary_min": "avg", "salary_max": "avg"}) \
                                    .withColumnRenamed("avg(salary_min)", "avg_salary_min") \
                                    .withColumnRenamed("avg(salary_max)", "avg_salary_max")
    avg_salary_by_address.show()

    # Visualize average salary by address
    plt.figure(figsize=(10, 6))
    avg_salary_by_address.toPandas().plot(kind="bar", x="address", y=["avg_salary_min", "avg_salary_max"])
    plt.title("Average Salary by Address")
    plt.xlabel("Address")
    plt.ylabel("Average Salary (VND)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Stop the Spark session
    spark.stop()
