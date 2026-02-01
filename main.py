from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    spark = SparkSession.builder \
        .appName("TVSeriesAnalysis") \
        .getOrCreate()
    
    # Read JSON with multiLine option
    df = spark.read.option("multiLine", "true").json("/opt/spark-data/tvs.json")

    # 1. Creators' names for status 'Canceled'
    # created_by — array of structs, so we use explode
    canceled_creators = df.filter(F.col("status") == "Canceled") \
        .withColumn("creator", F.explode(F.col("created_by"))) \
        .select(F.col("creator.name").alias("creator_name")) \
        .distinct()

    # Countrys with popularity > 5.0
    # origin_country - array of strings
    popular_countries = df.filter(F.col("popularity") > 5.0) \
        .withColumn("country", F.explode(F.col("origin_country"))) \
        .select("country") \
        .distinct()

    # 3. Series with number_of_episodes < 100
    short_series = df.filter(F.col("number_of_episodes") < 100) \
        .select("name", "number_of_episodes")

    # Results display
    print("--- TASK 1: Canceled Series Creators ---")
    canceled_creators.show(truncate=False)

    print("--- TASK 2: Popular Countries (> 5.0) ---")
    popular_countries.show(truncate=False)

    print("--- TASK 3: Short Series (< 100 eps) ---")
    short_series.show(10)

    # Save results to Parquet
    short_series.write.mode("overwrite").parquet("/opt/spark-data/output/short_series_results")

    spark.stop()

if __name__ == "__main__":
    main()