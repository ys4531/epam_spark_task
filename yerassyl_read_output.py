from pyspark.sql import SparkSession

# Step 1: Create Spark Session
spark = SparkSession.builder.appName("Read_Enriched_Data").getOrCreate()

# Path to the output directory containing Parquet files
output_path = "/data/output/enriched_data"

# Read the Parquet files from the output directory
enriched_data_df = spark.read.parquet(output_path)

# Step 2: Show a sample of the enriched data
enriched_data_df.show(20)  # Show the first 5 rows of the dataframe

# Step 3: Inspect the schema to understand the data structure
enriched_data_df.printSchema()

# Stop the Spark session
spark.stop()
