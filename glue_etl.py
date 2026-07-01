import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_date, to_timestamp, year, month, dayofweek, current_timestamp

# 1. READ ARGUMENTS (Added 'new_s3_file' to capture Lambda's payload)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'new_s3_file'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Extract the incoming file path string
uploaded_file_path = args['new_s3_file']
print(f"Lambda triggered this job. Processing single file: {uploaded_file_path}")

# 2. READ ONLY THE NEWLY UPLOADED CSV FILE FROM S3
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [uploaded_file_path]},
    format="csv",  # <-- Changed from "parquet" to "csv"
    format_options={
        "withHeader": True,        # Required: Treats the first row as column names
        "optimizePerformance": True
    }
)

# 3. CONVERT TO SPARK DATAFRAME FOR EASY TRANSFORMATION
df = datasource.toDF()

# 4. APPLY ALL TRANSFORMATIONS
df_transformed = df \
    .withColumn("booking_date", to_date(col("booking_date"), "yyyy-MM-dd")) \
    .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss.SSSSSS")) \
    .withColumn("total_revenue", col("booking_amount") + col("cleaning_fee") + col("service_fee")) \
    .withColumn("average_nightly_rate", col("booking_amount") / col("nights_booked")) \
    .withColumn("booking_year", year(col("booking_date"))) \
    .withColumn("booking_month", month(col("booking_date"))) \
    .withColumn("booking_day_of_week", dayofweek(col("booking_date")))

# Handle Nulls
df_transformed = df_transformed.na.fill({
    "booking_amount": 0, 
    "cleaning_fee": 0, 
    "service_fee": 0,
    "booking_status": "unknown"
})

# Filter out broken rows
df_transformed = df_transformed.filter( 
    (col("nights_booked") > 0) & 
    (col("created_at") <= current_timestamp())
)

# 5. CONVERT BACK TO DYNAMIC FRAME AND WRITE TO S3 (PARQUET FORMAT)
# Note: It still saves as optimized Parquet for your Databricks pipeline!
output_dynamic_frame = DynamicFrame.fromDF(df_transformed, glueContext, "output_dynamic_frame")

glueContext.write_dynamic_frame.from_options(
    frame = output_dynamic_frame,
    connection_type = "s3",
    connection_options = {
        "path": "s3://*****s-s3-data/airbnb_clean/"  
    },
    format = "parquet"
)

job.commit()
