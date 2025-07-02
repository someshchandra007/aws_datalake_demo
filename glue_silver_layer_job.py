import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, date_format

#pass paras
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH', 'DATABASE_NAME', 'TABLE_NAME']
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = args['INPUT_PATH']
output_path = args['OUTPUT_PATH']
database_name = args['DATABASE_NAME']
table_name = args['TABLE_NAME']

df_raw = spark.read.json(input_path)

# Flattening the jos here before storing in silve layer
df_flat = df_raw.select(
    to_timestamp(col("event_time")).alias("event_time_ts"),  
    col("product_id"),
    col("item_id"),
    col("price")
).withColumn("event_ts", date_format(col("event_time_ts"), "yyyyMMddHHmm"))

# Deduplicate rows(to demo cleansing)
df_flat = df_flat.dropDuplicates(["product_id", "item_id", "event_time_ts"])

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.catalog.setCurrentDatabase(database_name)


df_flat.write \
    .mode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .partitionBy("event_ts") \
    .saveAsTable(f"{database_name}.{table_name}")

job.commit()
