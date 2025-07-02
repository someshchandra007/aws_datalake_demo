import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import sum
import boto3

# paras pasing
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

datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path], "recurse": True},
    format="parquet",
    transformation_ctx="datasource"
)

df = datasource.toDF()
df.createOrReplaceTempView("product_sales")

aggregated_df = spark.sql(f"""
    SELECT sum(price) AS total_price, product_id
    FROM product_sales
    GROUP BY product_id
""")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.catalog.setCurrentDatabase(database_name)

aggregated_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .option("path", output_path) \
    .partitionBy("product_id") \
    .saveAsTable(f"{database_name}.{table_name}")

job.commit()
