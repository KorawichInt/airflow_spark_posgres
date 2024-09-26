import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType

# Create spark session
spark = (SparkSession
         .builder
         .getOrCreate()
         )

####################################
# Parameters
####################################
prompts_file = sys.argv[1]
postgres_db = sys.argv[2]
postgres_user = sys.argv[3]
postgres_pwd = sys.argv[4]

####################################
# Read CSV Data
####################################
print("######################################")
print("READING CSV FILES")
print("######################################")

df_prompts_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .load(prompts_file)
)

####################################
# Load data to Postgres
####################################
print("######################################")
print("LOADING POSTGRES TABLES")
print("######################################")

(
    df_prompts_csv.write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.prompts")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("overwrite")
    .save()
)
