import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create spark session
spark = (SparkSession
         .builder
         .getOrCreate()
         )

####################################
# Parameters
####################################
postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]

####################################
# Read Postgres
####################################
print("######################################")
print("READING POSTGRES TABLES")
print("######################################")

df_prompts = (
    spark.read
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.prompts")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .load()
)

# Limit to first 10 rows
# df_result = df_prompts.limit(10)
df_result = (
    df_prompts
    .limit(10)
)

print("######################################")
print("EXECUTING QUERY AND SHOWING RESULTS")
print("######################################")

# Show the result in logs (this will be displayed in Airflow task logs)
df_result.show()

# Save result to a CSV file for potential further review
df_result.coalesce(1).write.format("csv").mode("overwrite").save(
    "/usr/local/spark/assets/data/output_postgre_prompts", header=True
    # "/src/spark/assets/data/output_postgre_prompts", header=True
)
