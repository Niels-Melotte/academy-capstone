from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json

config = { "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.1.2",
"spark.hadoop.fs.s3a.aws.credentials.provider":"com.amazonaws.auth.EnvironmentVariableCredentialsProvider"}
conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

s3_path = 's3a://dataminded-academy-capstone-resources/raw/open_aq/data_part_1.json'

df = spark.read.json(s3_path)

df2 = df.select(df["date.local"])
df2.show()

