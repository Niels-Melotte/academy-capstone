from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json,to_utc_timestamp
import boto3
from botocore.exceptions import ClientError
import json

pkgs = [
    "net.snowflake:snowflake-jdbc:3.13.3",
    "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1",
    "org.apache.hadoop:hadoop-aws:3.1.2"
]

config = {  "spark.hadoop.fs.s3a.aws.credentials.provider":"com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            "spark.jars.packages": ",".join(pkgs)}
conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

s3_path = 's3a://dataminded-academy-capstone-resources/raw/open_aq/'

def ingest(s3_path):
    return spark.read.json(s3_path)


def transform(df):
    df = df.withColumn("co_longitude",df.coordinates.longitude)
    df = df.withColumn("co_latitude",df.coordinates.latitude)
    df = df.withColumn("date_local",to_utc_timestamp(df.date.local,'Europe/Brussels'))
    df = df.withColumn("date_utc",to_utc_timestamp(df.date.utc,'UTC'))

    df = df.drop("date","coordinates")

    return df

def get_secret(secret_name):
    region_name = "eu-west-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            print("The requested secret can't be decrypted using the provided KMS key:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            print("An error occurred on service side:", e)
    else:
        # Secrets Manager decrypts the secret value using the associated KMS CMK
        # Depending on whether the secret was a string or binary, only one of these fields will be populated
        if 'SecretString' in get_secret_value_response:
            text_secret_data = get_secret_value_response['SecretString']
            return text_secret_data
        else:
            binary_secret_data = get_secret_value_response['SecretBinary']
        return
def load(df):
    sf_secrets = json.loads(get_secret('snowflake/capstone/login'))

    sfOptions = {
    "sfURL":sf_secrets["URL"],
    "sfUser" :sf_secrets["USER_NAME"],
    "sfPassword" : sf_secrets["PASSWORD"],
    "sfDatabase" : sf_secrets["DATABASE"],
    "sfSchema" : "NIELS",
    "sfWarehouse" : sf_secrets["WAREHOUSE"]
    }

    df.write.format("net.snowflake.spark.snowflake").options(**sfOptions).option("dbtable", "capstone_niels").mode("overwrite").save()
    return

if __name__ == "__main__":

    df = transform(ingest(s3_path))
    load(df)
