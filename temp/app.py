import argparse
import boto3

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.dataframe import DataFrame

from nielscapstone.common.spark import ClosableSparkSession, transform, SparkLogger
from nielscapstone.transformations.shared import add_ds


DataFrame.transform = transform


def main():
    parser = argparse.ArgumentParser(description="niels-capstone")
    parser.add_argument(
        "-d", "--date", dest="date", help="date in format YYYY-mm-dd", required=True
    )
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=True
    )
    args = parser.parse_args()

    with ClosableSparkSession("niels-capstone") as session:
        run(session, args.env, args.date)


def run(spark: SparkSession, environment: str, date: str):
    """Main ETL script definition.

    :return: None
    """
    # execute ETL pipeline
    logger = SparkLogger(spark)
    logger.info(f"Executing job for {environment} on {date}")
    data = extract_data(spark, date)
    transformed = transform_data(data, date)
    load_data(spark, transformed)


def extract_data(spark: SparkSession, date: str) -> DataFrame:
    """Load data from a source

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    s3_path = 's3a://dataminded-academy-capstone-resources2/raw/open_aq/'

    return spark.read.json(s3_path)


def transform_data(data: DataFrame, date: str) -> DataFrame:
    """Transform original dataset.

    :param data: Input DataFrame.
    :param date: The context date
    :return: Transformed DataFrame.
    """

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

def load_data(spark: SparkSession, data: DataFrame):
    """Writes the output dataset to some destination

    :param data: DataFrame to write.
    :return: None
    """
    
    sf_secrets = json.loads(get_secret('snowflake/capstone/login'))

    sfOptions = {
    "sfURL":sf_secrets["URL"],
    "sfUser" :sf_secrets["USER_NAME"],
    "sfPassword" : sf_secrets["PASSWORD"],
    "sfDatabase" : sf_secrets["DATABASE"],
    "sfSchema" : "NIELS",
    "sfWarehouse" : sf_secrets["WAREHOUSE"]
    }

    data.write.format("net.snowflake.spark.snowflake").options(**sfOptions).option("dbtable", "capstone_niels").mode("overwrite").save()
    return

if __name__ == "__main__":
    main()
