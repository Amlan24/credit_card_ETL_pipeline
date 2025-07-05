
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import boto3


class CreditCardDataIngestion:
    def __init__(self, spark_session):
        self.spark = spark_session

        self.s3_client = boto3.client('s3')



    def ingest_transaction_data(self, source_path, target_path):
        """
        The bread and butter - gett ing data from A to B safely
        """


        df = (self.spark.read
          .option("encryption", "SSE-KMS")
          .option("kmsKeyId", "arn:aws:kms:region:account:key/key-id")
          .format("parquet")  # change as per requirement
          .load(source_path))

    # Delta for schema evolution
        (df.write
        .format("delta")
         .mode("append")
         .partitionBy("transaction_date", "merchant_category")  #change as per business requirement!
         .option("mergeSchema", "true")  # Handle schema drift
         .save(target_path))