from pyspark.sql.functions import col, countDistinct, count, max as spark_max, current_timestamp
from datetime import datetime, timedelta

class DataQualityFramework:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.quality_metrics = {}

    def run_quality_checks(self, df, table_name):
        """
        Runs all data quality checks and logs results.
        """
        checks = {
            'completeness': self.check_completeness(df),
            'uniqueness': self.check_uniqueness(df, ['transaction_id']),
            'validity': self.check_validity(df),
            'consistency': self.check_consistency(df),
            'timeliness': self.check_timeliness(df)
        }

        # Log quality metrics
        self.publish_metrics(table_name, checks)

        # Fail fast on bad quality
        if not self.meets_quality_threshold(checks):
            raise Exception(f"Data quality check failed for {table_name}")

        return checks

    def check_completeness(self, df):
        """
        Checks for nulls in critical fields.
        """
        critical_fields = ['transaction_id', 'customer_id', 'amount', 'transaction_date']
        completeness_scores = {}

        total_count = df.count()

        for field in critical_fields:
            null_count = df.filter(col(field).isNull()).count()
            completeness_scores[field] = round(1 - (null_count / total_count), 3)

        return completeness_scores

    def check_uniqueness(self, df, key_columns):
        """
        Ensures primary keys are unique.
        """
        total_count = df.count()
        unique_count = df.select(*key_columns).distinct().count()
        uniqueness_score = round(unique_count / total_count, 3)

        return {'uniqueness_score': uniqueness_score}

    def check_validity(self, df):
        """
        Checks domain constraints.
        """
        invalid_amount_count = df.filter(col("amount") <= 0).count()
        total_count = df.count()
        validity_score = round(1 - (invalid_amount_count / total_count), 3)

        return {'valid_transaction_amounts': validity_score}

    def check_consistency(self, df):
        """
        Checks consistency between fields if needed.
        Stub implementation.
        """
        # For example: credit card type must match number pattern
        # Placeholder: assume always consistent
        return {'consistency_score': 1.0}

    def check_timeliness(self, df):
        """
        Checks if data is recent.
        """
        max_timestamp = df.select(spark_max("transaction_date")).collect()[0][0]

        if max_timestamp is None:
            return {'timeliness_score': 0.0}

        now = datetime.now()
        is_timely = max_timestamp >= now - timedelta(days=1)

        return {'timeliness_score': 1.0 if is_timely else 0.0}


    def meets_quality_threshold(self, checks, threshold=0.95):
        """
        Returns False if any score falls below threshold.
        """
        for metric in checks.values():
            for score in metric.values():
                if score < threshold:
                    return False
        return True

    def real_time_fraud_detection(self, transaction_schema, fraud_detection_udf, feature_columns):
        """
        Real-time fraud detection using Kafka + ML + Delta.
        """
        transaction_stream = (self.spark
                              .readStream
                              .format("kafka")
                              .option("kafka.bootstrap.servers", "kafka-cluster:9092")
                              .option("subscribe", "credit-card-transactions")
                              .option("startingOffsets", "latest")
                              .load())

        parsed_stream = (transaction_stream
                         .select(from_json(col("value").cast("string"), transaction_schema).alias("data"))
                         .select("data.*"))

        fraud_predictions = (parsed_stream
                             .withColumn("fraud_score", fraud_detection_udf(struct([col(c) for c in feature_columns])))
                             .withColumn("is_fraud", when(col("fraud_score") > 0.8, True).otherwise(False)))

        query = (fraud_predictions
                 .writeStream
                 .format("delta")
                 .outputMode("append")
                 .option("checkpointLocation", "/delta/checkpoints/fraud-detection")
                 .trigger(processingTime='10 seconds')
                 .start("/delta/fraud-alerts"))

        return query

    def optimize_delta_tables(self):
        """
        Delta table optimization & maintenance.
        """
        self.spark.sql("OPTIMIZE credit_card_transactions ZORDER BY (customer_id, transaction_date)")
        self.spark.sql("VACUUM credit_card_transactions RETAIN 168 HOURS")
        self.spark.sql("ANALYZE TABLE credit_card_transactions COMPUTE STATISTICS FOR ALL COLUMNS")