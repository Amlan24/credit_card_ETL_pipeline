from pyspark.sql.functions import *

def create_analytical_datasets(silver_df):
    """
    Creat ing datasets that actually answer business questions
    """
    # Customer spending patterns - always a hit with the business
    customer_metrics = (silver_df
    .groupBy("customer_id", "transaction_date")
    .agg(
        sum("amount").alias("daily_spend"),
        count("*").alias("transaction_count"),
        countDist inct("merchant_id").alias("unique_merchants"),
        avg("amount").alias("avg_transaction_amount")
        ))

    # Fraud detection features - this is where it gets interest ing
    fraud_features = (silver_df
                      .withColumn("velocity_1h",
                                  count("*").over(Window
                                                  .partitionBy("customer_id")
                                                  .orderBy("transaction_timestamp")
                                                  .rangeBetween(-3600, 0)))  # Transactions in last hour
                      .withColumn("amount_deviation",
                                  abs(col("amount") - avg("amount").over(Window
                                                                         .partitionBy("customer_id")
                                                                         .orderBy("transaction_timestamp")
                                                                         .rowsBetween(-30,
                                                                                      -1)))))  # Deviation from recent average

    return customer_metrics, fraud_features