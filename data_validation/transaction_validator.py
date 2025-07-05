from pyspark.sql.functions import  col, current_date

def validate_transaction_data(df):
    """
    Check data quality
    """
    # I've seen it all: negative amounts, future dates, invalid card numbers...
    quality_checks = {
        'null_check': df.filter(col("transaction_id").isNull()).count() == 0,
        'amount_range': df.filter((col("amount") <= 0) | (col("amount") > 50000)).count() == 0,
        'date_validity': df.filter(col("transaction_date") > current_date()).count() == 0,
        'card_format': df.filter(~col("card_number").rlike("^[0-9]{16}$")).count() == 0
    }

    # Always log your quality metrics - future you will thank present you
    for check, passed in quality_checks.items():
        print(f"Quality check {check}: {'PASSED' if passed else 'FAILED'}")

    return all(quality_checks.values()), quality_checks