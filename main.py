from data_ingestion.credit_card_data_ingestion import CreditCardDataIngestion
from data_validation.transaction_validator import validate_transaction_data
from analytics.analytical_datasets import create_analytical_datasets
from data_encryption.encryption import DataEncryption
from data_quality.quality_framework import DataQualityFramework
from monitoring.pipeline_metrics import publish_pipeline_metrics

def run_pipeline():
    # 1. Ingest
    df = CreditCardDataIngestion().load_data()

    # 2. Validate
    df_validated = validate_transaction_data(df)

    # 3. Encrypt
    df_encrypted = DataEncryption().encrypt(df_validated)

    # 4. Create analytical datasets
    silver_df = create_analytical_datasets(df_encrypted)

    # 5. Quality & Fraud Detection
    dq = DataQualityFramework()
    dq.real_time_fraud_detection()
    dq.optimize_delta_tables()

    # 6. Publish Metrics
    publish_pipeline_metrics("etl", 30.5, len(silver_df), 2)

if __name__ == "__main__":
    run_pipeline()
