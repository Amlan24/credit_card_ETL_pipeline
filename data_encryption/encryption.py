from cryptography.fernet import Fernet
import base64
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType

class DataEncryption:
    def __init__(self, kms_key_id):
        self.kms_client = boto3.client('kms')

        self.key_id = kms_key_id

    key = Fernet.generate_key()
    cipher_suite = Fernet(key)

    def simple_encrypt(value):
        if value is None:
            return None
        key = Fernet.generate_key()
        cipher_suite = Fernet(key)
        return cipher_suite.encrypt(value.encode()).decode()
    # Cache your encryption keys

    def encrypt_pii_fields(self, df, sensitive_columns):
        """
        Encrypt the really sensitive stuff
        """
        encrypt_udf = udf(lambda x: self.simple_encrypt(x), StringType())

        for column in sensitive_columns:
        # Create a UDF for encryption
        df = df.withColumn(f"{column}_encrypted",
                       encrypt_udf(col(column)))

    # Drop the original columns
        return df.drop(*sensitive_columns)