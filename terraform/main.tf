resource "aws_s3_bucket" "credit_card_data_lake" {
  bucket = "credit-card-data-lake-${random_id.bucket_suffix.hex}"

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        kms_master_key_id = aws_kms_key.data_encryption.arn
        sse_algorithm     = "aws:kms"
      }
    }
  }

  versioning {
    enabled = true
  }

  lifecycle_configuration {
    rule {
      enabled = true

      transition {
        days          = 30
        storage_class = "STANDARD_IA"
      }

      transition {
        days          = 90
        storage_class = "GLACIER"
      }
    }
  }
}
