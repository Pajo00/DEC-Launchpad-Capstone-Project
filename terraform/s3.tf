locals {
  s3_tags = {
    Service-Name = "S3"
  }
}

# Raw data bucket
resource "aws_s3_bucket" "raw_data" {
  bucket = "dec-capstone-joshua-raw-data"
  tags   = merge(local.generic_tag, local.s3_tags)
}

# Block all public access
resource "aws_s3_bucket_public_access_block" "raw_data_access" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable versioning
resource "aws_s3_bucket_versioning" "raw_data_versioning" {
  bucket = aws_s3_bucket.raw_data.id

  versioning_configuration {
    status = "Enabled"
  }
}