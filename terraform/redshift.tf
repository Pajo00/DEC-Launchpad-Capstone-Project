locals {
  redshift_tags = {
    Service-Name = "Redshift"
  }
}

# IAM Role for Redshift
resource "aws_iam_role" "redshift_role" {
  name = "dec-capstone-joshua-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(local.redshift_tags, local.generic_tag)
}

# IAM Policy Document
data "aws_iam_policy_document" "redshift_role_policy" {
  statement {
    sid = "S3ReadAndWrite"
    actions = [
      "s3:*List*",
      "s3:*Get*",
      "s3:*Put*"
    ]
    resources = [
      "arn:aws:s3:::dec-capstone-joshua-raw-data",
      "arn:aws:s3:::dec-capstone-joshua-raw-data/*"
    ]
  }
}

# IAM Policy Resource
resource "aws_iam_policy" "redshift_policy" {
  name   = "dec-capstone-joshua-redshift-policy"
  policy = data.aws_iam_policy_document.redshift_role_policy.json
}

# Attach Policy to Role
resource "aws_iam_policy_attachment" "redshift_role_policy_attach" {
  name       = "dec-capstone-joshua-redshift-policy-attach"
  roles      = [aws_iam_role.redshift_role.name]
  policy_arn = aws_iam_policy.redshift_policy.arn
}

# Get Redshift username from SSM
data "aws_ssm_parameter" "redshift_db_username" {
  name = "/dec-capstone-joshua/redshift/db_username"
}

# Get Redshift password from SSM
data "aws_ssm_parameter" "redshift_db_password" {
  name = "/dec-capstone-joshua/redshift/db_password"
}

# Provision Redshift Cluster
resource "aws_redshift_cluster" "main" {
  cluster_identifier  = "dec-capstone-joshua-cluster"
  database_name       = "dec_capstone_joshua"
  master_username     = data.aws_ssm_parameter.redshift_db_username.value
  master_password     = data.aws_ssm_parameter.redshift_db_password.value
  node_type           = "ra3.large"
  cluster_type        = "single-node"
  publicly_accessible = true
  skip_final_snapshot = true
  iam_roles           = [aws_iam_role.redshift_role.arn]

  tags = merge(local.generic_tag, local.redshift_tags)
}