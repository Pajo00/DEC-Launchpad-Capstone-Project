locals {
  redshift_tags = {
    Service-Name = "Redshift"
  }
}

# iam role for redshift to access s3
resource "aws_iam_role" "redshift_role" {
  name = "dec-capstone-joshua-redshift-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Sid       = ""
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(local.redshift_tags, local.generic_tag)
}

# iam policy document
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

# iam policy resource
resource "aws_iam_policy" "redshift_policy" {
  name   = "dec-capstone-joshua-redshift-policy"
  policy = data.aws_iam_policy_document.redshift_role_policy.json
}

# attach policy to role
resource "aws_iam_policy_attachment" "redshift_role_policy_attach" {
  name       = "dec-capstone-joshua-redshift-policy-attach"
  roles      = [aws_iam_role.redshift_role.name]
  policy_arn = aws_iam_policy.redshift_policy.arn
}

# get redshift username from ssm
data "aws_ssm_parameter" "redshift_db_username" {
  name = "/dec-capstone-joshua/redshift/db_username"
}

# get redshift password from ssm
data "aws_ssm_parameter" "redshift_db_password" {
  name = "/dec-capstone-joshua/redshift/db_password"
}

# get the default vpc
data "aws_vpc" "default" {
  default = true
}

# create a security group for redshift
resource "aws_security_group" "redshift_sg" {
  name        = "dec-capstone-joshua-redshift-sg"
  description = "security group for redshift cluster"
  vpc_id      = data.aws_vpc.default.id

  # allow inbound traffic on redshift port from anywhere
  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "redshift public access"
  }

  # allow all outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.generic_tag
}

# provision redshift cluster
resource "aws_redshift_cluster" "main" {
  cluster_identifier     = "dec-capstone-joshua-cluster"
  database_name          = "dec_capstone_joshua"
  master_username        = data.aws_ssm_parameter.redshift_db_username.value
  master_password        = data.aws_ssm_parameter.redshift_db_password.value
  node_type              = "ra3.large"
  cluster_type           = "single-node"
  publicly_accessible    = true
  skip_final_snapshot    = true
  iam_roles              = [aws_iam_role.redshift_role.arn]
  vpc_security_group_ids = [aws_security_group.redshift_sg.id]

  tags = merge(local.generic_tag, local.redshift_tags)
}