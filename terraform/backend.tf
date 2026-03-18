terraform {
  backend "s3" {
    bucket  = "dec-capstone-joshua-tfstate"
    key     = "prod/terraform.tfstate"
    region  = "us-east-1"
    profile = "personal"
    encrypt = true
  }
}