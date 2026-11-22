output "raw_bucket" {
  description = "Bucket for the nightly extracts."
  value       = aws_s3_bucket.this["raw"].id
}

output "lakehouse_bucket" {
  description = "Bucket for the Iceberg tables."
  value       = aws_s3_bucket.this["lakehouse"].id
}

output "pipeline_role_arn" {
  description = "Role the pipeline assumes."
  value       = aws_iam_role.pipeline.arn
}

output "pipeline_policy_arn" {
  description = "The least-privilege S3 policy attached to the role."
  value       = aws_iam_policy.pipeline.arn
}

output "logs_bucket" {
  description = "Bucket receiving the S3 access logs of the data buckets."
  value       = aws_s3_bucket.logs.id
}

output "data_key_arn" {
  description = "Customer-managed KMS key encrypting every bucket."
  value       = aws_kms_key.data.arn
}
