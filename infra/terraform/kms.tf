# Customer-managed key for the pipeline's buckets: rotation on, and access
# controlled by IAM (the pipeline role may use it, nothing else is granted)
resource "aws_kms_key" "data" {
  description             = "Encrypts the ecom raw, lakehouse and log buckets."
  enable_key_rotation     = true
  deletion_window_in_days = 7
}

resource "aws_kms_alias" "data" {
  name          = "alias/${var.name_prefix}-${var.environment}-data"
  target_key_id = aws_kms_key.data.key_id
}
