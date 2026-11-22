data "aws_caller_identity" "current" {}

# The role the pipeline (loader, dbt publish step) runs as
resource "aws_iam_role" "pipeline" {
  name        = "${var.name_prefix}-${var.environment}-pipeline"
  description = "Runs the ecom pipeline: writes raw extracts, reads them, writes the lakehouse."

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
    }]
  })
}

# Least privilege: list and read/write objects in the two buckets only.
# No bucket deletion, no policy changes, nothing outside these buckets.
data "aws_iam_policy_document" "pipeline" {
  statement {
    sid       = "ListPipelineBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [for b in aws_s3_bucket.this : b.arn]
  }

  statement {
    sid       = "AppendRawExtracts"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${aws_s3_bucket.this["raw"].arn}/*"]
  }

  statement {
    sid       = "ReadWriteLakehouse"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.this["lakehouse"].arn}/*"]
  }

  # Objects are encrypted with the data key; using it needs these, on it only
  statement {
    sid       = "UseDataKey"
    actions   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey"]
    resources = [aws_kms_key.data.arn]
  }
}

resource "aws_iam_policy" "pipeline" {
  name        = "${var.name_prefix}-${var.environment}-pipeline-s3"
  description = "S3 access for the ecom pipeline, limited to its two buckets."
  policy      = data.aws_iam_policy_document.pipeline.json
}

resource "aws_iam_role_policy_attachment" "pipeline" {
  role       = aws_iam_role.pipeline.name
  policy_arn = aws_iam_policy.pipeline.arn
}
