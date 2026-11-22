variable "region" {
  description = "AWS region."
  type        = string
  default     = "ca-central-1"
}

variable "environment" {
  description = "Deployment environment, used in names and tags."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod."
  }
}

variable "name_prefix" {
  description = "Prefix for every resource name."
  type        = string
  default     = "ecom"
}

variable "use_emulator" {
  description = "Point the AWS provider at the local emulator (Moto) instead of AWS."
  type        = bool
  default     = true
}

variable "emulator_endpoint" {
  description = "Emulator URL (infra/docker-compose.yml)."
  type        = string
  default     = "http://localhost:15000"
}

variable "raw_noncurrent_days" {
  description = "Days to keep old versions of raw extracts before deleting them."
  type        = number
  default     = 90
}
