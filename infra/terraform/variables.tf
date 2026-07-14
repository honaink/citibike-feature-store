variable "name" {
  description = "Prefix for every resource."
  type        = string
  default     = "citibike-fs"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "allowed_cidrs" {
  description = "CIDR blocks allowed to reach the dashboard and API, e.g. [\"203.0.113.7/32\"]."
  type        = list(string)
}

variable "image_tag" {
  description = "Tag of the app and EMR images in ECR."
  type        = string
  default     = "latest"
}

variable "scope" {
  description = "Slice of the Citi Bike system: \"jc\" (Jersey City + Hoboken) or \"nyc\"."
  type        = string
  default     = "jc"

  validation {
    condition     = contains(["jc", "nyc"], var.scope)
    error_message = "scope must be \"jc\" or \"nyc\"."
  }
}

variable "emr_release" {
  description = "EMR Serverless release. Its Spark version must match pyspark in requirements.txt."
  type        = string
  default     = "emr-7.14.0"
}

variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}

variable "services_desired_count" {
  description = "Set to 0 to stop the always-on ECS services without destroying anything."
  type        = number
  default     = 1
}
