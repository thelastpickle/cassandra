# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

terraform {
  # OpenTofu, not Terraform.  Two things here need 1.9 or newer and one needs 1.10, so 1.10 is the floor:
  #
  #  - variables.tf validates var.size_pools_to_quotas against var.quota_headroom, and a validation that
  #    refers to another variable is 1.9 onwards.  Under 1.8 it is a load error, not a wrong answer.
  #  - the s3 backend's `use_lockfile` in the example below is 1.10 onwards.  A floor that let 1.9 through
  #    would accept the configuration and then reject the one line an operator uncomments to share state.
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    # Only for the OIDC provider thumbprint the cluster autoscaler's IRSA role needs.  See the comment
    # on aws_iam_openid_connect_provider in iam-autoscaler.tf: moving the autoscaler to Pod Identity
    # would delete this provider requirement along with the OIDC provider itself.
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    # Only to zip ../spend-guard.py for the Lambda in spend-guard.tf.  A Lambda takes its code as a zip or
    # from S3, and nothing in the aws provider builds one; a bucket for a single 30 KB file would be a second
    # resource to create, empty and destroy.
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }

  # No backend is configured, so state is a local ./terraform.tfstate.  That is enough for one person
  # provisioning one cluster they alone destroy.
  #
  # A cluster more than one person applies to needs shared state.  Two local state files are two
  # divergent copies of the truth: the second person to apply sees an empty state, tries to create a
  # cluster that already exists, and the failure leaves neither file describing what is running.
  # Configure an s3 backend here before the second person runs `tofu apply`:
  #
  #   backend "s3" {
  #     bucket       = "<a bucket in the same account>"
  #     key          = "cassandra-jenkins/1-cluster.tfstate"
  #     region       = "<the bucket's region, not necessarily var.region>"
  #     use_lockfile = true    # s3-native locking, no DynamoDB table needed
  #     encrypt      = true
  #   }
}

provider "aws" {
  region = var.region

  # Applied to every resource this configuration creates that supports tags.  Node group tags are set
  # per resource on top of these; the cluster autoscaler's ASG tags are aws_autoscaling_group_tag
  # resources and are deliberately outside default_tags, see node-groups.tf.
  default_tags {
    tags = var.tags
  }
}
