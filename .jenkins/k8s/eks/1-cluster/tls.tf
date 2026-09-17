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

data "aws_route53_zone" "jenkins" {
  count = local.serves_public_name ? 1 : 0

  zone_id = var.dns_hosted_zone_id
}

resource "aws_acm_certificate" "jenkins" {
  count = local.serves_public_name ? 1 : 0

  domain_name       = var.jenkins_hostname
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true

    precondition {
      condition     = var.dns_hosted_zone_id != ""
      error_message = "jenkins_hostname needs dns_hosted_zone_id: the validation records below have to be written into the zone that holds the name, and this configuration cannot find that zone on its own."
    }

    precondition {
      condition = endswith(
        var.jenkins_hostname,
        trimsuffix(one(data.aws_route53_zone.jenkins[*].name), ".")
      )
      error_message = "jenkins_hostname must lie inside the zone named by dns_hosted_zone_id."
    }
  }
}

resource "aws_route53_record" "certificate_validation" {
  count = local.serves_public_name ? 1 : 0

  zone_id = var.dns_hosted_zone_id
  name    = local.certificate_validation_option.resource_record_name
  type    = local.certificate_validation_option.resource_record_type
  records = [local.certificate_validation_option.resource_record_value]
  ttl     = 60

  allow_overwrite = true
}
