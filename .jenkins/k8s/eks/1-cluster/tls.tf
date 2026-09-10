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

# The name Jenkins serves on, and the certificate for it.  Everything here is created only when
# var.jenkins_hostname is set; a cluster without it is reachable at the load balancer's own name.
#
# What this layer does and does not do, because the split is not obvious:
#
#   here          the hosted zone is read, a certificate is requested, and the DNS records that prove
#                 ownership of the name to Amazon are written
#   external-dns  writes the record that points the name at the load balancer, from an annotation on
#                 the Jenkins Service; see addons.tf
#   ../Makefile   puts the name and the certificate ARN in the environment for layer 2
#   ../2-platform annotates the Service, so the load balancer terminates TLS
#
# The address record is not written here for a reason of ordering, not of taste.  The load balancer is
# created by Kubernetes when the Jenkins Service is created, which is layer 2, so its name is not
# knowable in layer 1.  Writing the record here would mean layer 1 running after layer 2.

# The zone, read rather than created.  See var.dns_hosted_zone_id for why it is not created here.
#
# Its name is what the certificate's precondition below checks the hostname against, and what
# external-dns is given as a domain filter in addons.tf.
data "aws_route53_zone" "jenkins" {
  count = local.serves_public_name ? 1 : 0

  zone_id = var.dns_hosted_zone_id
}

# ---------------------------------------------------------------------------------------------------
# The certificate
# ---------------------------------------------------------------------------------------------------

# One name, DNS validated.  Not a wildcard: this certificate is for the CI endpoint, and a wildcard for
# the zone would be a credential for every future name in it, held by a cluster that is rebuilt often.
#
# ACM certificates are free, and an unvalidated request costs nothing but is deleted by ACM after 72
# hours of PENDING_VALIDATION.  If delegation takes longer than that, `tofu apply` requests a new one.
resource "aws_acm_certificate" "jenkins" {
  count = local.serves_public_name ? 1 : 0

  domain_name       = var.jenkins_hostname
  validation_method = "DNS"

  lifecycle {
    # Renewal and a changed name both replace the certificate, and the load balancer in layer 2 holds a
    # reference to the old ARN until layer 2 runs again.  Creating the new one first means the reference
    # is never to a certificate that has been deleted.
    create_before_destroy = true

    precondition {
      condition     = var.dns_hosted_zone_id != ""
      error_message = "jenkins_hostname needs dns_hosted_zone_id: the validation records below have to be written into the zone that holds the name, and this configuration cannot find that zone on its own."
    }

    precondition {
      # `endswith` on the zone name with its trailing dot removed, so `ci.example.org` is accepted in
      # zone `example.org` and rejected in zone `example.net`.  Without this the certificate is requested,
      # the validation records are written into the wrong zone, and the only symptom is a certificate
      # that stays PENDING_VALIDATION forever with nothing to say why.
      condition = endswith(
        var.jenkins_hostname,
        trimsuffix(one(data.aws_route53_zone.jenkins[*].name), ".")
      )
      error_message = "jenkins_hostname must lie inside the zone named by dns_hosted_zone_id."
    }
  }
}

# The record proving to Amazon that this account controls the name.  ACM names it, and its content is read
# from the certificate rather than composed here.
#
# `count`, not `for_each` over the validation options: the names come from a resource attribute, so OpenTofu
# cannot know a map's keys until the certificate exists and refuses to plan instances it cannot identify.
# One name, one record; a second name on the certificate means rewriting this block.
resource "aws_route53_record" "certificate_validation" {
  count = local.serves_public_name ? 1 : 0

  zone_id = var.dns_hosted_zone_id
  name    = local.certificate_validation_option.resource_record_name
  type    = local.certificate_validation_option.resource_record_type
  records = [local.certificate_validation_option.resource_record_value]
  ttl     = 60

  # ACM re-uses a validation record across certificates for the same name, so a replaced certificate can
  # ask for a record that already exists.  Overwriting it is correct: the value it holds came from an
  # earlier request for the same name.
  allow_overwrite = true
}

# There is deliberately no `aws_acm_certificate_validation` resource, which would wait for ISSUED.  ACM
# reaches ISSUED by resolving the record above through public DNS, which needs the parent domain's holder to
# have delegated this zone to the nameservers `tofu output dns_nameservers` reports: done at a registrar, by
# a person, and not scriptable from here.
#
# So the wait would block for its timeout and then fail with the cluster built.  Without it, apply finishes
# and the certificate becomes ISSUED on its own within minutes of the delegation landing.
#
# Nothing is lost: ../Makefile reads the status before applying the TLS half of layer 2 and 3-smoke asserts
# it, so an unvalidated certificate leaves Jenkins on plain HTTP rather than pointing a load balancer at a
# certificate that cannot be served.
