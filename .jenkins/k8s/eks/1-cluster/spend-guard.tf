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

locals {
  spend_caps = {
    daily   = var.spend_cap_daily_usd
    weekly  = var.spend_cap_weekly_usd
    monthly = var.spend_cap_monthly_usd
  }

  # One cap is enough to be worth a guard, and none means the operator was asked and said no.  The whole
  # file is gated on this, so a cluster with no caps carries no Lambda, no schedule, no topic and no cost.
  spend_guard_enabled = length([for cap in values(local.spend_caps) : cap if cap != null]) > 0

  spend_guard_name = "${var.cluster_name}-spend-guard"

  spend_caps_parameter  = "/${var.cluster_name}/spend-guard/caps"
  spend_state_parameter = "/${var.cluster_name}/spend-guard/state"

  spend_metric_namespace = "CassandraJenkins/SpendGuard"

  spend_agent_asg_names = [
    for name in sort(keys(local.agent_node_group_specs)) : local.node_group_asg_names[name]
  ]

  # An Auto Scaling group's ARN carries a uuid AWS chose, which is not in state here; the name is.  A
  # wildcard in the uuid segment and the exact name in the last is the form AWS documents for this.
  spend_agent_asg_arns = [
    for name in local.spend_agent_asg_names :
    "arn:${data.aws_partition.current.partition}:autoscaling:${var.region}:${data.aws_caller_identity.current.account_id}:autoScalingGroup:*:autoScalingGroupName/${name}"
  ]

  spend_parameter_arn_prefix = "arn:${data.aws_partition.current.partition}:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter"

  spend_guard_function_arn = "arn:${data.aws_partition.current.partition}:lambda:${var.region}:${data.aws_caller_identity.current.account_id}:function:${local.spend_guard_name}"
}

check "spend_caps_are_ordered" {
  assert {
    condition = (var.spend_cap_daily_usd == null || var.spend_cap_weekly_usd == null
    || var.spend_cap_weekly_usd >= var.spend_cap_daily_usd)
    error_message = "spend_cap_weekly_usd is under spend_cap_daily_usd, so the week brakes the pools before a day ever could."
  }

  assert {
    condition = (var.spend_cap_weekly_usd == null || var.spend_cap_monthly_usd == null
    || var.spend_cap_monthly_usd >= var.spend_cap_weekly_usd)
    error_message = "spend_cap_monthly_usd is under spend_cap_weekly_usd, so the month brakes the pools before a week ever could."
  }
}

# JSON rather than three parameters: the guard reads them together, and three reads of three parameters can
# see two of them from before an edit and one from after.
resource "aws_ssm_parameter" "spend_caps" {
  count = local.spend_guard_enabled ? 1 : 0

  name        = local.spend_caps_parameter
  description = "Daily, weekly and monthly spend caps in USD for ${var.cluster_name}, read by ${local.spend_guard_name}"
  type        = "String"
  value       = jsonencode(local.spend_caps)
}

resource "aws_ssm_parameter" "spend_guard_state" {
  count = local.spend_guard_enabled ? 1 : 0

  name        = local.spend_state_parameter
  description = "Cost Explorer figures kept by ${local.spend_guard_name}; the Lambda owns this value"
  type        = "String"
  value       = jsonencode({ version = 1, cost_as_of = null, cost_by_day = {} })

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_sns_topic" "spend_alerts" {
  count = local.spend_guard_enabled ? 1 : 0

  name         = "${var.cluster_name}-spend-alerts"
  display_name = "Spend alerts for ${var.cluster_name}"
}

data "aws_iam_policy_document" "spend_alerts" {
  count = local.spend_guard_enabled ? 1 : 0

  statement {
    sid     = "BudgetsAndAlarmsMayPublish"
    effect  = "Allow"
    actions = ["sns:Publish"]

    principals {
      type        = "Service"
      identifiers = ["budgets.amazonaws.com", "cloudwatch.amazonaws.com"]
    }

    resources = [one(aws_sns_topic.spend_alerts[*].arn)]

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sns_topic_policy" "spend_alerts" {
  count = local.spend_guard_enabled ? 1 : 0

  arn    = one(aws_sns_topic.spend_alerts[*].arn)
  policy = one(data.aws_iam_policy_document.spend_alerts[*].json)
}

# A pending subscription until the address confirms it, which is a link in an email and not something an
# apply can do.  Nothing fails while it is pending; the alerts go to the topic and to CloudWatch regardless.
resource "aws_sns_topic_subscription" "spend_email" {
  count = local.spend_guard_enabled && var.spend_alert_email != "" ? 1 : 0

  topic_arn = one(aws_sns_topic.spend_alerts[*].arn)
  protocol  = "email"
  endpoint  = var.spend_alert_email
}

data "archive_file" "spend_guard" {
  type        = "zip"
  output_path = "${path.module}/.terraform/spend-guard.zip"

  source {
    content  = file("${path.module}/../spend-guard.py")
    filename = "spend_guard.py"
  }
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  name               = local.spend_guard_name
  description        = "Spend guard for ${var.cluster_name}: reads spend, suspends the agent pools' Launch process"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  statement {
    sid    = "ReadWhatTheAccountWasCharged"
    effect = "Allow"
    # Cost Explorer takes no resource-level permission at all, and its data is the whole account's.  That is
    # the same scope the caps are written against; see ../spend-guard.py.
    actions   = ["ce:GetCostAndUsage"]
    resources = ["*"]
  }

  statement {
    sid       = "ReadWhatIsRunning"
    effect    = "Allow"
    actions   = ["cloudwatch:GetMetricData", "ec2:DescribeInstanceStatus"]
    resources = ["*"]
  }

  statement {
    sid       = "PublishItsOwnMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = [local.spend_metric_namespace]
    }
  }

  statement {
    sid    = "ReadTheAgentGroups"
    effect = "Allow"
    # DescribeAutoScalingGroups takes no resource-level permission; the two that change one do.
    actions   = ["autoscaling:DescribeAutoScalingGroups"]
    resources = ["*"]
  }

  statement {
    sid       = "ReadWhenItWasDeployed"
    effect    = "Allow"
    actions   = ["lambda:GetFunctionConfiguration"]
    resources = [local.spend_guard_function_arn]
  }

  statement {
    sid       = "BrakeAndReleaseTheAgentGroups"
    effect    = "Allow"
    actions   = ["autoscaling:SuspendProcesses", "autoscaling:ResumeProcesses"]
    resources = local.spend_agent_asg_arns
  }

  statement {
    sid     = "ReadTheCapsAndItsCache"
    effect  = "Allow"
    actions = ["ssm:GetParameter"]
    resources = ["${local.spend_parameter_arn_prefix}${local.spend_caps_parameter}",
    "${local.spend_parameter_arn_prefix}${local.spend_state_parameter}"]
  }

  statement {
    sid       = "WriteItsCache"
    effect    = "Allow"
    actions   = ["ssm:PutParameter"]
    resources = ["${local.spend_parameter_arn_prefix}${local.spend_state_parameter}"]
  }

  statement {
    sid       = "SayWhatItDid"
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [one(aws_sns_topic.spend_alerts[*].arn)]
  }

  statement {
    sid       = "WriteItsOwnLog"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${one(aws_cloudwatch_log_group.spend_guard[*].arn)}:*"]
  }
}

resource "aws_iam_role_policy" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  name   = "spend-guard"
  role   = one(aws_iam_role.spend_guard[*].id)
  policy = one(data.aws_iam_policy_document.spend_guard[*].json)
}

# Created here rather than left to Lambda, for the reason the control plane's group is: Lambda creates it on
# first write with no expiry, and every evaluation prints its whole report into it.
resource "aws_cloudwatch_log_group" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  name              = "/aws/lambda/${local.spend_guard_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  function_name = local.spend_guard_name
  description   = "Compares ${var.cluster_name}'s spend against its caps, and suspends the agent pools when one is met"
  role          = one(aws_iam_role.spend_guard[*].arn)

  filename         = data.archive_file.spend_guard.output_path
  source_code_hash = data.archive_file.spend_guard.output_base64sha256
  handler          = "spend_guard.handler"

  runtime = "python3.13"

  # arm64 because nothing in this function is compiled and it is cheaper per millisecond.
  architectures = ["arm64"]

  timeout     = 120
  memory_size = 256

  environment {
    # The same names ../Makefile exports into .eks-env, so that `make spend` and this Lambda read one set of
    # names and not two.  AWS_REGION is set by the runtime itself and is deliberately not repeated here.
    variables = {
      EKS_CLUSTER_NAME              = var.cluster_name
      EKS_SPEND_CAPS_PARAMETER      = local.spend_caps_parameter
      EKS_SPEND_STATE_PARAMETER     = local.spend_state_parameter
      EKS_SPEND_AGENT_ASG_NAMES     = join(" ", local.spend_agent_asg_names)
      EKS_SPEND_ALERT_TOPIC_ARN     = one(aws_sns_topic.spend_alerts[*].arn)
      EKS_SPEND_METRIC_NAMESPACE    = local.spend_metric_namespace
      EKS_SPEND_COST_METRIC         = var.spend_cost_metric
      EKS_SPEND_PRICE_PER_VCPU_HOUR = tostring(var.spend_price_per_vcpu_hour)
      EKS_SPEND_FIXED_USD_PER_DAY   = tostring(var.spend_fixed_usd_per_day)
      EKS_SPEND_CE_REFRESH_HOURS    = tostring(var.spend_cost_refresh_hours)
    }
  }

  depends_on = [
    aws_iam_role_policy.spend_guard,
    aws_cloudwatch_log_group.spend_guard,
  ]
}

resource "aws_cloudwatch_event_rule" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  name                = local.spend_guard_name
  description         = "Runs ${local.spend_guard_name} every ${var.spend_guard_interval_minutes} minutes"
  schedule_expression = "rate(${var.spend_guard_interval_minutes} minutes)"
}

resource "aws_cloudwatch_event_target" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  rule = one(aws_cloudwatch_event_rule.spend_guard[*].name)
  arn  = one(aws_lambda_function.spend_guard[*].arn)
}

resource "aws_lambda_permission" "spend_guard" {
  count = local.spend_guard_enabled ? 1 : 0

  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = one(aws_lambda_function.spend_guard[*].function_name)
  principal     = "events.amazonaws.com"
  source_arn    = one(aws_cloudwatch_event_rule.spend_guard[*].arn)
}

resource "aws_cloudwatch_metric_alarm" "spend_guard_stalled" {
  count = local.spend_guard_enabled ? 1 : 0

  alarm_name        = "${local.spend_guard_name}-stalled"
  alarm_description = "${local.spend_guard_name} has not reported a spend figure for ${var.spend_guard_interval_minutes * 6} minutes, so nothing is watching what ${var.cluster_name} spends."

  namespace   = local.spend_metric_namespace
  metric_name = "EvaluationOk"
  dimensions  = { ClusterName = var.cluster_name }

  statistic           = "Sum"
  period              = var.spend_guard_interval_minutes * 120
  evaluation_periods  = 3
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"

  alarm_actions = [one(aws_sns_topic.spend_alerts[*].arn)]
  ok_actions    = [one(aws_sns_topic.spend_alerts[*].arn)]
}

resource "aws_budgets_budget" "spend" {
  for_each = {
    for window, cap in local.spend_caps : window => cap
    if cap != null && var.enable_spend_budgets && contains(["daily", "monthly"], window)
  }

  name         = "${var.cluster_name}-${each.key}"
  budget_type  = "COST"
  limit_amount = tostring(each.value)
  limit_unit   = "USD"
  time_unit    = each.key == "daily" ? "DAILY" : "MONTHLY"

  # A fixed reference point rather than "now": a start date derived from the current time would move on
  # every plan, and Budgets uses it only as the origin the periods are counted from.
  time_period_start = "2020-01-01_00:00"

  # 80% is the one worth having.  A notification at 100% arrives when the guard has already braked the pools,
  # and says nothing that the guard's own message did not.
  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 80
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_sns_topic_arns = [one(aws_sns_topic.spend_alerts[*].arn)]
  }

  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 100
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_sns_topic_arns = [one(aws_sns_topic.spend_alerts[*].arn)]
  }

  depends_on = [aws_sns_topic_policy.spend_alerts]
}
