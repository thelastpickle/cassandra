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

# A cap on what this cluster may spend, and something that acts on it.
#
# Everything here exists because the four ceilings in ../README.md are ceilings on size and none of them is a
# ceiling on money.  A cluster inside every quota can hold hundreds of on-demand nodes for as long as builds
# keep queueing, and nothing in AWS stops that: a service quota is a limit on what may exist at once, and a
# bill is a limit on nothing at all.
#
# The pieces, and there are only five ideas in them:
#
#   the caps        three numbers in one SSM parameter, read live so one can be raised without a deploy
#   the guard       ../spend-guard.py as a Lambda, on a schedule, which decides and acts
#   the brake       Launch suspended on the agent Auto Scaling groups, which is all the guard ever changes
#   the alarm       on the guard's own heartbeat metric, because a dead guard is silent by nature
#   the budgets     AWS Budgets for the console's own view of the same two windows, and for a second alert
#                   path that does not depend on any of the above
#
# Read the docstring at the top of ../spend-guard.py before changing any of it.  It carries why spend is
# estimated rather than billed, why unknown spend brakes, and what the brake does to a running build.
#
# Nothing here is created unless at least one cap is set; see local.spend_guard_enabled.  `make caps` asks
# for the caps and writes them to spend-caps.auto.tfvars, which OpenTofu loads on its own.

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

  # Under the cluster's own name, so two clusters in one account do not share caps or state.  The caps are a
  # parameter rather than a Lambda environment variable because a cap has to be raisable in the minute
  # somebody is watching a build stop; the environment needs a deploy.
  spend_caps_parameter  = "/${var.cluster_name}/spend-guard/caps"
  spend_state_parameter = "/${var.cluster_name}/spend-guard/state"

  spend_metric_namespace = "CassandraJenkins/SpendGuard"

  # Every agent group's Auto Scaling group, and deliberately not the controller's.  The brake stops new
  # agents; the controller holds the build queue and jenkins_home, and stopping it would abandon whatever is
  # running for the sake of one instance.  var.spend_cap_* are chosen against that, and ../spend-caps.py
  # refuses a cap under what the controller alone costs.
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

  # Built from its parts, and not read off aws_lambda_function.  The policy document below names the function,
  # the role policy holds that document, and the function waits on the role policy, so reading the attribute
  # is a cycle:
  #
  #   Error: Cycle: data.aws_iam_policy_document.spend_guard, aws_iam_role_policy.spend_guard,
  #                 aws_lambda_function.spend_guard
  #
  # The name is known at plan time, being var.cluster_name and a literal, so the ARN is too.  The Auto Scaling
  # ARNs above are built for a different reason, a uuid this configuration never sees, and the two together
  # are why nothing in this file reads an ARN it can spell.
  spend_guard_function_arn = "arn:${data.aws_partition.current.partition}:lambda:${var.region}:${data.aws_caller_identity.current.account_id}:function:${local.spend_guard_name}"
}

# A weekly cap under a daily one, or a monthly under a weekly, makes the smaller window's cap unreachable:
# the wider window is met first and brakes the pools before the narrower one is approached.  A warning and
# not an error, because it is a fact about two numbers and not about this configuration; `check` is what
# OpenTofu has for that.
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

# ---------------------------------------------------------------------------------------------------
# The caps, and the guard's own state
# ---------------------------------------------------------------------------------------------------

# JSON rather than three parameters: the guard reads them together, and three reads of three parameters can
# see two of them from before an edit and one from after.
resource "aws_ssm_parameter" "spend_caps" {
  count = local.spend_guard_enabled ? 1 : 0

  name        = local.spend_caps_parameter
  description = "Daily, weekly and monthly spend caps in USD for ${var.cluster_name}, read by ${local.spend_guard_name}"
  type        = "String"
  value       = jsonencode(local.spend_caps)
}

# Where the guard keeps its Cost Explorer figures, created empty here and owned by the Lambda from then on.
# ignore_changes for the same reason node-groups.tf ignores desired_size: something else owns this value at
# runtime, and without it every plan after an evaluation proposes emptying it, which costs a cent a
# day in Cost Explorer calls and loses the days the fit is made from.
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

# ---------------------------------------------------------------------------------------------------
# Who is told
# ---------------------------------------------------------------------------------------------------

resource "aws_sns_topic" "spend_alerts" {
  count = local.spend_guard_enabled ? 1 : 0

  name         = "${var.cluster_name}-spend-alerts"
  display_name = "Spend alerts for ${var.cluster_name}"
}

# The guard publishes with its own role, so no policy is needed for it.  Budgets is a different matter: it
# publishes as a service principal and is refused without this, with an error at apply that names the topic
# and not the missing permission.  CloudWatch is listed for the same reason, against the alarm below.
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

# ---------------------------------------------------------------------------------------------------
# The guard
# ---------------------------------------------------------------------------------------------------

# One file, zipped here, with no dependency to install: ../spend-guard.py uses boto3, which the Lambda
# runtime carries, and the `aws` CLI when it is run from a terminal instead.  See class Aws there.
#
# Renamed inside the zip, because a Lambda handler is `<module>.<function>` and `spend-guard` is not a name
# an import statement can carry.  The file keeps the directory's naming, and the module gets a legal one.
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

# Written out rather than taking a managed policy, and scoped where AWS allows scoping.  The two that
# matter: the pools this may act on are named one by one, so a second cluster's groups are out of reach; and
# PutMetricData, which takes no resource, is held to this namespace by condition.
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

  # Not for the Lambda, which never asks.  `make spend` asks when it finds the brake on and no figure of its
  # own that justifies it, because the answer is then which copy of the file set it; see deployed_guard in
  # ../spend-guard.py.  It is granted here so that one role covers both callers.
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

  # Pinned to a minor, unlike everything else here, because a Lambda runtime is retired on a published date
  # and an unpinned one does not exist.  List what the account may use with:
  #   aws lambda list-runtimes  # or the runtimes table in the Lambda documentation
  runtime = "python3.13"

  # arm64 because nothing in this function is compiled and it is cheaper per millisecond.
  architectures = ["arm64"]

  # An evaluation is two or three AWS calls and some arithmetic, and Cost Explorer is the slow one, at a few
  # seconds.  120 is generous against that and still fails rather than hanging for the schedule's whole
  # interval.
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

# ---------------------------------------------------------------------------------------------------
# The schedule
# ---------------------------------------------------------------------------------------------------

# Five minutes by default, and the interval is what the overshoot is measured in: the pools at full size cost
# a few hundred dollars an hour, so a cap can be passed by an interval's worth of spend before anything acts.
# See var.spend_guard_interval_minutes.
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

# ---------------------------------------------------------------------------------------------------
# Who watches the guard
# ---------------------------------------------------------------------------------------------------

# A guard that has stopped running reports nothing, which is what an unguarded cluster also reports.  The
# heartbeat metric separates them: the guard publishes EvaluationOk on every evaluation, 1 when it read a
# figure and 0 when it could not, and treat_missing_data = "breaching" is what makes no data at all an alarm
# rather than a gap in a graph.
#
# The window is six evaluations, so a single throttled evaluation does not page anybody, and it is derived
# from var.spend_guard_interval_minutes rather than written down.  A fixed 300-second period against an
# interval of 31 minutes or more leaves most periods with no datapoint at all, and treat_missing_data =
# "breaching" then pages for a guard that is working.
#
# Each period is two intervals, not one.  A period equal to the interval relies on the schedule never
# drifting: two datapoints landing in one period and none in the next is a breach on a healthy guard, and
# EventBridge does not promise to the second.
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

# ---------------------------------------------------------------------------------------------------
# AWS Budgets, for the console's own view
# ---------------------------------------------------------------------------------------------------

# These enforce nothing.  They exist because a budget is where whoever pays the bill will look, and because
# they are a second alert path that shares no code with the guard: they are AWS's own reading of AWS's own
# billing data, so the two disagreeing is itself worth knowing.
#
# Two of them, and no more, because the first two budgets in an account cost nothing and each one after that
# is charged per day.  Confirm that against the current pricing page before adding a third.
#
# There is no weekly budget because AWS Budgets has no weekly period: DAILY, MONTHLY, QUARTERLY and ANNUALLY
# are the four it takes.  The weekly cap is the guard's alone, which is most of why the guard computes its
# own windows rather than reading a budget's state.
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
