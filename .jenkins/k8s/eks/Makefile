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

# The join between the three layers, and the only place they meet.
#
# Data flows one way: layer 1 owns AWS and prints what it made, this file turns that into environment
# variables, layers 2 and 3 read only those.  Nothing in 2-platform or 3-smoke opens a tfstate file, and
# nothing in 1-cluster talks to Kubernetes.
#
# Read README.md before the first apply. `make plan` is where the ceilings the account allows are shown, and
# `make caps` is what decides how much this may spend before the pools are stopped.

CLUSTER_DIR   := 1-cluster
PLATFORM_DIR  := 2-platform
SMOKE_DIR     := 3-smoke
CASSANDRA_DIR := ../../..

# The cloud-neutral half of this tooling, shared with the sibling directory for every other cloud.  The
# scripts here find it by walking up from their own location; see ../shared/README.md.
SHARED_DIR    := ../shared

# Generated, all gitignored.  ENV_FILE is `tofu output -raw environment`, assembled in layer 1's outputs.tf.
# POOLS_FILE is the agent pools as JSON for check-pool-fit.py, CTL_FILE the controller pool for
# controller-fit.py.  VALUES_FILE is jenkins-eks-overrides.yaml plus this site's name and certificate;
# generated, not committed, because a certificate ARN names an AWS account, and present only with
# jenkins_hostname.  QUOTA_FILE and CTL_FIT_FILE hold what the account allows and whether the controller can
# hold it, read live because AWS grants a quota increase on its own schedule.
ENV_FILE    := .eks-env
POOLS_FILE  := .eks-agent-pools.json
CTL_FILE    := .eks-controller-pool.json
VALUES_FILE := .eks-jenkins-values.yaml
QUOTA_FILE  := .eks-vcpu-quota
CTL_FIT_FILE:= .eks-controller-fit

# The three spend caps, written by spend-caps.py and loaded by every tofu command in $(CLUSTER_DIR) on its
# own.  Not in `clean`, and not generated in the sense the files above are: it holds an answer somebody was
# asked for, and deleting it would ask again.  Gitignored as *.auto.tfvars.
CAPS_FILE   := $(CLUSTER_DIR)/spend-caps.auto.tfvars

# The account's on-demand vCPU quota, turned into a node ceiling by vcpu-quota.py.  Out of ENV_FILE and the
# `env` target because `make env` reads local state and needs no credentials, and this needs two AWS calls.
# Read live every run rather than cached in tfstate: AWS grants a quota increase on its own schedule.
QUOTA_ENV = set -a; . ./$(QUOTA_FILE); set +a;

TOFU     ?= tofu
HELMFILE ?= helmfile

# What build-jenkins-values.py builds VALUES_FILE on top of.  run-ci takes one --values-override, so a site
# with customisations of its own (.jenkins/k8s/README.md keeps those in
# `~/.cassandra-ci/<site>-overrides.yaml`) copies jenkins-eks-overrides.yaml there, adds to it, and names it:
#
#   make jenkins VALUES_BASE=~/.cassandra-ci/<site>-overrides.yaml
#
# The copy has to track jenkins-eks-overrides.yaml afterwards, so keep the site's file to the site's own keys.
VALUES_BASE ?= $(PLATFORM_DIR)/jenkins-eks-overrides.yaml

# Extra arguments, for the flows that take them:
#   make plan  TOFU_ARGS='-var cluster_name=mck--cassandra-jenkins'
#   make smoke SMOKE_ARGS=--build
#   make jenkins RUN_CI_ARGS='-p pre-commit'
TOFU_ARGS   ?=
SMOKE_ARGS  ?=
RUN_CI_ARGS ?=
# `make spend SPEND_ARGS=--days` prints every day behind the totals, `--json` the same as JSON, and
# `--no-cost-explorer` reports from the figures already kept rather than paying a cent for fresh ones.  `make caps CAPS_ARGS='--daily 900 --weekly 3000 --monthly 9000'` sets
# the caps without being asked, for a machine with no terminal.
SPEND_ARGS  ?=
CAPS_ARGS   ?=
# `make quota CONTROLLER_FIT_ARGS=--warn-only` deploys past a controller that cannot hold the configured
# pools.  Refusing is the default: see controller-fit.py for why a warning was not enough.
CONTROLLER_FIT_ARGS ?=

# The autoscaler image follows the control plane's Kubernetes minor; see 2-platform.  Set this to pin a
# patch version instead of taking the newest published for that minor.
CLUSTER_AUTOSCALER_IMAGE_TAG ?=

.PHONY: help init fmt validate plan apply kubeconfig env quota caps caps-check spend helm-diff autoscaler jenkins platform smoke test destroy clean

# Every prerequisite list here is an order and not a set: `platform` is the autoscaler and then Jenkins,
# `apply` asks for the caps before it creates anything, and `smoke` reads an environment `env` writes.  Under
# `make -j` those run together, which is a prompt printed into the middle of an apply at best.
.NOTPARALLEL:

help:
	@echo "Layer 1, OpenTofu, owns AWS:"
	@echo "  init         install providers"
	@echo "  plan         show what would change            TOFU_ARGS='-var ...'"
	@echo "  apply        create or update the cluster"
	@echo "  destroy      delete everything layer 1 made"
	@echo
	@echo "Layer 2, Helmfile and .build/run-ci:"
	@echo "  kubeconfig   add a kubectl context named after the cluster"
	@echo "  autoscaler   install the cluster autoscaler"
	@echo "  jenkins      install Jenkins, through run-ci   RUN_CI_ARGS='-p pre-commit'"
	@echo "                                                VALUES_BASE=~/.cassandra-ci/<site>-overrides.yaml"
	@echo "  platform     autoscaler then jenkins"
	@echo
	@echo "Layer 3, assertions:"
	@echo "  smoke        check a deployed cluster          SMOKE_ARGS=--build RUN_CI_ARGS='-b trunk'"
	@echo
	@echo "No cluster and no credentials needed:"
	@echo "  fmt          rewrite the HCL to canonical form"
	@echo "  validate     tofu fmt -check and tofu validate"
	@echo "  test         validate, plus the shared, check-pool-fit, vcpu-quota, controller-fit,"
	@echo "               spend-caps, spend-guard, build-jenkins-values and cpus"
	@echo "               regression tests"
	@echo
	@echo "Needs an applied layer 1, because it reads the cluster's own state:"
	@echo "  env          write $(ENV_FILE), $(POOLS_FILE) and $(CTL_FILE) from tofu output"
	@echo "  quota        show how many nodes the account's quotas allow, and whether the"
	@echo "               controller can hold them. Fails when it cannot;"
	@echo "               CONTROLLER_FIT_ARGS=--warn-only deploys anyway"
	@echo "               Before the first apply, \`make plan\` prints the same ceilings as"
	@echo "               the agent_node_ceiling output."
	@echo
	@echo "What this may spend:"
	@echo "  caps         ask for the daily, weekly and monthly spend caps, and write them"
	@echo "               CAPS_ARGS='--daily 900 --weekly 3000 --monthly 9000' sets them unasked"
	@echo "  spend        what has been spent against those caps, and whether the pools are"
	@echo "               stopped                            SPEND_ARGS='--days --json'"
	@echo
	@echo "  clean        remove those, $(VALUES_FILE), $(QUOTA_FILE), $(CTL_FIT_FILE), and .terraform"
	@echo "               (not $(CAPS_FILE), which holds an answer rather than a derived value)"

# ---------------------------------------------------------------------------------------------------
# Layer 1
# ---------------------------------------------------------------------------------------------------

init:
	$(TOFU) -chdir=$(CLUSTER_DIR) init

plan: init
	$(TOFU) -chdir=$(CLUSTER_DIR) plan $(TOFU_ARGS)

# caps-check first, so that nobody creates a cluster that can hold hundreds of on-demand nodes without having
# been asked what that may cost.  `plan` deliberately does not ask: a plan changes nothing, and a plan run
# before the caps are set shows a cluster with no spend guard in it, which is what it would be.
apply: caps-check init
	$(TOFU) -chdir=$(CLUSTER_DIR) apply $(TOFU_ARGS)

destroy: init
	$(TOFU) -chdir=$(CLUSTER_DIR) destroy $(TOFU_ARGS)

fmt:
	$(TOFU) fmt $(CLUSTER_DIR)

# No credentials and no state, so this is what CI can run: -backend=false leaves the providers installed and
# the backend uninitialised.  It cannot evaluate a data source, resolve an add-on version, or check a
# configuration_values string against its schema; those need an account, at apply.
validate:
	$(TOFU) fmt -check -diff $(CLUSTER_DIR)
	cd $(CLUSTER_DIR) && $(TOFU) init -backend=false -input=false >/dev/null && $(TOFU) validate

# ../cpus-test.sh is here and ../jenkins-test.sh is not, and the difference is what each needs.  cpus-test
# reads the Jenkinsfile and writes to a temporary directory; jenkins-test adds the jenkins helm repository
# and renders the chart, so it needs helm and the network.  .github/workflows/jenkins-check.yaml runs that
# one.  cpus() decides the parallelism of every agent this cluster starts, which is why it is checked here.
test: validate
	$(SHARED_DIR)/shared-test.sh
	$(SMOKE_DIR)/check-pool-fit-test.sh
	./vcpu-quota-test.sh
	./controller-fit-test.sh
	./spend-caps-test.sh
	./spend-guard-test.sh
	$(PLATFORM_DIR)/build-jenkins-values-test.sh
	../cpus-test.sh

# ---------------------------------------------------------------------------------------------------
# The join
# ---------------------------------------------------------------------------------------------------

# Regenerated every run, not cached against the tfstate's timestamp: a stale environment is how layer 2
# configures the cluster it built last week.
#
# The `echo` is required.  `tofu output -raw` prints no trailing newline, so the line appended below joined
# the last one: EKS_AGENT_NODE_GROUP_NAMES ended in `agents-small-bexport`, and 3-smoke reported that group
# as never discovered.  A joined line is a corrupt value, not a syntax error, so the shell sources it.
# The guard is the first line, and it is what every other target here inherits.  These outputs exist only in
# the state an apply wrote, and `tofu output -json <name>` on an empty state says "Output ... not found",
# which under `@` is a bare tofu error on the second command an operator runs.  Every target below depends on
# `env`, so one sentence here covers all of them.
env:
	@$(TOFU) -chdir=$(CLUSTER_DIR) output -json cluster_name >/dev/null 2>&1 || { \
	    echo "Layer 1 has not been applied, so there is nothing to read: these values come from the"; \
	    echo "cluster's own state, not from the configuration."; \
	    echo; \
	    echo "    make plan     # what would be created, and the ceilings the account allows"; \
	    echo "    make apply    # create it"; \
	    echo; \
	    exit 1; \
	}
	@{ $(TOFU) -chdir=$(CLUSTER_DIR) output -raw environment; echo; } > $(ENV_FILE)
	@$(TOFU) -chdir=$(CLUSTER_DIR) output -json agent_node_groups > $(POOLS_FILE)
	@$(TOFU) -chdir=$(CLUSTER_DIR) output -json controller_node_group > $(CTL_FILE)
	@echo "export EKS_AGENT_POOLS_FILE='$(abspath $(POOLS_FILE))'" >> $(ENV_FILE)
	@echo "Wrote $(ENV_FILE), $(POOLS_FILE) and $(CTL_FILE)"

# The account's ceiling, which no layer sees for itself: 2-platform caps the autoscaler with it, 3-smoke
# checks the pools against it, and neither may read it from the other.  See vcpu-quota.py.
#
# Pools asking for more vCPU than the account allows warn and succeed: that cluster runs, smaller than
# written for, and failing would leave no cluster at all.  Only a quota too small for one agent stops here.
quota: env
	@set -a; . ./$(ENV_FILE); set +a; \
	./vcpu-quota.py --pools "$${EKS_AGENT_POOLS_FILE}" --check --exports-to $(QUOTA_FILE) \
	    || { rm -f $(QUOTA_FILE); exit 1; }
	@echo
# controller-fit.py reports, and does not write.  The recommendation is applied by hand, and the two files
# have to move together: an instance type raised without the matching container limits buys nothing, and
# limits raised without the instance cannot be scheduled.  `./controller-fit.py --check` prints both edits.
#
# It refuses two different things, and only one of them is a judgement.  A controller without the headroom
# for the configured pools is slow, and $(CONTROLLER_FIT_ARGS) can accept that.  A requests figure above the
# node's allocatable is a pod that never starts, and nothing accepts it.
#
# --overrides is $(VALUES_BASE), because that is the file run-ci applies after the shared one and therefore
# the file that decides the controller's heap, requests and limits.  Without it this sizes a controller
# nobody deploys.
	@set -a; . ./$(ENV_FILE); set +a; \
	./controller-fit.py --pools $(POOLS_FILE) --controller $(CTL_FILE) \
	    --deployment $(CASSANDRA_DIR)/.jenkins/k8s/jenkins-deployment.yaml \
	    --overrides $(VALUES_BASE) \
	    --check --exports-to $(CTL_FIT_FILE) $(CONTROLLER_FIT_ARGS)

# ---------------------------------------------------------------------------------------------------
# What this may spend
# ---------------------------------------------------------------------------------------------------

# The caps are an answer somebody gives, not a value derived from the account, so this asks and writes rather
# than computing.  $(QUOTA_FILE) is sourced when it exists, for one figure: how many agent nodes the account
# allows, without which the prompt can state what the cluster costs standing still and not what a build costs.
# So `make quota` first gives a better prompt, and its absence is said out loud rather than assumed away.
caps:
	@nodes=''; \
	if [ -f ./$(QUOTA_FILE) ]; then . ./$(QUOTA_FILE); nodes="$${EKS_MAX_AGENT_NODES}"; fi; \
	EKS_MAX_AGENT_NODES="$${nodes}" ./spend-caps.py --prompt $(CAPS_ARGS)

# The gate on `apply`, and the reason `apply` can ask a question at all.
#
# Once $(CAPS_FILE) exists it never asks again, whatever is in it: `none` for all three windows is a recorded
# answer, and asking a second time would teach the reader to answer it blindly.  Without a terminal it fails
# with the command to run instead of prompting into a pipe that nobody is reading.
caps-check:
	@if [ -f $(CAPS_FILE) ]; then exit 0; fi; \
	if [ -t 0 ]; then \
	    echo "No spend cap has been set for this cluster yet."; \
	    echo "These agent pools can hold hundreds of on-demand nodes, and no service quota is a limit on"; \
	    echo "money. Three questions follow, and \`none\` is an answer to any of them."; \
	    echo; \
	    $(MAKE) caps; \
	else \
	    echo "No spend cap has been set for this cluster, and there is no terminal to ask at."; \
	    echo "Run one of these first:"; \
	    echo "    make caps"; \
	    echo "    make caps CAPS_ARGS='--daily 900 --weekly 3000 --monthly 9000 --email you@example.org'"; \
	    echo "    make caps CAPS_ARGS='--daily none --weekly none --monthly none'   # no cap at all"; \
	    exit 1; \
	fi

# What has been spent, from the same script and the same arithmetic the Lambda brakes on.  Reads only: it
# never suspends or resumes a pool, whatever the figures say.  Exits 1 over a cap or with the pools stopped,
# and 2 when spend could not be established, so it can be read by a machine as well as by a person.
#
# One Cost Explorer call, which is a cent, unless the figures the guard has kept are fresh enough or
# SPEND_ARGS carries --no-cost-explorer.
spend: env
	@set -a; . ./$(ENV_FILE); set +a; \
	if [ -z "$${EKS_SPEND_CAPS_PARAMETER}" ]; then \
	    echo "No spend cap is set for this cluster, so nothing watches what it spends and there is"; \
	    echo "nothing to report. \`make caps\` asks for the caps; \`make apply\` then creates the guard."; \
	    exit 1; \
	fi; \
	./spend-guard.py --report $(SPEND_ARGS)

# --alias, so the context is named exactly the cluster: 3-smoke/smoke-test.sh refuses a context that does
# not name it, and the default alias is the cluster ARN, which carries the account id into every kubeconfig.
kubeconfig: env
	@set -a; . ./$(ENV_FILE); set +a; \
	aws eks update-kubeconfig --region "$${AWS_REGION}" --name "$${EKS_CLUSTER_NAME}" \
	    --alias "$${EKS_CLUSTER_NAME}"

# ---------------------------------------------------------------------------------------------------
# Layer 2
# ---------------------------------------------------------------------------------------------------

# `helmfile apply` diffs first, and `helm diff` is a plugin rather than a subcommand.  Checked here because
# helmfile reports its absence as `unknown command "diff" for "helm"` under fifteen numbered helm arguments,
# which reads as a fault in helmfile.yaml.
#
# `helm plugin list`, not `helm diff version`: an installed plugin that cannot run is a different failure.
helm-diff:
	@helm plugin list 2>/dev/null | grep -q '^diff' || { \
	    echo "helmfile needs the helm-diff plugin, and helm has no plugin named diff."; \
	    echo "Install it with:"; \
	    echo; \
	    case "$$(helm version --short 2>/dev/null)" in \
	        v4*) echo "    helm plugin install --verify=false https://github.com/databus23/helm-diff" ;; \
	        *)   echo "    helm plugin install https://github.com/databus23/helm-diff" ;; \
	    esac; \
	    echo; \
	    echo "Helm 4 verifies plugin signatures by default and helm-diff publishes none, which is what"; \
	    echo "--verify=false is for. Helm 3 does not accept that flag."; \
	    exit 1; \
	}

# Resolved, not written down: the autoscaler is versioned against the Kubernetes minor it simulates
# scheduling for.  The newest patch for the cluster's minor wins, falling back to `.0`, which every released
# minor has.  A mismatch is quiet, so guessing wrong is worse than failing here.
autoscaler: helm-diff env quota
	@set -a; . ./$(ENV_FILE); set +a; $(QUOTA_ENV) \
	tag='$(CLUSTER_AUTOSCALER_IMAGE_TAG)'; \
	if [ -z "$${tag}" ]; then \
	    tag="$$(curl -fsSL https://registry.k8s.io/v2/autoscaling/cluster-autoscaler/tags/list 2>/dev/null \
	        | tr ',' '\n' | grep -o "v$${EKS_KUBERNETES_VERSION}\.[0-9][0-9]*" | sort -t. -k3 -n | tail -1)"; \
	fi; \
	if [ -z "$${tag}" ]; then \
	    tag="v$${EKS_KUBERNETES_VERSION}.0"; \
	    echo "Could not reach registry.k8s.io; falling back to $${tag}"; \
	fi; \
	echo "Cluster autoscaler $${tag} for Kubernetes $${EKS_KUBERNETES_VERSION}"; \
	export CLUSTER_AUTOSCALER_IMAGE_TAG="$${tag}"; \
	$(HELMFILE) --file $(PLATFORM_DIR)/helmfile.yaml apply

# run-ci installs Jenkins and nothing else does: it warns before dropping a live customisation and refuses an
# instanceCap no pool could meet, neither of which a `helm upgrade` here would do.
#
# The values file is generated every run, not only when there is a public name to add, because the agent
# ceiling goes in it too and that number belongs to the account: AWS grants a quota increase on its own
# schedule, so a committed file cannot hold it and a stale one caps Jenkins at yesterday's size.  Without
# jenkins_hostname it carries the ceiling alone, Jenkins staying reachable at the load balancer's name.
#
# The certificate status is read every run rather than trusted from layer 1's state: a certificate becomes
# ISSUED minutes after the delegation lands at a registrar, which no `tofu apply` sees, and the same check
# stops a Service pointing at a certificate ACM deleted for going 72 hours unvalidated.
jenkins: env quota
	@set -a; . ./$(ENV_FILE); set +a; $(QUOTA_ENV) \
	cert=''; \
	if [ -n "$${EKS_JENKINS_HOSTNAME}" ] && [ -n "$${EKS_TLS_CERTIFICATE_ARN}" ]; then \
	    status="$$(aws acm describe-certificate --region "$${AWS_REGION}" \
	        --certificate-arn "$${EKS_TLS_CERTIFICATE_ARN}" \
	        --query 'Certificate.Status' --output text 2>/dev/null)"; \
	    if [ "$${status}" = 'ISSUED' ]; then \
	        cert="$${EKS_TLS_CERTIFICATE_ARN}"; \
	    else \
	        echo "The certificate for $${EKS_JENKINS_HOSTNAME} is $${status:-unreadable}, not ISSUED."; \
	        echo "Jenkins stays on plain HTTP. ACM validates it once the hosted zone is delegated;"; \
	        echo "\`tofu -chdir=$(CLUSTER_DIR) output dns_nameservers\` prints what to set, and"; \
	        echo "README.md has the rest. Re-run \`make jenkins\` after that."; \
	    fi; \
	fi; \
	$(PLATFORM_DIR)/build-jenkins-values.py \
	    --base $(VALUES_BASE) \
	    --deployment $(CASSANDRA_DIR)/.jenkins/k8s/jenkins-deployment.yaml \
	    --hostname "$${EKS_JENKINS_HOSTNAME}" \
	    $${cert:+--certificate-arn $${cert}} \
	    --container-cap "$${EKS_MAX_AGENT_NODES}" \
	    --pools $(POOLS_FILE) \
	    --out $(VALUES_FILE); \
	values=".jenkins/k8s/eks/$(VALUES_FILE)"; \
	cd $(CASSANDRA_DIR) && .build/run-ci --only-setup \
	    --kubecontext "$${EKS_CLUSTER_NAME}" \
	    --values-override "$${values}" $(RUN_CI_ARGS)

# The autoscaler first: Jenkins' capacity check reads each pool's ceiling from the autoscaler's status
# configmap, so with none running every instanceCap goes unchecked, on the deploy where that matters most.
platform: autoscaler jenkins

# ---------------------------------------------------------------------------------------------------
# Layer 3
# ---------------------------------------------------------------------------------------------------

# RUN_CI_ARGS reaches run-ci here too, used only by SMOKE_ARGS=--build.  Exported, not passed as an argument:
# smoke-test.sh's flags and run-ci's are different sets and one argv cannot carry both.
#   make smoke SMOKE_ARGS=--build RUN_CI_ARGS='-r <url> -b <branch>'
smoke: env quota
	@set -a; . ./$(ENV_FILE); set +a; $(QUOTA_ENV) \
	RUN_CI_ARGS='$(RUN_CI_ARGS)' $(SMOKE_DIR)/smoke-test.sh $(SMOKE_ARGS)

# ---------------------------------------------------------------------------------------------------

clean:
	rm -f $(ENV_FILE) $(POOLS_FILE) $(CTL_FILE) $(VALUES_FILE) $(QUOTA_FILE) $(CTL_FIT_FILE)
	rm -rf $(CLUSTER_DIR)/.terraform
