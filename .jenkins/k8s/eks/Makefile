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

CLUSTER_DIR   := 1-cluster
PLATFORM_DIR  := 2-platform
SMOKE_DIR     := 3-smoke
CASSANDRA_DIR := ../../..

# The cloud-neutral half of this tooling, shared with the sibling directory for every other cloud.  The
# scripts here find it by walking up from their own location; see ../shared/README.md.
SHARED_DIR    := ../shared

ENV_FILE    := .eks-env
POOLS_FILE  := .eks-agent-pools.json
CTL_FILE    := .eks-controller-pool.json
VALUES_FILE := .eks-jenkins-values.yaml
QUOTA_FILE  := .eks-vcpu-quota
CTL_FIT_FILE:= .eks-controller-fit

CAPS_FILE   := $(CLUSTER_DIR)/spend-caps.auto.tfvars

QUOTA_ENV = set -a; . ./$(QUOTA_FILE); set +a;

TOFU     ?= tofu
HELMFILE ?= helmfile

VALUES_BASE ?= $(PLATFORM_DIR)/jenkins-eks-overrides.yaml

TOFU_ARGS   ?=
SMOKE_ARGS  ?=
RUN_CI_ARGS ?=
SPEND_ARGS  ?=
CAPS_ARGS   ?=
# `make quota CONTROLLER_FIT_ARGS=--warn-only` deploys past a controller that cannot hold the configured
# pools.  Refusing is the default: see controller-fit.py for why a warning was not enough.
CONTROLLER_FIT_ARGS ?=

# The autoscaler image follows the control plane's Kubernetes minor; see 2-platform.  Set this to pin a
# patch version instead of taking the newest published for that minor.
CLUSTER_AUTOSCALER_IMAGE_TAG ?=

.PHONY: help init fmt validate plan apply kubeconfig env quota caps caps-check spend helm-diff autoscaler jenkins platform smoke test destroy clean

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

init:
	$(TOFU) -chdir=$(CLUSTER_DIR) init

plan: init
	$(TOFU) -chdir=$(CLUSTER_DIR) plan $(TOFU_ARGS)

apply: caps-check init
	$(TOFU) -chdir=$(CLUSTER_DIR) apply $(TOFU_ARGS)

destroy: init
	$(TOFU) -chdir=$(CLUSTER_DIR) destroy $(TOFU_ARGS)

fmt:
	$(TOFU) fmt $(CLUSTER_DIR)

validate:
	$(TOFU) fmt -check -diff $(CLUSTER_DIR)
	cd $(CLUSTER_DIR) && $(TOFU) init -backend=false -input=false >/dev/null && $(TOFU) validate

test: validate
	TOFU="$(TOFU)" bash ./pool-sizing-test.sh
	$(SHARED_DIR)/shared-test.sh
	$(SMOKE_DIR)/check-pool-fit-test.sh
	./vcpu-quota-test.sh
	./controller-fit-test.sh
	./spend-caps-test.sh
	./spend-guard-test.sh
	$(PLATFORM_DIR)/build-jenkins-values-test.sh
	python3 $(PLATFORM_DIR)/build-jenkins-values-test.py
	../cpus-test.sh

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
	@set -eu; work=$$(mktemp -d "./.env.XXXXXX"); trap 'rm -rf "$$work"' EXIT; \
	$(TOFU) -chdir=$(CLUSTER_DIR) output -raw environment > "$$work/env"; \
	$(TOFU) -chdir=$(CLUSTER_DIR) output -json agent_node_groups > "$$work/pools"; \
	$(TOFU) -chdir=$(CLUSTER_DIR) output -json controller_node_group > "$$work/controller"; \
	printf "\nexport EKS_AGENT_POOLS_FILE='%s'\n" '$(abspath $(POOLS_FILE))' >> "$$work/env"; \
	mv "$$work/pools" "$(POOLS_FILE)"; mv "$$work/controller" "$(CTL_FILE)"; mv "$$work/env" "$(ENV_FILE)"
	@echo "Wrote $(ENV_FILE), $(POOLS_FILE) and $(CTL_FILE)"

quota: env
	@set -a; . ./$(ENV_FILE); set +a; \
	./vcpu-quota.py --pools "$${EKS_AGENT_POOLS_FILE}" --check --exports-to $(QUOTA_FILE) \
	    || { rm -f $(QUOTA_FILE); exit 1; }
	@echo
	@set -a; . ./$(ENV_FILE); set +a; \
	./controller-fit.py --pools $(POOLS_FILE) --controller $(CTL_FILE) \
	    --deployment $(CASSANDRA_DIR)/.jenkins/k8s/jenkins-deployment.yaml \
	    --overrides $(VALUES_BASE) \
	    --check --exports-to $(CTL_FIT_FILE) $(CONTROLLER_FIT_ARGS)

caps:
	@nodes=''; \
	if [ -f ./$(QUOTA_FILE) ]; then . ./$(QUOTA_FILE); nodes="$${EKS_MAX_AGENT_NODES}"; fi; \
	EKS_MAX_AGENT_NODES="$${nodes}" ./spend-caps.py --prompt $(CAPS_ARGS)

caps-check:
	@if [ -f $(CAPS_FILE) ]; then ./spend-caps.py --check --out "$(CAPS_FILE)"; exit $$?; fi; \
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
	    --out $(VALUES_FILE) || exit $$?; \
	values=".jenkins/k8s/eks/$(VALUES_FILE)"; \
	cd $(CASSANDRA_DIR) && .build/run-ci --only-setup \
	    --kubecontext "$${EKS_CLUSTER_NAME}" \
	    --values-override "$${values}" $(RUN_CI_ARGS)

# The autoscaler first: Jenkins' capacity check reads each pool's ceiling from the autoscaler's status
# configmap, so with none running every instanceCap goes unchecked, on the deploy where that matters most.
platform: autoscaler jenkins

smoke: env quota
	@set -a; . ./$(ENV_FILE); set +a; $(QUOTA_ENV) \
	RUN_CI_ARGS='$(RUN_CI_ARGS)' $(SMOKE_DIR)/smoke-test.sh $(SMOKE_ARGS)

clean:
	rm -f $(ENV_FILE) $(POOLS_FILE) $(CTL_FILE) $(VALUES_FILE) $(QUOTA_FILE) $(CTL_FIT_FILE)
	rm -rf $(CLUSTER_DIR)/.terraform
