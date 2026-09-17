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
SHARED_DIR    := ../shared

ENV_FILE     := .gke-env
POOLS_FILE   := .gke-agent-pools.json
CTL_FILE     := .gke-controller-pool.json
VALUES_FILE  := .gke-jenkins-values.yaml
QUOTA_FILE   := .gke-vcpu-quota
CTL_FIT_FILE := .gke-controller-fit
CAPS_FILE    := $(CLUSTER_DIR)/spend-caps.auto.tfvars

QUOTA_ENV = set -a; . ./$(QUOTA_FILE); set +a;

TOFU     ?= tofu
HELMFILE ?= helmfile
KUBECTL  ?= kubectl

VALUES_BASE ?= $(PLATFORM_DIR)/jenkins-gke-overrides.yaml

TOFU_ARGS           ?=
SMOKE_ARGS          ?=
RUN_CI_ARGS         ?=
SPEND_ARGS          ?=
CAPS_ARGS           ?=
CONTROLLER_FIT_ARGS ?=

.PHONY: help init fmt validate plan apply kubeconfig env quota caps caps-check spend helm-diff \
        storageclass externaldns jenkins platform smoke test destroy clean

.NOTPARALLEL:

help:
	@echo "Layer 1, OpenTofu, owns GCP:"
	@echo "  init         install providers"
	@echo "  plan         show what would change            TOFU_ARGS='-var ...'"
	@echo "  apply        create or update the cluster"
	@echo "  destroy      delete everything layer 1 made"
	@echo
	@echo "Layer 2, Helmfile, kubectl and .build/run-ci:"
	@echo "  kubeconfig   add a kubectl context named after the cluster"
	@echo "  storageclass create the pd-ssd storage class the controller's volume uses"
	@echo "  externaldns  install external-dns, when layer 1 made its service account"
	@echo "  jenkins      install Jenkins, through run-ci   RUN_CI_ARGS='-p pre-commit'"
	@echo "                                                VALUES_BASE=~/.cassandra-ci/<site>-overrides.yaml"
	@echo "  platform     storageclass, externaldns, then jenkins"
	@echo
	@echo "Layer 3, assertions:"
	@echo "  smoke        check a deployed cluster          SMOKE_ARGS=--build RUN_CI_ARGS='-b trunk'"
	@echo
	@echo "No cluster and no credentials needed:"
	@echo "  fmt          reformat the HCL the way tofu fmt wants it"
	@echo "  validate     tofu fmt -check and tofu validate"
	@echo "  test         validate and mocked provisioning tests, plus variables, shared,"
	@echo "               check-pool-fit, vcpu-quota, controller-fit,"
	@echo "               spend-caps, spend-guard, build-jenkins-values and cpus"
	@echo "               regression tests"
	@echo
	@echo "Needs an applied layer 1, because it reads the cluster's own state:"
	@echo "  env          write $(ENV_FILE), $(POOLS_FILE) and $(CTL_FILE) from tofu output"
	@echo "  quota        show how many nodes the project's quotas allow, and whether the"
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
	$(TOFU) fmt -recursive $(CLUSTER_DIR) tests

validate:
	$(TOFU) fmt -check -diff -recursive $(CLUSTER_DIR)
	$(TOFU) fmt -check -diff tests
	cd $(CLUSTER_DIR) && $(TOFU) init -backend=false -input=false >/dev/null && $(TOFU) validate

test: validate
	TOFU="$(TOFU)" bash ./variables-test.sh
	TOFU="$(TOFU)" bash ./pool-sizing-test.sh
	$(TOFU) -chdir=$(CLUSTER_DIR) test -filter=tests/provisioning.tftest.hcl
	$(SHARED_DIR)/shared-test.sh
	$(SMOKE_DIR)/check-pool-fit-test.sh
	bash $(SMOKE_DIR)/smoke-test-test.sh
	./vcpu-quota-test.sh
	TOFU="$(TOFU)" python3 vcpu-quota-test.py
	python3 make-env-test.py
	./controller-fit-test.sh
	python3 resource-fit-test.py
	./spend-caps-test.sh
	TOFU="$(TOFU)" python3 spend-caps-write-test.py
	./spend-guard-test.sh
	python3 spend-guard-runtime-test.py
	$(PLATFORM_DIR)/build-jenkins-values-test.sh
	python3 $(PLATFORM_DIR)/build-jenkins-values-test.py
	../cpus-test.sh

env:
	@$(TOFU) -chdir=$(CLUSTER_DIR) output -json cluster_name >/dev/null 2>&1 || { \
	    echo "Layer 1 has not been applied, so there is nothing to read: these values come from the"; \
	    echo "cluster's own state, not from the configuration."; \
	    echo; \
	    echo "    make plan     # what would be created, and the ceilings the project allows"; \
	    echo "    make apply    # create it"; \
	    echo; \
	    exit 1; \
	}
	@set -eu; work=$$(mktemp -d "./.env.XXXXXX"); trap 'rm -rf "$$work"' EXIT; \
	$(TOFU) -chdir=$(CLUSTER_DIR) output -raw environment > "$$work/env"; \
	$(TOFU) -chdir=$(CLUSTER_DIR) output -json agent_node_pools > "$$work/pools"; \
	$(TOFU) -chdir=$(CLUSTER_DIR) output -json controller_node_pool > "$$work/controller"; \
	printf "\nexport GKE_AGENT_POOLS_FILE='%s'\n" '$(abspath $(POOLS_FILE))' >> "$$work/env"; \
	mv "$$work/pools" "$(POOLS_FILE)"; mv "$$work/controller" "$(CTL_FILE)"; mv "$$work/env" "$(ENV_FILE)"
	@echo "Wrote $(ENV_FILE), $(POOLS_FILE) and $(CTL_FILE)"

quota: env
	@set -a; . ./$(ENV_FILE); set +a; \
	ceiling="$$($(TOFU) -chdir=$(CLUSTER_DIR) output -json agent_node_ceiling 2>/dev/null \
	    | jq -r '.pod_range.nodes // empty' 2>/dev/null || true)"; \
	./vcpu-quota.py --pools "$${GKE_AGENT_POOLS_FILE}" --controller-pool $(CTL_FILE) \
	    $${ceiling:+--pod-range-node-ceiling $${ceiling}} \
	    --check --exports-to $(QUOTA_FILE) \
	    || { rm -f $(QUOTA_FILE); exit 1; }
	@echo
	@set -a; . ./$(ENV_FILE); set +a; \
	./controller-fit.py --pools $(POOLS_FILE) --controller $(CTL_FILE) \
	    --deployment $(CASSANDRA_DIR)/.jenkins/k8s/jenkins-deployment.yaml \
	    --overrides $(VALUES_BASE) \
	    --check --exports-to $(CTL_FIT_FILE) $(CONTROLLER_FIT_ARGS)

kubeconfig: env
	@set -a; . ./$(ENV_FILE); set +a; \
	gcloud container clusters get-credentials "$${GKE_CLUSTER_NAME}" \
	    --region "$${GOOGLE_REGION}" --project "$${GOOGLE_PROJECT}"

caps:
	@nodes=''; \
	if [ -f ./$(QUOTA_FILE) ]; then . ./$(QUOTA_FILE); nodes="$${GKE_MAX_AGENT_NODES}"; fi; \
	GKE_MAX_AGENT_NODES="$${nodes}" ./spend-caps.py --prompt $(CAPS_ARGS)

# Asked once, and never again once the file exists, whatever is in it: `none` to all three is an answer.
caps-check:
	@if [ -f $(CAPS_FILE) ]; then ./spend-caps.py --check --out "$(CAPS_FILE)"; exit $$?; fi; \
	if [ -t 0 ]; then \
	    echo "No spend cap has been set for this cluster yet."; \
	    echo "These agent pools can hold hundreds of nodes, and no project quota is a limit on money."; \
	    echo "Three questions follow, and \`none\` is an answer to any of them."; \
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
	if [ -z "$${GKE_SPEND_CAPS_SECRET}" ]; then \
	    echo "No spend cap is set for this cluster, so nothing watches what it spends and there is"; \
	    echo "nothing to report. \`make caps\` asks for the caps; \`make apply\` then creates the guard."; \
	    exit 1; \
	fi; \
	./spend-guard.py --report $(SPEND_ARGS)

# helmfile apply diffs first, and helm's error for the missing plugin reads as a fault in helmfile.yaml.
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

storageclass: env
	@$(KUBECTL) --context "$$(set -a; . ./$(ENV_FILE); set +a; \
	    echo gke_$${GOOGLE_PROJECT}_$${GOOGLE_REGION}_$${GKE_CLUSTER_NAME})" \
	    apply -f $(PLATFORM_DIR)/storageclass-pd-ssd.yaml

externaldns: helm-diff env
	@set -a; . ./$(ENV_FILE); set +a; \
	if [ -z "$${GKE_EXTERNAL_DNS_SERVICE_ACCOUNT}" ]; then \
	    echo "enable_external_dns is false, so external-dns is not installed, and is removed if it was."; \
	    echo "A cluster with no public name needs none; see var.enable_external_dns."; \
	fi; \
	$(HELMFILE) --kube-context "gke_$${GOOGLE_PROJECT}_$${GOOGLE_REGION}_$${GKE_CLUSTER_NAME}" --file $(PLATFORM_DIR)/helmfile.yaml apply

# The certificate's state is read live rather than taken from layer 1's state, because it is a fact that changes
# without any apply: it becomes ACTIVE when the zone is delegated, which is a person's action at a registrar.
jenkins: env quota
	@set -a; . ./$(ENV_FILE); set +a; $(QUOTA_ENV) \
	certmap=''; \
	if [ -n "$${GKE_JENKINS_HOSTNAME}" ] && [ -n "$${GKE_TLS_CERTIFICATE_NAME}" ]; then \
	    state="$$(gcloud certificate-manager certificates describe "$${GKE_TLS_CERTIFICATE_NAME}" \
	        --project "$${GOOGLE_PROJECT}" --format='value(managed.state)' 2>/dev/null)"; \
	    if [ "$${state}" = 'ACTIVE' ]; then \
	        certmap="$${GKE_TLS_CERTIFICATE_MAP}"; \
	    else \
	        echo "The certificate for $${GKE_JENKINS_HOSTNAME} is $${state:-unreadable}, not ACTIVE."; \
	        echo "Jenkins stays on plain HTTP. Certificate Manager issues it once the zone is delegated;"; \
	        echo "\`tofu -chdir=$(CLUSTER_DIR) output dns_nameservers\` prints what to set, and"; \
	        echo "README.md has the rest. Re-run \`make jenkins\` after that."; \
	    fi; \
	fi; \
	$(PLATFORM_DIR)/build-jenkins-values.py \
	    --base $(VALUES_BASE) \
	    --deployment $(CASSANDRA_DIR)/.jenkins/k8s/jenkins-deployment.yaml \
	    --hostname "$${GKE_JENKINS_HOSTNAME}" \
	    $${certmap:+--certificate-map $${certmap}} \
	    --container-cap "$${GKE_MAX_AGENT_NODES}" \
	    --pools $(POOLS_FILE) \
	    --out $(VALUES_FILE) || exit $$?; \
	values=".jenkins/k8s/gke/$(VALUES_FILE)"; \
	context="gke_$${GOOGLE_PROJECT}_$${GOOGLE_REGION}_$${GKE_CLUSTER_NAME}"; \
	cd $(CASSANDRA_DIR) && .build/run-ci --only-setup \
	    --kubecontext "$${context}" \
	    --values-override "$${values}" $(RUN_CI_ARGS)

platform: storageclass externaldns jenkins

# RUN_CI_ARGS is exported rather than passed as argv: smoke-test.sh's flags and run-ci's are different sets, and
# one argv cannot carry both.
smoke: env quota
	@set -a; . ./$(ENV_FILE); set +a; $(QUOTA_ENV) \
	RUN_CI_ARGS='$(RUN_CI_ARGS)' $(SMOKE_DIR)/smoke-test.sh $(SMOKE_ARGS)

clean:
	rm -f $(ENV_FILE) $(POOLS_FILE) $(CTL_FILE) $(VALUES_FILE) $(QUOTA_FILE) $(CTL_FIT_FILE)
	rm -rf $(CLUSTER_DIR)/.terraform
