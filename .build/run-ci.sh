#!/bin/bash
#
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
#

# variables, with defaults
[ "x${CASSANDRA_DIR}" != "x" ] || CASSANDRA_DIR="$(readlink -f $(dirname "$0")/..)"
[ "x${KUBECONFIG}" != "x" ]    || KUBECONFIG="${HOME}/.kube/config"
[ "x${KUBE_NS}" != "x" ]       || KUBE_NS="default" # FIXME – doesn't work in other namespaces :shrug:

# pre-conditions
command -v kubectl >/dev/null 2>&1 || { echo >&2 "kubectl needs to be installed"; exit 1; }
command -v helm >/dev/null 2>&1 || { echo >&2 "helm needs to be installed"; exit 1; }
command -v gcloud >/dev/null 2>&1 || { echo >&2 "gcloud needs to be installed"; exit 1; }
command -v xz >/dev/null 2>&1 || { echo >&2 "xz needs to be installed"; exit 1; }
command -v curl >/dev/null 2>&1 || { echo >&2 "curl needs to be installed"; exit 1; }

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -c|--kubeconfig)
            KUBECONFIG="$2"  # This sets the KUBECONFIG variable to the next argument.
            shift            # This shifts the arguments to the left, discarding the current argument and moving to the next one.
            shift            # This is an additional shift to move to the argument after the option value.
            ;;
        -ctx|--kubecontext)
            unset KUBECONFIG
            KUBECONTEXT="$2" # This sets the KUBECONTEXT variable to the next argument.
            shift            # This shifts the arguments to the left, discarding the current argument and moving to the next one.
            shift            # This is an additional shift to move to the argument after the option value.
            ;;
        -n|--nodes)
            NODES="$2"
            shift
            shift
            ;;
        --check-nodes)
            CHECK_NODES="true"
            shift
            ;;
        -t|--stages)
            STAGES="$2"
            shift
            shift
            ;;
        -u|--repo-url)
            REPO_URL="$2"
            shift
            shift;;
        -b|--repo-branch)
            REPO_BRANCH="$2"
            shift
            shift
            ;;
        --tear-down)
            TEAR_DOWN="$2"
            shift
            shift
            ;;
        -h|--help)
            echo "Usage: your_script.sh [options]"
            echo "Options:"
            echo "  -c, --kubeconfig <file>          Specify the path to a Kubeconfig file."
            echo "  -ctx, --kubecontext <context>    Specify the Kubernetes context to use."
            echo "  --check-nodes                     Check GKE nodes are configured correctly to use. Attempts to fix breakages. Does nothing else."
            echo "  -n|--nodes <value>               Number of k8s nodes to use (resizes the current context cluster to this)."
            echo "  -u|--repo-url <url>              Specify the repository URL."
            echo "  -b|--repo-branch <branch>        Specify the repository branch."
            echo "  -t|--stages <value>              Include test stage/s. '*' means everything."
            echo "  --tear-down <value>              Tear down Jenkins Instance and Jenkins Operator, true or false."
            echo "  --help                           Show this help message."
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [ -z "$KUBECONFIG" ] && [ -z "$KUBECONTEXT" ]; then
    echo "Please provide either the path to the kubeconfig using -c|--kubeconfig option or the kubecontext using -ctx|--kubecontext option."
    exit 1
fi

# This sets the kubeconfig and kubecontext if provided
if [ -n "$KUBECONFIG" ]; then
    export KUBECONFIG="$KUBECONFIG"
fi

if [ -n "$KUBECONTEXT" ]; then
    kubectl config use-context "$KUBECONTEXT"
fi

if ! kubectl get namespace ${KUBE_NS} >/dev/null 2>/dev/null ; then
    kubectl create namespace ${KUBE_NS}
fi

# FIXME – not working with a blank "" argument
if ! [ -z "$STAGES" ]; then
    # The variable is not empty, so it has a value
    echo "STAGES is not empty. Its value is: $STAGES"
else
    # The variable is empty, so assign a default value
    STAGES="*"
    echo "STAGES is empty. Assigning default value: $STAGES"
fi

if [ -n "$REPO_URL" ]; then
    # The variable is not empty, so it has a value
    echo "REPO_URL is not empty. Its value is: $REPO_URL"
else
    # The variable is empty, so assign a default value
    REPO_URL="https://github.com/infracloudio/cassandra.git"
    echo "REPO_URL is empty. Assigning default value: $REPO_URL"
fi

if [ -n "$REPO_BRANCH" ]; then
    # The variable is not empty, so it has a value
    echo "REPO_BRANCH is not empty. Its value is: $REPO_BRANCH"
else
    # The variable is empty, so assign a default value
    REPO_BRANCH="infracloud/cassandra-5.0"
    echo "REPO_BRANCH is empty. Assigning default value: $REPO_BRANCH"
fi

if [ -n "$TEAR_DOWN" ]; then
    # The variable is not empty, so it has a value
    echo "TEAR_DOWN is not empty. Its value is: $TEAR_DOWN"
else
    # The variable is empty, so assign a default value
    TEAR_DOWN=false
    echo "TEAR_DOWN is empty. Assigning default value: $TEAR_DOWN"
fi
echo

# FIXME multi-user not implemented. changing any env in jenkins-deployment.yaml redeploys the whole jenkins stack.
#       we just need to pass the 3 env variables through the yaml to seed job.
JOB_NAME="${USER}-cassandra"
gke_project=$(kubectl config current-context | awk -F"_" '{print $2}')
gke_zone=$(kubectl config current-context | awk -F"_" '{print $3}')
gke_cluster=$(kubectl config current-context | awk -F"_" '{print $4}')

clone_repo_on_gke_node_hack() {
    # TODO auto-scaling can only work if we listen to new nodes and perform the following actions on them (before they are used)
    echo "  checking $1…"
    gcloud compute ssh --zone=${gke_zone} --project=${gke_project} $1 --ssh-flag="-q" \
        -- "( test -d /home/jenkins/agent/workspace/${JOB_NAME} && git -C /home/jenkins/agent/workspace/${JOB_NAME} rev-parse ) \
                || ( rm -rf /home/jenkins/agent/workspace/${JOB_NAME} \
                    && sudo mkdir -p /home/jenkins/agent/workspace/ \
                    && sudo chmod -R ag+rwx /home/jenkins/agent/workspace/ \
                    && ( until git clone -b ${REPO_BRANCH} ${REPO_URL} /home/jenkins/agent/workspace/${JOB_NAME} ; do rm -rf /home/jenkins/agent/workspace/${JOB_NAME} ; done ) \
                    && git config --global --add safe.directory /home/jenkins/agent/workspace/${JOB_NAME} )"

    gcloud compute ssh --zone=${gke_zone} --project=${gke_project} $1 --ssh-flag="-q" \
        --  "sudo chmod -R ag+rwx /home/jenkins/agent/workspace/ ; \
             git -C /home/jenkins/agent/workspace/${JOB_NAME} clean -qxdff ; \
             git -C /home/jenkins/agent/workspace/${JOB_NAME} reset --hard HEAD ; \
             git -C /home/jenkins/agent/workspace/${JOB_NAME} remote set-url origin ${REPO_URL} ; \
             until git -C /home/jenkins/agent/workspace/${JOB_NAME} fetch origin ; do sleep 1 ; done ; \
             git -C /home/jenkins/agent/workspace/${JOB_NAME} checkout ${REPO_BRANCH} ; \
             git -C /home/jenkins/agent/workspace/${JOB_NAME} reset --hard origin/${REPO_BRANCH} ; \
             sudo mkdir -p /home/jenkins/.m2/repository ; \
             sudo chown -R 1000:1000 /home/jenkins "
}

clone_repo_on_gke_nodes_hack() {
    export JOB_NAME REPO_URL REPO_BRANCH gke_project gke_zone gke_cluster LC_ALL=""
    export -f clone_repo_on_gke_node_hack
    # take care – this swallows all std out|err , git clones may not be correct under errors
    kubectl get nodes | grep -v NAME | cut -d ' ' -f1 | xargs -P0 -I{} bash -c "clone_repo_on_gke_node_hack {}" >/dev/null 2>/dev/null
}

if [ -n "$CHECK_NODES" ] ; then
    echo "Checking $(kubectl get nodes | grep -v NAME | wc -l) gke nodes…"
    export JOB_NAME REPO_URL REPO_BRANCH gke_project gke_zone gke_cluster LC_ALL=""
    export -f clone_repo_on_gke_node_hack
    kubectl get nodes | grep -v NAME | cut -d ' ' -f1 | xargs -I{} bash -c "clone_repo_on_gke_node_hack {}"
    exit $? 
fi

if [ -n "$NODES" ]; then
    echo "Resizing GKE cluster to ${NODES} nodes"
    gcloud container clusters resize ${gke_cluster} --node-pool default-pool --zone=${gke_zone} --project=${gke_project} --num-nodes ${NODES} --quiet
    sleep 4
    # TODO – prompt to restore to original size when job is finished
fi

# HACK – make sure cassandra is git cloned on all nodes. only works on gke.
clone_repo_on_gke_nodes_hack &

sed -e "s|<REPO_BRANCH>|\"${REPO_BRANCH}\"|" \
    -e "s|<REPO_URL>|\"${REPO_URL}\"|" \
    -e "s|<JOB_NAME>|\"${JOB_NAME}\"|" \
    ${CASSANDRA_DIR}/.jenkins/k8s/jenkins-deployment.yaml > ${CASSANDRA_DIR}/build/jenkins-deployment.yaml

# Add Helm Jenkins Operator repository
echo "Adding Helm repository for Jenkins Operator..."
helm repo add --namespace ${KUBE_NS} jenkins https://raw.githubusercontent.com/jenkinsci/kubernetes-operator/master/chart

# Install Jenkins Operator using Helm
echo -n "Installing Jenkins Operator..."
helm upgrade --namespace ${KUBE_NS} --install jenkins-operator jenkins/jenkins-operator --set jenkins.enabled=false --set jenkins.backup.enabled=false --version 0.8.0-beta.2  >/dev/null
echo -e "\rInstalled Jenkins Operator       "

spin='-\|/'
spinner=0

# deploy jenkins Instance=
kubectl apply --namespace ${KUBE_NS} -f ${CASSANDRA_DIR}/build/jenkins-deployment.yaml

while ! ( kubectl --namespace ${KUBE_NS} get pods | grep seed-job-agent | grep " 1/1 " | grep -q " Running" ) ; do
        spinner=$(( (spinner+1) %4 ))
        echo -ne "\rJenkins installing ${spin:$spinner:1}"
        sleep 1
done
echo -en "\r"
kubectl rollout status deployment/jenkins-operator -n ${KUBE_NS}

# Brand Jenkins with the nice Cassandra logo
kubectl exec -n ${KUBE_NS} jenkins-jenkins -- curl -sS https://svn.apache.org/repos/asf/comdev/project-logos/originals/cassandra-6.svg -o /var/lib/jenkins/war/images/svgs/logo.svg

# cannot proceed past this point until clone_repo_on_gke_nodes_hack has completed
echo -n "Waiting for $(kubectl get nodes | grep -v NAME | wc -l) GKE nodes git cloning…"
wait >/dev/null 2>/dev/null
echo -e "\rGKE nodes ready                                                             "

# Port-forward the Jenkins service to access it locally
export RESULTS_DIR_BASE="${CASSANDRA_DIR}/build/ci-$(date +'%Y-%m-%d--%H%m%S')__"
port_forward_keepalive() {
    while ! ( compgen -G "${RESULTS_DIR_BASE}*/log*" >/dev/null ) ; do
        nohup kubectl port-forward svc/jenkins-operator-http-jenkins 8080:8080 >/dev/null 2>/dev/null
    done
}
port_forward_keepalive &
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT
echo "'kubectl port-forward svc/jenkins-operator-http-jenkins 8080:8080' running in background. Jenkins UI at http://127.0.0.1:8080 ( jenkins-operator:$(kubectl get secret jenkins-operator-credentials-jenkins -o jsonpath="{.data.password}" | base64 --decode) )"

# Trigger a new build and capture the response headers
echo -n "Trigger build with: curl -i -X POST http://127.0.0.1:8080/job/${JOB_NAME}/buildWithParameters -u jenkins-operator:<token> --data-urlencode \"STAGES=${STAGES}\"   "
while 
    spinner=$(( (spinner+1) %4 ))
    echo -ne "\b${spin:${spinner}:1}"
    TOKEN=$(kubectl  get secret jenkins-operator-credentials-jenkins -o jsonpath="{.data.token}" | base64 --decode)
    response_headers=$(curl -s -i -X POST http://127.0.0.1:8080/job/${JOB_NAME}/buildWithParameters -u jenkins-operator:$TOKEN --data-urlencode "STAGES=${STAGES}" 2>&1) 
    ! ( echo "${response_headers}" | grep -q "201 Created" )
do sleep 1 ; done
echo -e "\b\b"

# delete seed, as it's bound to each user
curl -X DELETE http://127.0.0.1:8080/job/jenkins-operator-job-dsl-seed/ -u jenkins-operator:${TOKEN}

# Wait for the build number to become available (querying the API)
queue_url=$(echo "${response_headers}" | grep -i "Location" | awk -F ": " '{print $2}' | tr -d '\r')
queue_item_number=$(basename "${queue_url}")
# Construct the complete URL to retrieve build information
queue_json_url="http://localhost:8080/queue/item/${queue_item_number}/api/json"
build_number=""
while [ -z "${build_number}" ] || [ "${build_number}" == "null" ]; do
    build_info=$(curl -s "${queue_json_url}" -u jenkins-operator:${TOKEN})
    build_number=$(echo "${build_info}" | jq -r '.executable.number')
    if [ -z "${build_number}" ] || [ "$build_number" == "null" ]; then
        spinner=$(( (spinner+1) %4 ))
        echo -ne "\rBuild number not available yet ${spin:${spinner}:1}"
        sleep 1
    fi
done
echo -e "\rBuild number ${build_number}                  "

# Running build…

# pod's WORKDIR is '\' so strip leading slash to avoid tar warnings
BUILD_DIR="var/lib/jenkins/jobs/${JOB_NAME}/builds"
POD_NAME=jenkins-jenkins
LATEST_BUILD=$(kubectl exec -it -n ${KUBE_NS} ${POD_NAME} -- /bin/bash -c "ls -t ${BUILD_DIR}  | head -n 1" | tr -d '\r')
CONSOLE_LOG_FILE="${BUILD_DIR}/${build_number}/log"

# Define a function to check if "FINISHED" is in the consoleLog
check_finished() {
    kubectl exec -n ${KUBE_NS} ${POD_NAME} -- cat "${CONSOLE_LOG_FILE}" | grep -q "Finished: "
}

# Continuously check for "FINISHED"
while ( ! ( check_finished ) ) ; do
    spinner=$(( (spinner+1) %4 ))
    echo -ne "\rBuild in progress ${spin:${spinner}:1}"
    sleep 1
done
echo -e "\rBuild finished        "

# Download the results and logs
RESULTS_DIR="${RESULTS_DIR_BASE}${build_number}"
mkdir -p ${RESULTS_DIR}

# remove individual junit xml files
kubectl exec -n ${KUBE_NS} ${POD_NAME} -- rm -rf ${BUILD_DIR}/${build_number}/archive/test/output

if kubectl exec -n ${KUBE_NS} ${POD_NAME} -- test -d ${BUILD_DIR}/${build_number}/archive; then
    kubectl cp -n ${KUBE_NS} ${POD_NAME}:${BUILD_DIR}/${build_number}/archive ${RESULTS_DIR}/archive
fi

# jenkins console log
kubectl cp -n ${KUBE_NS} ${POD_NAME}:${CONSOLE_LOG_FILE} ${RESULTS_DIR}/log

# kill the port-forwarding process
( ps -fax | grep port-forward | grep kubectl | head -n1 | awk -F " " '{ print $2 }' | xargs kill -9 ) >/dev/null 2>/dev/null

# wait for kubectl cp and xz and port_forward_keepalive
wait >/dev/null 2>/dev/null

if [ "$TEAR_DOWN" != "false" ]; then
    kubectl delete --namespace ${KUBE_NS} -f ${CASSANDRA_DIR}/build/jenkins-deployment.yaml
    helm uninstall --namespace ${KUBE_NS} jenkins-operator
    # really delete everything
    #kubectl delete "$(kubectl api-resources --namespaced=true --verbs=delete -o name | tr "\n" "," | sed -e 's/,$//')" --all
fi

# Print a summary
echo
if grep -q "BUILD FAILED" ${RESULTS_DIR}/log ; then
    echo "---"
    grep -A3 "BUILD FAILED" ${RESULTS_DIR}/log
fi
echo "---"
echo "File and logs in ${RESULTS_DIR/$(pwd)\//}"
if test -e ${RESULTS_DIR}/archive/test/html/index.html ; then
    echo "HTML test results in ${RESULTS_DIR/$(pwd)\//}/archive/test/html/index.html"
fi
echo "---"
grep "Finished: " ${RESULTS_DIR}/log
xz ${RESULTS_DIR}/log

