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
POD_NAME="-c jenkins cassius-jenkins-0"
JOB_NAME="cassandra"
LOCAL_RESULTS_BASEDIR="${CASSANDRA_DIR}/build/ci/"

# pre-conditions
command -v kubectl >/dev/null 2>&1 || { echo >&2 "kubectl needs to be installed"; exit 1; }
command -v helm >/dev/null 2>&1 || { echo >&2 "helm needs to be installed"; exit 1; }
command -v gcloud >/dev/null 2>&1 || { echo >&2 "gcloud needs to be installed"; exit 1; }
command -v xz >/dev/null 2>&1 || { echo >&2 "xz needs to be installed"; exit 1; }
command -v curl >/dev/null 2>&1 || { echo >&2 "curl needs to be installed"; exit 1; }
command -v awk >/dev/null 2>&1 || { echo >&2 "awk needs to be installed"; exit 1; }

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -c|--kubeconfig)
            KUBECONFIG="$2"  # This sets the KUBECONFIG variable to the next argument.
            shift            # This shifts the arguments to the left, discarding the current argument and moving to the next one.
            shift            # This is an additional shift to move to the argument after the option value.
            ;;
        -x|--kubecontext)
            unset KUBECONFIG
            KUBECONTEXT="$2" # This sets the KUBECONTEXT variable to the next argument.
            shift            # This shifts the arguments to the left, discarding the current argument and moving to the next one.
            shift            # This is an additional shift to move to the argument after the option value.
            ;;
        -r|--repository)
            REPO_URL="$2"
            shift
            shift;;
        -b|--branch)
            REPO_BRANCH="$2"
            shift
            shift
            ;;
        -p|--profile)
            PROFILE="$2"
            shift
            shift
            ;;
        -e|--profile-custom-regexp)
            PROFILE_CUSTOM_REGEXP="$2"
            shift
            shift
            ;;
        -j|--jdk)
            JDK="$2"
            shift
            shift
            ;;
        -d|--dtest-repository)
            DTEST_REPO="$2"
            shift
            shift
            ;;
        -k|--dtest-branch)
            DTEST_BRANCH="$2"
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
            echo "  -c|--kubeconfig <file>              Specify the path to a Kubeconfig file."
            echo "  -x|--kubecontext <context>          Specify the Kubernetes context to use."
            echo "  -r|--repository <url>               Specify the repository URL."
            echo "  -b|--branch <branch>                Specify the repository branch."
            echo "  -p|--profile <value>                CI pipeline profile to run."
            echo "  -e|--profile-custom-regexp <value>  Regexp for stages when using the custom profile. See `testSteps` in Jenkinsfile for list of stages. Example: stress.*|jvm-dtest.*"
            echo "  -j|--jdk <value>                    Restrict to only builds using JDK."
            echo "  -d|--dtest-repository <value>       Specify the dtest repository branch."
            echo "  -k|--dtest-branch <value>           Specify the dtest repository branch."
            echo "  --tear-down <value>                 Tear down Jenkins Instance and Jenkins Operator, true or false."
            echo "  --help                              Show this help message."
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [ -z "$KUBECONFIG" ] && [ -z "$KUBECONTEXT" ]; then
  echo "Please provide either the path to the kubeconfig using -c|--kubeconfig option or the kubecontext using -x|--kubecontext option."
  exit 1
fi

# This sets the kubeconfig and kubecontext if provided
[ -n "$KUBECONFIG" ] && export KUBECONFIG="$KUBECONFIG"
[ -n "$KUBECONTEXT" ] && kubectl config use-context "$KUBECONTEXT"

kubectl get namespace ${KUBE_NS} >/dev/null 2>/dev/null || kubectl create namespace ${KUBE_NS}

if [ -n "$REPO_URL" ]; then echo "REPO_URL is: $REPO_URL"
else REPO_URL="https://github.com/apache/cassandra.git" ; echo "REPO_URL defaults to: $REPO_URL" ; fi

if [ -n "$REPO_BRANCH" ]; echo "REPO_BRANCH is: $REPO_BRANCH"
else REPO_BRANCH="trunk" ; echo "REPO_BRANCH defaults to: $REPO_BRANCH" ; fi

if ! [ -z "$PROFILE" ]; then echo "PROFILE is: $PROFILE"
else PROFILE="skinny" ; echo "PROFILE defaults to: $STAGES" ; fi

if ! [ -z "$PROFILE_CUSTOM_REGEXP" ]; then echo "PROFILE_CUSTOM_REGEXP is: $PROFILE_CUSTOM_REGEXP" ; fi

if ! [ -z "$JDK" ]; then echo "JDK is: $JDK" ; fi

if ! [ -z "$DTEST_REPO" ]; then echo "DTEST_REPO is: $DTEST_REPO"
else DTEST_REPO="https://github.com/apache/cassandra-dtest.git" ; echo "DTEST_REPO defaults to: $DTEST_REPO" ; fi

if ! [ -z "$DTEST_BRANCH" ]; then echo "DTEST_BRANCH is: $DTEST_BRANCH"
else DTEST_BRANCH="trunk" ; echo "DTEST_BRANCH defaults to: $DTEST_BRANCH" ; fi

if [ -n "$TEAR_DOWN" ]; echo "TEAR_DOWN is: $TEAR_DOWN"
else TEAR_DOWN=false ; echo "TEAR_DOWN defaults to: $TEAR_DOWN" ; fi


# Add Helm Jenkins Operator repository
echo "Adding Helm repository for Jenkins Operator..."
helm repo add jenkins https://charts.jenkins.io
helm repo update

# Install Jenkins Operator using Helm
echo -n "Installing Jenkins Operator..."
helm upgrade --install --namespace ${KUBE_NS} -f ${CASSANDRA_DIR}/.jenkins/k8s/jenkins-deployment.yaml cassius jenkins/jenkins --wait

spin='-\|/'
spinner=0

node_cleaner() {
  # aggressively remove pods and nodes that are agents and no longer running the agent
  while true ; do 
    for n in $(kubectl get nodes --no-headers -o "jsonpath={.items[*].metadata.name}" ) ; do 
      if echo $n | grep -q "agent" ; then
        bash ${CASSANDRA_DIR}/.jenkins/k8s/check_node.sh $n & 
      fi
    done
    wait
  done
}

echo "Backgrounding aggressive nodes cleaner task…"
export -f node_cleaner
node_cleaner &
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT


# Brand Jenkins with the nice Cassandra logo
kubectl exec --namespace ${KUBE_NS} ${POD_NAME} -- curl -sS https://svn.apache.org/repos/asf/comdev/project-logos/originals/cassandra-6.svg -o /var/jenkins_cache/war/images/svgs/logo.svg

# variables
ip="$(kubectl describe svc --namespace ${KUBE_NS} cassius-jenkins -c jenkins | grep 'LoadBalancer Ingress' | awk '{print $NF}')"
user="admin"
password="$(kubectl exec --namespace ${KUBE_NS} svc/cassius-jenkins -c jenkins -- /bin/cat /run/secrets/additional/chart-admin-password && echo)"
cookie_file=$(mktemp)
crumb="$(curl -sL --user ${user}:${password} --cookie-jar ${cookie_file} http://${ip}:8080/crumbIssuer/api/json | jq -r '.crumb')"
token="$(curl -sX POST --user ${user}:${password} --cookie-jar ${cookie_file} -H \"Jenkins-Crumb:${crumb}\" http://${ip}:8080/me/descriptorByName/jenkins.security.ApiTokenProperty/generateNewToken --data 'newTokenName=run-ci-script' | jq -r '.data.tokenValue')"
password=""

# FIXME
# - variables in check_node.sh

def wait_for_build_number() {
  # Wait for the build number to become available (querying the API)
  queue_url=$(echo "${response_headers}" | grep -i "Location" | awk -F ": " '{print $2}' | tr -d '\r')
  queue_item_number=$(basename "${queue_url}")
  # Construct the complete URL to retrieve build information
  queue_json_url="http://${ip}:8080/queue/item/${queue_item_number}/api/json"
  build_number=""
  while [ -z "${build_number}" ] || [ "${build_number}" == "null" ]; do
      build_info=$(curl  -sL --user ${user}:${token} "${queue_json_url}")
      build_number=$(echo "${build_info}" | jq -r '.executable.number')
      if [ -z "${build_number}" ] || [ "$build_number" == "null" ]; then
          spinner=$(( (spinner+1) %4 ))
          echo -ne "\rBuild number not available yet ${spin:${spinner}:1}"
          sleep 1
      fi
  done
  echo -e "\rBuild number ${build_number}                  "
  return ${build_number}
}

# Try a non-parameter build – required initially and after helm updates
response_headers=$(curl -siLX POST --user ${user}:${token} http://${ip}:8080/job/${JOB_NAME}/build 2>&1)
if echo "${response_headers}" | grep -q "201 Created" ; then
  echo -n "Initialising job with : \`curl -siLX POST --user ${user}:<token> http://${ip}:8080/job/${JOB_NAME}/build\`   "
  wait_for_build_number()
fi


# Trigger a new build and capture the response headers
post_args="--data-urlencode \"repository=${REPO_URL}\ --data-urlencode \"branch=${REPO_BRANCH}\"  --data-urlencode \"profile=${PROFILE}\" --data-urlencode \"profile_custom_regexp=${PROFILE_CUSTOM_REGEXP}\" --data-urlencode \"architecture=amd64\" --data-urlencode \"jdk=${JDK}\"  --data-urlencode \"dtest_repository=${DTEST_REPO}\" --data-urlencode \"dtest_branch=${DTEST_BRANCH}\""
echo -n "Trigger build with: curl -LX POST --user ${user}:<token> http://${ip}:8080/job/${JOB_NAME}/buildWithParameters ${post_args}   "
while 
    spinner=$(( (spinner+1) %4 ))
    echo -ne "\b${spin:${spinner}:1}"
    response_headers=$(curl -siLX POST --user ${user}:${token} http://${ip}:8080/job/${JOB_NAME}/buildWithParameters ${post_args} 2>&1)
    ! ( echo "${response_headers}" | grep -q "201 Created" )
do sleep 1 ; done
echo -e "\b\b"
build_number=wait_for_build_number()                "


# Running build…

# pod's WORKDIR is '\' so strip leading slash to avoid tar warnings
BUILD_DIR="var/jenkins_home/jobs/${JOB_NAME}/builds"
LATEST_BUILD=$(kubectl exec -it --namespace ${KUBE_NS} ${POD_NAME} -- /bin/bash -c "ls -t ${BUILD_DIR}  | head -n 1 | tr -d '\r'")
CONSOLE_LOG_FILE="${BUILD_DIR}/${build_number}/log"

# Define a function to check if "FINISHED" is in the consoleLog
check_finished() {
  kubectl exec --namespace ${KUBE_NS} ${POD_NAME} -- cat "${CONSOLE_LOG_FILE}" | grep -q "Finished: "
}

# Continuously check for "FINISHED"
while ( ! ( check_finished ) ) ; do
    spinner=$(( (spinner+1) %4 ))
    echo -ne "\rBuild in progress ${spin:${spinner}:1}"
    sleep 1
done
echo -e "\rBuild finished        "

# Download the results and logs
LOCAL_RESULTS_DIR="${LOCAL_RESULTS_BASEDIR}/${ip/./-}/${build_number}"

# clean remote individual junit xml files
echo "Downloading build artefacts…"
kubectl exec --namespace ${KUBE_NS} ${POD_NAME} -- rm -rf ${BUILD_DIR}/${build_number}/archive/test/output

if kubectl exec --namespace ${KUBE_NS} ${POD_NAME} -- test -d ${BUILD_DIR}/${build_number}/archive; then
  ( kubectl cp --retries 10 --namespace ${KUBE_NS} ${POD_NAME}:${BUILD_DIR}/${build_number}/archive ${LOCAL_RESULTS_DIR} ) &
fi
( kubectl cp --retries 10 --namespace ${KUBE_NS} ${POD_NAME}:${CONSOLE_LOG_FILE} ${LOCAL_RESULTS_DIR}/log ) &

# kill the node_cleaner process
( ps -fax | grep node_cleaner | grep -v grep | head -n1 | awk -F " " '{ print $2 }' | xargs kill -9 ) >/dev/null 2>/dev/null

# wait for backgrounded tasks
wait >/dev/null 2>/dev/null

# rename ci_summary.html and results_details.tar.xz for sharing
CI_SUMMARY_FILE="${LOCAL_RESULTS_DIR}/ci_summary_${ip/./-}_${build_number}.html"
CI_DETAILS_FILE="${LOCAL_RESULTS_DIR}/results_details_${ip/./-}_${build_number}.tar.xz"
mv ${LOCAL_RESULTS_DIR}/ci_summary.html ${CI_SUMMARY_FILE}
mv ${LOCAL_RESULTS_DIR}/results_details.tar.xz ${CI_DETAILS_FILE}

if [ "$TEAR_DOWN" != "false" ]; then
  # leaving the jenkins controller running is ~$10/day
  helm uninstall --namespace ${KUBE_NS} cassius jenkins/jenkins
  # really delete everything
  #kubectl delete "$(kubectl api-resources --namespaced=true --verbs=delete -o name | tr "\n" "," | sed -e 's/,$//')" --all
fi

# Print a summary
echo
if grep -q "BUILD FAILED" ${LOCAL_RESULTS_DIR}/log ; then
  echo "---"
  grep -A3 "BUILD FAILED" ${LOCAL_RESULTS_DIR}/log
fi
echo "---"
if test -e ${CI_SUMMARY_FILE} ; then
  echo "CI Summary in ${CI_SUMMARY_FILE/$(pwd)\//}, details in ${CI_DETAILS_FILE/$(pwd)\//}"
fi
echo "Logs in ${LOCAL_RESULTS_DIR/$(pwd)\//}/stage-logs/ and ${LOCAL_RESULTS_DIR/$(pwd)\//}/test/logs/"
echo "---"
grep "Finished: " ${LOCAL_RESULTS_DIR}/log
xz ${LOCAL_RESULTS_DIR}/log
