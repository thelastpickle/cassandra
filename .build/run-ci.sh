#!/bin/bash

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
            KUBECONTEXT="$2" # This sets the KUBECONTEXT variable to the next argument.
            shift            # This shifts the arguments to the left, discarding the current argument and moving to the next one.
            shift            # This is an additional shift to move to the argument after the option value.
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

# Install Helm

# Check if Helm is already installed
if ! command -v helm &>/dev/null; then
    echo "Helm not found. Installing Helm..."
    curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 # enhance without downloading the script
    chmod 700 get_helm.sh
    ./get_helm.sh
else
    echo "Helm is already installed."
fi

# Add Helm Jenkins Operator repository
echo "Adding Helm repository for Jenkins Operator..."
helm repo add jenkins https://raw.githubusercontent.com/jenkinsci/kubernetes-operator/master/chart

# Install Jenkins Operator using Helm
echo "Installing Jenkins Operator..."
helm upgrade --install jenkins-operator jenkins/jenkins-operator --set jenkins.enabled=false --set jenkins.backup.enabled=false --version 0.8.0-beta.2 

echo "Jenkins Operator installed successfully!" # condition to check if above command was success

# deploy jenkins Instance TODO jenkins file parameter
kubectl apply -f jenkins-deployment.yaml

# TODO wait for job pods
#kubectl rollout wait pod/ [--for=<condition>] 

#sleep 120

# get job pod name - TODO parametrise the job name
#jobPodName=$(kubectl get po | grep k8sagent-e2e | cut -d " " -f1)

# get the file - TODO parameterize the path to fetch path
#kubectl exec -n default $jobPodName -- tar cf - /home/jenkins/agent/workspace/k8s-e2e | tar xf - -C .
