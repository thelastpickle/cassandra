# Installation

## Preparation

Following tools must be installed and available:

* kubectl
* gcloud
* helm

Add Helm repository for Jenkins

```
helm repo add jenkins https://charts.jenkins.io
helm repo update
```

## Infrastructure

```
# pick a cluster name that is identifiable to you
CLUSTER_NAME="$(whoami)_cassandra-jenkins"
# choose your closest (low-carbon) zone
ZONE="us-central1-c"

# cluster and controller node
gcloud container clusters create ${CLUSTER_NAME} --machine-type e2-standard-8 --disk-type=pd-ssd --num-nodes 1 --node-labels=cassandra.jenkins.controller=true --autoscaling-profile optimize-utilization --zone ${ZONE}

# small resource nodes
gcloud container node-pools create agents-small --cluster ${CLUSTER_NAME} --machine-type n2-highcpu-4 --disk-type=pd-ssd --enable-autoscaling --spot --num-nodes=0 --min-nodes=0 --max-nodes=50 --node-labels=cassandra.jenkins.agent=true,cassandra.jenkins.agent.small=true --zone ${ZONE}

# medium resource nodes
#  preference (by cost): n2-highcpu-8, c3-highcpu-8, n4-highcpu-8, n1-highcpu-16
gcloud container node-pools create agents-medium --cluster ${CLUSTER_NAME} --machine-type n2-highcpu-8 --disk-type=pd-ssd --enable-autoscaling --spot --num-nodes=0 --min-nodes=0 --max-nodes=100 --node-labels=cassandra.jenkins.agent=true,cassandra.jenkins.agent.medium=true --zone ${ZONE}

# large resource nodes
gcloud container node-pools create agents-large --cluster ${CLUSTER_NAME} --machine-type n2-standard-8 --disk-type=pd-ssd --enable-autoscaling --spot --num-nodes=0 --min-nodes=0 --max-nodes=160 --node-labels=cassandra.jenkins=cassandra.jenkins.agent=true,cassandra.jenkins.agent.large=true --zone ${ZONE}

# auth (and make default context)
gcloud container clusters get-credentials cassius --zone ${ZONE}

helm repo add jenkins https://charts.jenkins.io
helm repo update
helm upgrade --install -f jenkins-deployment.yaml cassius jenkins/jenkins --wait

# get the server's address
kubectl describe svc cassius-jenkins | grep 'LoadBalancer Ingress'

# get the jenkins' password
kubectl exec -it svc/cassius-jenkins -c jenkins -- /bin/cat /run/secrets/additional/chart-admin-password && echo

# open http://<server_address>:8080
```

This leaves the controller running, a single e2-standard-8 instance. All other node-pools downscale to zero.

### Local-only Access

If you want only local private access to Jenkins, do the following.

Comment these lines before running `heml upgrade …`
```
#  serviceType: LoadBalancer
#  ingress:
#    enabled: "true"
```
Run the heml upgrade and get the password as usual
```
helm upgrade --install -f values.yaml cassius jenkins/jenkins --wait

# get the jenkins' password
kubectl exec -it svc/cassius-jenkins -c jenkins -- /bin/cat /run/secrets/additional/chart-admin-password && echo

# port-forward 8080 to the private jenkins
kubectl port-forward svc/cassius-jenkins 8080:8080

# open http://localhost:8080
```

