#!/bin/bash
#

if kubectl describe node $1 | grep -q "\(agent-dind\|cassius\)" || ( sleep 10 &&  kubectl describe node $1 | grep -q "\(agent-dind\|cassius\)" ) || ( sleep 10 &&  kubectl describe node $1 | grep -q "\(agent-dind\|cassius\)" ) || ( sleep 10 &&  kubectl describe node $1 | grep -q "\(agent-dind\|cassius\)" ) || ( sleep 10 &&  kubectl describe node $1 | grep -q "\(agent-dind\|cassius\)" ) || ( sleep 10 &&  kubectl describe node $1 | grep -q "\(agent-dind\|cassius\)" ) ; then
 echo "node $1 in use ($(kubectl describe node $1 | grep "\(agent-dind\|cassius\)"))"
else
 echo "deleting dangling $1 …"
 kubectl cordon $1  2>/dev/null
 kubectl drain --ignore-daemonsets $1 2>/dev/null 
 kubectl delete node $1 2>/dev/null

 # TODO – also implement for eks and azure
 gcloud compute instances delete --quiet --project cassandra-oss --zone europe-north1-a $1  2>/dev/null
fi
