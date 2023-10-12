#!/bin/bash
KUBE_NS=default
POD_NAME=jenkins-example
BUILD_DIR=/var/lib/jenkins/jobs/k8s-e2e/builds
LATEST_BUILD=$(kubectl exec -it jenkins-example -- /bin/bash -c "ls -t $BUILD_DIR | head -n 1" | tr -d '\r')

CONSOLE_LOG_FILE="$BUILD_DIR/$LATEST_BUILD/log"

# Define a function to check if "FINISHED" is in the consoleLog
check_finished() {
    kubectl exec -n default jenkins-example -- cat "$CONSOLE_LOG_FILE" | grep -q "Finished"
}

# Continuously check for "FINISHED"
while true; do
    if check_finished; then
        echo "Build has finished."
        break
    else
        echo "Build is still in progress."
    fi
    sleep 5  # Adjust the sleep interval as needed
done

LOCAL_DIR=.
mkdir -p $LOCAL_DIR/$LATEST_BUILD
kubectl cp -n $KUBE_NS $POD_NAME:$BUILD_DIR/$LATEST_BUILD/log $LOCAL_DIR/$LATEST_BUILD/log
kubectl cp -n $KUBE_NS $POD_NAME:$BUILD_DIR/$LATEST_BUILD/junitResult.xml $LOCAL_DIR/$LATEST_BUILD/junitResult.xml