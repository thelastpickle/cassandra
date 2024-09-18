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
# checks that a k8s pod, as specified in $1, is still usable, removing it if not.
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
