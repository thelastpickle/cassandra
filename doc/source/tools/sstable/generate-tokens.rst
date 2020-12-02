.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

generate-tokens
------------

Pre-generates tokens for a datacenter with the given number of nodes using the token allocation algorithm. Useful in edge-cases when generated tokens needs to be known in advance of bootstrapping nodes. In nearly all cases it is best to just let the bootstrapping nodes automatically generate their own tokens.
ref: https://issues.apache.org/jira/browse/CASSANDRA-16205


Usage
^^^^^
generate-tokens -n NODES -t TOKENS --rf REPLICATION_FACTOR [--partitioner PARTITIONER] [--racks RACK_NODE_COUNTS]


===================================                   ================================================================================
    -n,--nodes <arg>                                  Number of nodes.
    -t,--tokens <arg>                                 Number of tokens/vnodes per node.
    --rf <arg>                                        Replication factor.
    --partitioner <arg>                               Database partitioner, either Murmur3Partitioner or RandomPartitioner.
    --racks <arg>                                     Number of nodes per rack, separated by commas. Must add up to the total node count. For example, 'generate-tokens -n 30 -t 8 --rf 3 --racks 10,10,10' will generate tokens for three racks of 10 nodes each.
===================================                   ================================================================================


This command, if used, is expected to be run before the Cassandra node is first started. The output from the command is used to configure the nodes `num_tokens` setting in the `cassandra.yaml`
