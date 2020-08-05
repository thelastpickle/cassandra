#!/bin/sh

# abort script if a command fails
set -e
 
# DOING IN DOCKERFILE
#git clone https://gitbox.apache.org/repos/asf/cassandra.git ${BUILD_DIR}/cassandra && \
#chmod -R a+rw ${BUILD_DIR}

# trunk will be the initial branch
cd ${BUILD_DIR}/cassandra; 
git checkout doc_redo_asciidoc; 
ant jar;
cd doc; python3 gen-nodetool-docs.py; python3 convert_yaml_to_adoc.py;

# and clean to get ready for the next branch
ant realclean;

#cd ${BUILD_DIR}/cassandra;
#git checkout doc_redo_asciidoc3.11;
#ant jar;
#cd doc; python3 gen-nodetool-docs.py; python3 convert_yaml_to_adoc.py;



