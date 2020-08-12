#!/bin/sh

# abort script if a command fails
set -e

# -Check if we have an argument for a reference repository
# -If we have a reference repository, then copy changes to branch that is changed (optional)
# Run python scripts to generate nodetool output and cassandra.yaml
# Run antora

USE_REPOSITORY="${1}"

# @TODO: Consider changing the argument to be a path to a repository rather than a flag??
if [ "${USE_REPOSITORY}" = "true" ]
then
  cd "${REF_DIR}"/cassandra
  ref_branch=$(git branch | grep "\*" | cut -d' ' -f2)
  modified_files=$(git status | grep modified | tr -s ' ' | cut -d' ' -f2)

  cd "${BUILD_DIR}"/cassandra
  git checkout "${ref_branch}"
  git pull --rebase

  for file_itr in ${modified_files}
  do
    cp "${REF_DIR}"/cassandra/"${file_itr}" "${BUILD_DIR}"/cassandra/"${file_itr}"
  done
fi

# we are in build directory at this point
BRANCH_LIST="doc_redo_asciidoc doc_redo_asciidoc3.11"

cd "${BUILD_DIR}"/cassandra
for branch_name in ${BRANCH_LIST}
do
  ant jar
  cd doc
  git checkout "${branch_name}"
  python3 gen-nodetool-docs.py
  YAML_INPUT="${BUILD_DIR}"/cassandra/conf/cassandra.yaml
  YAML_OUTPUT="${BUILD_DIR}"/cassandra/doc/source/modules/cassandra/pages/configuration/cass_yaml_file.adoc
  #YAML_OUTPUT=doc/build_gen/"${branch_name}"/cass_yaml_file.adoc
  python3 convert_yaml_to_adoc.py ${YAML_INPUT} ${YAML_OUTPUT}

  # need to add,commit changes
  git add && git commit -m "${branch_name} nodetool changes
  #mkdir -p doc/build_gen/"${branch_name}"/
  #mv source/modules/cassandra/pages/tools/nodetool/ doc/build_gen/"${branch_name}"/
  #mv source/modules/cassandra/pages/configuration/cass_yaml_file.adoc doc/build_gen/"${branch_name}"/

  # After copy we will have:
  # ACTUALLY HAVE doc/doc/build_gen/<branch_name>
  # doc/build_gen/<branch_name>
  #  - nodetool/
  #  - cass_yaml_file.adoc
  ant realclean
done

# run antora
# You can set these variables from the command line.
ANTORAOPTS    = DOCSEARCH_ENABLED=true DOCSEARCH_ENGINE=lunr DOCSEARCH_INDEX_VERSION=latest
ANTORAYAML    = site.yml
ANTORACMD     = antora

$(ANTORAOPTS) $(ANTORACMD) $(ANTORAYAML)


# DOING IN DOCKERFILE
#git clone https://gitbox.apache.org/repos/asf/cassandra.git ${BUILD_DIR}/cassandra && \
#chmod -R a+rw ${BUILD_DIR}

# trunk will be the initial branch
#cd ${BUILD_DIR}/cassandra && git checkout doc_redo_asciidoc;
#ant jar;
#cd doc && python3 gen-nodetool-docs.py && python3 convert_yaml_to_adoc.py;

# and clean to get ready for the next branch
#ant realclean;

#cd ${BUILD_DIR}/cassandra && git checkout doc_redo_asciidoc3.11;
#ant jar;
#cd doc; python3 gen-nodetool-docs.py; python3 convert_yaml_to_adoc.py;
