#!/bin/sh

# abort script if a command fails
set -e

# -Check if we have an argument for a reference repository
# -If we have a reference repository, then copy changes to branch that is changed (optional)
# -Run python scripts to generate nodetool output and cassandra.yaml
# Run antora

USE_REPOSITORY="${1}"
export CASSANDRA_USE_JDK11=true

# @TODO: Consider changing the argument to be a path to a repository rather than a flag??

echo "If this is your first time running this container script, go get a coffee - it's going to take awhile."
if [ "${USE_REPOSITORY}" = "preview" ]
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

echo "checking out branch"
for branch_name in ${BRANCH_LIST}
do
  git checkout "${branch_name}"
  echo "building jar files"
  ant jar
  cd doc
  # generate the nodetool docs
  echo "generate nodetool"
  python3 gen-nodetool-docs.py
  # generate cassandra.yaml doc file
  echo "generate cassandra.yaml"
  YAML_INPUT="${BUILD_DIR}"/cassandra/conf/cassandra.yaml
  YAML_OUTPUT="${BUILD_DIR}"/cassandra/doc/source/modules/cassandra/pages/configuration/cass_yaml_file.adoc
  python3 convert_yaml_to_adoc.py ${YAML_INPUT} ${YAML_OUTPUT}

  # need to add,commit changes before changing branches
  git add . && git commit -m "Generated nodetool and configuration documentation for ${branch_name}"
  echo "clean up"
  ant realclean
done

# run antora
# You can set these variables from the command line.
ANTORAOPTS = DOCSEARCH_ENABLED=true DOCSEARCH_ENGINE=lunr DOCSEARCH_INDEX_VERSION=latest
ANTORAYAML = site.yml
ANTORACMD  = antora

DOCSEARCH_ENABLED=true DOCSEARCH_ENGINE=lunr DOCSEARCH_INDEX_VERSION=latest antora site.yml
#${ANTORAOPTS} ${ANTORACMD} ${ANTORAYAML}

