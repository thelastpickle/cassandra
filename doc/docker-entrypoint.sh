#!/bin/bash

# Abort script if a command fails
set -e

usage() {
    cat << EOF
This container generates the Apache Cassandra documentation.

Usage:
$ docker run -i -t -p $WEB_SERVER_PORT:$WEB_SERVER_PORT -v <PATH_TO_CASSANDRA_REPOSITORY_LOCAL_COPY>:/reference/cassandra cassandra-docs:latest [OPTIONS] <GIT_EMAIL_ADDRESS> <GIT_USER_NAME>

Options
 -b     Build mode; either "preview" or "production". Defaults to "preview".
 -k     Path to GPG private key to perform signed commits.
 -n     Skip nodetool and Cassandra YAML document generation in preview mode. By default nodetool and Cassandra YAML
          configuration documentation is generated. If this option is used the documentation will be missing in preview
          mode. This option has no effect when running in "production" mode.
 -h     Help and usage.
EOF
    exit 2
}

while getopts "b:k:nh" opt_flag; do
  case $opt_flag in
    b)
        BUILD_MODE=$OPTARG
        ;;
    k)
        GPG_KEY_PATH=$OPTARG
        ;;
    n)
        GENERATE_NODETOOL_AND_CONFIG_DOCS="false"
        ;;
    h)
        usage
        ;;
  esac
done

shift $(($OPTIND - 1))

if [ "$#" -eq 0 ]
then
    usage
fi

GIT_EMAIL_ADDRESS=${1}
GIT_USER_NAME=${2}

# Setup git and ssh
git config --global user.email "${GIT_EMAIL_ADDRESS}"
git config --global user.name "${GIT_USER_NAME}"

if [ -n "${GPG_KEY_PATH}" ]
then
  gpg --import "${GPG_KEY_PATH}"
  GPG_KEY_ID=$(gpg --list-secret-keys --keyid-format LONG | grep sec  | tr -s ' ' | cut -d' ' -f2 | cut -d'/' -f2)
  git config --global user.signingkey "${GPG_KEY_ID}"
  git config --global gpg.program gpg
  git config --global commit.gpgsign true
  export GPG_TTY=$(tty)
fi

echo
echo "Running in '${BUILD_MODE}' mode!"
echo

export CASSANDRA_USE_JDK11=true

echo "If this is your first time running this container script, go get a coffee - it's going to take awhile."
if [ "${BUILD_MODE}" = "preview" ]
then
  cd "${REF_DIR}"/cassandra
  ref_branch=$(git branch | grep "\*" | cut -d' ' -f2)

  echo "Getting list of cleanly modified files in your repository."
  if ! modified_files=$(git status | grep -v "both modified" | grep "modified" | tr -s ' ' | cut -d' ' -f2)
  then
    echo "Error quering git repository. Aborting."
  fi

  cd "${BUILD_DIR}"/cassandra
  git checkout "${ref_branch}"
  git pull --rebase

  for file_itr in ${modified_files}
  do
    echo "Copying modified file '${file_itr}' to '${BUILD_DIR}/cassandra/${file_itr}'."
    cp "${REF_DIR}"/cassandra/"${file_itr}" "${BUILD_DIR}"/cassandra/"${file_itr}"
  done
fi

# we are in build directory at this point
BRANCH_LIST="doc_redo_asciidoc doc_redo_asciidoc3.11"

cd "${BUILD_DIR}"/cassandra

if [ "${GENERATE_NODETOOL_AND_CONFIG_DOCS}" = "true" ] || [ "${BUILD_MODE}" = "production" ]
then
  for branch_name in ${BRANCH_LIST}
  do
    echo "Checking out branch '${branch_name}'"
    git checkout "${branch_name}"

    echo "Building JAR files"
    ant jar

    # change into doc directory and push the current directory to the stack
    pushd doc
    # generate the nodetool docs
    echo "Generating Cassandra nodetool documentation."
    python3 gen-nodetool-docs.py

    # generate cassandra.yaml doc file
    echo "Generating Cassandra configuration documentation."
    YAML_INPUT="${BUILD_DIR}"/cassandra/conf/cassandra.yaml
    YAML_OUTPUT="${BUILD_DIR}"/cassandra/doc/source/modules/cassandra/pages/configuration/cass_yaml_file.adoc
    python3 convert_yaml_to_adoc.py "${YAML_INPUT}" "${YAML_OUTPUT}"

    # need to add,commit changes before changing branches
    git add .
    git commit -m "Generated nodetool and configuration documention for ${branch_name}."

    # change back to previous directory on the stack
    popd
    ant realclean
  done
else
  echo "Skipping the generation of Cassandra nodetool and configuration (YAML) documentation."
fi

# *************************
# CHANGE THIS TO trunk AFTER TESTING!!!!
# *************************
# Antora is run only from one branch (trunk)
git checkout doc_redo_asciidoc
cd doc

# run antora
echo "Building the docs site with antora."
export DOCSEARCH_ENABLED=true
export DOCSEARCH_ENGINE=lunr
export NODE_PATH="$(npm -g root)"
export DOCSEARCH_INDEX_VERSION=latest
antora --generator antora-site-generator-lunr site.yml

if [ "${BUILD_MODE}" = "preview" ]
then
  echo "Starting webserver."
  python3 -m http.server "${WEB_SERVER_PORT}"
fi

