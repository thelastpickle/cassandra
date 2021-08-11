FROM ubuntu:18.04
# Set up non-root user, 'build', with default uid:gid
# This allows passing --build-arg to use local host user's uid:gid:
#   $ docker-compose build \
#     --build-arg UID=$(id -u) \
#     --build-arg GID=$(id -g) \
#     cassandra-website
ARG UID=1000
ARG GID=1000
ARG BUILD_USER=build
RUN echo "Setting up user 'build' with UID=${UID} GID=${GID}"
RUN groupadd --gid ${GID} --non-unique ${BUILD_USER}
RUN useradd --create-home --shell /bin/bash \
    --uid ${UID} --gid ${GID} --non-unique ${BUILD_USER}
# INSTALL wget, python3, java11, and other tools required to build the docs
RUN apt-get update && \
    apt-get install -y \
        wget \
        gpg \
        python3 \
        openjdk-11-jdk \
        git \
        make \
        ant \
        ant-optional

# INSTALL nodejs and nvm
ARG NODE_PACKAGE="node-v12.16.2-linux-x64.tar.gz"
RUN wget https://nodejs.org/download/release/v12.16.2/${NODE_PACKAGE} && \
    tar -C /usr/local --strip-components 1 -xzf ${NODE_PACKAGE} && \
    rm ${NODE_PACKAGE}

# Use npm to install Antora globally
# and antora-lunr for site search
RUN npm i -g @antora/cli@2.3 @antora/site-generator-default@2.3
RUN npm i -g antora-lunr antora-site-generator-lunr

ENV BUILD_DIR "/antora"
ENV REF_DIR "/reference"
ENV BUILD_MODE "preview"
ENV GIT_EMAIL_ADDRESS ""
ENV GIT_USER_NAME ""
ENV GPG_KEY_PATH ""
ENV GENERATE_NODETOOL_AND_CONFIG_DOCS "true"
ENV WEB_SERVER_PORT "5000"

# Setup directories for building the docs
#  Give the build user rw access to everything in the build directory,
#   neccessary for the ASF 'websites' jenkins agent (which can't chown)
# *************************
# CHANGE THIS TO REAL REPO AFTER TESTING!!!!
# *************************
RUN mkdir -p ${BUILD_DIR}/cassandra && \
    git clone https://github.com/polandll/cassandra.git ${BUILD_DIR}/cassandra && \
    mkdir -p ${BUILD_DIR}/cassandra/doc/build_gen && \
    chmod -R a+rw ${BUILD_DIR}/cassandra && \
    mkdir -p ${REF_DIR}/cassandra && \
    chmod -R a+rw ${REF_DIR}/cassandra

# set the USER to `build`, the working directory to /antora,
# and copy the shell script and run

EXPOSE ${WEB_SERVER_PORT}/tcp

USER ${BUILD_USER}
WORKDIR ${BUILD_DIR}
COPY docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]
CMD [""]

