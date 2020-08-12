FROM ubuntu:18.04
# Set up non-root user, 'build', with default uid:gid
# This allows passing --build-arg to use local host user's uid:gid:
#   $ docker-compose build \
#     --build-arg UID=$(id -u) \
#     --build-arg GID=$(id -g) \
#     cassandra-website
ARG UID=1000
ARG GID=1000
RUN echo "Setting up user 'build' with UID=${UID} GID=${GID}"
RUN groupadd --gid $GID --non-unique build
RUN useradd --create-home --shell /bin/bash \
    --uid $UID --gid $GID --non-unique build
# INSTALL wget, python3, java11, and other tools required to build the docs
RUN apt-get update && \
    apt-get install -y \
        wget \
        python3 \
        openjdk-8-jdk \
        git \
        make \
        ant \
        ant-optional
# INSTALL nodejs and nvm
ARG NODE_PACKAGE="node-v12.16.2-linux-x64.tar.gz"
RUN wget https://nodejs.org/download/release/v12.16.2/${NODE_PACKAGE} && \
    tar -C /usr/local --strip-components 1 -xzf ${NODE_PACKAGE} && \
    rm ${NODE_PACKAGE}
# INSTALL yarn
#RUN npm install yarn -g
# Use npm to install Antora globally
# and antora-lunr for site search
RUN npm i -g @antora/cli@2.3 @antora/site-generator-default@2.3 
RUN npm i -g antora-lunr
ENV BUILD_DIR="/antora"
ENV REF_DIR="/reference"
ENV USE_REPOSITORY="false"
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
USER build
WORKDIR $BUILD_DIR
COPY docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]
CMD [""]

