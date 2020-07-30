# Dockerfile for building the Cassandra documentation.
# If wanting to regenerate the documentation from scratch,
# run `ant realclean` from the root directory of this project.

FROM python:2.7

WORKDIR /usr/src/code

# LLP 07.28.2020 CHANGE THIS TO ANTORA
# install nodejs and nvm
# install antora
# install any antora extensions

# FROM ANTORA DOCKER (https://gitlab.com/antora/docker-antora.git)

FROM node:10-alpine

ENV NODE_PATH /usr/local/share/.config/yarn/global/node_modules

RUN apk --no-cache add curl findutils jq \
    && yarn global add --ignore-optional --silent @antora/cli@latest @antora/site-generator-default@latest \
    && rm -rf $(yarn cache dir)/* \
    && find $(yarn global dir)/node_modules/asciidoctor.js/dist/* -maxdepth 0 -not -name node -exec rm -rf {} \; \
    && find $(yarn global dir)/node_modules/handlebars/dist/* -maxdepth 0 -not -name cjs -exec rm -rf {} \; \
    && find $(yarn global dir)/node_modules/handlebars/lib/* -maxdepth 0 -not -name index.js -exec rm -rf {} \; \
    && find $(yarn global dir)/node_modules/isomorphic-git/dist/* -maxdepth 0 -not -name for-node -exec rm -rf {} \; \
    && rm -rf $(yarn global dir)/node_modules/moment/min \
    && rm -rf $(yarn global dir)/node_modules/moment/src \
    && rm -rf $(yarn global dir)/node_modules/source-map/dist \
    && rm -rf /tmp/*

WORKDIR /antora

COPY docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["docker-entrypoint.sh"]

CMD ["antora"]

#-----------------OLD STUFF---------
RUN apt-get update

RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - \
    && add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ \
    && apt-get update \
    && apt-get install -y adoptopenjdk-11-hotspot ant

RUN apt-get clean

CMD CASSANDRA_USE_JDK11=true ant gen-doc \
    && echo "The locally built documentation can be found here:\n\n    build/html/index.html\n\n"
