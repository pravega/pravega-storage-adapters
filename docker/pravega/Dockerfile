#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

#FROM openjdk:8u212-jre-alpine3.9
ARG DOCKER_IMAGE_REGISTRY=asdrepo.isus.emc.com:8103/nautilus
ARG DOCKER_IMAGE_REPOSITORY=pravega
ARG DOCKER_IMAGE_VERSION=0.8.0-2609.150d5861-issue-4907-bindings
ARG PRAVEGA_IMAGE=${DOCKER_IMAGE_REGISTRY}/${DOCKER_IMAGE_REPOSITORY}:${DOCKER_IMAGE_VERSION}

ARG PRAVEGA_BINDINGS_VERSION=0.8.0-2577.15b0f5e-3.ac45db3-SNAPSHOT
ARG HDFS_JAR=bindings-hdfs/build/libs/pravega-bindings-hdfs-${PRAVEGA_BINDINGS_VERSION}.jar
ARG S3_JAR=bindings-extendeds3/build/libs/pravega-bindings-extendeds3-${PRAVEGA_BINDINGS_VERSION}.jar

FROM ${PRAVEGA_IMAGE} as docker-builder
WORKDIR /opt/pravega
COPY ${HDFS_JAR} /opt/pravega/lib
COPY ${S3_JAR} /opt/pravega/lib

RUN chmod +x -R /opt/pravega/lib/

ENTRYPOINT [ "/opt/pravega/scripts/entrypoint.sh" ]
