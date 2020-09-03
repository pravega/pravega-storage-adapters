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
ARG DOCKER_IMAGE_REGISTRY=pravega/pravega-storage-adapters
ARG DOCKER_IMAGE_REPOSITORY=pravega
ARG DOCKER_IMAGE_VERSION=0.9.0-2625.fd290ad-SNAPSHOT
ARG PRAVEGA_IMAGE=${DOCKER_IMAGE_REGISTRY}/${DOCKER_IMAGE_REPOSITORY}:${DOCKER_IMAGE_VERSION}

ARG PRAVEGA_BINDINGS_VERSION=0.9.0-2625.fd290ad-SNAPSHOT
ARG HDFS_JAR=hdfs/build/libs/pravega-hdfs-${PRAVEGA_BINDINGS_VERSION}.jar
ARG S3_JAR=extendeds3/build/libs/pravega-extendeds3-${PRAVEGA_BINDINGS_VERSION}.jar

FROM ${PRAVEGA_IMAGE} as docker-builder
WORKDIR /opt/pravega
COPY ${HDFS_JAR} /opt/pravega/lib/pravega-bindings-hdfs-${PRAVEGA_BINDINGS_VERSION}.jar
COPY ${S3_JAR} /opt/pravega/lib/pravega-bindings-extendeds3-${PRAVEGA_BINDINGS_VERSION}.jar

RUN chmod +x -R /opt/pravega/lib/

ENTRYPOINT [ "/opt/pravega/scripts/entrypoint.sh" ]
