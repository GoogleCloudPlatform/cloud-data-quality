# This Dockerfile is intended to build an image that is used for the
# CI/CD Cloud Builds.
#
# Use the official Debian slim image for a lean production container.
# https://hub.docker.com/_/debian
# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM debian:11-slim

RUN set -x

ENV PYTHON_VERSION="3.9.7"

# Python install etc
RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get upgrade -yq && \
        apt install -yq --no-install-recommends \
        curl git wget make build-essential libssl-dev zlib1g-dev \
        libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
        libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev \
        libffi-dev liblzma-dev  \
        zip unzip git python3-distutils python3-apt \        
        tzdata sudo lsb-release apt-utils ca-certificates && \        
        rm -rf /var/lib/apt/lists/*
        


COPY ./scripts scripts
RUN ./scripts/install_development_dependencies.sh --silent
RUN ./scripts/install_python3.sh "${PYTHON_VERSION}"

# Install gcloud CLI
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz

RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh

ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin
