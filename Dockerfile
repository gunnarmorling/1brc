#
#  Copyright 2023 The original authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# syntax=docker/dockerfile:1
# Use an official Fedora Linux as a base image
FROM fedora:latest as build-sdkman
ARG JAVA_VERSIONS
# Maintain a variable providing a list of required packages (for easier maintenance)
ENV _PACKAGES="curl zip unzip"
# Maintain a variable providing a list of java versions to always install (for easier maintenance)
ENV _JAVA_VERSIONS="21.0.1-open"
# Install necessary packages
RUN dnf -y update && dnf -y install ${_PACKAGES}
# Clear DNF caches to reduce image size
RUN dnf clean all
# Install SDKMAN!
RUN curl -s "https://get.sdkman.io" | bash
# this SHELL command is needed to allow using source
SHELL ["/bin/bash", "-c"]
# Look for SDKMAN and load it into the shell session
RUN echo "source /root/.sdkman/bin/sdkman-init.sh" >> /root/.bashrc
# Install default java versions
RUN for JAVA_VERSION in $_JAVA_VERSIONS; do source "/root/.sdkman/bin/sdkman-init.sh" && sdk install java $JAVA_VERSION;  done
# Install jbang
RUN source "/root/.sdkman/bin/sdkman-init.sh" && sdk install jbang
# Install SDK versions
RUN for JAVA_VERSION in $JAVA_VERSIONS; do source "/root/.sdkman/bin/sdkman-init.sh" && sdk install java $JAVA_VERSION;  done

FROM fedora:latest as build-1brc
ARG PACKAGES
# Maintain a variable providing a list of required packages (for easier maintenance)
ENV _PACKAGES="diffutils perl-Digest-SHA hyperfine jq numactl bc gcc zlib-devel git"

COPY --from=build-sdkman /root/.sdkman /root/.sdkman
# Source sdkman in all shells (alternative to adding to PATH)
# this SHELL command is needed to allow using source
SHELL ["/bin/bash", "-c"]
# Look for SDKMAN and load it into the shell session
RUN echo "source /root/.sdkman/bin/sdkman-init.sh" >> /root/.bashrc
# Manually add sdkman to PATH
ENV PATH="/root/.sdkman/bin:$PATH"
# Install necessary packages
RUN dnf -y update && dnf -y install ${_PACKAGES}
# Install requested extra build packages
RUN if [ -n "$PACKAGES" ] ; then dnf -y install $PACKAGES ; else echo "PACKAGES is empty, run docker build --build_arg PACKAGES=<extra packages to install in the image> ." ; fi
# Clear DNF caches to reduce image size
RUN dnf clean all

ARG REPOSITORY="https://github.com/gunnarmorling/1brc.git"
# Checkeout the repository
RUN git clone $REPOSITORY

WORKDIR 1brc
# Initialize SDKMAN and download project dependencies to work offline
RUN source "/root/.sdkman/bin/sdkman-init.sh" \
# Cache local repo
    && ./mvnw dependency:go-offline \
# Build the project
    && ./mvnw package

FROM build-1brc
# Set the working directory in the container to /app
WORKDIR /1brc
# Copy the files from the workspace
COPY / .