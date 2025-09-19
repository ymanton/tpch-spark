FROM ubuntu:20.04

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --assume-yes --no-install-recommends \
        ca-certificates \
        openjdk-11-jdk-headless \
        build-essential \
        wget

WORKDIR /opt

RUN wget -qO- https://github.com/sbt/sbt/releases/download/v1.10.0/sbt-1.10.0.tgz | tar -xzv

WORKDIR /opt/tpch-spark

COPY --chown=spark . .

RUN cd dbgen \
    && make

RUN /opt/sbt/bin/sbt package
