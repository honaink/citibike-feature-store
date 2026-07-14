# EMR Serverless custom image: the EMR base image plus Python 3.11, Feast and this project.
# The release must match `emr_release` in infra/terraform (EMR 7.14 rejects mismatches).
# Build from the repository root: docker build -f infra/emr/Dockerfile .
ARG EMR_RELEASE=emr-7.14.0
FROM public.ecr.aws/emr-serverless/spark/${EMR_RELEASE}:latest

USER root

# Feast needs Python 3.10+. Spark keeps its own pyspark; it is not installed here.
RUN dnf install -y python3.11 python3.11-pip && dnf clean all

COPY infra/emr/requirements.txt /tmp/requirements.txt
RUN python3.11 -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY pyproject.toml /opt/citibike/
COPY src /opt/citibike/src
COPY feature_repo /opt/citibike/feature_repo
RUN python3.11 -m pip install --no-cache-dir --no-deps /opt/citibike

ENV FEAST_REPO_PATH=/opt/citibike/feature_repo \
    FEAST_USAGE=False

# EMR Serverless runs the image as hadoop.
USER hadoop:hadoop
