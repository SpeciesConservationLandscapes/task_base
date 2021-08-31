FROM python:3.9.6-slim-buster
LABEL maintainer="saanobhaai"

ENV DEBIAN_FRONTEND noninteractive
ENV LANG C.UTF-8
ENV LANGUAGE C.UTF-8
ENV LC_ALL C.UTF-8

RUN apt-get update \
    && apt-get install -y --no-install-recommends git less nano

RUN pip install --upgrade pip setuptools wheel
RUN pip install earthengine-api==0.1.254 \
    && pip install gitpython==3.1.14 \
    && pip install google-api-python-client==1.12.5

WORKDIR /app
