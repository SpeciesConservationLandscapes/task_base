FROM python:3.9.6-slim-buster
LABEL maintainer="saanobhaai"

ENV DEBIAN_FRONTEND noninteractive
ENV LANG C.UTF-8
ENV LANGUAGE C.UTF-8
ENV LC_ALL C.UTF-8

RUN apt-get update
RUN apt-get install -y git less nano
RUN apt-get install -y gdal-bin libgdal-dev g++

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN /usr/local/bin/python -m pip install --no-cache-dir \
    gdal==2.4.0 \
    earthengine-api==0.1.254 \
    gitpython==3.1.14 \
    google-api-python-client==1.12.5

WORKDIR /app
