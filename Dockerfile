FROM python:3.7-alpine3.10
LABEL maintainer="saanobhaai"

ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

RUN set -ex \
    && apk update -q \
    && apk upgrade -q \
    && apk add -q \
        build-base \
        git \
        less \
        nano \
    && pip install earthengine-api
