FROM debian:buster-slim
# Starting with python image results in two python versions, one in /usr/local/bin and one in /usr/bin
#FROM python:3.9.6-slim-buster
LABEL maintainer="saanobhaai"

ENV DEBIAN_FRONTEND noninteractive
ENV LANG C.UTF-8
ENV LANGUAGE C.UTF-8
ENV LC_ALL C.UTF-8

RUN apt-get update
RUN apt-get install -y python3 python3-pip git less nano
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN ln -s /usr/bin/pip3 /usr/bin/pip
# including python-gdal (but not python3-gdal) installs python 2 but is the only thing that installs gdal_merge.py
RUN apt-get install -y gdal-bin python3-gdal libgdal-dev
# To go with the pip route below -- couldn't get it to build
#RUN apt-get install -y gdal-bin libgdal-dev build-essential g++
#RUN apt-get install -y python3 python3-pip python3-gdal gdal-bin git less nano
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
#ENV CFLAGS=`gdal-config --cflags`

RUN pip install --upgrade pip setuptools wheel
#RUN /usr/bin/pip install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==`gdal-config --version`
RUN pip install earthengine-api==0.1.254 \
    && pip install gitpython==3.1.14 \
    && pip install google-api-python-client==1.12.5

WORKDIR /app
