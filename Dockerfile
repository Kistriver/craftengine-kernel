FROM python:3
MAINTAINER Alexey Kachalov <kachalov@kistriver.com>

LABEL \
Vendor="Kistriver" \
Version="##CE_VER##" \
Description="This image is used to start CRAFTEngine core"

EXPOSE 2011

RUN \
apt-get update && \
# apt-get upgrade -y && \
apt-get install -y liblua5.2-dev

WORKDIR /home/craftengine
COPY libs.tmp /usr/lib/ce-deps
COPY build.tmp /home/craftengine
RUN pip3 install -r requirements.txt

CMD PYTHONPATH="/usr/lib/ce-deps/":"${PYTHONPATH}" python3 -u __main__.py
