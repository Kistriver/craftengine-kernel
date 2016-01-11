FROM debian:8
MAINTAINER Alexey Kachalov <kachalov@kistriver.com>

LABEL \
Vendor="Kistriver" \
Version="##CE_VER##" \
Description="This image is used to start CRAFTEngine core"

RUN \
apt-get update && \
apt-get upgrade -y && \
apt-get install -y python3 python3-dev python3-pip #liblua5.2-dev

COPY requirements.txt /home/craftengine/requirements.txt
WORKDIR /home/craftengine
RUN pip3 install -r requirements.txt

COPY src /home/craftengine
COPY VERSION.tmp /home/craftengine/VERSION
COPY LICENSE /home/craftengine/LICENSE
CMD python3 __main__.py
