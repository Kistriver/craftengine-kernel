.PHONY: build docker clean run

CE_VER := $(shell git describe --always --tag)

build:
	cp -R src build.tmp
	cp LICENSE build.tmp/LICENSE
	cp requirements.txt build.tmp/requirements.txt
	echo $(CE_VER) > build.tmp/VERSION

docker: build
	if ! [ -e libs.tmp/ddp ]; then git clone git@bitbucket.org:Kistriver/darkdist-protocol.git libs.tmp/ddp; fi
	if ! [ -e libs.tmp/pycraftengine ]; then git clone git@git.kistriver.com:kistriver/craftengine-python.git libs.tmp/pycraftengine; fi
	cp Dockerfile Dockerfile.tmp
	sed -i "s|##CE_VER##|$(CE_VER)|" Dockerfile.tmp
	docker build -t kistriver/ce-kernel:$(CE_VER) -f Dockerfile.tmp .
	docker tag -f kistriver/ce-kernel:$(CE_VER) kistriver/ce-kernel

run: docker
	docker run --name ce-kernel -ti --rm -e REDIS_PORT=6379 --link ce-redis:redis -v /var/run/docker.sock:/var/run/docker.sock --privileged kistriver/ce-kernel

clean:
	-rm -rf */__pycache__
	-rm -r *.tmp
