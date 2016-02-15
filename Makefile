.PHONY: build docker clean run

CE_VER := $(shell git describe --always --tag)

build:
	-rm -rf build.tmp
	cp -R src build.tmp
	cp LICENSE build.tmp/LICENSE
	cp requirements.txt build.tmp/requirements.txt
	echo $(CE_VER) > build.tmp/VERSION

docker: build
	if ! [ -e libs.tmp/ddp ]; then git clone git@git.kistriver.com:kistriver/ddp.git libs.tmp/ddp; fi
	cp Dockerfile Dockerfile.tmp
	sed -i "s|##CE_VER##|$(CE_VER)|" Dockerfile.tmp
	docker build -t kistriver/ce-kernel:$(CE_VER) -f Dockerfile.tmp .
	docker tag -f kistriver/ce-kernel:$(CE_VER) kistriver/ce-kernel

run: docker
	docker run --name ce-kernel -ti --rm -e REDIS_PORT=6379 -e CE_PROJECT_NAME="CRAFTEngine" -e CE_NODE_NAME="alpha" \
	--link ce-redis:redis -p 2011:2011 -v /var/run/docker.sock:/var/run/docker.sock --privileged kistriver/ce-kernel

cluster: docker
	-docker rm -f ce-kernel-alpha ce-kernel-beta
	docker run --name ce-kernel-alpha -d -e REDIS_PORT=6379 -e REDIS_DB=0 -e CE_PROJECT_NAME="CRAFTEngine" \
	-e CE_NODE_NAME="alpha" --link ce-redis:redis -p 2011:2011 \
	-v /var/run/docker.sock:/var/run/docker.sock --privileged kistriver/ce-kernel
	docker run --name ce-kernel-beta -d -e REDIS_PORT=6379 -e REDIS_DB=2 -e CE_PROJECT_NAME="CRAFTEngine" \
	-e CE_NODE_NAME="beta" --link ce-redis:redis -p 2012:2012 \
	-v /var/run/docker.sock:/var/run/docker.sock --privileged kistriver/ce-kernel

clean:
	-rm -rf */__pycache__
	-rm -r *.tmp
