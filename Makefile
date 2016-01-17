.PHONY: build clean run

CE_VER := $(shell git describe --always --tag)

build:
	if ! [ -e src/craftengine/utils/ddp ]; then git clone git@bitbucket.org:Kistriver/darkdist-protocol.git src/craftengine/utils/ddp; fi
	echo $(CE_VER) > VERSION.tmp
	cp Dockerfile Dockerfile.tmp
	sed -i "s|##CE_VER##|$(CE_VER)|" Dockerfile.tmp
	docker build -t kistriver/ce-kernel:$(CE_VER) -f Dockerfile.tmp .
	docker tag -f kistriver/ce-kernel:$(CE_VER) kistriver/ce-kernel
	rm -f Dockerfile.tmp VERSION.tmp

run: build
	docker run --name ce-kernel -ti --rm -e REDIS_PORT=6379 --link ce-redis:redis -v /var/run/docker.sock:/var/run/docker.sock --privileged kistriver/ce-kernel

clean:
	-rm -rf */__pycache__
	-rm -rf src/craftengine/utils/ddp
	-rm -r *.tmp
