CRAFTEngine Kernel
==================

How to run
----------

Run Redis and add test service:
```bash
docker run --name redis-ce redis
redis-ce> SET DICT:REGISTRY:GLOBAL:kernel.plugins "{\"0\": {\"token\": \"test_token\", \"name\": \"test\", \"image\": \"kistriver/ce-python\", \"permissions\": [\"kernel\", \"registry\"]}}"
```

Run CE Kernel:
```bash
make run
```
