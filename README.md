CRAFTEngine Kernel
==================

How to run
----------

Run Redis and add test service:
```bash
docker run --name redis-ce redis
redis-ce> SET DICT:REGISTRY:NODE:alpha:kernel.services "{\"test\": {\"token\": \"test_token\", \"image\": \"kistriver/py-test\", \"permissions\": [\"kernel\", \"registry\", \"event\"]}}"
```

Compile CE Kernel:
```bash
make run
```

Create CE Kernel Docker image:
```bash
make docker
```

Run CE Kernel:
```bash
make run
```
