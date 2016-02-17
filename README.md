CRAFTEngine Kernel
==================

How to run
----------

Add test service:
* Start Kernel
* Stop Kernel
* Find meta:kernel/services in redis and copy `data_id`
* Paste into data:`data_id`:{service_name}:
```
{
    "token": "test_token",
    "image": "kistriver/py-test",
    "permissions": [
        "kernel",
        "registry",
        "event"
    ],
    "scale": 2,
    "command": null
}
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
