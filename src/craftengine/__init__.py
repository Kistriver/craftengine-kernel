# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import logging
import signal
from docker import Client as Docker

from craftengine.middleware.redis import Redis
from craftengine.utils.modules import (
    KernelModule,
    KernelModuleSingleton,
)
from craftengine.utils.registry import (
    Registry,
    PermanentRegistry,
    GlobalRegistry,
)

# TODO: Remove it
logging.basicConfig(format="\033[0m[%(levelname)s] %(message)s\033[0m", level="DEBUG")


class Kernel(KernelModuleSingleton):
    _env = None

    def init(self, *args, **kwargs):
        self._env = {}
        for k, v in kwargs.items():
            try:
                self._env[k] = v
            except TypeError:
                pass

        signal.signal(signal.SIGTERM, self.exit)
        signal.signal(signal.SIGINT, self.exit)
        signal.signal(signal.SIGPWR, self.exit)

        Registry().set("kernel.redis", Redis(
            host=self.env.get("REDIS_HOST", "redis"), 
            port=int(self.env.get("REDIS_PORT", 6379)),
            db=int(self.env.get("REDIS_DB", 0)),
            password=self.env.get("REDIS_PASSWORD", None),
        ))

        Registry().hash("api.methods")
        Registry().hash("api.plugins")

        Registry().set("kernel.docker", Docker(base_url="unix://var/run/docker.sock"))

        pls = GlobalRegistry().get("kernel.plugins")
        docker = Registry().get("kernel.docker")
        for pl in pls.values():
            try:
                docker.remove_container(container=pl["name"], force=True)
            except:
                pass
            docker.create_container(
                image=pl["image"],
                detach=True,
                name=pl["name"],
                environment={
                    "CE_TOKEN": pl["token"],
                    "CE_NAME": pl["name"],
                },
                labels={
                    "CRAFTEngine": "True",
                },
            )
            docker.start(container=pl["name"])

    def exit(self, *args, **kwargs):
        super().exit(*args, **kwargs)
        server = Registry().get("server")
        server.shutdown()

        pls = GlobalRegistry().get("kernel.plugins")
        docker = Registry().get("kernel.docker")
        for pl in pls.values():
            try:
                docker.remove_container(container=pl["name"], force=True)
            except:
                pass

    def serve(self):
        from craftengine.utils.rpc import run_server
        run_server("0.0.0.0", 5000)
        self.exit()

    @property
    def env(self):
        return self._env
