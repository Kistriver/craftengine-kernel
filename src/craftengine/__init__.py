# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import logging
import signal
import threading
import time
import redis
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
from craftengine.utils.exceptions import KernelException
from craftengine import registry


class Kernel(KernelModuleSingleton):
    alive = None
    _env = None
    redis_l = None
    redis_g = None
    l = None
    g = None

    def init(self, *args, **kwargs):
        self.alive = True
        threading.current_thread().setName("kernel")
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

        self.redis_l = redis.Redis(
            host=self.env.get("REDIS_HOST", "redis"),
            port=int(self.env.get("REDIS_PORT", 6379)),
            db=int(self.env.get("REDIS_DB", 0)),
            password=self.env.get("REDIS_PASSWORD", None),
        )
        self.l = registry.Local()

        for key, t in {
            "kernel.env": "hash",
            "kernel.services": "hash",
        }.items():
            try:
                self.l.create(key, data_type=t)
            except registry.ConsistencyException:
                pass

        Registry().hash("api.methods")
        Registry().hash("api.plugins")

        Registry().set("kernel.docker", Docker(base_url="unix://var/run/docker.sock"))

    def exit(self, *args, **kwargs):
        if not self.alive:
            return
        self.alive = False

        try:
            logging.info("Stopping kernel...")
            super().exit(*args, **kwargs)

            pls = GlobalRegistry().get("kernel.plugins")
            docker = Registry().get("kernel.docker")
            for pl in pls.values():
                try:
                    docker.remove_container(container=pl["name"], force=True)
                    logging.info("'%s' service stopped" % pl["name"])
                except:
                    pass

            server = Registry().get("server")
            server.shutdown()
        except:
            logging.exception("")
            self.alive = True

    def serve(self):
        from craftengine.utils.rpc import RpcServer
        server = RpcServer((
            self.env.get("rpc.host", "0.0.0.0"),
            int(self.env.get("rpc.port", 2011))
        ))
        Registry().set("server", server)
        threading.Thread(target=server.serve_forever, name="kernel.rpc").start()
        while self.alive and server.alive is None:
            time.sleep(0.1)

        if not server.alive:
            raise KernelException("RPC Server start failed")

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
            logging.info("'%s' service started" % pl["name"])

        while self.alive and server.alive:
            time.sleep(1)

    @property
    def env(self):
        return self._env
