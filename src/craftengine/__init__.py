# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import logging
import signal
import threading
import time

import redis
from docker import Client as Docker

from craftengine.modules import KernelModuleSingleton
from craftengine.exceptions import KernelException
from craftengine import (
    registry,
    service,
    rpc,
    node,
)


class Kernel(KernelModuleSingleton):
    alive = None
    _env = None
    redis_l = None
    redis_g = None
    l = None
    g = None
    service = None
    rpc = None
    node = None
    docker = None

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

        self.redis_l = redis.Redis(
            host=self.env.get("REDIS_HOST", "redis"),
            port=int(self.env.get("REDIS_PORT", 6379)),
            db=int(self.env.get("REDIS_DB", 0)),
            password=self.env.get("REDIS_PASSWORD", None),
        )
        self.l = registry.Local()

        for key, t in {
            "kernel/env": "hash",
            "kernel/services": "hash",
        }.items():
            try:
                self.l.create(key, data_type=t, handler=None, handler_lua="""\
function(method, key, data)
    if method == "get" then
        return true
    else
        return false
    end
end\
""")
            except registry.ConsistencyException:
                pass

        redis_g = self.l.get("kernel/env", keys=["REDIS_HOST", "REDIS_PORT", "REDIS_DB", "REDIS_PASSWORD"])
        self.redis_g = redis.Redis(
            host=redis_g["REDIS_HOST"],
            port=redis_g["REDIS_PORT"],
            db=redis_g["REDIS_DB"],
            password=redis_g["REDIS_PASSWORD"],
        )
        self.g = registry.Global()

        self.service = service.Service()

        self.rpc = rpc.Rpc()

        np = self.l.get("kernel/env", keys=["node"]).get("node")
        np = {} if np is None else np
        node_host, node_port = np.get("host", "0.0.0.0"), np.get("port", 2011)
        self.node = node.Rpc(host=node_host, port=node_port)

        try:
            self.kernel.g.create("kernel/nodes", data_type="hash")
        except registry.ConsistencyException:
            pass
        # self.kernel.g.set("kernel/nodes", keys={self.kernel.env["CE_NODE_NAME"]: self.rpc.real_host})

        self.docker = Docker(base_url="unix://var/run/docker.sock")

    def exit(self, *args, **kwargs):
        if not self.alive:
            return
        self._alive = False

        try:
            logging.info("Stopping kernel...")
            super().exit(*args, **kwargs)

            lst = self.service.list()
            for k, v in lst.items():
                self.service.stop(k)

            self.rpc.exit(*args, **kwargs)
        except Exception as e:
            logging.exception(e)
            self.alive = True

    def serve(self):
        threading.Thread(target=self.rpc.serve, name="kernel.rpc").start()
        while self.alive and self.rpc.alive is None:
            time.sleep(0.1)

        if not self.rpc.alive:
            raise KernelException("RPC Server start failed")

        lst = self.service.list()
        for k, v in lst.items():
            self.service.start(k, num=v.get("scale", 1))

        while self.alive and self.rpc.alive:
            time.sleep(1)

    @property
    def env(self):
        return self._env
