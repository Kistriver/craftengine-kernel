# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import logging
import json

from craftengine import KernelModule, KernelModuleSingleton


def redis_dict(prefix):
    class GlobalDict(dict):
        def __getitem__(self, item):
            value = Registry().get("kernel.redis").get("DICT:%s:%s" % (prefix, item))
            if value is None:
                raise KeyError

            value = json.loads(value.decode("utf-8"))
            return value

        def __setitem__(self, key, value):
            try:
                value = json.dumps(value)
            except (TypeError, ValueError):
                raise

            Registry().get("kernel.redis").set("DICT:%s:%s" % (prefix, key), value)

        def __delitem__(self, key):
            Registry().get("kernel.redis").delete("DICT:%s:%s" % (prefix, key))
    return GlobalDict()


class RegistryMixIn(KernelModule):
    _registry = None

    def init(self, *args, **kwargs):
        self._registry = {}

    def get(self, key):
        logging.debug("Get value(%s)" % key)
        try:
            return self._registry[key]
        except KeyError:
            raise KeyError

    def set(self, key, data, namespace=None, has_permission=None):
        logging.debug("Set value(%s)" % key)
        self._registry[key] = data

    def delete(self, key):
        logging.debug("Delete value(%s)" % key)
        del self._registry[key]

    def stack(self, key, namespace=None, has_permission=None):
        logging.debug("Create stack(%s)" % key)
        self._registry[key] = []

    def rpush(self, key, data):
        logging.debug("Push value(%s)" % key)
        self._registry[key].append(data)

    def rpop(self, key):
        logging.debug("Pop value(%s)" % key)
        return self._registry[key].pop()

    def lpush(self, key, data):
        logging.debug("Push value(%s)" % key)
        # TODO: Atomic operation
        self._registry[key][1:-1] = self._registry[key]
        self._registry[key][0] = data

    def lpop(self, key):
        logging.debug("Pop value(%s)" % key)
        # TODO: Atomic operation
        r = self._registry[key][0]
        del self._registry[key][0]
        return r

    def hash(self, key, namespace=None, has_permission=None):
        logging.debug("Create hash(%s)" % key)
        self._registry[key] = {}

    def hget(self, key, index):
        logging.debug("Get value(%s, %s)" % (key, index))
        return self._registry[key][index]

    def hset(self, key, index, data):
        logging.debug("Set value(%s, %s)" % (key, index))
        self._registry[key][index] = data

    def hdelete(self, key, index):
        logging.debug("Delete value(%s, %s)" % (key, index))
        del self._registry[key][index]


class Registry(RegistryMixIn, KernelModuleSingleton):
    pass


class PermanentRegistry(RegistryMixIn, KernelModuleSingleton):
    def init(self, *args, **kwargs):
        self._registry = redis_dict("REGISTRY:NODE:%s" % self.kernel.env.node)


class GlobalRegistry(RegistryMixIn, KernelModuleSingleton):
    def init(self, *args, **kwargs):
        self._registry = redis_dict("REGISTRY:GLOBAL")
