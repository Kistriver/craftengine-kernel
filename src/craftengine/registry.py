# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"


import hashlib
import time
import random
import json

from craftengine.utils.exceptions import KernelException
from craftengine import KernelModule


class RegistryException(KernelException):
    pass


class LockException(RegistryException):
    pass


class ConsistencyException(RegistryException):
    pass


class _RegistryH(object):
    def __init__(self, r):
        self.r = r

    def create(self, key):
        raise NotImplementedError

    def get(self, key, **kwargs):
        raise NotImplementedError

    def set(self, key, **kwargs):
        raise NotImplementedError


class _RegistryHStr(_RegistryH):
    def create(self, key):
        self.r.rd.set(self.r.data_key(key), "")

    def get(self, key, **kwargs):
        return self.r.rd.get(self.r.data_key(key))

    def set(self, key, **kwargs):
        try:
            data = kwargs["data"]
        except KeyError:
            raise TypeError
        return self.r.rd.set(self.r.data_key(key), data)


class _RegistryHHash(_RegistryH):
    def create(self, key):
        pass

    def get(self, key, **kwargs):
        keys = list(kwargs.get("keys", []))
        if len(keys) == 0:
            return self.r.rd.hmgetall(self.r.data_key(key))
        else:
            data_bin = self.r.rd.hmget(self.r.data_key(key), keys)
            data = {}
            for i in range(len(keys)):
                data[keys[i]] = data_bin[i]
            return data

    def set(self, key, **kwargs):
        try:
            keys = dict(kwargs["keys"])
        except KeyError:
            raise TypeError

        return self.r.rd.hmset(self.r.data_key(key), keys)


class _RegistryHSet(_RegistryH):
    pass


class _RegistryHSSet(_RegistryH):
    pass


class _DataLock(object):
    def __init__(self, r, key, mode=None):
        self.r = r
        self.key = key
        self.mode = "ro" if mode is None else mode

    def __enter__(self):
        meta = self.r.meta_get(self.key)
        if meta["lock"] != self.r.valid_lock_type("rw"):
            raise LockException
        self.r.meta_id_incr(self.key, meta["id"])
        return self.r.meta_set(self.key, {"lock": self.mode})

    def __exit__(self, *_):
        self.r.meta_set(self.key, {"lock": "rw"})


class _Registry(KernelModule):
    DATA_TYPES = {
        "str": 0,
        "string": 0,
        "hash": 1,
        "map": 1,
        "array": 1,
        "set": 2,
        "sorted_set": 2,
        "sset": 2,
    }

    LOCK_TYPES = {
        "rw": 0,
        "write": 0,
        "ro": 1,
        "r": 1,
        "read": 1,
        "n": 2,
        "na": 2,
        "lock": 2,
    }

    rd = None
    _handlers = None
    prefix = ""

    def init(self, *args, **kwargs):
        self._handlers = [
            _RegistryHStr(self),
            _RegistryHHash(self),
            _RegistryHSet(self),
            _RegistryHSSet(self),
        ]

    def prefixed(self, key, prefix, postfix=None):
        prefix = "%s:" % prefix if prefix else ""
        prefix += "%s:" % self.prefix if self.prefix else ""
        postfix = ":%s" % postfix if postfix else ""

        return "".join([prefix, key, postfix])

    def meta_get(self, key):
        data_bin = self.rd.hgetall(self.meta_key(key))
        if len(data_bin) == 0:
            raise KeyError

        data = {}
        for k, v in data_bin.items():
            data[k.decode("utf-8")] = v.decode("utf-8")

        data["id"] = int(data["id"])
        data["lock"] = int(data["lock"])
        data["type"] = int(data["type"])
        data["perms"] = json.loads(data["perms"])

        return data

    def meta_key(self, key):
        return self.prefixed(key, prefix="meta")

    def data_key(self, key):
        return self.prefixed(key, prefix="data")

    def _meta_set(self, key, meta):
        meta["perms"] = json.dumps(meta["perms"])
        meta["lock"] = self.valid_lock_type(meta["lock"])
        meta["type"] = self.valid_data_type(meta["type"])
        return self.rd.hmset(self.meta_key(key), meta)

    def meta_set(self, key, fields):
        meta = self.meta_get(key)
        for k in meta.keys():
            if k in fields.keys():
                meta[k] = fields[k]

        self._meta_set(key, meta)
        return meta

    def meta_id_incr(self, key, rev_id):
        new_id = self.rd.hincrby(self.meta_key(key), "id")
        if new_id != rev_id + 1:
            self.rd.hincrby(self.meta_key(key), "id", -1)
            raise ConsistencyException
        else:
            return new_id

    def meta_init(self, key, data_type, perms, handler, handler_lua):
        meta = {
            "id": 0,
            "type": data_type,
            "perms": perms,
            "handler": handler,
            "handler_lua": handler_lua,
            "lock": "na",
            "data_id": hashlib.sha512(str(random.random()).encode("utf-8")).hexdigest() +
                       hashlib.sha512(str(time.time()).encode("utf-8")).hexdigest(),
        }

        try:
            self.meta_get(key)
        except KeyError:
            self._meta_set(key, meta)
            return self.meta_get(key)
        else:
            raise ConsistencyException

    def valid_data_type(self, data_type):
        if isinstance(data_type, str) and data_type.lower() in self.DATA_TYPES.keys():
            return self.DATA_TYPES[data_type]
        elif isinstance(data_type, int) and data_type in self.DATA_TYPES.values():
            return data_type
        else:
            raise TypeError(repr(data_type))

    def valid_lock_type(self, lock_type):
        if isinstance(lock_type, str) and lock_type.lower() in self.LOCK_TYPES.keys():
            return self.LOCK_TYPES[lock_type]
        elif isinstance(lock_type, int) and lock_type in self.LOCK_TYPES.values():
            return lock_type
        else:
            raise TypeError(repr(lock_type))

    def data_handler(self, data_type):
        data_type = self.valid_data_type(data_type)
        return self._handlers[data_type]

    def create(self, key, perms=None, handler=None, handler_lua=None, data_type=None):
        kwargs = {
            "key": str(key),
            "perms": [] if perms is None else list(perms),
            "handler": "" if handler is None else str(handler),
            "handler_lua": "" if handler_lua is None else str(handler_lua),
            "data_type": self.valid_data_type("str" if data_type is None else data_type),
        }

        meta = self.meta_init(**kwargs)
        self.data_handler(kwargs["data_type"]).create(meta["data_id"])
        return self.meta_set(key, {"lock": "rw"})

    def get(self, key, **kwargs):
        meta = self.meta_get(key)
        if meta is None:
            raise KeyError

        if meta["lock"] in [self.valid_lock_type(x) for x in ["rw", "ro"]]:
            return self.data_handler(meta["type"]).get(meta["data_id"], **kwargs)
        else:
            raise LockException

    def set(self, key, **kwargs):
        with _DataLock(self, key) as meta:
            return self.data_handler(meta["type"]).set(meta["data_id"], **kwargs)


class Local(_Registry):
    def init(self):
        super().init()
        self.rd = self.kernel.redis_l
        self.prefix = ""


class Global(_Registry):
    def init(self):
        super().init()
        self.rd = self.kernel.redis_g
        self.prefix = "global"
