# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import hashlib
import time
import random
import json
import logging

import lupa
from craftengine.exceptions import ModuleException
from craftengine.modules import KernelModule


class RegistryException(ModuleException):
    pass


class LockException(RegistryException):
    pass


class ConsistencyException(RegistryException):
    pass


class AccessException(RegistryException):
    pass


class _RegistryH(object):
    def __init__(self, r):
        self.r = r

    def create(self, key):
        raise NotImplementedError

    def get(self, meta_key, key, **kwargs):
        raise NotImplementedError

    def set(self, meta_key, key, **kwargs):
        raise NotImplementedError

    def rem(self, meta_key, key, **kwargs):
        raise NotImplementedError


class _RegistryHStr(_RegistryH):
    def create(self, key):
        self.r.rd.set(self.r.data_key(key), "")

    def get(self, meta_key, key, **kwargs):
        data = self.r.rd.get(self.r.data_key(key))
        if data is None:
            return None
        else:
            data = data.decode("utf-8")
            return json.loads(data)

    def set(self, meta_key, key, **kwargs):
        try:
            data = kwargs["data"]
        except KeyError:
            raise TypeError
        data = json.dumps(data)

        return self.r.rd.set(self.r.data_key(key), data)

    def rem(self, meta_key, key, **kwargs):
        self.r.meta_rem(meta_key)
        return self.r.rd.delete(self.r.data_key(key))


class _RegistryHHash(_RegistryH):
    def create(self, key):
        pass

    def get(self, meta_key, key, **kwargs):
        keys = list(kwargs.get("keys", []))
        if len(keys) == 0:
            data_bin = self.r.rd.hgetall(self.r.data_key(key))
            data = {}
            for k, v in data_bin.items():
                try:
                    data[k.decode("utf-8")] = json.loads(v.decode("utf-8"))
                except AttributeError:
                    data[k.decode("utf-8")] = None
            return data
        else:
            data_bin = self.r.rd.hmget(self.r.data_key(key), keys)
            data = {}
            for i in range(len(keys)):
                try:
                    data[keys[i]] = json.loads(data_bin[i].decode("utf-8"))
                except AttributeError:
                    data[keys[i]] = None
            return data

    def set(self, meta_key, key, **kwargs):
        try:
            keys = dict(kwargs["keys"])
        except KeyError:
            raise TypeError
        for k in keys.keys():
            keys[k] = json.dumps(keys[k])

        return self.r.rd.hmset(self.r.data_key(key), keys)

    def rem(self, meta_key, key, **kwargs):
        try:
            keys = dict(kwargs["keys"])
        except KeyError:
            keys = self.r.rd.hkeys(self.r.data_key(key))

        p = self.r.rd.pipeline()
        for k in keys:
            p.hdel(self.r.data_key(key), k)
        p.execute()
        self.r.meta_rem(meta_key)


class _RegistryHSet(_RegistryH):
    def create(self, key):
        raise NotImplementedError

    def get(self, meta_key, key, **kwargs):
        raise NotImplementedError

    def set(self, meta_key, key, **kwargs):
        raise NotImplementedError

    def rem(self, meta_key, key, **kwargs):
        raise NotImplementedError


class _RegistryHSSet(_RegistryH):
    def create(self, key):
        raise NotImplementedError

    def get(self, meta_key, key, **kwargs):
        raise NotImplementedError

    def set(self, meta_key, key, **kwargs):
        raise NotImplementedError

    def rem(self, meta_key, key, **kwargs):
        raise NotImplementedError


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
        try:
            self.r.meta_set(self.key, {"lock": "rw"})
        except KeyError:
            pass


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
    _rpc = None

    def init(self, *args, **kwargs):
        self._handlers = [
            _RegistryHStr(self),
            _RegistryHHash(self),
            _RegistryHSet(self),
            _RegistryHSSet(self),
        ]

    def prefixed(self, key, prefix, postfix=None):
        _prefix = prefix
        prefix = "%s:" % self.prefix if self.prefix else ""
        prefix += "%s:" % _prefix if _prefix else ""
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
        data["handler"] = json.loads(data["handler"])

        return data

    def meta_key(self, key):
        return self.prefixed(key, prefix="meta")

    def data_key(self, key):
        return self.prefixed(key, prefix="data")

    def _meta_set(self, key, meta):
        meta["lock"] = self.valid_lock_type(meta["lock"])
        meta["type"] = self.valid_data_type(meta["type"])
        meta["handler"] = json.dumps(meta["handler"])
        return self.rd.hmset(self.meta_key(key), meta)

    def meta_set(self, key, fields):
        meta = self.meta_get(key)
        for k in meta.keys():
            if k in fields.keys():
                meta[k] = fields[k]

        self._meta_set(key, meta)
        return self.meta_get(key)

    def meta_id_incr(self, key, rev_id):
        new_id = self.rd.hincrby(self.meta_key(key), "id")
        if new_id != rev_id + 1:
            self.rd.hincrby(self.meta_key(key), "id", -1)
            raise ConsistencyException
        else:
            return new_id

    def meta_init(self, key, data_type, handler, handler_lua):
        meta = {
            "id": 0,
            "type": data_type,
            "handler": handler,
            "handler_lua": handler_lua,
            "lock": "na",
            "data_id":
                hashlib.sha512(str(random.random()).encode("utf-8")).hexdigest() +
                hashlib.sha512(str(time.time()).encode("utf-8")).hexdigest(),
        }

        try:
            self.meta_get(key)
        except KeyError:
            self._meta_set(key, meta)
            return self.meta_get(key)
        else:
            raise ConsistencyException

    def meta_rem(self, key):
        keys = self.rd.hkeys(self.meta_key(key))
        p = self.rd.pipeline()
        if len(keys) == 0:
            raise KeyError
        for k in keys:
            p.hdel(self.meta_key(key), k)
        p.execute()

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

    def handler(self, h, hl, *data):
        if h is None:
            self.handler_lua(hl, data)
        elif h is True:
            return
        elif h is False:
            raise AccessException
        elif isinstance(h, list):
            try:
                self.rpc_handler(h, data)
            except AccessException:
                raise
            except Exception as e:
                logging.exception(e)
                self.handler_lua(hl, data)
        else:
            raise TypeError(type(h))

    @staticmethod
    def handler_lua(h, data):
        if h is None or h is True:
            pass
        elif h is False:
            raise AccessException
        elif isinstance(h, str):
            lua = lupa.LuaRuntime(unpack_returned_tuples=True)
            result = lua.eval(h)(*data)
            if not result:
                raise AccessException
        else:
            raise TypeError(type(h))

    def rpc_handler(self, h, data):
        service, method = h
        result = self._rpc.sync_exec()(service, method)(*data)
        if not result:
            raise AccessException

    def create(self, key, handler=None, handler_lua=None, data_type=None):
        kwargs = {
            "key": str(key),
            "handler": None if handler is None else list(handler),
            "handler_lua": "function(method, key, data) return true end" if handler_lua is None else str(handler_lua),
            "data_type": self.valid_data_type("str" if data_type is None else data_type),
        }

        meta = self.meta_init(**kwargs)
        self.data_handler(kwargs["data_type"]).create(meta["data_id"])
        return self.meta_set(key, {"lock": "rw"})

    def get(self, key, **kwargs):
        meta = self.meta_get(key)
        if meta is None:
            raise KeyError

        self.handler(meta["handler"], meta["handler_lua"], "get", key, kwargs)

        if meta["lock"] in [self.valid_lock_type(x) for x in ["rw", "ro"]]:
            return self.data_handler(meta["type"]).get(key, meta["data_id"], **kwargs)
        else:
            raise LockException

    def set(self, key, **kwargs):
        with _DataLock(self, key) as meta:
            self.handler(meta["handler"], meta["handler_lua"], "set", key, kwargs)
            return self.data_handler(meta["type"]).set(key, meta["data_id"], **kwargs)

    def rem(self, key, **kwargs):
        with _DataLock(self, key, mode="na") as meta:
            self.handler(meta["handler"], meta["handler_lua"], "rem", key, kwargs)
            return self.data_handler(meta["type"]).rem(key, meta["data_id"], **kwargs)


class Local(_Registry):
    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        self.rd = self.kernel.redis_l
        self.prefix = ""


class Global(_Registry):
    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        self.rd = self.kernel.redis_g
        self.prefix = "global"
