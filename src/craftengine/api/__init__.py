# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import logging
import traceback
import time

from craftengine.utils.exceptions import ModuleException
import craftengine
from craftengine.utils import registry


class ApiException(ModuleException):
    pass


class PermissionException(ApiException):
    pass


class AuthException(ApiException):
    pass


class Api(object):
    @classmethod
    def perms(cls, *perms):
        def wrapper(f):
            def wrap(name, *args, **kwargs):
                if name is None:
                    return f(*args, **kwargs)

                plugin = None
                for pl in registry.GlobalRegistry().get("kernel.plugins").values():
                    if name == pl["name"]:
                        plugin = pl

                if not plugin:
                    raise PermissionException

                for p in perms:
                    if p in plugin["permissions"]:
                        return f(*args, **kwargs)
                else:
                    return f(*args, **kwargs)
                raise PermissionException
            return wrap
        return wrapper

    @staticmethod
    def execute(data):
        identificator = None
        try:
            method, args, kwargs, identificator = data[:4]

            plugin = None
            if method != "kernel.auth":
                try:
                    pls = registry.Registry().get("api.plugins")
                    plugin = list(pls.keys())[list(pls.values()).index(request.fileno)]
                except ValueError:
                    raise AuthException

            function = registry.Registry().hget("api.methods", method)
            data = function(plugin, *args, **kwargs)
        except Exception as e:
            error = ["%s.%s" % (getattr(e, "__module__", "__built_in__"), e.__class__.__name__), str(e), traceback.extract_tb(e.__traceback__)]
            data = []
            logging.exception(e)
        else:
            error = []

        if identificator is None:
            return None

        return identificator, error, data

    @staticmethod
    def add(name, method=None):
        def wrapper(f):
            _add(name, f)

            def wrap(*args, **kwargs):
                return f(*args, **kwargs)
            return wrap

        def _add(name, method):
            registry.Registry().hset("api.methods", name, method)

        if method is not None:
            _add(name, method)
        else:
            return wrapper

    @staticmethod
    def request(plugin, method, args=None, kwargs=None, callback=None):
        args = () if args is None else args
        kwargs = {} if kwargs is None else kwargs
        identificator = None if callback is None else time.time()
        request = [
            method,
            args,
            kwargs,
            identificator,
        ]

        if identificator is not None:
            registry.Registry().hset("api.requests", identificator, callback)

        fn = registry.Registry().hget("api.plugins", plugin)
        cli = registry.Registry().hget("api.pool", fn)
        cli.response.append(request)
        cli.stream(cli.STREAMOUT)
        return identificator

    @staticmethod
    def response(data):
        try:
            callback = registry.Registry().hget("api.requests", data[0])
        except KeyError:
            return

        err_handler = lambda err, id: logging.error("%s: %s\nTraceback: %s", *err)
        if isinstance(callback, tuple):
            callback, err_handler = callback

        try:
            if len(data[1]) == 0:
                callback(data[2], data[0])
            else:
                err_handler(data[1], data[0])
        except:
            logging.exception("Could not process response")


class Kernel(Api):
    @staticmethod
    @Api.add("kernel.auth")
    @Api.perms()
    def auth(plugin, token):
        pl = None
        pls = registry.GlobalRegistry().get("kernel.plugins")
        for pli in pls.values():
            if pli["name"] == plugin:
                pl = pli

        if pl is None:
            return False

        if pl["token"] == token:
            registry.Registry().hset("api.plugins", pl["name"], request.fileno)
            return True
        else:
            return False

    @staticmethod
    @Api.add("kernel.exit")
    @Api.perms("kernel", "kernel.exit")
    def exit():
        craftengine.Kernel().exit()
        return True

    @staticmethod
    @Api.add("kernel.env")
    @Api.perms("kernel", "kernel.env")
    def env():
        return craftengine.Kernel().env


class Registry(Api):
    @staticmethod
    @Api.add("registry.local.get")
    @Api.perms("registry", "registry.local", "registry.local.get")
    def get(key):
        return registry.Registry().get(key)

    @staticmethod
    @Api.add("registry.local.set")
    @Api.perms("registry", "registry.local", "registry.local.set")
    def set(key, data, namespace=None, has_permission=None):
        return registry.Registry().set(key, data, namespace, has_permission)

    @staticmethod
    @Api.add("registry.local.delete")
    @Api.perms("registry", "registry.local", "registry.local.delete")
    def delete(key):
        return registry.Registry().delete(key)

    @staticmethod
    @Api.add("registry.local.stack")
    @Api.perms("registry", "registry.local", "registry.local.stack")
    def stack(key, namespace=None, has_permission=None):
        return registry.Registry().stack(key, namespace, has_permission)

    @staticmethod
    @Api.add("registry.local.puth")
    @Api.perms("registry", "registry.local", "registry.local.rpush")
    def rpush(key, data):
        return registry.Registry().rpush(key, data)

    @staticmethod
    @Api.add("registry.local.rpop")
    @Api.perms("registry", "registry.local", "registry.local.rpop")
    def rpop(key):
        return registry.Registry().rpop(key)

    @staticmethod
    @Api.add("registry.local.lpush")
    @Api.perms("registry", "registry.local", "registry.local.lpush")
    def lpush(key, data):
        return registry.Registry().lpush(key, data)

    @staticmethod
    @Api.add("registry.local.lpop")
    @Api.perms("registry", "registry.local", "registry.local.lpop")
    def lpop(key):
        return registry.Registry().lpop(key)

    @staticmethod
    @Api.add("registry.local.hash")
    @Api.perms("registry", "registry.local", "registry.local.hash")
    def hash(key, namespace=None, has_permission=None):
        return registry.Registry().hash(key, namespace, has_permission)

    @staticmethod
    @Api.add("registry.local.hget")
    @Api.perms("registry", "registry.local", "registry.local.hget")
    def hget(key, index):
        return registry.Registry().hget(key, index)

    @staticmethod
    @Api.add("registry.local.hset")
    @Api.perms("registry", "registry.local", "registry.local.hset")
    def hset(key, index, data):
        return registry.Registry().hset(key, index, data)

    @staticmethod
    @Api.add("registry.local.hdelete")
    @Api.perms("registry", "registry.local", "registry.local.hdelete")
    def hdelete(key, index):
        return registry.Registry().hdelete(key, index)


class GlobalRegistry(Api):
    @staticmethod
    @Api.add("registry.global.get")
    @Api.perms("registry", "registry.global", "registry.global.get")
    def get(key):
        return registry.GlobalRegistry().get(key)

    @staticmethod
    @Api.add("registry.global.set")
    @Api.perms("registry", "registry.global", "registry.global.set")
    def set(key, data, namespace=None, has_permission=None):
        return registry.GlobalRegistry().set(key, data, namespace, has_permission)

    @staticmethod
    @Api.add("registry.global.delete")
    @Api.perms("registry", "registry.global", "registry.global.delete")
    def delete(key):
        return registry.GlobalRegistry().delete(key)

    @staticmethod
    @Api.add("registry.global.stack")
    @Api.perms("registry", "registry.global", "registry.global.stack")
    def stack(key, namespace=None, has_permission=None):
        return registry.GlobalRegistry().stack(key, namespace, has_permission)

    @staticmethod
    @Api.add("registry.global.rpush")
    @Api.perms("registry", "registry.global", "registry.global.rpush")
    def rpush(key, data):
        return registry.GlobalRegistry().rpush(key, data)

    @staticmethod
    @Api.add("registry.global.rpop")
    @Api.perms("registry", "registry.global", "registry.global.rpop")
    def rpop(key):
        return registry.GlobalRegistry().rpop(key)

    @staticmethod
    @Api.add("registry.global.lpush")
    @Api.perms("registry", "registry.global", "registry.global.lpush")
    def lpush(key, data):
        return registry.GlobalRegistry().lpush(key, data)

    @staticmethod
    @Api.add("registry.global.lpop")
    @Api.perms("registry", "registry.global", "registry.global.lpop")
    def lpop(key):
        return registry.GlobalRegistry().lpop(key)

    @staticmethod
    @Api.add("registry.global.hash")
    @Api.perms("registry", "registry.global", "registry.global.hash")
    def hash(key, namespace=None, has_permission=None):
        return registry.GlobalRegistry().hash(key, namespace, has_permission)

    @staticmethod
    @Api.add("registry.global.hget")
    @Api.perms("registry", "registry.global", "registry.global.hget")
    def hget(key, index):
        return registry.GlobalRegistry().hget(key, index)

    @staticmethod
    @Api.add("registry.global.hset")
    @Api.perms("registry", "registry.global", "registry.global.hset")
    def hset(key, index, data):
        return registry.GlobalRegistry().hset(key, index, data)

    @staticmethod
    @Api.add("registry.global.hdelete")
    @Api.perms("registry", "registry.global", "registry.global.hdelete")
    def hdelete(key, index):
        return registry.GlobalRegistry().hdelete(key, index)


class PermanentRegistry(Api):
    @staticmethod
    @Api.add("registry.permanent.get")
    @Api.perms("registry", "registry.permanent", "registry.permanent.get")
    def get(key):
        return registry.PermanentRegistry().get(key)

    @staticmethod
    @Api.add("registry.permanent.set")
    @Api.perms("registry", "registry.permanent", "registry.permanent.set")
    def set(key, data, namespace=None, has_permission=None):
        return registry.PermanentRegistry().set(key, data, namespace, has_permission)

    @staticmethod
    @Api.add("registry.permanent.delete")
    @Api.perms("registry", "registry.permanent", "registry.permanent.delete")
    def delete(key):
        return registry.PermanentRegistry().delete(key)

    @staticmethod
    @Api.add("registry.permanent.stack")
    @Api.perms("registry", "registry.permanent", "registry.permanent.stack")
    def stack(key, namespace=None, has_permission=None):
        return registry.PermanentRegistry().stack(key, namespace, has_permission)

    @staticmethod
    @Api.add("registry.permanent.rpush")
    @Api.perms("registry", "registry.permanent", "registry.permanent.rpush")
    def rpush(key, data):
        return registry.PermanentRegistry().rpush(key, data)

    @staticmethod
    @Api.add("registry.permanent.rpop")
    @Api.perms("registry", "registry.permanent", "registry.permanent.rpop")
    def rpop(key):
        return registry.PermanentRegistry().rpop(key)

    @staticmethod
    @Api.add("registry.permanent.lpush")
    @Api.perms("registry", "registry.permanent", "registry.permanent.lpush")
    def lpush(key, data):
        return registry.PermanentRegistry().lpush(key, data)

    @staticmethod
    @Api.add("registry.permanent.lpop")
    @Api.perms("registry", "registry.permanent", "registry.permanent.lpop")
    def lpop(key):
        return registry.PermanentRegistry().lpop(key)

    @staticmethod
    @Api.add("registry.permanent.hash")
    @Api.perms("registry", "registry.permanent", "registry.permanent.hash")
    def hash(key, namespace=None, has_permission=None):
        return registry.PermanentRegistry().hash(key, namespace, has_permission)

    @staticmethod
    @Api.add("registry.permanent.hget")
    @Api.perms("registry", "registry.permanent", "registry.permanent.hget")
    def hget(key, index):
        return registry.PermanentRegistry().hget(key, index)

    @staticmethod
    @Api.add("registry.permanent.hset")
    @Api.perms("registry", "registry.permanent", "registry.permanent.hset")
    def hset(key, index, data):
        return registry.PermanentRegistry().hset(key, index, data)

    @staticmethod
    @Api.add("registry.permanent.hdelete")
    @Api.perms("registry", "registry.permanent", "registry.permanent.hdelete")
    def hdelete(key, index):
        return registry.PermanentRegistry().hdelete(key, index)
