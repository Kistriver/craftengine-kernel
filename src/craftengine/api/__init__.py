# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

"""
========================================
ATTENTION!

HAVE TO REFACTOR
========================================
"""

import logging
import traceback
import time
import threading

from craftengine.utils.exceptions import ModuleException
import craftengine
from craftengine.utils import event


class ApiException(ModuleException):
    pass


class PermissionException(ApiException):
    pass


class AuthException(ApiException):
    pass


class Api(object):
    proxy_property = lambda x: lambda *args, **kwargs: x
    proxy_method = lambda x: lambda request, plugin, *args, **kwargs: x(*args, **kwargs)

    @staticmethod
    def bind(name, method, perms=None):
        perms = [] if perms is None else perms
        if not isinstance(perms, list):
            perms = [perms]
        registry.Registry().hset("api.methods", name, (method, perms))

    @staticmethod
    def has_permission(perms, reqs):
        perms_splitter = lambda x: [i.split(".") for i in x]
        def perms_merger(perms):
            merged = {}
            for perm in perms:
                if len(perm) == 0:
                    merged["*"] = True
                    continue
                if perm[0] not in merged.keys():
                    merged[perm[0]] = []

                if perm[0] == "*":
                    merged["*"] = True
                else:
                    merged[perm[0]].append(perm[1:])

            for k in merged.keys():
                if merged[k] is not True:
                    merged[k] = perms_merger(merged[k])

            return merged

        perms = perms_merger(perms_splitter(perms))
        reqs = perms_splitter(reqs)

        getter = lambda src, x: src.get("*", src.get(x, False))
        for reqsi in reqs:
            src = perms
            for r in reqsi:
                g = getter(src, r)
                if isinstance(g, dict):
                    src = g
                elif g:
                    return True
                else:
                    raise PermissionException(str(".".join(reqsi)))
        else:
            return True

    @staticmethod
    def execute(data, request):
        exc_methods = ["auth"]
        identificator = None
        try:
            method, args, kwargs, identificator = data[:4]

            plugin = None
            if method not in exc_methods:
                try:
                    pls = registry.Registry().get("api.authed")
                    plugin = pls[request.fileno]
                except KeyError:
                    raise AuthException

            function, perms_reqs = registry.Registry().hget("api.methods", method)
            if method not in exc_methods:
                pls = registry.GlobalRegistry().get("kernel.plugins")
                for pli in pls.values():
                    if pli["name"] == plugin:
                        pl = pli
                threading.current_thread().setName(pl["name"])
                Api.has_permission(pl["permissions"], perms_reqs)

            data = function(request, plugin, *args, **kwargs)
        except Exception as e:
            error = [
                "%s.%s" % (
                    getattr(e, "__module__", "__built_in__"),
                    e.__class__.__name__,
                ),
                str(e),
                traceback.format_exc(),
            ]
            data = []
            logging.exception(e)
        else:
            error = []

        if identificator is None:
            return None

        return identificator, error, data

    @staticmethod
    def request(node, service, plugin, method, args=None, kwargs=None, callback=None):
        args = () if args is None else args
        kwargs = {} if kwargs is None else kwargs
        identificator = None if callback is None else "%s:%s" % (service, time.time())
        request = [
            node,
            service,
            method,
            args,
            kwargs,
            identificator,
        ]

        if identificator is not None:
            registry.Registry().hset("api.requests", identificator, callback)

        fn = registry.Registry().hget("api.plugins", plugin)
        cli = registry.Registry().hget("api.pool", fn[0])
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


def kernel_auth(request, p, plugin, token):
    pl = None
    pls = registry.GlobalRegistry().get("kernel.plugins")
    for pli in pls.values():
        if pli["name"] == plugin:
            pl = pli

    if pl is None:
        return False

    if pl["token"] == token:
        with registry.Registry()._lock:
            try:
                a = registry.Registry().hget("api.plugins", pl["name"])
            except KeyError:
                a = []
            a.append(request.fileno)
            registry.Registry().hset("api.plugins", pl["name"], a)
        registry.Registry().hset("api.authed", request.fileno, pl["name"])
        return True
    else:
        return False


def event_register(request, plugin, *args, **kwargs):
    if len(args) > 1:
        args = (args[0],) + ((args[1], request, plugin),) + tuple(*args[2:])

    if "callback" in kwargs.keys():
        kwargs["callback"] = kwargs["callback"], request, plugin

    return event.Event().register(*args, **kwargs)

Api.bind("auth", kernel_auth)
Api.bind("logger.log", Api.proxy_method(logging.log))
Api.bind("logger.debug", Api.proxy_method(logging.debug))
Api.bind("logger.info", Api.proxy_method(logging.info))
Api.bind("logger.warning", Api.proxy_method(logging.warning))
Api.bind("logger.error", Api.proxy_method(logging.error))
Api.bind("logger.critical", Api.proxy_method(logging.critical))
Api.bind(
    "env",
    Api.proxy_property(craftengine.Kernel().env),
    "kernel.env"
)

for ns in [
    "local",
    "global",
    "permanent",
]:
    for m in [
        "get",
        "set",
        "delete",
        "stack",
        "rpush",
        "rpop",
        "lpush",
        "lpop",
        "hash",
        "hget",
        "hset",
        "hdelete",
    ]:
        Api.bind(
            "registry.%s.%s" % (ns, m),
            Api.proxy_method(registry.Registry().__getattribute__(m)),
            "registry.%s.%s"
        )

Api.bind(
    "event.register",
    event_register,
    "event.register"
)
Api.bind("event.initiate", Api.proxy_method(event.Event().initiate), "event.initiate")
Api.bind("event.info", Api.proxy_method(event.Event().info), "event.info")
