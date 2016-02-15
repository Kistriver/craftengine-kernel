# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import logging
import traceback

from craftengine.utils import rpc

# Service
# ["connect", "service", "instance", "token", {"params": True}] <-
# ["connect", "status"] ->
# ["request", ["node", "service", "instance"], "method", ("args"), {"kwargs": True}, "rid"] <-
# ["request", ["req_from_n", "req_from_s", "req_from_i"], "method", ("args"), {"kwargs": True}, "rid"] ->
# ["response", "data", "error", "rid"] <->


class Router(rpc.Router):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._binds = {
            "request": self.process_request,
            "response": self.process_response,
            "connect": self.process_connect,
        }

    def request(self, service_info, requested_service_info, requested_sock, method, args, kwargs, rid):
        node, service, instance = self.name, service_info["service"][0], service_info["service"][1]
        fn = requested_service_info["fn"]
        requested_sock[3].append(["request", [node, service, instance], method, args, kwargs, rid])

        if rid is not None:
            requested_service_info["responses"][rid] = service_info

        self.epollout(fn)

    def response(self, service_info, sock, data, error, rid):
        response = ["response", data, error, rid]
        try:
            responsed_service_info = service_info["responses"][rid]
            fn = responsed_service_info["fn"]
            sock[3].append(response)
            self.epollout(fn)
        except KeyError:
            logging.exception("")

    def process_connect(self, fn, data):
        fn = int(fn)
        (service, instance), token, params = data
        service_data = self.kernel.service.list().get(service)
        if service_data is None:
            raise rpc.RouteException("Service doesn't exist")

        if token != service_data["token"]:
            raise rpc.RouteException("Invalid token")

        if not 1 <= instance <= service_data.get("scale", 1):
            raise rpc.RouteException("Unexpected instance")

        info = self.create_info(fn=fn, service=(service, instance))
        self.put_service(service, instance, info)
        logging.info("Service authed: `%s`[%i]" % (service, instance))
        self._sockets[fn][2] = (service, instance)

    def process_request(self, service_info, data):
        rid = None
        try:
            (node, service, instance), method, args, kwargs, rid = data
            logging.debug(data)
            instance = self.BALANCED_INSTANCE if instance is None else int(instance)
            if node not in ["__local__", self.name]:
                self.kernel.node.proxy(
                    node,
                    self,
                    "request",
                    data,
                )
            else:
                requested_service_info = self.get_service(service, instance)
                self.request(
                    service_info,
                    requested_service_info,
                    self._sockets[requested_service_info["fn"]],
                    method,
                    args,
                    kwargs,
                    rid,
                )
        except Exception as e:
            logging.exception(e)
            if rid is None:
                self.socket_close(service_info["fn"])
            else:
                error = [
                    "%s.%s" % (
                        getattr(e, "__module__", "__built_in__"),
                        e.__class__.__name__,
                    ),
                    str(e),
                    traceback.format_exc(),
                ]

                self.response(
                    service_info,
                    self._sockets[service_info["fn"]],
                    None,
                    error,
                    rid,
                )

    def process_response(self, service_info, data):
        rid = None
        try:
            data, error, rid = data
            logging.debug(data)
            self.response(
                service_info,
                self._sockets[service_info["fn"]],
                data,
                error,
                rid,
            )
        except Exception as e:
            logging.exception(e)
            if rid is not None:
                error = [
                    "%s.%s" % (
                        getattr(e, "__module__", "__built_in__"),
                        e.__class__.__name__,
                    ),
                    str(e),
                    traceback.format_exc(),
                ]

                self.response(
                    service_info,
                    self._sockets[service_info["fn"]],
                    None,
                    error,
                    rid,
                )


class Rpc(rpc.Rpc):
    router = Router
    port = 1998
    host = "0.0.0.0"
