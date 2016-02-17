# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import socket
import select
import logging
import threading
import traceback
import time

from ddp import DdpSocket
from craftengine.exceptions import ModuleException
from craftengine.modules import KernelModule

# Service
# ["connect", "service", "instance", "token", {"params": True}] <-
# ["connect", "status"] ->
# ["request", ["node", "service", "instance"], "method", ("args"), {"kwargs": True}, "rid"] <-
# ["request", ["req_from_n", "req_from_s", "req_from_i"], "method", ("args"), {"kwargs": True}, "rid"] ->
# ["response", "data", "error", "rid"] <->


# Node
# ["connect_node", "node_name", "token", {"params": True}] <-
# ["connect_node" "status"] ->
# ["proxy", "node_name", ["req_from_n", "req_from_s", "req_from_i"], "command", "rid"] <->
# ["proxy_status", "error", "rid"] <-


class RpcException(ModuleException):
    pass


class RouteException(RpcException):
    pass


class ServiceException(RpcException):
    pass


class NodeException(RpcException):
    pass


class BaseHandler(object):
    def __init__(self, router):
        self.rpc = router.rpc
        self.kernel = router.rpc.kernel
        self.router = router
        self.binds = {}

    def socket_receive(self, fn):
        sock = self.router.get_socket(fn)["socket"]
        try:
            data = DdpSocket().decode(sock)
        except IndexError as e:
            logging.debug(e)
            self.socket_close(fn)
            raise RouteException("Problems with connection")

        self.process(fn, data)

    def socket_send(self, fn):
        sock_info = self.router.get_socket(fn)
        sock, send_data = sock_info["socket"], sock_info["send_data"]
        while len(send_data) > 0:
            data = send_data.pop(0)
            try:
                DdpSocket().encode(data, socket=sock)
            except IndexError as e:
                logging.debug(e)
                self.socket_close(fn)
                raise RouteException("Problems with connection")
        self.router.epollin(fn)

    def socket_close(self, fn):
        sock = self.router.get_socket(fn)["socket"]
        self.rpc.epoll.unregister(fn)
        self.router.del_socket(fn)
        sock.close()

    def process(self, fn, data, add=None):
        case = data.pop(0)
        if case in self.binds.keys():
            self.binds[case](fn, data, add)
        else:
            raise RouteException("Unexpected route case: %s" % case)


class RegularHandler(BaseHandler):
    PROCESS_SERVICE = "connect"
    PROCESS_NODE = "connect_node"

    def __init__(self, router):
        super().__init__(router)
        self.binds = {
            self.PROCESS_SERVICE: self.process_service,
            self.PROCESS_NODE: self.process_node,
        }

    def socket_close(self, fn):
        host, port = self.router.get_socket(fn)["address"]
        logging.info("Closed connection (%s:%i)" % (host, port))
        super().socket_close(fn)

    def process_service(self, fn, data, _=None):
        service, instance, token, params = data
        service_data = self.kernel.service.list().get(service)
        if service_data is None:
            raise RouteException("Service doesn't exist")

        if token != service_data["token"]:
            raise RouteException("Invalid token")

        if not 1 <= instance <= service_data.get("scale", 1):
            raise RouteException("Unexpected instance")

        services_handler = self.router.get_handler(self.router.SOCK_SERVICE)
        services_handler.put_service(service, instance, fn)
        logging.info("Service authed: `%s`[%i]" % (service, instance))

    def process_node(self, fn, data, _=None):
        node, token, params = data
        node_data = self.kernel.g.get("kernel/nodes", keys=[node]).get(node)
        if node_data is None:
            raise RouteException("Node doesn't exist")

        if token != node_data["token"]:
            raise RouteException("Invalid token")

        node_handler = self.router.get_handler(self.router.SOCK_NODE)
        node_handler.put_node(node, fn)
        logging.info("Node authed: `%s`" % node)


class ServiceHandler(BaseHandler):
    BALANCED_INSTANCE = 0

    PROCESS_REQUEST = "request"
    PROCESS_RESPONSE = "response"

    def __init__(self, router):
        super().__init__(router)
        self.binds = {
            self.PROCESS_REQUEST: self.process_request,
            self.PROCESS_RESPONSE: self.process_response,
        }
        self._services = {}
        self._services_fn = {}
        self._balancing_instances = {}
        self._lock = threading.RLock()

    def socket_close(self, fn):
        service, instance = self.get_service_by_socket(fn)
        logging.info("Closed connection with service `%s`[%i]" % (service, instance))
        super().socket_close(fn)

    def request(self, fn, req_from, req, method, args, kwargs, rid):
        node, service, instance = req
        requested_fn = self.get_service(service, instance)
        requested_sock_info = self.router.get_socket(requested_fn)

        requested_sock_info["send_data"].append([
            self.PROCESS_REQUEST,
            req_from,
            method,
            args,
            kwargs,
            rid,
        ])

        if rid is not None:
            requested_sock_info["responses"][rid] = (fn, req_from)

        self.router.epollout(requested_fn)

    def process_request(self, fn, data, add=None):
        if add is None:
            from_service = self.get_service_by_socket(fn)
            req_from = self.router.name, from_service[0], from_service[1]
        else:
            req_from = add

        rid = None
        try:
            (node, service, instance), method, args, kwargs, rid = data
            logging.debug(data)
            instance = self.BALANCED_INSTANCE if instance is None else int(instance)
            if node not in ["__local__", self.router.name]:
                handler = self.router.get_handler(self.router.SOCK_NODE)
                node_fn = handler.get_node(node)
                handler.process(node_fn, [
                    handler.PROCESS_PROXY,
                    node,
                    req_from,
                    [self.PROCESS_REQUEST, (node, service, instance), method, args, kwargs, rid],
                    self.router.generate_id(),
                ])

                if rid is not None:
                    sock_info = self.router.get_socket(node_fn)
                    sock_info["responses"][rid] = (fn, req_from)
            else:
                req = node, service, instance

                self.request(fn, req_from, req, method, args, kwargs, rid)
        except Exception as e:
            logging.exception(e)
            if rid is None:
                self.socket_close(fn)
            else:
                error = [
                    "%s.%s" % (
                        getattr(e, "__module__", "__built_in__"),
                        e.__class__.__name__,
                    ),
                    str(e),
                    traceback.format_exc(),
                ]

                self.process(fn, [self.PROCESS_RESPONSE, None, error, rid])

    def process_response(self, fn, data, _=None):
        response, error, rid = data
        sock_info = self.router.get_socket(fn)
        response_fn, req_from = sock_info["responses"][rid]

        response_sock_info = self.router.get_socket(response_fn)
        if response_sock_info["type"] == self.router.SOCK_SERVICE:
            response_sock_info["send_data"].append([
                self.PROCESS_RESPONSE,
                response,
                error,
                rid,
            ])
            self.router.epollout(response_fn)
        else:
            from_service = self.get_service_by_socket(fn)
            handler = self.router.get_handler(self.router.SOCK_NODE)
            resp_from = self.router.name, from_service[0], from_service[1]
            handler.process(response_fn, [
                handler.PROCESS_PROXY,
                req_from[0],
                resp_from,
                [self.PROCESS_RESPONSE, response, error, rid],
                self.router.generate_id(),
            ])

        del sock_info["responses"][rid]

    def put_service(self, service, instance, fn):
        try:
            self.get_service(service)
        except RouteException:
            self._services[service] = {instance: fn}
            self.router.set_type_socket(fn, self.router.SOCK_SERVICE)
            self._services_fn[fn] = (service, instance)
        else:
            try:
                self.get_service(service, instance)
            except RouteException:
                pass
            else:
                self.socket_close(self._services[service][instance])
            finally:
                self._services[service][instance] = fn
                self.router.set_type_socket(fn, self.router.SOCK_SERVICE)
                self._services_fn[fn] = (service, instance)

    def get_service(self, service, instance=None):
        try:
            instances = self._services[service]
        except KeyError:
            raise RouteException("Service doesn't exist")
        if instance is None:
            return instances
        elif instance == self.BALANCED_INSTANCE:
            with self._lock:
                instances_names = list(instances.keys())
                length = len(instances)
                counter = self._balancing_instances.get(service, -1) + 1
                if counter >= length:
                    counter %= length
                self._balancing_instances[service] = counter
                return instances[instances_names[counter]]
        else:
            try:
                return instances[instance]
            except KeyError:
                raise RouteException("Unexpected instance")

    def del_service(self, service, instance):
        try:
            fn = self._services[service][instance]
            self.socket_close(fn)
            del self._services_fn[fn]
        except KeyError:
            pass
        del self._services[service][instance]

    def get_service_by_socket(self, fn):
        return self._services_fn[fn]


class NodeHandler(BaseHandler):
    PROCESS_PROXY = "proxy"
    PROCESS_PROXY_STATUS = "proxy_status"

    def __init__(self, router):
        super().__init__(router)
        self.binds = {
            self.PROCESS_PROXY: self.process_proxy,
            self.PROCESS_PROXY_STATUS: self.process_proxy_status,
        }
        self._nodes = {}
        self._nodes_fn = {}
        self._lock = threading.RLock()

    def socket_close(self, fn):
        node = self.get_node_by_socket(fn)
        logging.info("Closed connection with node `%s`" % node)
        super().socket_close(fn)

    def process_proxy(self, fn, data, _=None):
        node, req_from, command, rid = data
        if node == self.router.name:
            handler = self.router.get_handler(self.router.SOCK_SERVICE)
            handler.process(fn, command, req_from)
        else:
            proxy_node = self.get_node(node)
            sock_info = self.router.get_socket(proxy_node)
            sock_info["send_data"].append([
                self.PROCESS_PROXY,
                node,
                req_from,
                command,
                rid,
            ])
            self.router.epollout(proxy_node)

    def process_proxy_status(self, fn, data, _=None):
        error, rid = data

    def put_node(self, node, fn):
        try:
            self.get_node(node)
            self.socket_close(self._nodes[node])
        except RouteException:
            pass

        self._nodes[node] = fn
        self.router.set_type_socket(fn, self.router.SOCK_NODE)
        self._nodes_fn[fn] = node

    def get_node(self, node):
        try:
            return self._nodes[node]
        except KeyError:
            raise RouteException("Node doesn't exist")

    def del_node(self, node):
        try:
            fn = self._nodes[node]
            self.socket_close(fn)
            del self._nodes_fn[fn]
        except KeyError:
            pass
        del self._nodes[node]

    def get_node_by_socket(self, fn):
        return self._nodes_fn[fn]


class Router(object):
    SOCK_REG = 0
    SOCK_SERVICE = 1
    SOCK_NODE = 2

    def __init__(self, rpc):
        self.rpc = rpc
        self.kernel = self.rpc.kernel
        # TODO
        # self.name = self.kernel.l.get("kernel/env", keys=["name"])["name"]
        self.name = self.kernel.env["CE_NODE_NAME"]
        self._sockets = {}
        self._handlers = {
            self.SOCK_REG: RegularHandler(self),
            self.SOCK_SERVICE: ServiceHandler(self),
            self.SOCK_NODE: NodeHandler(self),
        }

    def add_socket(self, sock, address, sock_type=None):
        sock_type = self.SOCK_REG if sock_type is None else sock_type
        # [socket, address, type, send_data, responses]
        self.rpc.epoll.register(sock, select.EPOLLIN)
        self._sockets[sock.fileno()] = [sock, address, sock_type, [], {}]

    def get_socket(self, fn):
        sock, address, sock_type, send_data, responses = self._sockets[fn]
        return {
            "socket": sock,
            "address": address,
            "type": sock_type,
            "send_data": send_data,
            "responses": responses,
        }

    def set_type_socket(self, fn, t):
        self._sockets[fn][2] = t

    def del_socket(self, fn):
        del self._sockets[fn]

    def get_handler(self, t):
        return self._handlers[t]

    def epoll(self, event, file_no):
        try:
            sock_info = self.get_socket(file_no)
            handler = self.get_handler(sock_info["type"])
        except KeyError:
            logging.exception(file_no)
            return

        try:
            if event & select.EPOLLIN:
                handler.socket_receive(file_no)
            elif event & select.EPOLLOUT:
                handler.socket_send(file_no)
            elif event & select.EPOLLHUP:
                handler.socket_close(file_no)
        except Exception as e:
            logging.exception(e)
            handler.socket_close(file_no)

    def epollin(self, fn):
        self.rpc.epoll.modify(fn, select.EPOLLIN)

    def epollout(self, fn):
        self.rpc.epoll.modify(fn, select.EPOLLOUT)

    def generate_id(self):
        return "%s" % (time.time())

    def stop(self):
        for fn in self._sockets.copy().keys():
            try:
                sock_info = self.get_socket(fn)
                handler = self.get_handler(sock_info["type"])
                handler.socket_close(fn)
            except Exception as e:
                logging.exception(e)


class Rpc(KernelModule):
    _stop = None

    socket = None
    router = None
    epoll = None

    host = "0.0.0.0"
    port = 2011

    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        self.router = Router(self)
        self.host = self.host if kwargs.get("host") is None else kwargs.get("host")
        self.port = self.port if kwargs.get("port") is None else int(kwargs.get("port"))
        self._stop = False
        self._alive = None

    def serve(self):
        logging.info("Starting server (%s:%i)" % (self. host, self.port))
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, int(self.port)))
        self.socket.listen(1)
        self.socket.setblocking(0)

        self.epoll = select.epoll()
        self.epoll.register(self.socket.fileno(), select.EPOLLIN)
        self._alive = True

        try:
            while self.alive:
                events = self.epoll.poll(1)
                for file_no, event in events:
                    if file_no == self.socket.fileno():
                        try:
                            connection, address = self.socket.accept()
                            self.router.add_socket(sock=connection, address=address)
                        except Exception as e:
                            logging.exception(e)
                    else:
                        try:
                            self.router.epoll(event, file_no)
                        except Exception as e:
                            logging.exception(e)
        except Exception as e:
            self.stop()
            if self.alive:
                logging.exception(e)
                self.serve()
        else:
            self.stop()

    def node(self, node):
        try:
            self_node = self.router.name
            node_data = self.kernel.g.get("kernel/nodes", keys=[node]).get(node)
            self_node_data = self.kernel.g.get("kernel/nodes", keys=[self_node]).get(self_node)
            address = tuple(node_data["address"])
            connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            connection.connect(address)
            self.router.add_socket(sock=connection, address=address, sock_type=self.router.SOCK_NODE)
            fn = connection.fileno()
            self.router.get_handler(self.router.SOCK_NODE).put_node(node, fn)
            sock_info = self.router.get_socket(fn)
            token = self_node_data["token"]

            sock_info["send_data"].append([
                RegularHandler.PROCESS_NODE,
                self_node,
                token,
                {},
            ])
            self.router.epollout(fn)
        except Exception as e:
            logging.exception(e)

    def stop(self):
        if self._stop:
            return
        self._stop = True

        try:
            self.router.stop()
        except Exception as e:
            logging.exception(e)

        try:
            self.epoll.unregister(self.socket.fileno())
        except Exception as e:
            logging.exception(e)
        try:
            self.epoll.close()
        except Exception as e:
            logging.exception(e)
        try:
            self.socket.close()
        except Exception as e:
            logging.exception(e)

    def exit(self, *args, **kwargs):
        super().exit(*args, **kwargs)
        self.stop()
