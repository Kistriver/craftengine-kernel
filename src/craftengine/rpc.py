# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import socket
import select
import logging
import threading
import traceback
from multiprocessing.dummy import Pool as ThreadPool
from ddp import DdpSocket


from craftengine.utils.exceptions import ModuleException
from craftengine import KernelModule

# RPC
# ["connect", "service", "instance", "token", {"params": True}]
# ["connect_node", "node_name", "instance", "token", {"params": True}]
# ["request", ["node", "service", "instance"], "method", ("args"), {"kwargs": True}, "identificator"]
# ["response", "data", "error", "identificator"]

# Service
# ["request", ["req_node", "req_service", "req_instance"], "method", ("args"), {"kwargs": True}, "identificator"]
# ["response", "data", "error", "identificator"]


class RpcException(ModuleException):
    pass


class RouteException(RpcException):
    pass


class ServiceException(RpcException):
    pass


class Service(object):
    socket = None
    rpc = None

    host = None
    port = None

    node = None
    service = None
    instance = None

    send_data = None
    responses = None

    def __init__(self, rpc, connection, address):
        logging.debug("New connection: (%s:%i)" % address)
        self.rpc = rpc
        self.socket = connection
        self.host, self.port = address
        self.send_data = []
        self.responses = {}
        self.rpc.epoll.register(self.socket.fileno(), select.EPOLLIN)

    def epollin(self):
        self.rpc.epoll.modify(self.socket.fileno(), select.EPOLLIN)

    def epollout(self):
        self.rpc.epoll.modify(self.socket.fileno(), select.EPOLLOUT)

    def request(self, service_obj, method, args, kwargs, identificator):
        node, service, instance = service_obj.node, service_obj.service, service_obj.instance
        self.send_data.append(["request", [node, service, instance], method, args, kwargs, identificator])

        if identificator is not None:
            self.responses[identificator] = service_obj

        self.epollout()

    def response(self, data, error, identificator):
        response = ["response", data, error, identificator]
        try:
            self.responses[identificator].send_data.append(response)
        except KeyError:
            self.send_data.append(response)

        self.epollout()

    def auth(self, node, service, instance):
        logging.info("Service authed: `%s`[%i]" % (service, instance))
        self.node = node
        self.service = service
        self.instance = instance


class Router(object):
    def __init__(self, rpc):
        self.rpc = rpc
        self.kernel = self.rpc.kernel
        # TODO
        # self.name = self.kernel.l.get("kernel/env", keys=["name"])["name"]
        self.name = self.kernel.env["CE_NODE_NAME"]
        self._connected = {}
        self._services = {}
        self._connections_balancing = {}
        self._connected_lock = threading.RLock()

    def new_connection(self, sock):
        connection, address = sock.accept()
        service = Service(self.rpc, connection, address)
        self._services[connection.fileno()] = service

    def epoll(self, event, file_no):
        try:
            service = self._services[file_no]
        except KeyError:
            return

        try:
            service = self._services[file_no]

            if event & select.EPOLLIN:
                self.socket_receive(service)
            elif event & select.EPOLLOUT:
                self.socket_send(service)
            elif event & select.EPOLLHUP:
                self.socket_close(service)
        except Exception as e:
            logging.exception(e)
            self.socket_close(service)

    def socket_receive(self, service):
        try:
            data = DdpSocket().decode(service.socket)
            self.process(service, data)
        except IndexError as e:
            logging.debug(e)
            self.socket_close(service)

    def socket_send(self, service):
        sock = service.socket
        while len(service.send_data) > 0:
            data = service.send_data.pop(0)
            try:
                DdpSocket().encode(data, socket=sock)
            except IndexError as e:
                logging.debug(e)
                self.socket_close(service)
        service.epollin()

    def socket_close(self, service):
        if service.service is not None:
            logging.info("Close connection `%s`[%i]" % (service.service, service.instance))
        else:
            logging.info("Close connection (%s:%i)" % (service.host, service.port))
        self.rpc.epoll.unregister(service.socket.fileno())
        del self._services[service.socket.fileno()]
        self._del_connected(service.service, service.instance)
        service.socket.close()

    def process(self, service_obj, data):
        case = data.pop(0)
        if case == "connect":
            self._process_connect(service_obj, data)
        elif case == "connect_node":
            self._process_connect_node(service_obj, data)
        elif case == "request":
            self._process_request(service_obj, data)
        elif case == "response":
            self._process_response(service_obj, data)
        else:
            raise RouteException("Unexpected route case: %s" % case)

    def _process_connect(self, service_obj, data):
        (service, instance), token, params = data
        service_data = self.kernel.service.list().get(service)
        if service_data is None:
            raise RouteException("Service doesn't exist")

        if token != service_data["token"]:
            raise RouteException("Invalid token")

        if not 1 <= instance <= service_data.get("scale", 1):
            raise RouteException("Unexpected instance")

        self._put_connected(service, instance, service_obj)
        service_obj.auth(self.name, service, instance)

    def _process_connect_node(self, service_obj, data):
        (node, instance, token), params = data
        node_data = self.kernel.g.get("kernel/nodes", keys=[node])[0]
        if node_data is None:
            raise RouteException("Node doesn't exist")

        if token != node_data["token"]:
            raise RouteException("Invalid token")

        if not 1 <= instance <= node_data.get("scale", 1):
            raise RouteException("Unexpected instance")

        self._put_connected("__global__:%s" % node, instance, service_obj)
        service_obj.auth(self.name, node, instance)

    def _process_request(self, service_obj, data):
        identificator = None
        try:
            (node, service, instance), method, args, kwargs, identificator = data
            instance = True if instance is None else int(instance)
            if node not in ["__local__", self.name]:
                service = "__global__:%s" % node
            # TODO: Balancing
            request_service_obj = self._get_connected(service, instance)[0]
            request_service_obj.request(
                service_obj,
                method,
                args,
                kwargs,
                identificator,
            )
        except Exception as e:
            logging.exception(e)
            if identificator is None:
                self.socket_close(service_obj)
            else:
                error = [
                    "%s.%s" % (
                        getattr(e, "__module__", "__built_in__"),
                        e.__class__.__name__,
                    ),
                    str(e),
                    traceback.format_exc(),
                ]

                service_obj.response(
                    None,
                    error,
                    identificator,
                )

    def _process_response(self, service_obj, data):
        identificator = None
        try:
            data, error, identificator = data
            service_obj.response(
                data,
                error,
                identificator,
            )
        except Exception as e:
            logging.exception(e)
            if identificator is not None:
                error = [
                    "%s.%s" % (
                        getattr(e, "__module__", "__built_in__"),
                        e.__class__.__name__,
                    ),
                    str(e),
                    traceback.format_exc(),
                ]

                service_obj.response(
                    None,
                    error,
                    identificator,
                )

    def _put_connected(self, service, instance, service_obj):
        with self._connected_lock:
            try:
                self._get_connected(service)
            except RouteException:
                self._connected[service] = {instance: [service_obj]}
            else:
                try:
                    self._get_connected(service, instance)
                except RouteException:
                    self._connected[service][instance] = [service_obj]
                else:
                    self._connected[service][instance].append(service_obj)

    def _get_connected(self, service, instance=None):
        try:
            connections = self._connected[service]
        except KeyError:
            raise RouteException("Service doesn't exist")
        if instance is None:
            return connections
        elif instance is True:
            with self._connected_lock:
                instances = list(connections.keys())
                length = len(instances)
                counter = self._connections_balancing.get(service, -1) + 1
                if counter >= length:
                    counter %= length
                self._connections_balancing[service] = counter
                return connections[instances[counter]]
        else:
            try:
                return connections[instance]
            except KeyError:
                raise RouteException("Unexpected instance")

    def _del_connected(self, service, instance):
        del self._connected[service][instance]


class Rpc(KernelModule):
    _stop = None

    socket = None
    router = None
    epoll = None

    real_host = None
    host = None
    port = None

    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        self.router = Router(self)
        self.real_host = kwargs.get("host")
        self.host = "0.0.0.0"
        self.port = kwargs.get("port")
        self.port = 2011 if self.port is None else int(self.port)
        self._stop = False

    def serve(self):
        logging.info("Starting server (%s:%i)" % (self. host, self.port))
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, int(self.port)))
        self.socket.listen(1)
        self.socket.setblocking(0)

        self.epoll = select.epoll()
        self.epoll.register(self.socket.fileno(), select.EPOLLIN)

        try:
            logging.info("Started")
            while self.alive:
                events = self.epoll.poll(1)
                for file_no, event in events:
                    if file_no == self.socket.fileno():
                        try:
                            self.router.new_connection(self.socket)
                        except Exception as e:
                            logging.exception(e)
                    else:
                        try:
                            self.router.epoll(event, file_no)
                        except Exception as e:
                            logging.exception(e)
        finally:
            self.stop()
            if self.alive:
                logging.exception("Exception has thrown: restarting server")
                self.serve()

    def stop(self):
        if self._stop:
            return
        self._stop = True

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
