# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import socket
import select
import logging
import threading

from ddp import DdpSocket
from craftengine.exceptions import ModuleException
from craftengine.modules import KernelModule


class RpcException(ModuleException):
    pass


class RouteException(RpcException):
    pass


class ServiceException(RpcException):
    pass


class Router(object):
    BALANCED_INSTANCE = True

    def __init__(self, rpc):
        self.rpc = rpc
        self.kernel = self.rpc.kernel
        # TODO
        # self.name = self.kernel.l.get("kernel/env", keys=["name"])["name"]
        self.name = self.kernel.env["CE_NODE_NAME"]
        self._sockets = {}
        self._services = {}
        self._balancing_instances = {}
        self._lock = threading.RLock()
        self._binds = {}

    def new_connection(self, sock, address):
        # socket, address, (service, instance), requests, responses
        self._sockets[sock.fileno()] = [sock, address, None, []]
        self.rpc.epoll.register(sock.fileno(), select.EPOLLIN)

    def create_info(self, **kwargs):
        return {
            "fn": kwargs["fn"],
            "responses": {},
            "service": kwargs["service"],
        }

    def epollin(self, fn):
        self.rpc.epoll.modify(fn, select.EPOLLIN)

    def epollout(self, fn):
        self.rpc.epoll.modify(fn, select.EPOLLOUT)

    def epoll(self, event, file_no):
        try:
            service = self._sockets[file_no][2]
            service_info = file_no if service is None else self.get_service(*service)
        except KeyError:
            logging.exception("")
            return

        try:
            if event & select.EPOLLIN:
                self.socket_receive(file_no)
            elif event & select.EPOLLOUT:
                self.socket_send(file_no)
            elif event & select.EPOLLHUP:
                self.socket_close(file_no)
        except Exception as e:
            logging.exception(e)
            self.socket_close(file_no)

    def socket_receive(self, fn):
        sock = self._sockets[fn][0]
        service = self._sockets[fn][2]
        service_info = fn if service is None else self.get_service(*service)
        try:
            data = DdpSocket().decode(sock)
            self.process(service_info, data)
        except IndexError as e:
            logging.debug(e)
            self.socket_close(fn)

    def socket_send(self, fn):
        sock = self._sockets[fn][0]
        while len(self._sockets[fn][3]) > 0:
            data = self._sockets[fn][3].pop(0)
            try:
                DdpSocket().encode(data, socket=sock)
            except IndexError as e:
                logging.debug(e)
                self.socket_close(fn)
        self.epollin(fn)

    def socket_close(self, fn):
        sock, addr, service, send_data = self._sockets[fn]
        if service is not None:
            logging.info("Close connection `%s`[%i]" % (service[0], service[1]))
        else:
            logging.info("Close connection (%s:%i)" % (addr[0], addr[1]))
        del self._sockets[sock.fileno()]
        self.del_service(service[0], service[1])
        self.rpc.epoll.unregister(sock.fileno())
        sock.close()

    def process(self, service_info, data):
        case = data.pop(0)

        try:
            cb = self._binds[case]
        except KeyError:
            raise RouteException("Unexpected route case: %s" % case)

        try:
            cb(service_info, data)
        except Exception as e:
            logging.exception(e)

    def put_service(self, service, instance, info):
        try:
            self.get_service(service)
        except RouteException:
            self._services[service] = {instance: info}
        else:
            try:
                self.get_service(service, instance)
            except RouteException:
                pass
            else:
                self.socket_close(self._services[service][instance]["fn"])
            finally:
                self._services[service][instance] = info

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
        del self._services[service][instance]

    def stop(self):
        raise NotImplementedError


class Rpc(KernelModule):
    _stop = None

    socket = None
    router = Router
    epoll = None

    host = None
    port = None

    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        self.router = self.router if kwargs.get("router") is None else kwargs.get("router")
        self.router = self.router(self)
        self.host = self.host if kwargs.get("host") is None else kwargs.get("host")
        self.port = self.port if kwargs.get("port") is None else int(kwargs.get("port"))
        self._stop = False
        self._alive = None

    def serve(self):
        logging.info("Starting RPC (%s:%i)" % (self. host, self.port))
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
                            self.router.new_connection(connection, address)
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

    def stop(self):
        if self._stop:
            return
        self._stop = True

        self.router.stop()

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
