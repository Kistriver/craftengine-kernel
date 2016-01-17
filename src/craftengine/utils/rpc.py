# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import socket
import select
import logging
import errno
import threading

from craftengine.utils.registry import Registry
from craftengine import api
from craftengine.utils.ddp import DdpSocket


class RequestHandler(object):
    host = None
    port = None

    connection = None
    serializer = None

    STREAMIN = select.EPOLLIN
    STREAMOUT = select.EPOLLOUT
    streamst = STREAMIN

    fileno = None

    def __init__(self, rpc):
        self.rpc = rpc
        self.erpcid = 0
        self.host = None
        self.port = None
        self.fileno = 0
        self._send = []
        self.connection = None
        self.serializer = DdpSocket()
        self.response = []

        try:
            (self.connection, (self.host, self.port)) = self.rpc.s.accept()
        except:
            logging.exception("")
            self.close()
            return

        self.fileno = self.connection.fileno()
        logging.info("Client connected(%s:%d)" % (self.host, self.port))
        logging.debug("Client fileno: %i" % self.fileno)
        self.connection.setblocking(False)
        try:
            self.rpc.epoll.register(self.fileno, select.EPOLLIN)
        except:
            logging.exception("Epoll register")
            self.close()
            return
        self.stream(self.STREAMIN)

        return

    def epollin(self):
        try:
            data = self.serializer.decode(self.connection)
            if len(data) == 4:
                response = api.Api().execute(data, request=self)
                if response is None:
                    return
                else:
                    self.response.append(response)
            else:
                # We've got response on our request
                api.Api.response(data)
                return
        except:
            logging.exception("")
            self.close()
        self.stream(self.STREAMOUT)

    def epollout(self):
        try:
            while len(self.response) > 0:
                r = self.response[0]
                del self.response[0]
                try:
                    self.serializer.encode(r, socket=self.connection)
                except:
                    logging.exception("")
                    self.close()
                self.stream(self.STREAMIN)
        except:
            logging.exception("")

    def epollhup(self):
        self.close()

    def close(self):
        try:
            logging.info("Client disconnected(%s:%d)" % (self.host, self.port))
        except TypeError:
            logging.info("Client disconnected")
        try:
            self.rpc.epoll.unregister(self.fileno)
        except:
            pass
        try:
            self.connection.close()
        except:
            pass
        try:
            Registry().hdelete("api.pool", self.fileno)
        except KeyError:
            pass

    def stream(self, sttype):
        st = self.streamst
        self.streamst = sttype
        try:
            self.rpc.epoll.modify(self.fileno, sttype)
        except OSError:
            self.close()
        except ValueError:
            logging.exception("")
        return st


class Server(object):
    alive = True
    epoll = None
    s = None

    host = None
    port = None

    handler = None

    def __init__(self, params, request_handler):
        Registry().hash("api.pool")
        Registry().hash("api.requests")
        self. host, self.port = params
        self.handler = request_handler

    def serve_forever(self):
        try:
            self.socketserver()
        except:
            logging.exception("Could not start socket server")
            return

        try:
            self.epollreg()
        except IOError as e:
            if e.errno != errno.EINTR:
                logging.exception("Epollreg error")
                return

    def shutdown(self):
        self.alive = False
        try:
            self.epoll.unregister(self.s.fileno())
        except:
            pass
        try:
            self.epoll.close()
        except:
            pass
        try:
            self.s.close()
        except:
            pass

    def socketserver(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.s.bind((self.host, int(self.port)))
        self.s.listen(1)
        self.s.setblocking(0)

    def epollreg(self):
        self.epoll = select.epoll()
        self.epoll.register(self.s.fileno(), select.EPOLLIN)

        try:
            while self.alive:
                events = self.epoll.poll(1)
                for fileno, event in events:
                    if fileno == self.s.fileno():
                        self.newcli()
                    else:
                        try:
                            fni = Registry().hget("api.pool", fileno)
                        except KeyError:
                            continue
                        if event & select.EPOLLIN:
                            fni.epollin()
                        elif event & select.EPOLLOUT:
                            fni.epollout()
                        elif event & select.EPOLLHUP:
                            fni.epollhup()
        finally:
            self.shutdown()
            if self.alive:
                logging.exception("Exception has thrown: restarting server")
                self.serve_forever()

    def newcli(self):
        def _create(srv):
            clentinst = srv.handler(srv)
            Registry().hset("api.pool", clentinst.fileno, clentinst)

        threading.Thread(target=_create, args=(self,), name="CE-RPC").start()


def run_server(host=None, port=None):
    server = Server((host, port), RequestHandler)
    Registry().set("server", server)
    logging.info('Starting server...')
    try:
        logging.info("Started")
        server.serve_forever()
    except Exception as exc:
        logging.exception(exc)
