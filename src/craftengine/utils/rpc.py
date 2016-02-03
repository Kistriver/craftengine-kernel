# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import socket
import select
import logging
import errno
import threading
from multiprocessing.dummy import Pool as ThreadPool

from craftengine.utils.registry import Registry
from craftengine import api
from ddp import DdpSocket


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
        self.alive = None
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
        self.alive = True
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
        def cb(r):
            if r is None:
                return
            self.response.append(r)
            self.stream(self.STREAMOUT)

        try:
            data = self.serializer.decode(self.connection)
        except IndexError:
            self.close()
            return

        try:
            if len(data) == 4:
                self.rpc.workers.apply_async(
                    api.Api().execute,
                    (data,),
                    {"request": self},
                    callback=cb,
                )
                #response = api.Api().execute(data, request=self)
                #if response is None:
                #    return
                #else:
                #    self.response.append(response)
            else:
                # We've got response on our request
                api.Api.response(data)
                self.stream(self.STREAMOUT)
                return
        except:
            logging.exception("")
            self.close()
        #self.stream(self.STREAMOUT)

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
        if not self.alive:
            return
        self.alive = False

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


class RpcServer(object):
    alive = None
    workers = None

    epoll = None
    s = None

    host = None
    port = None

    handler = RequestHandler

    def __init__(self, params):
        Registry().hash("api.pool")
        Registry().hash("api.requests")
        self. host, self.port = params
        logging.info('Starting server(%s:%i)' % (self. host, self.port))

    def make_workers(self):
        self.workers = ThreadPool(8)

    def serve_forever(self):
        try:
            self.socketserver()
        except:
            logging.exception("Could not start socket server")
            return

        try:
            self.make_workers()
        except:
            logging.exception("Could not start workers")
            return

        try:
            self.epollreg()
        except IOError as e:
            if e.errno != errno.EINTR:
                logging.exception("Epollreg error")
                return

    def shutdown(self):
        logging.info("Stopping RPC server...")
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

        self.alive = True
        try:
            while self.alive:
                events = self.epoll.poll(1)
                for fileno, event in events:
                    if fileno == self.s.fileno():
                        clentinst = self.handler(self)
                        Registry().hset("api.pool", clentinst.fileno, clentinst)
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
