# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import redis
import logging


class Redis:
    _initargs = None
    _pool = None
    _rd = None

    def __init__(self, host="localhost", port=6379, db=0, password=None):
        self._pool = redis.ConnectionPool(host=host, port=port, db=db)
        self._initargs = [host, port, db, password]
        try:
            self._rd = redis.StrictRedis(connection_pool=self._pool)
            if password is not None:
                self._rd.execute_command("AUTH %s" % str(password))
            else:
                self._rd.info()
        except redis.exceptions.ConnectionError:
            logging.error("Connection to Redis refused")
            raise
        except redis.exceptions.ResponseError:
            logging.error("Could not auth")
            raise

    def reqsafe(decorated):
        def wrapper(self, *args, **kwargs):
            try:
                ans = decorated(self, *args, **kwargs)
                return ans
            except redis.exceptions.ConnectionError:
                logging.error("Connection to Redis refused")
                raise
            except redis.exceptions.ResponseError:
                try:
                    self.__init__(*self._initargs)
                    ans = decorated(self, *args, **kwargs)
                    return ans
                except:
                    logging.exception("Could not process request")
                    raise
            except:
                logging.exception("")
                raise

        return wrapper

    @reqsafe
    def get(self, name):
        return self._rd.get(name)

    @reqsafe
    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        return self._rd.set(name, value, ex, px, nx, xx)

    @reqsafe
    def incr(self, name, amount=1):
        return self._rd.incr(name, amount)

    @reqsafe
    def pipeline(self, transaction=True, shard_hint=None):
        return self._rd.pipeline(transaction, shard_hint)

    @reqsafe
    def keys(self, pattern):
        return self._rd.keys(pattern)

    @reqsafe
    def delete(self, name):
        return self._rd.delete(name)
