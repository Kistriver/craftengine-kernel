# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import logging


class KernelModule(object):
    kernel = None
    _alive = None

    alive = property(fget=lambda self: self._alive)

    def __init__(self, *args, **kwargs):
        logging.debug("Loading kernel module: %s" % self.__class__.__name__)
        from craftengine import Kernel
        self.kernel = Kernel()
        self.init(*args, **kwargs)
        self._alive = True

    def init(self, *args, **kwargs):
        """
        Basic initialization
        """

    def init_mod(self, *args, **kwargs):
        """
        Module initialization
        """

    def exit(self, *args, **kwargs):
        logging.debug("Unloading kernel module: %s" % self.__class__.__name__)
        self._alive = False


class KernelModuleSingleton(KernelModule):
    _instance = None
    _no_init = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, *args, **kwargs):
        if self.__class__._no_init:
            return
        self.__class__._no_init = True
        super().__init__(*args, **kwargs)
