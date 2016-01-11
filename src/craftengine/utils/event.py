# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"


import logging

from craftengine import KernelModuleSingleton


class Event(KernelModuleSingleton):
    #
    # Only for one plugin
    #
    N_PLUGIN = 1

    #
    # All plugins on this node
    #
    N_LOCAL = 2

    #
    # All nodes
    #
    N_GLOBAL = 3

    _callbacks = None

    def init(self, *args, **kwargs):
        self._callbacks = {}

    def _add_callback(self, name, callback, has_permission, namespace):
        if name not in self._callbacks.keys():
            self._callbacks[name] = []
        self._callbacks[name].append((callback, has_permission, namespace))
        return len(self._callbacks[name]) - 1

    def _has_permission(self, name, namespace):
        return True

    def register(self, name, callback, namespace=None, has_permission=None):
        namespace = namespace if namespace is not None else self.N_PLUGIN
        has_permission = has_permission if has_permission is not None else self._has_permission
        return self._add_callback(name, callback, has_permission, namespace)

    def initiate(self, name, data, mutable_data=False, namespace=None):
        """
        Initiate event
        :param name: name of event
        :param data: data passing to callback
        :param mutable_data: is `data` mutable from call to call
        :param namespace: namespace of event
        :return: `data`
        """
        namespace = namespace if namespace is not None else self.N_PLUGIN
        try:
            callbacks = self._callbacks[name]
        except KeyError:
            return data

        for callback, has_permission, cb_namespace in callbacks:
            if namespace > cb_namespace:
                continue

            if has_permission(name, namespace) is True:
                # noinspection PyBroadException
                try:
                    cb_data = callback(name, data)
                    if mutable_data:
                        data = cb_data
                except Exception:
                    pass

        if namespace == self.N_GLOBAL:

            for node in self.core.node.list():
                n_data = node.event.initiate(name, data, mutable_data, self.N_LOCAL)
                if mutable_data:
                    data = n_data

        return data

    def info(self, name):
        try:
            callback, has_permission, namespace = self._callbacks[name]
            return {
                "namespace": namespace,
            }
        except KeyError:
            return False
