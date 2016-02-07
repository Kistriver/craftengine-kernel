# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"


import logging

from craftengine import KernelModuleSingleton
from craftengine.utils.exceptions import KernelException
from craftengine.utils.registry import (
    Registry,
    PermanentRegistry,
)


class ServiceException(KernelException):
    pass


class ServiceNotFoundException(ServiceException):
    pass


class CollisionException(ServiceException):
    pass


class Service(KernelModuleSingleton):
    def is_service(self, service):
        return service in self.list().keys()

    def service_name(self, service, num):
        return "ce_%s_%s_service_%i_%s" % (self.kernel.env["CE_PROJECT_NAME"], self.kernel.env["CE_NODE_NAME"], num, service)

    def list(self):
        try:
            return self.kernel.l.get("kernel.services")
        except KeyError:
            self.kernel.l.create("kernel.services")
            return {}

    def start(self, service, num=None, force=None, remove=None):
        if not self.is_service(service):
            raise ServiceNotFoundException

        num = 1 if num is None else int(num)
        force = True if force is None else bool(force)
        remove = True if remove is None else bool(remove)

        for i in range(1, num + 1):
            self._start(service, i, force, remove)

    def _start(self, service, num, force, remove):
        docker = Registry().get("kernel.docker")

        container_name = self.service_name(service, num)
        try:
            if remove:
                docker.remove_container(container=container_name, force=force)
        except:
            logging.exception("")

        service_info = self.list()[service]

        try:
            docker.create_container(
                image=service_info["image"],
                detach=True,
                name=container_name,
                environment={
                    "CE_TOKEN": service_info["token"],
                    "CE_NAME": service,
                },
                labels={
                    "CRAFTEngine": "True",
                    "Service": service,
                },
            )
            docker.start(container=container_name)
            logging.info("'%s'[%i] service started" % (service, num))
        except:
            logging.exception("")

    def stop(self, service):
        docker = Registry().get("kernel.docker")
        for i in range(1, self.list()[service].get("scale", 1) + 1):
            try:
                docker.stop(container=self.service_name(service, i))
                logging.info("'%s'[%i] service stopped" % (service, i))
            except:
                logging.exception("Error stopping service '%s'[%i]" % (service, i))

    def add(self, service, image, permissions):
        if self.is_service(service):
            raise CollisionException

        PermanentRegistry().hset(
            "kernel.services",
            service,
            {
                "image": image,
                "permissions": permissions,
                # TODO: token on each kernel load
                "token": "TOKEN",
            }
        )

    def remove(self, service):
        if not self.is_service(service):
            raise ServiceNotFoundException

        self.stop(service)
        docker = Registry().get("kernel.docker")
        for i in range(1, self.list()[service].get("scale", 1) + 1):
            try:
                docker.remove_container(container=self.service_name(service, i), force=True)
                logging.info("'%s'[%i] service removed" % (service, i))
            except:
                logging.exception("Error removing service '%s'[%i]" % (service, i))

    def scale(self, service, num, force=None):
        if not self.is_service(service):
            raise ServiceNotFoundException

        force = True if force is None else bool(force)

        docker = Registry().get("kernel.docker")

        service_status = docker.containers(
            filters={
                "label": "Service=\"%s\" CRAFTEngine=True" % service
            },
        )

        if len(service_status) > num:
            for scale_i in range(num + 1, service_status + 1):
                try:
                    docker.remove_container(container=self.service_name(service, scale_i), force=force)
                except:
                    pass

        elif len(service_status) < num:
            for scale_i in range(service_status + 1, num + 1):
                self._start(service, scale_i, force, True)
