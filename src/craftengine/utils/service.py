# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"


import logging

from craftengine import KernelModuleSingleton, Kernel
from craftengine.utils.exceptions import KernelException
from craftengine.utils.registry import (
    Registry,
    PermanentRegistry,
    GlobalRegistry,
)


class ServiceException(KernelException):
    pass


class ServiceNotFoundException(ServiceException):
    pass


class CollisionException(ServiceException):
    pass


class Service(KernelModuleSingleton):
    is_service = lambda service: service in Service.list().keys()
    service_name = lambda service, num: "ce_%s_service_%i_%s" % (Kernel().env["CE_PROJECT_NAME"], num, service)

    @staticmethod
    def list():
        try:
            lst = PermanentRegistry().get("kernel.services")
        except KeyError:
            PermanentRegistry().hash("kernel.services")
            lst = {}

        return lst

    @staticmethod
    def start(service, num=None, force=None, remove=None):
        if not Service.is_service(service):
            raise ServiceNotFoundException

        num = 1 if num is None else int(num)
        force = True if force is None else bool(force)
        remove = True if remove is None else bool(remove)

        for i in range(1, num + 1):
            Service._start(service, i, force, remove)

    @staticmethod
    def _start(service, num, force, remove):
        docker = Registry().get("kernel.docker")

        container_name = Service.service_name(service, num)
        try:
            if remove:
                docker.remove_container(container=container_name, force=force)
        except:
            logging.exception("")

        service_info = Service.list()[service]

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

    def stop(self):
        pass

    @staticmethod
    def add(service, image, permissions):
        if Service.is_service(service):
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

    @staticmethod
    def remove(service):
        if not Service.is_service(service):
            raise ServiceNotFoundException

        Service.stop(service)
        PermanentRegistry().hdelete("kernel.services", service)

    @staticmethod
    def scale(service, num, force=None):
        if not Service.is_service(service):
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
                    docker.remove_container(container=Service.service_name(service, scale_i), force=force)
                except:
                    pass

        elif len(service_status) < num:
            for scale_i in range(service_status + 1, num + 1):
                Service._start(service, scale_i, force, True)
