# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"


from craftengine.exceptions import ModuleException
from craftengine.modules import KernelModule


class PermissionsException(ModuleException):
    pass


class Permissions(KernelModule):
    def service_has_permission(self, req_service, target_service, target_method):
        services = self.kernel.service.list()
        perms = services[req_service].get("permissions", [])
        reqs = services[target_service].get("methods", {}).get(target_method, [])
        return self.has_permission(perms, reqs)

    def perms_merger(self, perms):
        merged = {}
        for perm in perms:
            if len(perm) == 0:
                merged["*"] = True
                continue
            if perm[0] not in merged.keys():
                merged[perm[0]] = []

            if perm[0] == "*":
                merged["*"] = True
            else:
                merged[perm[0]].append(perm[1:])

        for k in merged.keys():
            if merged[k] is not True:
                merged[k] = self.perms_merger(merged[k])

        return merged

    def has_permission(self, perms, reqs):
        """
        Has permission
        :param perms: presented permissions
        :param reqs: required permissions
        :return: bool
        :raise: PermissionsException
        """
        perms_splitter = lambda x: [i.split(".") for i in x]
        perms = self.perms_merger(perms_splitter(perms))
        reqs = perms_splitter(reqs)

        getter = lambda src, x: src.get("*", src.get(x, False))
        for reqsi in reqs:
            src = perms
            for r in reqsi:
                g = getter(src, r)
                if isinstance(g, dict):
                    src = g
                elif g:
                    return True
                else:
                    raise PermissionsException(str(".".join(reqsi)))
        else:
            return True