# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

from craftengine.utils import rpc

# RPC
# ["connect_node", "node_name", "instance", "token", {"params": True}] <-
# ["connect_node" "status"] ->
# ["proxy", "node_name", "command", "rid"] <->
# ["proxy_status", "error", "rid"] <-


class Router(rpc.Router):
    def __init__(self, rpc):
        super().__init__(rpc)
        self.binds = {
        }


class Rpc(rpc.Rpc):
    handler = Router
    port = 2011
    host = "0.0.0.0"

    def proxy(self, node, service_info, *data):
        pass
