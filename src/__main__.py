# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import os
import logging

from craftengine import Kernel


params = os.environ

logging.basicConfig(
    format="\033[0m[%(levelname)s][%(threadName)s][%(pathname)s:%(lineno)s][%(asctime)-15s] \n%(message)s\033[0m\n",
    level=params.get("kernel.logging.level", params.get("logging_level", "DEBUG")),
)

kernel = Kernel(**params)
kernel.serve()
