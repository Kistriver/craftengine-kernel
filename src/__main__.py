# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import os
import logging

from craftengine import Kernel


params = os.environ

logging.basicConfig(
    format="\033[0m[%(levelname)s][%(threadName)s][%(asctime)-15s] %(message)s\033[0m",
    level=params.get("kernel.logging.level", "INFO"),
)

kernel = Kernel(**params)
kernel.serve()
