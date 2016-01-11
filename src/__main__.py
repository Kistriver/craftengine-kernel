# -*- coding: utf-8 -*-
__author__ = "Alexey Kachalov"

import os
from craftengine import Kernel

params = os.environ
kernel = Kernel(**params)
kernel.serve()
