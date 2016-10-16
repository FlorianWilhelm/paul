# -*- coding: utf-8 -*-
"""
Additional utilities
"""

import logging

import numpy as np

__author__ = "Florian Wilhelm"
__copyright__ = "Florian Wilhelm"
__license__ = "gpl3"

_logger = logging.getLogger(__name__)


def tolist(obj):
    obj = obj if isinstance(obj, list) else [obj]
    return ','.join(obj)


def find_next_extremum(series):
    ext = series[0]
    pos = 0
    signum = np.sign(ext)
    for elem in series[1:]:
        if signum*ext < signum*elem:
            ext = elem
            pos += 1
        else:
            return pos, ext
