# -*- coding: utf-8 -*-
"""
Additional utilities
"""

import logging

__author__ = "Florian Wilhelm"
__copyright__ = "Florian Wilhelm"
__license__ = "gpl3"

_logger = logging.getLogger(__name__)


def tolist(obj):
    obj = obj if isinstance(obj, list) else [obj]
    return ','.join(obj)
