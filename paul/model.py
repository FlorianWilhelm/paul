# -*- coding: utf-8 -*-
"""
Stochastic model for price movement
"""
import logging

import numpy as np
import pandas as pd
import scipy as sp

__author__ = "Florian Wilhelm"
__copyright__ = "Florian Wilhelm"
__license__ = "gpl3"

_logger = logging.getLogger(__name__)


def resample(rule, df):
    return df.resample(rule, how=np.mean).interpolate()


# ToDo: Add pyflux model and Vikram model here
