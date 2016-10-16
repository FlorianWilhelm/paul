#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from paul.utils import find_next_extremum

__author__ = "Florian Wilhelm"
__copyright__ = "Florian Wilhelm"
__license__ = "new-bsd"


def test_find_next_extremum():
    s1 = [1, 2, 2, 1, -1]
    s2 = [-1, 2, 4]
    s3 = [-1, -3, -2, -4]
    assert find_next_extremum(s1) == (1, 2)
    assert find_next_extremum(s2) == (0, -1)
    assert find_next_extremum(s3) == (1, -3)
