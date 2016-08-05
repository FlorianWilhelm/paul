# -*- coding: utf-8 -*-
"""
Stochastic model for price movement
"""
import logging

import scipy as sp
import pymc3 as pm

__author__ = "Florian Wilhelm"
__copyright__ = "Florian Wilhelm"
__license__ = "new-bsd"

_logger = logging.getLogger(__name__)



with pm.Model() as model:
    sigma = pm.Exponential('sigma', 1./.02, testval=.1)
    mu = pm.Normal('mu', 0, sd=5, testval=.1)

    nu = pm.Exponential('nu', 1./10)
    logs = pm.GaussianRandomWalk('logs', tau=sigma**-2, shape=nu)

    r = pm.StudentT('r', nu, mu=mu, lam=pm.exp(2*logs), observed=returns.values[train])




    # Running the MCMC as suggested by PyMC3's getting started guide

    with model:
        start = pm.find_MAP(vars=[logs], fmin=sp.optimize.fmin_l_bfgs_b)

    with model:
        step = pm.NUTS(vars=[logs, mu, nu,sigma],scaling=start, gamma=.25)
        start2 = pm.sample(100, step, start=start)[-1]

        # Start next run at the last sampled position.
        step = pm.NUTS(vars=[logs, mu, nu,sigma],scaling=start2, gamma=.55)
        trace = pm.sample(2000, step, start=start2)
