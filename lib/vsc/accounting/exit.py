##
# Copyright 2020-2022 Vrije Universiteit Brussel
#
# This file is part of vsc-accounting-brussel,
# originally created by the HPC team of Vrij Universiteit Brussel (http://hpc.vub.be),
# with support of Vrije Universiteit Brussel (http://www.vub.be),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/vub-hpc/vsc-accounting-brussel
#
# vsc-accounting-brussel is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation v2.
#
# vsc-accounting-brussel is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with vsc-manage.  If not, see <http://www.gnu.org/licenses/>.
#
##
"""
Exit handler for vsc.accounting

@author: Alex Domingo (Vrije Universiteit Brussel)
"""

import sys

from vsc.utils import fancylogger


def error_exit(logger, message):
    """
    Log silent error on INFO logger level or higher
    Log error traceback on DEBUG logger level or below
    - logger: (object) fancylogger object of the caller
    - message: (string) error message to be printed
    """
    debug_level = logger.getEffectiveLevel() <= fancylogger.getLevelInt('DEBUG')

    # Log the error
    logger.error(message, exc_info=debug_level)

    # Exit
    sys.exit(1)
