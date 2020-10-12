#!/usr/bin/env python3
#
# Copyright 2020-2020 Vrije Universiteit Brussel
#
# This file is part of vsc-accounting-brussel,
# originally created by the HPC team of Vrij Universiteit Brussel (http://hpc.vub.be),
# with support of Vrije Universiteit Brussel (http://www.vub.be),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/sisc-hpc/vsc-accounting-brussel
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
vsc-accounting-brussel base distribution setup.py

@author: Alex Domingo (Vrije Universiteit Brussel)
"""

import vsc.install.shared_setup as shared_setup
from vsc.install.shared_setup import ad

# Read global version from version.py
CONSTANTS = {}
with open("lib/vsc/accounting/version.py") as fp:
    exec(fp.read(), CONSTANTS)

PACKAGE = {
    'version': CONSTANTS["VERSION"],
    'author': [ad],
    'maintainer': [ad],
    'python_requires': '~=3.6',
    'setup_requires': [
        'vsc-install >= 0.15.15',
    ],
    'install_requires': [
        'vsc-base >= 3.0.1',
        'vsc-utils >= 2.1.0',
        'vsc-config >= 3.3.4',
        'vsc-accountpage-clients >= 2.1.1',
        # needed to manage config files
        'appdirs',
        # needed to process accounting data
        'numpy >= 1.14.0',
        'pandas >= 0.23.0',
        # needed to retrieve accounting data
        'elasticsearch >= 7.0.0',
        'elasticsearch_dsl >= 7.0.0',
        # needed to render HTML pages
        'beautifulsoup4 >= 4.6.0',
        # needed to render plots
        'matplotlib >= 3.0.0',
    ],
    'excluded_pkgs_rpm': ['vsc'],  # vsc is default
    'package_data': {
        "vsc.accounting": [
            "config/vsc-accounting.ini",
            "data/html_main_style.html",
            "data/html_table_style.json",
            "data/example-nodegroups.json",
        ],
    },
    'zip_safe': False,
}


if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
