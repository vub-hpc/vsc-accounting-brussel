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
Parser of data files for vsc.accounting

@author: Alex Domingo (Vrije Universiteit Brussel)
"""

import os
import appdirs
import pkg_resources
import json

from bs4 import BeautifulSoup

from vsc.utils import fancylogger
from vsc.accounting.filetools import make_dir, copy_file
from vsc.accounting.exit import error_exit

DATA_DIR = 'vsc-accounting'
FORCE_INSTALL = False


class DataFile:
    """
    Parse contents of a data file in user's data dir
    Copy data file to user's data dir if it does not exist
    Supported formats: JSON, HTML
    """

    def __init__(self, datafile, mandatory=True, force_install=None):
        """
        Determine location of data file and read its contents
        Data files from package resources will be copied to user's data dir if needed
        - datafile: (string) name of the data file or full path to data file
        - mandatory: (boolean) mandatory files must already exist in user's data dir (or be installed)
        - force_install: (boolean) force copy of data file from package resources, superseeds FORCE_INSTALL
        """
        self.log = fancylogger.getLogger(name=self.__class__.__name__)

        # Fallback to FORCE_INSTALL if force_install is not set
        if force_install is None:
            force_install = FORCE_INSTALL

        if force_install:
            self.log.debug("Installation of data files is enforced")

        # Define paths holding package data files by order of preference
        # The 'data' folder in the package resources is set as a fallback location
        self.sys_data_dirs = (
            f'/etc/{DATA_DIR}',
            '/etc',
        )

        datafile = os.path.expanduser(datafile)

        if os.path.isabs(datafile):
            # Directly read data from absolute path
            self.datafile = datafile
            readable_file = True
        else:
            # Use datafile in user data directory
            self.datafile = os.path.join(appdirs.user_data_dir(DATA_DIR), datafile)
            # Copy data file from package contents (if it is missing in user's data dir or manually forced)
            # Failed copies are only fatal for mandatory data files
            try:
                self.install_pkgdata(datafile, force=force_install)
            except FileNotFoundError as err:
                readable_file = False
                if mandatory:
                    error_exit(self.log, err)
            else:
                readable_file = True

        if readable_file:
            # Read contents of data file
            try:
                self.read_data()
            except ValueError as err:
                error_exit(self.log, err)

    def install_pkgdata(self, filename, force=False):
        """
        Copy the corresponding datafile from the package's data, if it is missing in user's data or manually forced
        Package data files can be located inside the 'data' folder of the package or in '/etc' (self.sys_data_dirs)
        - filename: (string) name of the data file
        - force: (boolean) copy data file from package regardless of existence of user's data
        """
        # Locate existing data file
        tentative_sources = [os.path.join(pkgdir, filename) for pkgdir in self.sys_data_dirs]
        existing_sources = [data_path for data_path in tentative_sources if os.path.isfile(data_path)]

        if len(existing_sources) > 0:
            # Install data file from top hit
            pkg_data = existing_sources[0]
        else:
            # Install data file from package resources
            pkg_data = pkg_resources.resource_filename(__name__, filename)

        # Make own dir in user's data dir
        user_data_dir = appdirs.user_data_dir(DATA_DIR)
        make_dir(user_data_dir)

        # Copy data file to user's data dir (if missing or forced)
        file_copied = copy_file(pkg_data, self.datafile, force=force)

        if file_copied:
            self.log.warning("Installed data file '%s' into '%s'", pkg_data, user_data_dir)
        elif file_copied == False:
            errmsg = f"Data file not found: {filename}"
            raise FileNotFoundError(errmsg)

        return True

    def read_json(self):
        """
        Return contents of JSON file
        """
        try:
            with open(self.datafile, 'r') as jsonfile:
                jsondata = json.load(jsonfile)
        except FileNotFoundError as err:
            error_exit(self.log, f"Data file not found: {self.datafile}")
        except json.decoder.JSONDecodeError as err:
            error_exit(self.log, f"Data file in JSON format is malformed: {self.datafile}")
        else:
            self.log.debug("Data read from file: %s", self.datafile)
            return jsondata

    def read_html(self):
        """
        Return contents of HTML file
        """
        try:
            with open(self.datafile, 'r') as htmlfile:
                htmldump = htmlfile.read()
                htmldata = BeautifulSoup(htmldump, 'lxml')
        # There are no other exeptions to check, bs4 will make HTML compliant anything that you throw at it
        except FileNotFoundError as err:
            error_exit(self.log, f"Data file not found: {self.datafile}")
        else:
            self.log.debug("Data read from file: %s", self.datafile)
            return htmldata

    def read_data(self):
        """
        Read data file with appropriate parser depending on file extension
        """
        file_ext = self.datafile.split('.')[-1].lower()

        if file_ext == 'json':
            self.contents = self.read_json()
        elif file_ext == 'html':
            self.contents = self.read_html()
        else:
            errmsg = f"Unknown data file format: {self.datafile}"
            raise ValueError(errmsg)

    def save_json(self):
        """
        Save contents to data file in JSON format
        """
        try:
            with open(self.datafile, 'w', encoding='utf8') as jsonfile:
                json.dump(self.contents, jsonfile, indent=4, ensure_ascii=False)
        except FileNotFoundError as err:
            error_exit(self.log, f"Data file not found: {self.datafile}")
        else:
            self.log.debug("Data saved to file: %s", self.datafile)
            return True

    def save_data(self):
        """
        Save data file with appropriate parser depending on file extension
        """
        file_ext = self.datafile.split('.')[-1].lower()

        if file_ext == 'json':
            self.contents = self.save_json()
        else:
            errmsg = f"Unknown data file format: {self.datafile}"
            raise ValueError(errmsg)
