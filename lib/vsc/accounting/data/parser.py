##
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


class DataFile:
    """
    Parse contents of a data file in user's data dir
    Copy data file to user's data dir if it does not exist
    Supported formats: JSON, HTML
    """

    def __init__(self, datafile, mandatory=True):
        """
        Set the location of the data file, copy it if it does not exist and read its contents
        - datafile: (string) name of the data file or full path to data file
        - mandatory: (boolean) mandatory files must already exist in user's data dir or be copied from package
        """
        self.log = fancylogger.getLogger(name=self.__class__.__name__)

        datafile = os.path.expanduser(datafile)

        if os.path.isabs(datafile):
            # Directly read from absolute path
            self.datafile = {'path': datafile}
            # By default try to read file from absolute path
            self.datafile.update({'readable': True})
        else:
            # Use datafile in user data directory
            self.datafile = {
                'name': datafile,
                'dir': appdirs.user_data_dir('vsc-accounting'),
            }
            self.datafile.update({'path': os.path.join(self.datafile['dir'], self.datafile['name'])})

            # Copy data file from package contents (if it is missing in user's data dir)
            # Failed copies are only fatal for mandatory data files
            try:
                self.copy_pkgdata()
            except FileNotFoundError as err:
                self.datafile.update({'readable': False})
                if mandatory:
                    error_exit(self.log, err)
            else:
                self.datafile.update({'readable': True})

        # Read contents of data file
        if self.datafile['readable']:
            try:
                self.read_data()
            except ValueError as err:
                error_exit(self.log, err)

    def copy_pkgdata(self, force=False):
        """
        If datafile is missing, copy the corresponding datafile from the package's data
        Package data files are located inside the 'data' folder of the package
        - force: (boolean) copy data file from package regardless of existence of user's data
        """
        # Make own dir in user's data dir
        make_dir(self.datafile['dir'])

        # Copy data file to user's data dir
        pkgdata = pkg_resources.resource_filename(__name__, self.datafile['name'])
        file_copied = copy_file(pkgdata, self.datafile['path'], force=force)

        if file_copied:
            self.log.warning("Created user data file '%s' from package file", self.datafile['path'])
        elif file_copied == False:
            errmsg = f"Data file not found: {self.datafile['path']}"
            raise FileNotFoundError(errmsg)

        return file_copied

    def read_json(self):
        """
        Return contents of JSON file
        """
        try:
            with open(self.datafile['path'], 'r') as jsonfile:
                jsondata = json.load(jsonfile)
        except FileNotFoundError as err:
            error_exit(self.log, f"Data file not found: {self.datafile['path']}")
        except json.decoder.JSONDecodeError as err:
            error_exit(self.log, f"Data file in JSON format is malformed: {self.datafile['path']}")
        else:
            self.log.debug("Data read from file: %s", self.datafile['path'])
            return jsondata

    def read_html(self):
        """
        Return contents of HTML file
        """
        try:
            with open(self.datafile['path'], 'r') as htmlfile:
                htmldump = htmlfile.read()
                htmldata = BeautifulSoup(htmldump, 'lxml')
        # There are no other exeptions to check, bs4 will make HTML compliant anything that you throw at it
        except FileNotFoundError as err:
            error_exit(self.log, f"Data file not found: {self.datafile['path']}")
        else:
            self.log.debug("Data read from file: %s", self.datafile['path'])
            return htmldata

    def read_data(self):
        """
        Read data file with appropriate parser depending on file extension
        """
        file_ext = self.datafile['path'].split('.')[-1].lower()

        if file_ext == 'json':
            self.contents = self.read_json()
        elif file_ext == 'html':
            self.contents = self.read_html()
        else:
            errmsg = f"Unknown data file format: {self.datafile['path']}"
            raise ValueError(errmsg)

    def save_json(self):
        """
        Save contents to data file in JSON format
        """
        try:
            with open(self.datafile['path'], 'w', encoding='utf8') as jsonfile:
                json.dump(self.contents, jsonfile, indent=4, ensure_ascii=False)
        except FileNotFoundError as err:
            error_exit(self.log, f"Data file not found: {self.datafile['path']}")
        else:
            self.log.debug("Data saved to file: %s", self.datafile['path'])
            return True

    def save_data(self):
        """
        Save data file with appropriate parser depending on file extension
        """
        file_ext = self.datafile['path'].split('.')[-1].lower()

        if file_ext == 'json':
            self.contents = self.save_json()
        else:
            errmsg = f"Unknown data file format: {self.datafile['path']}"
            raise ValueError(errmsg)
