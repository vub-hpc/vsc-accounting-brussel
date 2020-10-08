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
Parser of configuration files for vsc.accounting

@author: Alex Domingo (Vrije Universiteit Brussel)
"""

import os
import appdirs
import pkg_resources
import configparser

from vsc.utils import fancylogger
from vsc.accounting.filetools import make_dir, copy_file
from vsc.accounting.exit import error_exit


class ConfigFile:
    """
    Parse contents of a config file in user's config dir
    Generate default config file in user's config dir if it does not exist
    """

    def __init__(self, filename):
        """
        Set the location of the config file, create it if it does not exist and read its contents
        - filename: (string) name of the config file
        """
        self.log = fancylogger.getLogger(name=self.__class__.__name__)

        self.usercfg = {'name': filename}

        # Directories that can contain config files, ordered by preference
        config_dirs = (
            appdirs.user_config_dir('vsc-accounting'),
            '/etc/vsc-accounting',
            '/etc',
        )

        # Check existence of config files
        existing_configs = [d for d in config_dirs if os.path.isfile(os.path.join(d, self.usercfg['name']))]

        if len(existing_configs) > 0:
            # Use config file from top hit
            self.usercfg.update({'dir': existing_configs[0]})
            dbgmsg_path = os.path.join(self.usercfg['dir'], self.usercfg['name'])
            self.log.debug("Found existing configuration file: %s", dbgmsg_path)
        else:
            # Copy default config file to user's dir if config file is not found
            self.usercfg.update({'dir': appdirs.user_config_dir('vsc-accounting')})
            self.copy_pkgdefault()

        # Read contents of config file
        self.opts = configparser.ConfigParser()
        try:
            self.read()
        except FileNotFoundError as err:
            error_exit(self.log, err)

    def copy_pkgdefault(self, force=False):
        """
        Copy the corresponding sample from the project's package to destination config dir
        Sample config files are located inside the 'config' folder of the package
        - force: (boolean) copy sample regardless of existence of user's config
        """
        # Make own dir in user's config dir
        make_dir(self.usercfg['dir'])

        # Copy sample file to user's config dir
        pkgcfg_path = pkg_resources.resource_filename(__name__, self.usercfg['name'])
        usrcfg_path = os.path.join(self.usercfg['dir'], self.usercfg['name'])
        file_copied = copy_file(pkgcfg_path, usrcfg_path, force=force)

        if file_copied:
            self.log.warning("Installed sample configuration file into '%s'", usrcfg_path)

    def get(self, section, option, fallback=None, mandatory=True):
        """
        Wrapper of configparse.get() with error handling
        - section: (string) name of section in config settings
        - option: (string) name of option in config settings
        - mandatory: (boolean) configuration option is essential
        - fallback: (object) fallback value to be passed to get()
        """
        if self.opts.has_option(section, option):
            config_value = self.opts.get(section, option, fallback=fallback)
            self.log.debug("Succesfully read configuration option: %s > %s", section, option)
            return config_value
        elif not mandatory:
            dbgmsg = "Configuration option %s > %s not found, using fallback value '%s'"
            self.log.debug(dbgmsg, section, option, fallback)
            return fallback
        else:
            errmsg = f"Configuration option {section} > {option} not found"
            raise KeyError(errmsg)

    def get_digit(self, section, option, fallback=0, mandatory=True):
        """
        Return config value if it is as a positive integer
        - section: (string) name of section in config settings
        - option: (string) name of option in config settings
        - mandatory: (boolean) configuration option is essential
        - fallback: (object) fallback value to be passed to get()
        """
        conf_digit = self.get(section, option, fallback=fallback, mandatory=mandatory)

        if str(conf_digit).isdigit():
            return int(conf_digit)
        elif conf_digit == fallback:
            return conf_digit
        elif not conf_digit:
            # Treat empty string as missing setting
            dbgmsg = "Numerical configuration option %s > %s is empty, using fallback value '%s'"
            self.log.debug(dbgmsg, section, option, fallback)
            return fallback
        else:
            errmsg = f"Configuration option {section} > {option} is not a positive integer: '{conf_digit}'"
            raise ValueError(errmsg)

    def read(self):
        """
        Wrapper of configparse.read() that raises error if config file is not found
        configparser.read() just returns an empty object if config file is not found
        """
        config_path = os.path.join(self.usercfg['dir'], self.usercfg['name'])
        if os.path.isfile(config_path):
            self.opts.read(config_path)
            self.log.debug("Succesfully parsed configuration file: %s", config_path)
        else:
            errmsg = f"Configuration file not found: {config_path}"
            raise FileNotFoundError(errmsg)


MainConf = ConfigFile('vsc-accounting.ini')
