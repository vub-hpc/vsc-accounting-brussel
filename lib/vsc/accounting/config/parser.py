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

CONFIG_DIR = 'vsc-accounting'


class ConfigFile:
    """
    Parse contents of a config file in user's config dir
    Generate default config file in user's config dir if it does not exist
    """

    def __init__(self):
        """
        Initialize logger and default settings
        """
        self.log = fancylogger.getLogger(name=self.__class__.__name__)

        # Directories that can contain config files, ordered by preference
        self.default_config_dirs = (
            appdirs.user_config_dir(CONFIG_DIR),
            f'/etc/{CONFIG_DIR}',
            '/etc',
        )

    def load(self, configfile):
        """
        Load contents of configuration file
        - configfile: (string) name or path of the config file
        """
        # Determine location of config file
        self.locate_config(configfile)

        # Read contents of config file
        self.opts = configparser.ConfigParser(interpolation=None)
        try:
            self.read()
        except FileNotFoundError as err:
            error_exit(self.log, err)

        return self

    def locate_config(self, configfile):
        """
        Determine location of config file, create it if it does not exist
        - configfile: (string) name of the config file
        """
        if os.path.isabs(configfile):
            # Use config file from absolute path
            self.usercfg = {
                'name': os.path.basename(configfile),
                'path': configfile,
            }
        elif configfile:
            # Locate config file and install it if necessary
            self.usercfg = {'name': configfile}

            # Check existence of config file in default directories
            tentative_configs = [os.path.join(confdir, self.usercfg['name']) for confdir in self.default_config_dirs]
            existing_configs = [config_path for config_path in tentative_configs if os.path.isfile(config_path)]

            if len(existing_configs) > 0:
                # Use config file from top hit
                self.usercfg.update({'path': existing_configs[0]})
                self.log.debug("Found existing configuration file: %s", self.usercfg['path'])
            else:
                # Install default config file in user's dir if config file is not found
                self.usercfg.update({'path': os.path.join(appdirs.user_config_dir(CONFIG_DIR), self.usercfg['name'])})
                self.copy_pkgdefault()
        else:
            error_exit(self.log, "Name of configuration file is needed")

    def copy_pkgdefault(self, force=False):
        """
        Copy the corresponding sample from the project's package to destination config dir
        Sample config files are located inside the 'config' folder of the package
        - force: (boolean) copy sample regardless of existence of user's config
        """
        # Make own dir in user's config dir
        make_dir(os.path.dirname(self.usercfg['path']))

        # Copy sample file to user's config dir
        pkgcfg_path = pkg_resources.resource_filename(__name__, self.usercfg['name'])
        file_copied = copy_file(pkgcfg_path, self.usercfg['path'], force=force)

        if file_copied:
            self.log.warning("Installed sample configuration file into '%s'", self.usercfg['path'])

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

        return self

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
        if os.path.isfile(self.usercfg['path']):
            self.opts.read(self.usercfg['path'])
            self.log.debug("Succesfully parsed configuration file: %s", self.usercfg['path'])
        else:
            errmsg = f"Configuration file not found: {self.usercfg['path']}"
            raise FileNotFoundError(errmsg)


MainConf = ConfigFile()
