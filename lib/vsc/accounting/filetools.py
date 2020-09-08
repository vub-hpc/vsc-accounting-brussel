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
File handling tools for vsc.accounting

@author: Alex Domingo (Vrije Universiteit Brussel)
"""

import os
import shutil

from vsc.utils import fancylogger
from vsc.accounting.exit import error_exit

logger = fancylogger.getLogger()


def check_abspath(fullpath):
    """
    Check if path is an absolute path
    - fullpath: (string) full path
    """
    if os.path.isabs(fullpath):
        return True
    else:
        errmsg = f"Provided path is not an absolute path: {fullpath}"
        raise ValueError(errmsg)


def check_dir(dirpath):
    """
    Check if path is an existing directory
    - dirpath: (string) path to directory
    """
    if os.path.isdir(dirpath):
        return True
    else:
        errmsg = f"Directory does not exist: {dirpath}"
        raise NotADirectoryError(errmsg)


def make_dir(dirpath):
    """
    Create directory in dir path if it does not exist
    - dirpath: (string) absolute path to directory
    """
    try:
        check_abspath(dirpath)
    except ValueError as err:
        error_exit(logger, err)

    try:
        os.makedirs(dirpath)
    except FileExistsError:
        if os.path.isdir(dirpath):
            logger.debug("Folder already exists: %s", dirpath)
            return False
        else:
            error_exit(logger, f"Path '{dirpath}' exists but is not a folder")
    except PermissionError:
        error_exit(logger, f"Permission denied to create folder: {dirpath}")
    else:
        logger.debug("Folder successfully created: %s", dirpath)
        return True


def copy_file(source, destination, force=False):
    """
    Copy file from source to destination if file in destination is missing
    Returns success of the copy operation
    - source: (string) absolute path to source file
    - destination: (string) absolute path to destination file
    - force: (boolean) copy file regardless of existence of destination
    """
    for filepath in [source, destination]:
        try:
            check_abspath(filepath)
        except ValueError as err:
            error_exit(logger, err)

    # Copy files if destination does not exist or force is enabled
    if not os.path.exists(destination) or force:
        try:
            shutil.copyfile(source, destination)
        except FileNotFoundError:
            logger.warning("Copy failed due to missing file: %s", source)
            return False
        except PermissionError:
            error_exit(logger, f"Permission denied to copy file '{source}' to '{destination}'")
        else:
            logger.debug("File '%s' succesfully copied to '%s'", source, destination)
            return True
    else:
        logger.debug("Nothing to copy, file already exists: %s", destination)
        return None


def find_available_path(filepath):
    """
    Check if given path does exist. If path exists generate a sensible variant that does not exist.
    Make parent folders if necessary.
    - filepath: (string) absolute path to a existing or non-existing file
    """
    try:
        check_abspath(filepath)
    except ValueError as err:
        error_exit(logger, err)

    # Make parent directories as needed
    parent_dir = os.path.dirname(filepath)
    make_dir(parent_dir)

    # Look for a file name that does not exist
    # Make variants of file name until we find one available
    replica = 0
    tentative_path = filepath
    while os.path.lexists(tentative_path):
        replica += 1
        if replica < 10000:
            # Prepend replica number to extension
            tentative_pathcut = filepath.split('.')
            tentative_pathcut.insert(-1, f"{replica:04}")
            tentative_path = '.'.join(tentative_pathcut)
        else:
            errmsg = f"Reached maximum number of replicas. Cannot find an available file name in path: {filepath}"
            raise FileExistsError(errmsg)

    logger.debug("Found available file name at path: %s", tentative_path)
    return tentative_path
