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
Retrieve user data cached in json file
Update user records with data from VSC account page

@author: Alex Domingo (Vrije Universiteit Brussel)
@author: Ward Poelmans (Vrije Universiteit Brussel)
"""

from datetime import date, datetime
from urllib.error import HTTPError, URLError

from vsc.utils import fancylogger
from vsc.config.base import ANTWERPEN, BRUSSEL, GENT, LEUVEN, INSTITUTE_LONGNAME
from vsc.accountpage.client import AccountpageClient
from vsc.accounting.exit import error_exit
from vsc.accounting.parallel import parallel_exec
from vsc.accounting.config.parser import MainConf, ConfigFile
from vsc.accounting.data.parser import DataFile

UNKNOWN_FIELD = 'Unknown'


class UserDB:
    """
    Handle a DB of user account data in a DataFile
    Sync user data with VSC account page
    """

    def __init__(self, requested_users):
        """
        Generate data base with user account data
        requested_users: (iterable) list of usernames to include in the data base
        """
        self.log = fancylogger.getLogger(name=self.__class__.__name__)

        # Set number of procs for parallel processing from configuration file
        try:
            self.max_procs = MainConf.get_digit('nodegroups', 'max_procs', fallback=None, mandatory=False)
        except (KeyError, ValueError) as err:
            error_exit(self.log, err)
        else:
            self.log.debug("Maximum number of processor set to %s", self.max_procs)

        # Check requested list of users
        try:
            self.users = list(requested_users)
        except TypeError as err:
            errmsg = f"Cannot generate user data base from non-iterable user list: {requested_users}"
            error_exit(self.log, errmsg)

        # Get token from configuration file to access VSC account page
        TokenConfig = ConfigFile()
        try:
            vsc_token_file = MainConf.get('userdb', 'vsc_token_file', fallback='vsc-access.ini', mandatory=False)
            self.vsc_token = TokenConfig.load(vsc_token_file).get('MAIN', 'access_token')
        except KeyError as err:
            error_exit(self.log, err)

        # Load user data base from local cache
        self.cache = self.load_db_cache()

        # Update list of users with their records in the cache
        for n, user in enumerate(self.users):
            if user in self.cache.contents['db']:
                self.users[n] = (user, self.cache.contents['db'][user])
            else:
                self.users[n] = (user, None)

        # Retrieve account data of requested users
        self.log.info(f"Retrieving {len(self.users)} user account records...")
        requested_records = parallel_exec(
            get_updated_record,  # worker function
            f"User account retrieval",  # label prefixing log messages
            self.users,  # stack of items to process
            self.cache.contents['valid_days'],  # record_validity: forwarded to worker function
            self.vsc_token,  # vsc_token: forwarded to worker function
            procs=self.max_procs,
            logger=self.log,
        )

        # Generate dict of user accounts and update cache
        self.records = dict()
        for user_record in requested_records:
            self.records.update(user_record)
            self.cache.contents['db'].update(user_record)

        # Save local cache
        self.cache.save_data()

    def init_db_cache(self):
        """
        Returns empty cache with db placeholder and default meta data
        """
        try:
            valid_days = MainConf.get_digit('userdb', 'default_valid_days', fallback=30, mandatory=False)
        except (KeyError, ValueError) as err:
            error_exit(self.log, err)
        else:
            empty_db = {'valid_days': valid_days, 'db': dict()}
            self.log.info(f"Initialized empty data base of users with a validity of %s days", valid_days)

        return empty_db

    def load_db_cache(self):
        """
        Read contents of user data base from local cache file
        If cache does not exist, inititalize it
        """
        # Use local cache file defined in configuration
        cache_file = MainConf.get('userdb', 'cache_file', fallback='userdb-cache.json', mandatory=False)
        cache = DataFile(cache_file, mandatory=False)

        if hasattr(cache, 'contents'):
            self.log.debug(f"Data base of users populated from local cache")
        else:
            self.log.warning(f"Data base of users not found in local cache: {cache.datafile}")
            cache.contents = self.init_db_cache()

        return cache


# The following functions fit more naturally into the class UserDB.
# However, in Python 3.6, ProcessPoolExecutor from multiprocessing cannot
# pickle functions from within classes. Any functions participating in the
# ProcessPoolExecutor have to be in the top level of the caller's module.
# Python 3.5, 3.7 and 3.8 do *not* have this restriction and can directly
# pickle from within the caller's class.
# See issue: https://bugs.python.org/issue29423


def user_basic_record(username, logger=None):
    """
    Generate basic user record from user name
    All VSC IDs are ascribed to their site, other usernames are identified as NetID users from ULB
    WARNING: old NetIDs from VUB without a VSC account are suposed to be already accounted in the cache file
    - username: (string) username of the account
    - logger: (object) fancylogger object of the caller
    """
    if logger is None:
        logger = fancylogger.getLogger()

    # Research field is always unknown in these cases
    user_record = {'field': UNKNOWN_FIELD}

    # Determine site of account
    site = (None, BRUSSEL, ANTWERPEN, LEUVEN, GENT)

    if username[0:3] == 'vsc' and username[3].isdigit():
        user_site_index = int(username[3])
        user_record.update({'site': INSTITUTE_LONGNAME[site[user_site_index]]})
        logger.warning(f"[{username}] account not found in VSC account page, record added with site information only")
    else:
        user_record.update({'site': "Université Libre de Bruxelles"})

    # Set timestamp to today
    user_record.update({'updated': date.today().isoformat()})

    return user_record


def get_vsc_record(username, vsc_token, logger=None):
    """
    Retrieve and update list of VSC users with data from VSC account page
    - username: (string) VSC ID or institute user of the VSC account
    - vsc_token: (string) access token to VSC account page
    - logger: (object) fancylogger object of the caller
    """
    if logger is None:
        logger = fancylogger.getLogger()

    vsc_api_client = AccountpageClient(token=vsc_token)

    # Get institute login of the VSC account attached to this username
    if username[0:3] == 'vsc' and username[3].isdigit():
        # VSC ID: query institute login to VSC account page
        logger.debug(f"[{username}] user treated as VSC ID")
        try:
            vsc_account = vsc_api_client.account[username].person.get()[1]
        except HTTPError as err:
            if err.code == 404:
                error_exit(logger, f"[{username}] VSC ID not found in VSC account page")
            else:
                error_exit(logger, f"[{username}] {err}")
        except (TimeoutError, URLError) as err:
            error_exit(logger, f"[{username}] connection to VSC account page timed out")
        else:
            vsc_login = {'username': vsc_account['institute_login'], 'site': vsc_account['institute']['name']}
            logger.debug(f"[{username}] VSC ID belongs to VSC account '{vsc_login['username']}'")
    else:
        # Others: assume NetID from Brussels
        logger.debug(f"[{username}] user treated as NetID")
        vsc_login = {'username': username, 'site': BRUSSEL}

    # Retrieve user data from VSC account page
    try:
        vsc_account = vsc_api_client.account.institute[vsc_login['site']].id[vsc_login['username']].get()[1]
    except HTTPError as err:
        if err.code == 404:
            logger.debug(f"[{username}] with VSC account '{vsc_login['username']}' not found")
            return None
        else:
            error_exit(logger, f"[{username}] {err}")
    except (TimeoutError, URLError) as err:
        error_exit(logger, f"[{username}] connection to VSC account page timed out")
    else:
        logger.debug(f"[{username}] user account record retrieved from VSC account '{vsc_login['username']}'")

        # only use first entry of research field
        user_field = vsc_account['research_field'][0]
        # use custom label for 'unknown' fields
        if user_field == 'unknown':
            user_field = UNKNOWN_FIELD

        user_record = {
            'field': user_field,
            'site': INSTITUTE_LONGNAME[vsc_account['person']['institute']['name']],
            'updated': date.today().isoformat(),
        }

        return user_record


def get_updated_record(user_record, record_validity, vsc_token, logger=None):
    """
    Return user record with up to date information
    First check local cache. If missing or outdated check VSC account page
    - user_record: (tuple) username and its account record
    - record_validity: (int) number of days that user records are valid
    - vsc_token: (string) access token to VSC account page
    - logger: (object) fancylogger object of the caller
    """
    if logger is None:
        logger = fancylogger.getLogger()

    # Unpack user record
    (username, record_data) = user_record

    # Existing user
    if record_data:
        logger.debug(f"[{username}] user account record exists in local cache")

        try:
            # Calculate age of existing record
            # Once we can use Python 3.7+, the following can be replaced with date.fromisoformat()
            record_date = datetime.strptime(record_data['updated'], '%Y-%m-%d').date()
        except ValueError as err:
            errmsg = f"[{username}] user account record in local cache is malformed"
            error_exit(logger, errmsg)
        else:
            record_age = date.today() - record_date

        if record_age.days > record_validity:
            fresh_record = get_vsc_record(username, vsc_token)
            if fresh_record:
                # Update outdated record with data from VSC account page
                record_data.update(fresh_record)
                logger.debug(f"[{username}] user account record updated from VSC account page")
            else:
                # Account missing in VSC account page, keep existing record in our data base
                record_data['updated'] = date.today().isoformat()
    # New user
    else:
        # Retrieve full record from VSC account page
        record_data = get_vsc_record(username, vsc_token)
        if not record_data:
            # Generate a default record for users not present in VSC account page
            record_data = user_basic_record(username)
            logger.debug(f"[{username}] new user account registered as member of {record_data['site']}")

    return {username: record_data}
