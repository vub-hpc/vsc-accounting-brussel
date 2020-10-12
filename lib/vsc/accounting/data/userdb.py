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

from concurrent import futures
from datetime import date, datetime
from urllib.error import HTTPError, URLError

from vsc.utils import fancylogger
from vsc.config.base import ANTWERPEN, BRUSSEL, GENT, LEUVEN, INSTITUTE_LONGNAME
from vsc.accountpage.client import AccountpageClient
from vsc.accounting.exit import error_exit, cancel_process_pool
from vsc.accounting.config.parser import MainConf, ConfigFile
from vsc.accounting.data.parser import DataFile

logger = fancylogger.getLogger()
fancylogger.logToScreen(True)
fancylogger.setLogLevelDebug()


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

        # Load all user data base from local cache
        self.cache = self.load_db_cache()

        # Retrieve account data of requested users
        self.log.info(f"Retrieving {len(self.users)} user account records...")
        self.records = self.gather_all_records(update_cache=True)

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
            self.log.debug(f"Data base of users read from local cache: {cache.datafile['path']}")
        else:
            self.log.warning(f"Data base of users not found in local cache: {cache.datafile['path']}")
            cache.contents = self.init_db_cache()

        return cache

    def user_basic_record(self, username):
        """
        Generate basic user record from user name
        All VSC IDs are ascribed to their site, other usernames are identified as NetID users from ULB
        WARNING: old NetIDs from VUB without a VSC account are suposed to be already accounted in the cache file
        - username: (string) username of the account
        """
        # Research field is always unknown in these cases
        user_record = {'field': 'Unknown'}

        # Determine site of account
        site_index = (BRUSSEL, ANTWERPEN, LEUVEN, GENT)

        if username[0:3] == 'vsc' and username[3].isdigit():
            user_record.update({'site': INSTITUTE_LONGNAME[site_index[vsc_id[3]]]})
        else:
            user_record.update({'site': "Université Libre de Bruxelles"})

        # Set timestamp to today
        user_record.update({'updated': date.today().isoformat()})

        return user_record

    def get_vsc_record(self, username):
        """
        Retrieve and update list of VSC users with data from VSC account page
        - username: (string) VSC ID or institute user of the VSC account
        """
        vsc_api_client = AccountpageClient(token=self.vsc_token)

        # Get institute login of the VSC account attached to this username
        if username[0:3] == 'vsc' and username[3].isdigit():
            # VSC ID: query institute login to VSC account page
            self.log.debug(f"[{username}] user treated as VSC ID")
            try:
                vsc_account = vsc_api_client.account[username].person.get()[1]
            except HTTPError as err:
                if err.code == 404:
                    error_exit(self.log, f"[{username}] VSC ID not found in VSC account page")
                else:
                    error_exit(self.log, f"[{username}] {err}")
            except (TimeoutError, URLError) as err:
                error_exit(self.log, f"[{username}] connection to VSC account page timed out")
            else:
                vsc_login = {'username': vsc_account['institute_login'], 'site': vsc_account['institute']['name']}
                self.log.debug(f"[{username}] VSC ID belongs to VSC account '{vsc_login['username']}'")
        else:
            # Others: assume NetID from Brussels
            self.log.debug(f"[{username}] user treated as NetID")
            vsc_login = {'username': username, 'site': BRUSSEL}

        # Retrieve user data from VSC account page
        try:
            vsc_account = vsc_api_client.account.institute[vsc_login['site']].id[vsc_login['username']].get()[1]
        except HTTPError as err:
            if err.code == 404:
                self.log.debug(f"[{username}] with VSC account '{vsc_login['username']}' not found")
                return None
            else:
                error_exit(self.log, f"[{username}] {err}")
        except (TimeoutError, URLError) as err:
            error_exit(self.log, f"[{username}] connection to VSC account page timed out")
        else:
            self.log.debug(f"[{username}] user account record retrieved from VSC account '{vsc_login['username']}'")

            user_record = {
                # only use first entry of research field
                'field': vsc_account['research_field'][0],
                'site': INSTITUTE_LONGNAME[vsc_account['person']['institute']['name']],
                'updated': date.today().isoformat(),
            }

            return user_record

    def get_updated_record(self, username):
        """
        Return user record with up to date information
        First check local cache. If missing or outdated check VSC account page
        - username: (string) username of the account
        """
        # Existing user
        if username in self.cache.contents['db']:
            # Retrieve record from existing local cache
            user_record = self.cache.contents['db'][username]
            self.log.debug(f"[{username}] user account record retrieved from local cache")

            try:
                # Once we can use Python 3.7+, the following can be replaced with date.fromisoformat()
                record_date = datetime.strptime(user_record['updated'], '%Y-%m-%d').date()
            except ValueError as err:
                errmsg = f"[{username}] user account record in local cache is malformed"
                error_exit(self.log, errmsg)
            else:
                record_age = date.today() - record_date

            if record_age.days > self.cache.contents['valid_days']:
                fresh_record = self.get_vsc_record(username)
                if fresh_record:
                    # Update outdated record with data from VSC account page
                    user_record.update(fresh_record)
                    self.log.debug(f"[{username}] user account record updated from VSC account page")
                else:
                    # Account missing in VSC account page, keep existing record in our data base
                    user_record['updated'] = date.today().isoformat()
        # New user
        else:
            # Retrieve full record from VSC account page
            user_record = self.get_vsc_record(username)
            if not user_record:
                # Generate a default record for users not present in VSC account page
                user_record = self.user_basic_record(username)
                self.log.debug(f"[{username}] new user account registered as member of {user_record['site']}")

        return {username: user_record}

    def gather_all_records(self, update_cache=False):
        """
        Retrieve user account records with up to date information for all users
        Record processing is parallelized using available processors on the machine
        - update_cache: (boolean) update user data base cache with new user records
        """
        records_db = dict()

        # Start process pool to retrieve all user records
        with futures.ProcessPoolExecutor(max_workers=self.max_procs) as executor:
            record_pool = {executor.submit(self.get_updated_record, user): user for user in self.users}
            for pid, completed_record in enumerate(futures.as_completed(record_pool)):
                try:
                    user_record = completed_record.result()
                except (futures.BrokenExecutor, futures.process.BrokenProcessPool) as err:
                    error_exit(self.log, f"Process pool executor to retrieve user account records failed")
                except futures.CancelledError as err:
                    # Child processes will be cancelled if any ends in error. Ignore error.
                    self.log.debug(f"Process [{pid}] to retrieve user account record cancelled successfully")
                    pass
                except SystemExit as exit:
                    if exit.code == 1:
                        # Child process ended in error. Cancel all remaining processes in the pool.
                        cancel_process_pool(self.log, record_pool, pid)
                        # Abort execution
                        errmsg = f"Retrieval of user account records failed. Aborting!"
                        error_exit(self.log, errmsg)
                else:
                    # Update contents of local cache
                    if update_cache:
                        self.cache.contents['db'].update(user_record)
                    # Add record to DB
                    records_db.update(user_record)

        return records_db
