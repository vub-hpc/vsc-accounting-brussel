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
Accounting data processing for vsc.accounting

@author: Alex Domingo (Vrije Universiteit Brussel)
@author: Stéphane Gérard (Vrije Universiteit Brussel)
@author: Ward Poelmans (Vrije Universiteit Brussel)
"""

import os
import re
import pandas as pd

from datetime import date, datetime

from vsc.utils import fancylogger
from vsc.accounting.exit import error_exit
from vsc.accounting.parallel import parallel_exec
from vsc.accounting.config.parser import MainConf
from vsc.accounting.elasticsearch import ElasticTorque
from vsc.accounting.data.userdb import UserDB


DATE_FORMAT = '%Y-%m-%d'  # Use ISO date format


class ComputeUnits:
    """
    Supported compute units and methods to convert them
    """

    def __init__(self, units='corehours'):
        """
        Define charcateristics of supported units and set the active one
        Compute units are normalized to days to work with a common reference independent
        of time resolutions (date_freq in ComputeTimeCount)
        - units: (string) selected units used to account compute time
        """
        self.log = fancylogger.getLogger(name=self.__class__.__name__)

        self.known_units = {
            'corehours': {'name': 'corehours', 'shortname': 'chd', 'freq': 'day', 'factor': 3600},
            'coredays': {'name': 'coredays', 'shortname': 'cdd', 'freq': 'day', 'factor': 86400},
        }

        self.set_units(units)

    def set_units(self, units):
        """
        Change active compute units
        """
        try:
            self.active_units = self.known_units[units]
        except KeyError as err:
            errmsg = f"Unknown compute units {units}: {err}"
            error_exit(self.log, errmsg)
        else:
            self.log.debug("Compute units set to '%s'", self.active_units['name'])

        # Generate normalized name of the units
        self.active_units['normname'] = self.active_units['name']
        if self.active_units['freq']:
            self.active_units['normname'] = f"{self.active_units['normname']}/{self.active_units['freq']}"

    def job_seconds_to_compute(self, job_time, used_cores, period_span):
        """
        Returns compute time per day using the active compute units
        Warning: this function is structured to work with individual variables, pd.Series or pd.DataFrames that contain
                 the following numerical parameters
        - job_time: (float) real used time in seconds
        - used_cores: (int) number of cores used during job_time
        - period_span: (int) length of time period
        """
        try:
            total_compute_units = job_time * used_cores / self.active_units['factor']
            daily_compute_units = total_compute_units / period_span
        except ValueError as err:
            error_exit(self.log, f"Compute time unit conversion to {self.active_units['name']} failed: {err}")
        else:
            return daily_compute_units


class ComputeTimeCount:
    """
    Creates data frames with compute time used on any group of nodes in a period of time
    The data frames index both time and name of group of nodes
    Data included in each data frame:
    - GlobalStats: compute capacity over time per group of nodes
    - GlobalStats: compute time used over time per group of nodes
    - GlobalStats: running jobs over time per group of nodes
    - GlobalStats: number of unique users over time per group of nodes
    - UserList, FieldList: set of entities included in data frames with corresponding aggregate data
    - UserCompute, FieldCompute: compute time used by each entity over time per group of nodes
    - UserJobs, FieldJobs: running jobs of each entity over time per group of nodes
    """

    def __init__(self, date_start, date_end, date_freq, compute_units='corehours'):
        """
        Inititalize data frames for the provided period of time
        - date_start, date_end: (date) limits of the period of time
        - date_freq: (pd.timedelta) string defining the frequency of time entries
        - compute_units: (string) units used to account compute time
        """
        self.log = fancylogger.getLogger(name=self.__class__.__name__)

        # Set global compute units and save them in here
        ComputeUnits.set_units(compute_units)
        self.compute_units = ComputeUnits.active_units

        # Use global date format
        self.dateformat = DATE_FORMAT

        # Set range of dates
        try:
            self.dates = self.set_dates(date_start, date_end, date_freq)
        except ValueError as err:
            error_exit(self.log, err)

        # Set number of procs for parallel processing from configuration file
        try:
            self.max_procs = MainConf.get_digit('nodegroups', 'max_procs', fallback=None, mandatory=False)
        except (KeyError, ValueError) as err:
            error_exit(self.log, err)
        else:
            self.log.debug("Maximum number of processor set to %s", self.max_procs)

        # Specifications of each groups of nodes
        self.NG = dict()

        # Index both dates and nodegroups (empty unless nodegroups are added)
        self.index = pd.MultiIndex.from_product([self.dates, []], names=['date', 'nodegroup'])

        # Compute time indexing both dates and nodegroups
        self.GlobalStats = pd.DataFrame(
            columns=['capacity', 'compute_time', 'running_jobs', 'unique_users'], index=self.index
        )

        # Aggregate stats (columns are dynamically added for each section)
        for section in ['User', 'Field', 'Site']:
            self.setattr(section + 'List', set())
            self.setattr(section + 'Compute', pd.DataFrame({}, index=self.index))
            self.setattr(section + 'Jobs', pd.DataFrame({}, index=self.index))

        # User account data
        self.UserAccounts = pd.DataFrame(columns=['user', 'field', 'site', 'updated'])
        self.UserAccounts = self.UserAccounts.set_index('user')

        self.log.debug("Global and aggregate data structures initialized")

    def set_dates(self, date_start, date_end, date_freq):
        """
        Return fixed frequency DatetimeIndex between date_start and date_end
        Performs additional checks on the validity of input parameters
        - date_start, date_end: (date) limits of the period of time
        - date_freq: (pd.timedelta) string defining the frequency of time entries
        """
        # Check number of days in the range
        t_delta = date_end - date_start
        if t_delta.days < 0:
            errmsg = f"End date [{date_end}] is earlier than the start date [{date_start}]"
            raise ValueError(errmsg)
        else:
            self.log.info("Requested period of time: %s days from %s to %s", t_delta.days, date_start, date_end)

        # Generate index with requested time periods
        idx_dates = pd.date_range(date_start, date_end, freq=date_freq)
        # Calculate frequency in days (idx_dates[0] always exists at this point)
        day_freq = ((idx_dates[0] + idx_dates.freq) - idx_dates[0]).days
        # Remove last element from the index of dates to avoid accounting stats beyond the end date
        idx_dates = idx_dates.delete(-1)

        # Check time resolution and range of dates
        if len(idx_dates) < 1:
            errmsg = f"Time resolution ({day_freq} days) is longer than requested period of time ({t_delta.days} days)"
            raise ValueError(errmsg)
        else:
            # Report effective time interval (can be different to requested dates due to frequency constraints)
            eff_start = idx_dates[0]
            eff_end = idx_dates[-1] + idx_dates.freq
            eff_delta = (eff_end - eff_start).days
            infomsg = "Effective period of time: %s days from %s to %s"
            self.log.info(infomsg, eff_delta, eff_start.strftime(self.dateformat), eff_end.strftime(self.dateformat))

            # Check for at least two data points in the index
            if len(idx_dates) == 1:
                errmsg = f"Time resolution ({day_freq} days) only allows for a single data point. Increase resolution."
                raise ValueError(errmsg)
            else:
                # Re-calculate frequency as average because some DateOffsets have non-fixed frequency
                day_freq = idx_dates.to_series().diff().mean().days
                self.log.info("Time resolution: %s days (%s)", day_freq, idx_dates.freqstr)

        return idx_dates

    def add_nodegroup(self, nodegroup, cores, hostlist):
        """
        Add the definition of a new node group to the accounting of stats
        - nodegroup: (string) name of the new group of nodes
        - cores: (integer) number of cores per node
        - hostlist: (list of dicts) each element should include
                    {regex: pattern of hostnames, n: number of nodes, start: date string, end: date string}
        """
        # Check number of cores
        if str(cores).isdigit():
            self.log.debug("'%s' cores per host: %s", nodegroup, cores)
        else:
            errmsg = f"Cores per host of nodegroup '{nodegroup}' are not a positive integer"
            error_exit(self.log, errmsg)

        # Update nodegroup host list with cores per node and add missing start and end datetimes
        for n, host in enumerate(hostlist):
            hostlist[n].update({'cores': cores})
            try:
                hostlist[n]['start'] = pd.Timestamp(host.get('start', date(2018, 1, 1)))
                hostlist[n]['end'] = pd.Timestamp(host.get('end', date.today()))
            except ValueError as err:
                errmsg = f"Dates of host {n} in nodegroup '{nodegroup}' are not in ISO format"
                error_exit(self.log, errmsg)
            else:
                dates_str = (
                    hostlist[n]['start'].strftime(self.dateformat),
                    hostlist[n]['end'].strftime(self.dateformat),
                )
                self.log.debug("'%s' host %s active period: %s to %s", nodegroup, n, *dates_str)

        # Add group of nodes
        self.NG.update({nodegroup: hostlist})
        self.log.debug("'%s' nodegroup succesfully defined", nodegroup)

        # Create corresponding indexes for this group of nodes
        multidx = ['date', 'nodegroup']
        ng_index = pd.MultiIndex.from_product([self.dates, [nodegroup]], names=multidx)
        self.index = self.index.append(ng_index)

        # Start with capacity stats of this nodegroup
        ng_capacity = pd.DataFrame([self.update_capacity(*dt) for dt in ng_index])
        ng_capacity = ng_capacity.set_index(multidx)
        self.log.debug("'%s' updated %s capacity records", nodegroup, ng_capacity.shape[0])

        # Retrieve compute stats of this nodegroup
        ng_compute = parallel_exec(
            count_computejobsusers,  # worker function
            f"'{nodegroup}' compute/job counter",  # label prefixing log messages
            ng_index.levels[0],  # stack of items to process
            (nodegroup, self.NG[nodegroup]),  # nodegroup_spec: forwarded to worker function
            procs=self.max_procs,
            logger=self.log,
            peruser=True,  # forwarded to worker function
        )
        # Serial version
        # ng_compute = [count_computejobsusers(n, *dt, peruser=True) for (n, dt) in enumerate(ng_index)]
        self.log.debug("'%s' retrieved %s compute time data records", nodegroup, len(ng_compute))

        # Unpack compue stats and create data frame with global compute stats
        ng_global, ng_peruser = zip(*ng_compute)
        ng_global = pd.DataFrame(ng_global).set_index(multidx)
        ng_global = pd.merge(ng_capacity, ng_global, left_index=True, right_index=True, sort=True)
        self.GlobalStats = self.GlobalStats.combine_first(ng_global)
        self.log.debug("'%s' Global stats completed with %s data records", nodegroup, self.GlobalStats.shape[0])

        # Unpack user stats and create data frame with user compute time and jobs
        ng_peruser = [(record['compute'], record['jobs']) for record in ng_peruser]
        ng_peruser_compute, ng_peruser_jobs = zip(*ng_peruser)
        ng_peruser_compute = pd.DataFrame(ng_peruser_compute).set_index(multidx)
        ng_peruser_jobs = pd.DataFrame(ng_peruser_jobs).set_index(multidx)
        ng_peruser_counters = [('Compute', ng_peruser_compute), ('Jobs', ng_peruser_jobs)]

        # Update list of active users with users from this nodegroup
        ng_users = set(ng_peruser_compute.columns)
        self.UserList.update(ng_users)
        self.log.debug("'%s' %s unique users added to accounting", nodegroup, len(ng_users))

        # Retrieve account data for users in this nodegroup
        ng_user_accounts = pd.DataFrame.from_dict(UserDB(ng_users).records, orient='index')
        ng_user_accounts.index.name = 'user'
        self.UserAccounts = self.UserAccounts.combine_first(ng_user_accounts)

        # Update user data and generate aggregates per field and site
        for counter_name, counter_data in ng_peruser_counters:
            # Order data by date
            counter_data.sort_index(level='date', ascending=True, inplace=True)
            # Add to respective data frame
            UserCounts = self.getattr('User' + counter_name)
            UserCounts = UserCounts.combine_first(counter_data).fillna(0)
            self.setattr('User' + counter_name, UserCounts)
            dbgmsg = "'%s' User %s stats completed with %s data records for %s users"
            self.log.debug(dbgmsg, nodegroup, counter_name.lower(), len(counter_data.index), len(counter_data.columns))

            for category in ['Field', 'Site']:
                # Aggregate user data per category
                ng_percategory = self.aggregate_account_category(counter_data, ng_user_accounts, category)
                aggregate_counts = (len(counter_data.columns), len(ng_percategory.columns))
                infomsg = "'%s' adding %s aggregates for %s users in %s '%s' categories"
                self.log.info(infomsg, nodegroup, counter_name.lower(), *aggregate_counts, category)
                # Add aggregate to global data structure
                CategoryCounts = self.getattr(category + counter_name)
                CategoryCounts = CategoryCounts.combine_first(ng_percategory).fillna(0)
                self.setattr(category + counter_name, CategoryCounts)
                # Update list of categories
                CategoryList = self.getattr(category + 'List')
                CategoryList.update(ng_percategory.columns)
                self.setattr(category + 'List', CategoryList)

    def update_capacity(self, period_start, nodegroup):
        """
        Returns dict with compute capacity in the given period of time for the nodegroup
        Nodes are accounted as active on a daily basis
        - period_start: (pd.timestamp) start of time interval
        - nodegroup: (string) name of group of nodes
        """
        compute_capacity = 0

        # Define length of current period
        period_end = period_start + period_start.freq
        period_span = period_end - period_start

        # Iterate over days inside given time period
        curr_day = period_start
        while curr_day < period_end:
            next_day = curr_day + pd.DateOffset(hours=24)
            # Check which hosts were active from start to end of this day
            for host in self.NG[nodegroup]:
                if host['start'] <= curr_day and host['end'] >= next_day:
                    # Update capacity with this active node group. Add full day of capacity (86400 s)
                    compute_capacity += ComputeUnits.job_seconds_to_compute(
                        86400, host['cores'] * host['n'], period_span.days
                    )
            curr_day = next_day

        return {'date': period_start, 'nodegroup': nodegroup, 'capacity': compute_capacity}

    def aggregate_account_category(self, sparse_user_data, user_accounts, account_category):
        """
        Aggregate user stats per account parameter
        - sparse_user_data: (pd.DataFrame) non-aggregated user data
        - user_accounts: (pd.DataFrame) user account information including their research field
        - account_category: (string) attribute in user account records to categorize aggregation
        """
        # Category names in DataFrame columns should be lowercase
        account_category = account_category.lower()

        # List users present in sparse data
        if sparse_user_data.columns.nlevels > 1:
            users = sparse_user_data.columns.get_level_values(0)
        else:
            users = sparse_user_data.columns

        # Get unique categories of users in sparse data
        categories = user_accounts.loc[users, account_category]
        # Add categories to column index of sparse data. Aggregate category always added to level 1.
        categories = categories.to_frame().reset_index().rename(columns={'index': 'user'})
        sparse_user_data.columns = pd.MultiIndex.from_frame(categories)

        # Aggregate data
        aggregate_category_data = sparse_user_data.groupby(axis=1, level=1).sum()

        return aggregate_category_data

    def aggregate_perdate(self, source, selection, destination=None):
        """
        Aggregate data in selected column per each date in time interval
        Add/Update the aggregation to destination data frame as a new column prefixed with "total"
        - source: (string) name of ComputeTimeCounter attribute with the source data
        - selection: (string) name of column to aggregate
        - destination: (string) name of ComputeTimeCounter attribute to store aggregation
        """
        if not destination:
            destination = source

        source_data = self.getattr(source)
        dest_data = self.getattr(destination)

        # Execute aggregation per date
        try:
            aggregate = source_data.loc[:, selection].groupby('date').sum()
        except KeyError:
            errmsg = f"Aggregation per date failed: {selection} data not found in {source}"
            error_exit(self.log, errmsg)

        aggregate_name = 'total_{}'.format(selection)
        if aggregate_name in dest_data.columns:
            # Update existing data in destination
            dest_data.update(aggregate.rename(aggregate_name))
            aggregate_action = 'Updated'
        else:
            # Add aggregation as new data to destination
            dest_data = dest_data.join(aggregate.rename(aggregate_name))
            aggregate_action = 'Added'

        self.setattr(destination, dest_data)
        self.log.debug("%s aggregation of %s per date in %s succesfully", aggregate_action, selection, destination)

        return True

    def add_percentage(self, source, absolute, reference, percent_name=None):
        """
        Add column to source DataFrame with percentage of absolute values in reference values
        - source: (string) name of DataFrame in ComputeTimeCount
        - absolute, reference: (string) calculate percentage as absolute / reference
        - percent_name: (string) name of column to save percentage data
        """
        source_data = self.getattr(source)
        # Calculate percentage avoiding divides by zero and replacing NaN with zeros
        percent_data = source_data.loc[:, absolute] / source_data.loc[:, reference].replace({0: float('nan')})
        percent_data = percent_data.fillna(0)

        if not percent_name:
            percent_name = 'percent_{}'.format(reference)

        if percent_name in source_data.columns:
            source_data.update(percent_data.rename(percent_name))
            percent_action = 'Updated'
        else:
            source_data = source_data.join(percent_data.rename(percent_name))
            percent_action = 'Added'

        self.setattr(source, source_data)
        self.log.debug("%s percentual data '%s' in %s succesfully", percent_action, percent_name, source)

    def rank_aggregate(self, aggregate):
        """
        Returns data frame with ranking of entities from aggregated data
        Supported aggregates: Users, Fields and Sites
        - aggregate: (string) name of aggregate
        """
        rankings = list()

        # Aggregate data per date
        entity_list = self.getattr(aggregate + 'List')
        compute_data = self.getattr(aggregate + 'Compute')
        compute_data = compute_data.loc[:, entity_list].groupby('date').sum()

        # Rank entities per compute time
        compute_rank = compute_data.sum(axis=0)
        compute_rank = compute_rank.rename('compute_time')
        rankings.append(compute_rank)
        self.log.debug("Ranked %s %ss by total compute time", len(compute_rank), aggregate)

        # Add mean compute time
        compute_count = compute_data.count(axis=0)
        mean_rank = compute_rank / compute_count
        mean_rank = mean_rank.rename('compute_average')
        rankings.append(mean_rank)
        self.log.debug("Ranked %s %ss by mean compute time", len(mean_rank), aggregate)

        # Add percentage to ranking
        total_compute = self.GlobalStats.loc[:, 'compute_time'].sum()
        percent_rank = compute_rank / total_compute
        percent_rank = percent_rank.rename('compute_percent')
        rankings.append(percent_rank)
        self.log.debug("Ranked %s %ss by percentage compute time", len(percent_rank), aggregate)

        # Add percentile to ranking
        sorted_percent = percent_rank.sort_values(ascending=False)
        percentile_rank = [sorted_percent.iloc[0 : i + 1].sum() for i in range(len(sorted_percent))]
        percentile_rank = pd.Series(percentile_rank, index=sorted_percent.index)
        percentile_rank = percentile_rank.rename('compute_percentile')
        rankings.append(percentile_rank)
        self.log.debug("Distributed %s %ss in percentiles by compute time", len(percentile_rank), aggregate)

        # Strip any index names
        for r, rank in enumerate(rankings):
            rankings[r].index.name = None
        # Combine all series in a single data frame
        rankings = pd.concat(rankings, axis=1, sort=False)
        rankings = rankings.sort_values(by=['compute_time'], ascending=False)

        # Calculate average length of time periods (some frequencies have periods of slightly different length)
        period_span = compute_data.index.to_series().diff().mean().days
        # Convert daily compute time to absolute compute time in the time period
        rankings['compute_time'] = rankings.loc[:, 'compute_time'] * period_span

        self.log.info("Ranking of %s %ss by compute time generated succesfully", len(rankings.index), aggregate)

        return rankings

    def unpack_indexes(self, target):
        """
        Returns dict with regular indexes of unique elements in the index or multiindex of target pd.DataFrame
        - target: (string) name of object
        """
        data_obj = self.getattr(target)

        index_lists = dict()
        if isinstance(data_obj, pd.DataFrame) or isinstance(data_obj, pd.Series):
            for level in range(data_obj.index.nlevels):
                idx = data_obj.index.levels[level]
                if isinstance(idx, pd.DatetimeIndex):
                    index_lists.update({idx.name: idx.strftime(self.dateformat)})
                else:
                    index_lists.update({idx.name: idx})
        else:
            index_lists = None

        return index_lists

    def getattr(self, target_name):
        """
        Wrapper around getattr with error handling
        Returns existing attribute in ComputeTimeCount
        - target_name: (string) name of attribute in ComputeTimeCount
        """
        try:
            target_attr = getattr(self, target_name)
        except AttributeError as err:
            errmsg = f"Attribute {target_name} not found in ComputeTimeCount object"
            error_exit(self.log, errmsg)
        else:
            return target_attr

    def setattr(self, target_name, local_data):
        """
        Wrapper around setattr with error handling
        - target_name: (string) name of attribute in ComputeTimeCount
        - local_data: (object) data to be saved in target attribute
        """
        try:
            setattr(self, target_name, local_data)
        except AttributeError as err:
            errmsg = f"Attribute {target_name} could not be set in ComputeTimeCount object"
            error_exit(self.log, errmsg)
        else:
            return True


# The following functions fit more naturally into the class ComputeTimeCount.
# However, in Python 3.6, ProcessPoolExecutor from multiprocessing cannot
# pickle functions from within classes. Any functions participating in the
# ProcessPoolExecutor have to be in the top level of the caller's module.
# Python 3.5, 3.7 and 3.8 do *not* have this restriction and can directly
# pickle from within the caller's class.
# See issue: https://bugs.python.org/issue29423


def count_computejobsusers(period_start, nodegroup_spec, peruser=False, logger=None):
    """
    Returns dict with global counters on running jobs, unique users and compute time
    Data source is a list of running jobs in the given time period and in the given nodegroup
    - period_start: (pd.timestamp) start of time interval
    - nodegroup_spec: (tuple) (nodegroup: [hosts])
      - nodegroup: (string) name of group of nodes
      - hosts: (list) [{regex: hostname pattern, n: number of nodes, start: date string, end: date string}]
    - peruser: (boolean) return additional dicts with stats per user
    - logger: (object) fancylogger object of the caller
    """
    if logger is None:
        logger = fancylogger.getLogger()

    # Unpack nodegroup_spec
    (nodegroup, ng_hosts) = nodegroup_spec

    # Define length of current period
    period_end = period_start + period_start.freq
    period_span = period_end - period_start

    # Generate unique ID for the query
    query_id = period_start.strftime(DATE_FORMAT)
    # Retrieve jobs from ElasticSearch
    jobs = get_joblist_ES(query_id, period_start, nodegroup_spec, logger=logger)

    # Calculate global counters
    total_jobs = len(jobs.index)
    jobs = jobs[~jobs.index.duplicated()]  # Remove duplicate jobs based on job ID
    running_jobs = len(jobs.index)
    duplicate_jobs = total_jobs - running_jobs
    compute_time = jobs.loc[:, 'compute'].sum()
    unique_users = len(jobs.loc[:, 'username'].unique())

    global_counters = {
        'date': period_start,
        'nodegroup': nodegroup,
        'compute_time': compute_time,
        'running_jobs': running_jobs,
        'unique_users': unique_users,
    }

    # Report hits and duplicates in this period of time
    period_msg = f"{period_span.days}d from {period_start.strftime(DATE_FORMAT)}"
    info_msg = f"'{nodegroup}' {period_msg}: {running_jobs:5d} hits {unique_users:5d} users"
    if duplicate_jobs > 0:
        info_msg += f" ({duplicate_jobs} duplicates removed)"
    logger.info(info_msg)

    # Normalize global non-compute counters to units per day
    for counter in ['running_jobs', 'unique_users']:
        global_counters[counter] /= period_span.days

    logger.debug("'%s' period [%s] global counters: %s", nodegroup, query_id, global_counters)

    if peruser:
        # Agregate stats per user
        # Counters for compute and jobs are kept in separate dicts to feed separate DataFrames
        peruser_counters = {
            'compute': jobs.groupby('username').sum(),
            'jobs': jobs.groupby('username').count(),
        }

        # Normalize counter of jobs to units per day
        peruser_counters['jobs']['compute'] /= period_span.days

        # Generate list of dicts with one dict per user with its counters and indexes
        # [{username: counter, date: date, nodegroup: nodegroup}, ...]
        for prop, users_counter in peruser_counters.items():
            users_entry = users_counter.transpose().to_dict('index')['compute']
            users_entry.update({'date': period_start, 'nodegroup': nodegroup})
            peruser_counters[prop] = users_entry
    else:
        peruser_counters = None

    # This can be quite verbouse
    # logger.debug("'%s' period [%s] normalized per user counters: %s", nodegroup, query_id, peruser_counters)

    return global_counters, peruser_counters


def get_joblist_ES(query_id, period_start, nodegroup_spec, logger=None):
    """
    Returns pd.DataFrame with list of jobs running in the current time period
    Data is retrieved from ElasticSearch
    Retrieves 'job end' events on this group of nodes
    Calculates used compute time by jobs in the period of time
    - query_id: (int) arbitrary identification number of the query
    - period_start: (pd.timestamp) start of time interval
    - nodegroup_spec: (tuple) (nodegroup: [hosts])
      - nodegroup: (string) name of group of nodes
      - hosts: (list) [{regex: hostname pattern, n: number of nodes, start: date string, end: date string}]
    - logger: (object) fancylogger object of the caller
    """
    if logger is None:
        logger = fancylogger.getLogger()

    # Unpack nodegroup_spec
    (nodegroup, ng_hosts) = nodegroup_spec

    # Define length of current period
    period_end = period_start + period_start.freq
    period_span = period_end - period_start

    # Connect to ElasticSearch
    ES = ElasticTorque(query_id)
    # Query indexes with data in this time period
    ES.set_index(period_start, period_end)
    # Match active nodes in this time period
    ES.query_usednodes(period_start, period_end, ng_hosts)
    # Filter "job end" events
    ES.filter_term('action.keyword', 'E')
    # Set time range of query
    ES.filter_timerange(period_start, period_end)
    # Set data fields to retrieve
    ES.set_source(extra=['jobid', 'username', 'exec_host', 'total_execution_slots'])
    logger.debug("'%s' ES query [%s]: %s", nodegroup, query_id, ES.search.to_dict())

    ES.hits = pd.DataFrame([hit.to_dict() for hit in ES.scan_hits()])
    logger.debug("'%s' ES query [%s] retrieved %s hits", nodegroup, query_id, len(ES.hits))

    # Calculate compute time for each job on this time period
    ES.hits['compute'] = ES.hits.apply(
        lambda row: job_span(
            pd.to_datetime(row['start_time'], format=ES.timeformat, errors='coerce'),
            pd.to_datetime(row['end_time'], format=ES.timeformat, errors='coerce'),
            period_start,
            period_end,
        ),
        axis=1,  # apply row wise
        result_type='reduce',  # return series if possible
    )

    # Account number of cores used by each job
    ES.hits['cores'] = ES.hits.apply(
        lambda row: corecount(nodegroup_spec, row['exec_host'], row['total_execution_slots']),
        axis=1,  # apply row wise
        result_type='reduce',  # return series if possible
    )
    # Convert compute time to units defined in ComputeUnits
    ES.hits['compute'] = ComputeUnits.job_seconds_to_compute(ES.hits['compute'], ES.hits['cores'], period_span.days)

    # Select username and compute time for each job
    jobs = pd.DataFrame(columns=['jobid', 'username', 'compute'])
    jobs = jobs.append(ES.hits.loc[:, ES.hits.columns.intersection(jobs.columns)], sort=False)
    jobs = jobs.set_index('jobid')
    logger.debug("'%s' ES query [%s] processed %s jobs", nodegroup, query_id, len(jobs))

    return jobs


def corecount(nodegroup_spec, job_hosts, totalcores=None):
    """
    Returns number of cores of hosts in job_hosts that belong to this group of nodes
    If all hosts match returns totalcores (if provided)
    Otherwise, it counts the number of cores in matching hosts
    - job_hosts: (list) hostnames allocated to job
    - nodegroup_spec: (tuple) (nodegroup: [hosts])
      - nodegroup: (string) name of group of nodes
      - hosts: (list) [{regex: hostname pattern, n: number of nodes, start: date string, end: date string}]
    - totalcores: (integer) number of cores used by job
    """
    corespec = list()
    corecount = 0

    # Unpack nodegroup_spec
    (nodegroup, ng_hosts) = nodegroup_spec

    # Take core specification for job hosts matching current node group
    for node in ng_hosts:
        corespec += [host.split('/')[1] for host in job_hosts if re.match(node['regex'], host)]

    if totalcores and len(corespec) == len(job_hosts):
        # All job hosts are in this nodegroup
        corecount = totalcores
    else:
        # Count the actual number of cores used in matching hosts
        # This count should only be needed in a few cases
        # (e.g. jobs that used non-GPU and GPU nodes simultaneously)
        coreranges = [numrange for spec in corespec for numrange in spec.split(',')]
        # Add to total the first core of all elements in list
        corecount += len(coreranges)
        # Add to total any additional cores in ranges of cores
        for numrange in coreranges:
            if '-' in numrange:
                corenum = numrange.split('-')
                corecount += int(corenum[1]) - int(corenum[0])

    return corecount


def job_span(job_start, job_end, period_start, period_end):
    """
    Returns seconds that job overlaps with period of time
    - job_start, job_end: (pd.datetime) start and end timestamps of the job
    - period_start, period_end: (pd.datetime) start and end timestamps of the period
    """
    # Pick latest start
    tstart = max(job_start, period_start)
    # Pick earliest end
    tend = min(job_end, period_end)

    tspan = pd.Timedelta(tend - tstart)
    tspan = tspan.total_seconds()

    return tspan


# Create global object with compute units to be shared between ComputeTimeCount and top level functions
ComputeUnits = ComputeUnits()
