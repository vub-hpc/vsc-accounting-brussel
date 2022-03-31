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
Data retrieval from CSV data files for vsc.accounting

@author Alex Domingo (Vrije Universiteit Brussel)
"""

import os
import re
import pandas as pd

from glob import glob

from vsc.utils import fancylogger
from vsc.accounting.exit import error_exit
from vsc.accounting.config.parser import MainConf

TARGET_HEADERS = ['start_time', 'end_time', 'jobid', 'username', 'hosts', 'cores']
DEFAULT_SEP = ','


class CSVJobData:
    """
    Methods to easily parse and filter job data in CSV files
    """

    def __init__(self, query_id):
        """
        Set configuration options for CSV file parsing
        - query_id: (int) arbitrary identification number of the query
        """
        self.log = fancylogger.getLogger(name=self.__class__.__name__)

        # Set query ID
        try:
            self.id = str(query_id)
        except ValueError as err:
            error_exit(self.log, err)

        # Default field to retrieve and format of timestamps
        self.fields = ['timestamp']
        self.timeformat = '%Y-%m-%dT%H:%M:%S.%fZ'

        try:
            # Index of CSV data files
            self.index = {
                'name': MainConf.get('csvdata', 'filename_format'),
                'dir': MainConf.get('csvdata', 'dir_path', fallback=os.getcwd(), mandatory=False),
                'walltime': MainConf.get('nodegroups', 'max_walltime'),
            }
        except KeyError as err:
            error_exit(self.log, err)

        # CSV table format
        self.timeformat = '%Y-%m-%dT%H:%M:%S'
        self.sep = MainConf.get('csvdata', 'sep', fallback=DEFAULT_SEP, mandatory=False)

        csv_headers = MainConf.get('csvdata', 'headers', fallback=TARGET_HEADERS, mandatory=False)
        if isinstance(csv_headers, str):
            csv_headers = csv_headers.split(',')

        if len(TARGET_HEADERS) == len(csv_headers):
            self.headers = dict(zip(csv_headers, TARGET_HEADERS))
        else:
            errmsg = (
                f"Given list of CSV headers ({len(csv_headers)}) "
                f"does not match target headers ({len(TARGET_HEADERS)})"
            )
            error_exit(self.log, errmsg)

    def set_index_files(self, period_start, period_end):
        """
        Set index of CSV files for query covering the given time period
        Pathaname pattern is determined from self.index
        - period_start, period_end: (pd.datetime) time limits of the query
        """
        indexlist = list()

        # End of time period is expanded with maximum walltime because we have to retrieve "Job End" events
        period_end += pd.Timedelta(self.index['walltime'])

        # Index of files with formatted dates
        if '%' in self.index['name']:
            index_dates = pd.date_range(period_start, period_end, freq='1D')
            csv_patterns = list(index_dates.strftime(self.index['name']).unique())
        else:
            csv_patterns = [f"{self.index['name']}*"]

        # Resolve index names to file paths
        path_patterns = [os.path.join(self.index['dir'], pattern) for pattern in csv_patterns]
        self.index['files'] = list()
        for pattern in path_patterns:
            self.index['files'].extend(glob(pattern))
        self.log.debug("CSV query [%s] using indexes: %s", self.id, ', '.join(self.index['files']))

    def expand_host_pattern(self, host_pattern):
        """
        Expand finite regex patterns defining hostnames of used nodes
        Assume one level of ranges maximum (eg: node[302,304-309,311-313,323])
        - host_pattern: (string) hostname with finite regex pattern
        """
        host_names = ''

        # Match pattern with basename plus numeric range
        match_groups = re.findall(r'([a-z0-9]*)\[([0-9,-]*?)\]', host_pattern)

        for group in match_groups:
            # Convert numeric series and ranges into a flat list
            numeric_series = group[1].split(',')
            for i, subgroup in enumerate(numeric_series):
                numeric_range = subgroup.split('-')
                if len(numeric_range) > 1:
                    numeric_range = [int(value) for value in numeric_range]
                    numeric_series[i] = [*range(*numeric_range)]
                else:
                    numeric_series[i] = [int(subgroup)]
            flat_series = [item for subseries in numeric_series for item in subseries]
            # Generate list of hostnames
            host_series = [group[0] + str(value) for value in flat_series]
            host_names += ', '.join(host_series)

        if match_groups:
            return host_names
        else:
            return host_pattern

    def nodes_filter(self, period_start, period_end, hostlist):
        """
        Generate regular expression to filter matching active group of nodes on given time period
        Nodes are accounted as active if at least active one whole day in time period
        - period_start, period_end: (pd.datetime) time limits of the query
        - hostlist: (list of dicts) each element should include
            {regex: pattern of hostnames, start: date string, end: date string}
        """
        # Active nodes from start to end of at least one whole day in this period
        nodes_regex = list()
        for node in hostlist:
            if node['end'] >= period_start + pd.Timedelta('1D') or node['start'] <= period_end - pd.Timedelta('1D'):
                nodes_regex += [node['regex']]

        # Pattern string of active nodes
        if len(nodes_regex) > 0:
            active_nodes_filter = '|'.join(nodes_regex)
            # replace capturing groups for matching groups (filter to be used with pd.str.contains)
            active_nodes_filter = active_nodes_filter.replace('(', '(?:')
            self.log.debug("ES query [%s] jobs in nodes: %s", self.id, active_nodes_filter)
        else:
            active_nodes_filter = ''
            self.log.debug("ES query [%s] no active nodes found", self.id)

        return active_nodes_filter

    def load_job_records(self, period_start, period_end, hostlist):
        """
        Load job data from CSV files into a pd.DataFrame
        Select columns matching the defined header labels/indexes
        Select records in given time period for given hosts
        - period_start, period_end: (pd.datetime) time limits of the query
        - hostlist: (list of dicts) each element should include
            {regex: pattern of hostnames, start: date string, end: date string}
        """
        empty_table = pd.DataFrame(columns=self.headers.values())

        # Index of CSV files with relevant data
        self.set_index_files(period_start, period_end)

        if len(self.index['files']) == 0:
            # Nothing to load, return empty DataFrame
            warnmsg = "CSV query [%s]: no data files found for time period '%s > %s'"
            self.log.warning(warnmsg, self.id, period_start, period_end)
            return empty_table

        # Filter of active nodes (regex) in this nodegroup
        active_nodes = self.nodes_filter(period_start, period_end, hostlist)

        if not active_nodes:
            # Nothing to load, return empty DataFrame
            warnmsg = "CSV query [%s]: no active nodes found for time period '%s > %s'"
            self.log.warning(warnmsg, self.id, period_start, period_end)
            return empty_table

        # Column labels or numeric indexes?
        labelled_columns = True
        if all([str(header).isdigit() for header in self.headers.keys()]):
            labelled_columns = False

        # Treat first row as headers with labelled columns
        file_header = 'infer' if labelled_columns else None

        # Read CSV files
        hits = list()
        for csv_file in self.index['files']:
            file_data = pd.read_csv(csv_file, sep=self.sep, header=file_header)
            hits.append(file_data)

        hits = pd.concat(hits, axis=0, ignore_index=True)

        # Slice target columns
        if labelled_columns:
            hits = hits.loc[:, self.headers.keys()]
        else:
            columns_idx = [int(idx) for idx in self.headers.keys()]
            hits = hits.iloc[:, columns_idx]

        # Rename columns to target names
        hits.columns = [self.headers[str(column_name)] for column_name in hits.columns]

        # Select all jobs that started before end of period
        hits['start_time'] = pd.to_datetime(hits['start_time'], format=self.timeformat, errors='coerce')
        job_max_start = period_end.strftime(self.timeformat)
        hits = hits.loc[hits['start_time'] < job_max_start]

        # Select all jobs that finished after start of period
        hits['end_time'] = pd.to_datetime(hits['end_time'], format=self.timeformat, errors='coerce')
        job_min_end = period_start.strftime(self.timeformat)
        hits = hits.loc[hits['end_time'] >= job_min_end]

        # Filter records with zero run time
        hits = hits.loc[hits['end_time'] - hits['start_time'] > pd.Timedelta(0)]
        # Filter duplicate records
        hits = hits.drop_duplicates()

        # Expand pattern of execution hosts and select all jobs in active nodes
        hits['hosts'] = hits.hosts.map(self.expand_host_pattern)
        active_hosts = hits.hosts.str.contains(active_nodes)
        hits = hits.loc[active_hosts]

        return hits
