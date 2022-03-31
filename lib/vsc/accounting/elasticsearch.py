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
Data retrieval from ElasticSearch for vsc.accounting

@author Alex Domingo (Vrije Universiteit Brussel)
"""

import pandas as pd
import re
import warnings

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import AuthorizationException, ConnectionError, ConnectionTimeout, NotFoundError, TransportError
from elasticsearch_dsl import Search

from vsc.utils import fancylogger
from vsc.accounting.exit import error_exit
from vsc.accounting.config.parser import MainConf, ConfigFile


class ElasticTorque:
    """
    Methods to easily configure queries to torque's logstash in an ElasticSearch instance
    """

    def __init__(self, query_id):
        """
        Set configuration options for the queries to ElasticSearch
        Establish connection to the server
        - query_id: (int) arbitrary identification number of the query
        """
        self.log = fancylogger.getLogger(name=self.__class__.__name__)

        # Set query ID
        try:
            self.id = str(query_id)
        except ValueError as err:
            error_exit(self.log, err)

        try:
            # URL of the ElasticSearch instance
            self.servers = MainConf.get('elasticsearch', 'server_url').split(',')
            # Index parameters
            self.index = {
                'name': MainConf.get('elasticsearch', 'index_name'),
                'walltime': MainConf.get('nodegroups', 'max_walltime'),
            }
        except KeyError as err:
            error_exit(self.log, err)

        # Connection settings
        es_connection = {'hosts': self.servers}

        # Get token from configuration file to access API
        TokenConfig = ConfigFile()
        try:
            es_token_file = MainConf.get('userdb', 'es_token_file', fallback='api-access.ini', mandatory=False)
            self.api_token = TokenConfig.load(es_token_file).get('MAIN', 'es_token')
        except KeyError as err:
            error_exit(self.log, err)

        if self.api_token:
            es_connection['api_key'] = self.api_token

        # Default field to retrieve and format of timestamps
        self.fields = ['@timestamp']
        self.timeformat = '%Y-%m-%dT%H:%M:%S.%fZ'

        try:
            self.client = Elasticsearch(**es_connection)
            self.search = Search(using=self.client)
            es_cluster = self.client.info()
        except AuthorizationException as err:
            self.log.debug("ES query [%s] connection with limited privileges established with ES cluster", self.id)
        except (ConnectionError, TransportError) as err:
            error_exit(self.log, f"ES query [{self.id}] connection to ElasticSearch server failed: {err}")
        except ConnectionTimeout as err:
            error_exit(self.log, f"ES query [{self.id}] connection to ElasticSearch server timed out")
        else:
            self.log.debug("ES query [%s] connection established with ES cluster: %s", self.id, es_cluster)


    def set_index(self, period_start, period_end):
        """
        Set index string for query covering the given time period
        Basename pattern is determined by self.index
        All required indexes are added to the index string
        - period_start, period_end: (pd.datetime) time limits of the query
        """
        indexlist = list()

        # End of time period is expanded with maximum walltime because we have to retrieve "Job End" events
        period_end += pd.Timedelta(self.index['walltime'])

        if '%' in self.index['name']:
            # Add all indexes covering time period in given format
            index_dates = pd.date_range(period_start, period_end, freq='1D')
            indexes = list(index_dates.strftime(self.index['name']).unique())
        else:
            # Use provided index name with wildcards
            indexes = [f"{self.index['name']}*"]

        # Set index string for ES query
        self.search = self.search.index(indexes)
        self.log.debug("ES query [%s] using indexes: %s", self.id, ','.join(indexes))

    def query_usednodes(self, period_start, period_end, hostlist):
        """
        Add regular expression to query matching active group of nodes on given time period
        Nodes are accounted as active if at least active one whole day in time period
        - period_start, period_end: (pd.datetime) time limits of the query
        - hostlist: (list of dicts) each element should include
            {regex: pattern of hostnames, start: date string, end: date string}
        """
        nodes_regex = list()

        for node in hostlist:
            # Add nodes that are active from start to end of at least one whole day in time period
            if node['end'] >= period_start + pd.Timedelta('1D') or node['start'] <= period_end - pd.Timedelta('1D'):
                nodes_regex += [node['regex']]

        # Add nodes to used nodes string for ES query
        if len(nodes_regex) > 0:
            regex_str = '(' + '|'.join(nodes_regex) + ')'
            self.search = self.search.query('regexp', **{'used_nodes': regex_str})
            self.log.debug("ES query [%s] jobs in nodes: %s", self.id, regex_str)
        else:
            self.search = self.search.query('match_none', {})
            self.log.debug("ES query [%s] no active nodes found", self.id)

        # Update fields to retrieve
        self.fields += ['used_nodes']

    def filter_term(self, field, value):
        """
        Set generic query filter by term for given field and value
        - field, value: (string)
        """
        self.search = self.search.filter('term', **{field: value})
        self.log.debug("ES query [%s] added filter by term: '%s: %s'", self.id, field, value)

        # Update fields to retrieve
        self.fields += [field]

    def filter_timerange(self, period_start, period_end):
        """
        Set query filter start_time and end_time
        - period_start, period_end: (pd.datetime) time limits of the query
        """
        # query for all jobs that started before end of period
        job_max_start = period_end.strftime(self.timeformat)
        # query for all jobs that finished after start of period
        job_min_end = period_start.strftime(self.timeformat)

        self.search = self.search.filter('range', **{'start_time': {'lt': job_max_start}})
        self.search = self.search.filter('range', **{'end_time': {'gte': job_min_end}})

        self.log.debug("ES query [%s] time interval: %s > %s", self.id, job_min_end, job_max_start)

        # Update fields to retrieve
        self.fields += ['start_time', 'end_time']

    def set_source(self, extra=None):
        """
        Define the fields retrieved by the query
        Main fields are automatically added to self.fields along the setup of the query.
        - extra: (list of strings) optional list of fields to add
        """
        if extra:
            self.fields += extra

        self.search = self.search.source(self.fields)
        self.log.debug("ES query [%s] retrieving fields: %s", self.id, ','.join(self.fields))

    def scan_hits(self):
        """
        Scan all hits in Search object and handle any errors
        """
        try:
            hits = [hit.to_dict() for hit in self.search.scan()]
        except NotFoundError as err:
            error_exit(self.log, f"ES query [{self.id}] search result not found: {err}")
        else:
            return hits

    def corecount(self, exec_hosts, active_hosts, totalcores=None):
        """
        Parse 'exec_host' formatted data and return number of cores of active hosts in this group of nodes
        If all hosts match, returns totalcores (if provided)
        - exec_hosts: (list) 'exec_host' string with hostnames allocated to job
        - active_hosts: (list of dicts) each element should include
            {regex: pattern of hostnames, start: date string, end: date string}
        - totalcores: (integer) number of cores used by job
        """
        corespec = list()
        corecount = 0

        # Take core specification for job hosts matching current node group
        for node in active_hosts:
            corespec += [host.split('/')[1] for host in exec_hosts if re.match(node['regex'], host)]

        if totalcores and len(corespec) == len(exec_hosts):
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


    def load_job_records(self, period_start, period_end, hostlist):
        """
        Load job data from ElasticSearch into a pd.DataFrame
        Select columns matching the defined header labels/indexes
        Select records in given time period for given hosts
        - period_start, period_end: (pd.datetime) time limits of the query
        - hostlist: (list of dicts) each element should include
            {regex: pattern of hostnames, start: date string, end: date string}
        """
        # Query indexes with data in this time period
        self.set_index(period_start, period_end)
        # Match active nodes in this time period
        self.query_usednodes(period_start, period_end, hostlist)
        # Filter "job end" events
        self.filter_term('action.keyword', 'E')
        # Set time range of query
        self.filter_timerange(period_start, period_end)
        # Set data fields to retrieve
        self.set_source(extra=['jobid', 'username', 'exec_host', 'total_execution_slots'])
        self.log.debug("ES query [%s]: %s", self.id, self.search.to_dict())

        hits = pd.DataFrame(self.scan_hits())
        self.log.debug("ES query [%s]: retrieved %s hits from ElasticSearch", self.id, len(hits))

        if hits.empty:
            # Add column headers on empty search results
            hits = pd.DataFrame(columns=self.fields)

        # Convert date strings to datetime objects
        hits['start_time'] = pd.to_datetime(hits['start_time'], format=self.timeformat, errors='coerce')
        hits['end_time'] = pd.to_datetime(hits['end_time'], format=self.timeformat, errors='coerce')

        # Account number of cores used by each job
        hits['cores'] = hits.apply(
            lambda row: self.corecount(row['exec_host'], hostlist, row['total_execution_slots']),
            axis=1,  # apply row wise
            result_type='reduce',  # return series if possible
        )

        return hits

