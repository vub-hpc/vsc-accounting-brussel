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
Data retrieval from ElasticSearch for vsc.accounting

@author Alex Domingo (Vrije Universiteit Brussel)
"""

import pandas as pd

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout, NotFoundError, TransportError
from elasticsearch_dsl import Search

from vsc.utils import fancylogger
from vsc.accounting.exit import error_exit
from vsc.accounting.config.parser import MainConf


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
                'freq': MainConf.get('elasticsearch', 'index_freq'),
                'walltime': MainConf.get('elasticsearch', 'max_walltime'),
            }
        except KeyError as err:
            error_exit(logger, err)

        # Default field to retrieve and format of timestamps
        self.fields = ['@timestamp']
        self.timeformat = '%Y-%m-%dT%H:%M:%S.%fZ'

        try:
            self.client = Elasticsearch(hosts=self.servers)
            self.search = Search(using=self.client)
            es_cluster = self.client.cluster.health()
        except (ConnectionError, TransportError) as err:
            error_exit(self.log, f"ES query [{self.id}] connection to ElasticSearch server failed: {err}")
        except ConnectionTimeout as err:
            error_exit(self.log, f"ES query [{self.id}] connection to ElasticSearch server timed out")
        else:
            dbgmsg = "ES query [%s] connection established with ES cluster: %s"
            self.log.debug(dbgmsg, self.id, es_cluster['cluster_name'])
            self.log.debug("ES query [%s] status of ES cluster is %s", self.id, es_cluster['status'])

    def set_index(self, period_start, period_end):
        """
        Set index string for query covering the given time period
        Basename and frequency of indexes is determined by self.index
        All required indexes are added to the index string
        - period_start, period_end: (pd.datetime) time limits of the query
        """
        indexlist = list()

        # End of time period is expanded with maximum walltime because we have to retrieve "Job End" events
        period_end += pd.Timedelta(self.index['walltime'])

        # Add all indexes (one per month) covering the given time period
        index_dates = pd.date_range(period_start, period_end, freq='1D')
        index_ym = index_dates.strftime('%Y.%m').unique()
        indexes = ['{}-{}'.format(self.index['name'], ym) for ym in index_ym]

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
            hits = [hit for hit in self.search.scan()]
        except NotFoundError as err:
            error_exit(self.log, f"ES query [{self.id}] search result not found: {err}")
        else:
            return hits
