#!/usr/bin/env python3
#
# Copyright 2020-2020 Vrije Universiteit Brussel
#
# This file is part of vsc-accounting-brussel,
# originally created by the HPC team of Vrije Universiteit Brussel (https://hpc.vub.be),
# with support of Vrije Universiteit Brussel (https://www.vub.be),
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
Generate accurate accounting reports about the computational resources used in an HPC cluster

Reports with global statistics:
 - compute-time: used compute time per node group. Top value is maximum compute capacity of the period
 - compute-percent: percentage of compute capacity used per node group
 - running-jobs: number of running jobs per node group
 - unique-users: number of unique users running jobs per node group
Batch of individual reports:
 - peruser-compute: used compute time per node group by each active user
 - peruser-percent: percentage of compute time used per node group by each active user
 - peruser-jobs: number of running jobs per node group by each active user
 - perfield-compute: used compute time per node group by each research field
 - perfield-percent: percentage of compute time used per node group by each research field
 - perfield-jobs: number of running jobs per node group by each research field
 - persite-compute: used compute time per node group by each research site
 - persite-percent: percentage of compute time used per node group by each research site
 - persite-jobs: number of running jobs per node group by each research site
Top rankings (pie charts and activity over time):
 - top-users: compute time used by the top percentiles of users across all selected node groups
 - top-users-percent: distribution of used compute time among top percentiles of users across all selected node groups
 - top-fields: compute time used by each research field across all selected node groups
 - top-fields-percent: distribution of used compute time among top research fields across all selected node groups
 - top-sites: compute time used by each research site across all selected node groups
 - top-sites-percent: distribution of used compute time among research sites across all selected node groups.

@author: Alex Domingo (Vrije Universiteit Brussel)
"""

import os
import argparse

from datetime import date, datetime

from vsc.utils import fancylogger
from vsc.accounting.version import VERSION
from vsc.accounting.exit import error_exit
from vsc.accounting.config.parser import MainConf
from vsc.accounting.data.parser import DataFile
from vsc.accounting.counters import ComputeTimeCount

import vsc.accounting.data.parser as dataparser
import vsc.accounting.reports as report

logger = fancylogger.getLogger()
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()


def valid_dirpath(dirpath):
    """
    Validate directory path passed through argparse
    """
    if os.path.isdir(dirpath):
        return dirpath
    else:
        errmsg = f"Directory '{dirpath}' does not exist"
        raise argparse.ArgumentTypeError(errmsg)


def valid_isodate(strdate):
    """
    Validate date string passed through argparse
    """
    try:
        # Can be replaced with date.fromisoformat in Python 3.7+
        iso_date = datetime.strptime(strdate, '%Y-%m-%d').date()
    except ValueError:
        errmsg = f"Date '{strdate}' is not a valid date in ISO format [YYYY-MM-DD]"
        raise argparse.ArgumentTypeError(errmsg)
    else:
        return iso_date


def main():
    # Core command line arguments
    cli_core = argparse.ArgumentParser(prog='accounting-report', add_help=False)
    cli_core.add_argument(
        '-v', '--version', action='version', version='%(prog)s from vsc-accounting-brussel v{}'.format(VERSION)
    )
    cli_core.add_argument(
        '-d', dest='debug', help='use debug log level', required=False, action='store_true'
    )
    cli_core.add_argument(
        '-i',
        dest='force_install',
        help='force (re)installation of any data files needed from package resources',
        required=False,
        action='store_true',
    )
    cli_core.add_argument(
        '-c',
        dest='config_file',
        help='path to configuration file (default: ~/.config/vsc-accounting/vsc-accouning.ini)',
        default='vsc-accounting.ini',
        required=False,
    )

    cli_core_args, cli_extra_args = cli_core.parse_known_args()

    # Debug level logs
    if cli_core_args.debug:
        fancylogger.setLogLevelDebug()
        logger.debug("Switched logging to debug verbosity")

    # Load configuration
    MainConf.load(cli_core_args.config_file)

    # Enforce (re)installation of data files
    if cli_core_args.force_install:
        dataparser.FORCE_INSTALL = True

    # Read nodegroup specs and default values
    try:
        nodegroups_spec = MainConf.get('nodegroups', 'specsheet')
        nodegroups_default = MainConf.get('nodegroups', 'default').split(',')
    except KeyError as err:
        error_exit(logger, err)
    else:
        nodegroups = DataFile(nodegroups_spec).contents

    # Reporting command line arguments
    cli = argparse.ArgumentParser(
        description='Generate accurate accounting reports about the computational resources used in an HPC cluster',
        parents=[cli_core],
    )
    cli.add_argument(
        '-s',
        dest='start_date',
        help='data retrieved from START_DATE [YYYY-MM-DD] at 00:00',
        required=True,
        type=valid_isodate,
    )
    cli.add_argument(
        '-e',
        dest='end_date',
        help='data retrieved until END_DATE [YYYY-MM-DD] at 00:00 (default: today)',
        default=date.today(),
        required=False,
        type=valid_isodate,
    )
    cli.add_argument(
        '-r',
        dest='resolution',
        help='time resolution of the accounting (default: day)',
        choices=['year', 'quarter', 'month', 'week', 'day'],
        default='day',
        required=False,
    )
    cli.add_argument(
        '-f',
        dest='report_format',
        help='format of the report document (default: SVG)',
        choices=['html', 'pdf', 'png', 'svg'],
        default='svg',
        required=False,
    )
    cli.add_argument(
        '-t', dest='csv', help='write report data table in a CSV file', required=False, action='store_true',
    )
    cli.add_argument(
        '-o',
        dest='output_dir',
        help='path to store output files (default: print working directory)',
        default=None,
        required=False,
        type=valid_dirpath,
    )
    cli.add_argument(
        '-u',
        dest="compute_units",
        help='compute time units (default: corehours)',
        choices=['corehours', 'coredays'],
        default='corehours',
        required=False,
    )
    cli.add_argument(
        '-n',
        dest='node_groups',
        help='node groups to include in the accounting report',
        choices=[*nodegroups],
        nargs='*',
        default=nodegroups_default,
        required=False,
    )
    cli.add_argument(
        'reports',
        help='accounting reports to generate',
        choices=[
            'compute-time',
            'compute-percent',
            'running-jobs',
            'unique-users',
            'peruser-compute',
            'peruser-percent',
            'peruser-jobs',
            'perfield-compute',
            'perfield-percent',
            'perfield-jobs',
            'persite-compute',
            'persite-percent',
            'persite-jobs',
            'top-users',
            'top-users-percent',
            'top-fields',
            'top-fields-percent',
            'top-sites',
            'top-sites-percent',
        ],
        nargs='+',
    )

    # Read command line arguments
    cli_args = cli.parse_args()

    # Set absolute path of output directory
    if cli_args.output_dir:
        basedir = os.path.abspath(os.path.expanduser(cli_args.output_dir))
    else:
        basedir = os.getcwd()
    logger.debug("Output directory set to: %s", basedir)

    # Convert time resolution to pandas DateOffset format
    pd_date_offsets = {'day': 'D', 'week': 'W-MON', 'month': 'MS', 'quarter': 'QS', 'year': 'AS'}
    date_offset = pd_date_offsets[cli_args.resolution]

    # Selection of node groups
    nodegroup_list = list(set(cli_args.node_groups))  # go through a set to remove duplicates

    # Account compute time on each node group in the requested period
    ComputeTime = ComputeTimeCount(
        cli_args.start_date, cli_args.end_date, date_offset, compute_units=cli_args.compute_units
    )

    for ng in nodegroup_list:
        logger.info("Processing jobs on %s nodes...", ng)
        ComputeTime.add_nodegroup(ng, nodegroups[ng]['cores'], nodegroups[ng]['hosts'])

    # Colors of each nodegroup
    plot_colors = {ng: nodegroups[ng]['color'] for ng in nodegroup_list}

    # Generate requested accounting reports
    report_save = [basedir, cli_args.report_format, cli_args.csv]
    report_generators = {
        'compute-time': (report.compute_time, [ComputeTime, plot_colors] + report_save),
        'compute-percent': (report.compute_percent, [ComputeTime, plot_colors] + report_save),
        'running-jobs': (report.global_measure, [ComputeTime, 'Running Jobs', plot_colors] + report_save),
        'unique-users': (report.global_measure, [ComputeTime, 'Unique Users', plot_colors] + report_save),
        'peruser-compute': (report.aggregates, [ComputeTime, 'User', 'Compute', False, plot_colors] + report_save),
        'peruser-percent': (report.aggregates, [ComputeTime, 'User', 'Compute', True, plot_colors] + report_save),
        'peruser-jobs': (report.aggregates, [ComputeTime, 'User', 'Jobs', False, plot_colors] + report_save),
        'perfield-compute': (report.aggregates, [ComputeTime, 'Field', 'Compute', False, plot_colors] + report_save),
        'perfield-percent': (report.aggregates, [ComputeTime, 'Field', 'Compute', True, plot_colors] + report_save),
        'perfield-jobs': (report.aggregates, [ComputeTime, 'Field', 'Jobs', False, plot_colors] + report_save),
        'persite-compute': (report.aggregates, [ComputeTime, 'Site', 'Compute', False, plot_colors] + report_save),
        'persite-percent': (report.aggregates, [ComputeTime, 'Site', 'Compute', True, plot_colors] + report_save),
        'persite-jobs': (report.aggregates, [ComputeTime, 'Site', 'Jobs', False, plot_colors] + report_save),
        'top-users': (report.top_users, [ComputeTime, False] + report_save),
        'top-users-percent': (report.top_users, [ComputeTime, True] + report_save),
        'top-fields': (report.top_fields, [ComputeTime, False] + report_save),
        'top-fields-percent': (report.top_fields, [ComputeTime, True] + report_save),
        'top-sites': (report.top_sites, [ComputeTime, False] + report_save),
        'top-sites-percent': (report.top_sites, [ComputeTime, True] + report_save),
    }

    for requested_report in cli_args.reports:
        report_generators[requested_report][0](*report_generators[requested_report][1])


if __name__ == "__main__":
    main()
