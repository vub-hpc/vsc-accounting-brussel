# VSC Accounting - Brussel

## Description

Toolset to generate accurate accounting reports about the computational resources used in an HPC cluster.

Main features:
* retrieval of job log records from an ELK instance
* retrieval of user account data from a local cache file and/or the VSC Account page
* computation of usage statistics based on used compute time, executed jobs or unique active users
* creation of accounting reports in various formats (see below)

## Requirements

Software dependencies
* Python 3.6+
  * vsc-base
  * vsc-utils
  * vsc-config
  * vsc-accountpage-clients
  * appdirs
  * numpy
  * pandas
  * matplotlib
  * jinja2
  * lxml
  * beautifulsoup4
  * elasticsearch
  * elasticsearch-dsl

Infrastructure
* Access to ELK server with the logstash of your job scheduler
* Access to VSC Account page

## Accounting Reports

Reports of the accounting stats can be generated in 3 formats:
* rendered as plots or charts: choose SVG, PNG, JPG image format (`-f`)
* rendered as plots/charts and including the resulting data in a separate CSV file (`-t`)
* in a HTML document containing the plot/chart and related data tables (`-f html`)

### Available reports

* Reports with global statistics:
  * **compute-time**: used compute time per node group. Top value is maximum compute capacity of the period
  * **compute-percent**: percentage of compute capacity used per node group
  * **running-jobs**: number of running jobs per node group
  * **unique-users**: number of unique users running jobs per node group
* Batch of individual reports:
  * **peruser-compute**: used compute time per node group by each active user
  * **peruser-percent**: percentage of compute time used per node group by each active user
  * **peruser-jobs**: number of running jobs per node group by each active user
  * **perfield-compute**: used compute time per node group by each research field
  * **perfield-percent**: percentage of compute time used per node group by each research field
  * **perfield-jobs**: number of running jobs per node group by each research field
  * **persite-compute**: used compute time per node group by each research site
  * **persite-percent**: percentage of compute time used per node group by each research site
  * **persite-jobs**: number of running jobs per node group by each research site
* Top rankings (pie charts and activity over time):
  * **top-users**: compute time used by the top percentiles of users across all selected node groups
  * **top-users-percent**: distribution of used compute time among top percentiles of users across all selected node groups
  * **top-fields**: compute time used by each research field across all selected node groups
  * **top-fields-percent**: distribution of used compute time among top research fields across all selected node groups
  * **top-sites**: compute time used by each research site across all selected node groups
  * **top-sites-percent**: distribution of used compute time among research sites across all selected node groups.

## Example

The following example generates 4 accounting reports:
* **compute-percent**: total use of compute time (in percentage)
* **running-jobs**: total amount of running jobs
* **top-users-percent**: ranking of users by percentage of compute time
* **top-sites-percent**: ranking of research sites by percentage of compute time

```bash
$ accounting-report compute-percent running-jobs top-users-percent top-sites-percent
  -s 2020-01-01 -e 2020-06-30 -f html -o general-nodes -n node-group1 node-group2 -t
```

Reports will cover the first six months of 2020 (`-s 2020-01-01 -e 2020-06-30`) of two specific groups of nodes in the cluster (`-n node-group1 node-group2`). They will be saved in the folder `general-nodes` in the current working directory (`-o`), in HTML format (`-f html`) and also saving all data tables as CSV files (`-t`).

## Data Sources

### ElasticSearch

The retrieval of data requires an ElasticSearch instance containing the log records from the job scheduler. The accounting queries for `JOB_END` events and uses those records to calculate the use of compute resources with precision to the second.

Supported resource managers:
* **Torque**: log entries should at least contain the timestamp record `@timestamp`. Reports included in `accounting-report` require also `action.keyword`, `start_time`, `end_time`, `used_nodes`, `jobid`, `username`, `exec_host`, `total_execution_slots`.

### User account data

The main source of user account data is a local cache file in JSON format. Records in it will be added as users are found in the accounting data. Any account data can be manually added to the local cache file. By default it will be located in `~/.local/share/vsc-accounting/userdb-cache.json`.

User accounts identified as VSC accounts will be requested from the VSC Account page and stored in the local cache file. Records in the local cache will be valid for 30 days (by default). The personal token to access the VSC Account API through `vsc-accountpage-clients` can be added to its own configuration file `vsc-access.ini` (by default) or to any other configuration file in the system by setting `userdb/vsc_token_file` in the main configuration file `vsc-accounting.ini`. Please keep the VSC access token secret.

## Data Selection

Accounting is limited to a defined period of time. At least the initial date (`-s`) is required to generate any of the accounting reports. By default, the accounting is computed with a resolution of days. It is also possible to manually request for lower resolutions (`-r`) and generate weekly, monthly, quaterly or yearly stats, as long as the defined time period can at least cover one time cycle in the requested resolution.

It is necessary to describe the compute characteristics of the nodes in the cluster and how they should be grouped. This information can be provided in a JSON file through the configuration parameter `nodegroups/specsheet`. The default specsheet file `default-nodegroup.json` defines a single group of nodes that will match any hostname in the cluster. Please check the provided file `example-nodegroups.json` for a more complex example with multiple groups of nodes. If multiple node groups are defined, it is possible to request the accounting stats on specific node groups (`-n`).

Attributes of each nodegroup in the spec file:
* Color: color name or hex value (used in some plots)
* Cores: number of cores of each node
* Hosts: list of hosts (or aggregates of hosts) to be included in the nodegroup
    * n: total number of hosts in this agregate
    * regex: pattern matching the hostnames of all hosts in this aggregate
    * start: first date in production (date in ISO format: `YYYY-MM-DD`)
    * end: last date in production (date in ISO format: `YYYY-MM-DD`)

Example file `example-nodegroups.json`:
```json
{
    "node-group1": {
        "color": "blue",
        "cores": 40,
        "hosts": [
            {
                "n": 10,
                "regex": "node00[0-9]",
                "start": "2018-01-01"
            }
        ]
    },
    "node-group2": {
        "color": "red",
        "cores": 16,
        "hosts": [
            {
                "n": 20,
                "regex": "node0[12][0-9]",
                "start": "2018-01-01",
                "end": "2018-08-09"
            },
            {
                "n": 10,
                "regex": "node03[0-9]",
                "start": "2018-01-01",
                "end": "2018-02-02"
            }
        ]
    }
```

## Location of configuration and data files

Configuration files and data files used by `vsc-accounting` are installed by default in the user's configuration directory `~/.config/vsc-accounting` and local share directory `~/.local/share/vsc-accouting` respectively. This is the preferred behaviour because all those files are subject to be edited by the user:

* Main configuration file:
  * default location `~/.config/vsc-accounting/vsc-accounting.ini`
  * location can be changed with command line argument `-c`
* VSC token: contains the token string in option `MAIN/access_token`
  * default location `~/.config/vsc-accounting/vsc-access.ini`
  * filename and path can be changed with the option `userdb/vsc_token_file` in the main configuration file `vsc-accounting.ini`
* Nodegroup Specsheet: defines logical groups of nodes in the cluster and their characteristics (see [Data Selection](#data-selection) for more information)
  * default location: `~/.local/share/vsc-accounting/`
  * filename and path can be changed in `nodegroups/specsheet` in the main configuration file `vsc-accounting.ini`
* Cache of user accounts: contains account records generated by `vsc-accounting` or retrieved from the VSC accoutn page. It can be edited to manually add user accounts.
  * default location: `~/.local/share/vsc-accounting/userdb-cache.json`
  * filename and path can be changed in `userdb/cache_file` in the main configuration file `vsc-accounting.ini`

Configuration files and data files can also be located in `/etc`. Any files defined in `vsc-accounting.ini` with a simple filename will be searched by order of preference in:

1. `~/.config/vsc-accounting` or `~/.local/share/vsc-accounting`
2. `/etc/vsc-accounting`
3. `/etc`
4. Package resources

Alternatively, the location of those files can be set to any arbitrary absolute path. In such a case, the files in those paths will be used from their current location.
