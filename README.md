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
  * beautifulsoup4
  * elasticsearch
  * elasticsearch_dsl

Infrastructure
* Access to ELK server with the logstash of your job scheduler
* Access to VSC Account page (optional)

## Data Sources

### ElasticSearch

The retrieval of data requires an ElasticSearch instance containing the log records from the job scheduler. The accounting queries for `JOB_END` events and uses those records to calculate the use of compute resources with precision to the second.

Supported resource managers:
* **Torque**: log entries should at least contain the timestamp record `@timestamp`. Reports included in `accounting-report` require also `action.keyword`, `start_time`, `end_time`, `used_nodes`, `jobid`, `username`, `exec_host`, `total_execution_slots`.

### User account data

The main source of user account data is a local cache file in JSON format. Records in it will be added as users are found in the accounting data. Any account data can be manually added to the local cache file. By default it will be located in `~/.local/share/vsc-accounting/userdb-cache.json`.

User accounts identified as VSC accounts will be requested from teh VSC Account page and stored in the local cache file. Records in the local cache will be valid for 30 days (by default). The personal token to access the VSC Account API through `vsc-accountpage-clients` can be added to the local `vsc-acconting.ini` configuration file. Please keep the contents of your configuration file secret.

## Data Selection

Accounting is limited to a defined time period. At least the initial date (`-s`) is required to generate any of the accounting reports. By default, the accounting is computed with a resolution of days. It is also possible to manually request for lower resolutions (`-r`) and generate weekly, monthly, quaterly or yearly stats, as long as the defined time period can at least cover one time cycle in the requested resolution.

It is necessary to define the compute characteristics of the nodes in the cluster and how they are grouped. This information can be provided in a JSON file through the configuration parameter `nodegroups/specsheet`. Please check the provided example file `example-nodegroups.json` for details on its structure. If the definition of the cluster contains multiple node groups, it is possible to request the accounting stats on specific node groups (`-n`).

Attributes of each nodegroup in the spec file:
* Color: color name or hex value (used in some plots)
* Cores: number of cores of each node
* Hosts: list of hosts (or aggregates of hosts) to be included in the nodegroup
    * n: total number of hosts in this agregate
    * regex: pattern matching the hostnames of all hosts in this aggregate
    * start: first date in production (date in ISO format: `YYYY-MM-DD`)
    * end: last date in production (date in ISO format: `YYYY-MM-DD`)

## Accounting Reports

Reports of the accounting stats can be generated in 3 formats:
* rendered as plots or charts: choose SVG, PNG, JPG image format (`-f`)
* rendered as plots/charts and including the resulting data in a separate CSV file (`-t`)
* in a HTML document containing the plot/chart and related data tables (`-f html`)

The following reports are available:
* **compute-time**: used compute time per node group. Top value is maximum compute capacity of the period
* **compute-percent**: percentage of compute capacity used per node group
* **running-jobs**: number of running jobs per node group
* **unique-users**: number of unique users running jobs per node group
* **peruser-compute**: used compute time per node group by each active user (separate plot per user)
* **peruser-percent**: percentage of compute time used per node group by each active user (separate plot per user)
* **peruser-jobs**: number of running jobs per node group by each active user (separate plot per user)
* **perfield-compute**: used compute time per node group by each research field (separate plot per field)
* **perfield-percent**: percentage of compute time used per node group by each research field (separate plot per field)
* **perfield-jobs**: number of running jobs per node group by each research field (separate plot per field)
* **persite-compute**: used compute time per node group by each research site (separate plot per site)
* **persite-percent**: percentage of compute time used per node group by each research site (separate plot per site)
* **persite-jobs**: number of running jobs per node group by each research site (separate plot per site)
* **top-users**: total compute time used by the top percentiles of users across selected node groups
* **top-users-percent**: percentage of total compute time used by the top percentiles of users across selected node groups
* **top-fields**: compute time used by each research field across selected node groups (plot over time and pie chart)
* **top-fields-percent**: percentage of compute time used by each research field across selected node groups (plot over time and pie chart)
* **top-sites**: total compute time used by each research site across selected node groups
* **top-sites-percent**: percentage of total compute time used by each research site across selected node groups.

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

