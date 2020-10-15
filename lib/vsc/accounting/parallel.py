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
Parallel engine to process tasks in vsc.accounting

@author: Alex Domingo (Vrije Universiteit Brussel)
"""

from concurrent import futures

from vsc.utils import fancylogger
from vsc.accounting.exit import error_exit


def parallel_exec(task, label, stack, *args, procs=None, logger=None, **kwargs):
    """
    Execute task in each item of stack in parallel
    Returns list with the resulting data
    - task: (method) function to pass to the parallel executor
    - label: (string) name of the task, used in log messages
    - stack: (iterable) list of items to be processed by task
    - procs: (int) number of processors
    - logger: (object) fancylogger object of the caller
    """
    if logger is None:
        logger = fancylogger.getLogger()

    data_collection = list()

    # Start process pool to execute all items in the stack
    with futures.ProcessPoolExecutor(max_workers=procs) as executor:
        task_pool = {
            executor.submit(task, item, *args, logger=logger, **kwargs): item for item in stack
        }
        for pid, completed_task in enumerate(futures.as_completed(task_pool)):
            try:
                data_batch = completed_task.result()
            except futures.process.BrokenProcessPool as err:
                # In Python 3.8+ there is also the exception futures.BrokenExecutor to consider
                error_exit(logger, f"{label}: process pool executor failed")
            except futures.CancelledError as err:
                # Child processes will be cancelled if any ends in error. Ignore error.
                logger.debug(f"{label}: process {pid} cancelled successfully")
                pass
            except SystemExit as exit:
                if exit.code == 1:
                    # Child process ended in error. Cancel all remaining processes in the pool.
                    cancel_process_pool(task_pool, pid, logger)
                    # Abort execution
                    errmsg = f"{label}: process {pid} failed. Aborting!"
                    error_exit(logger, errmsg)
            else:
                # Add counters to list
                data_collection.append(data_batch)

    return data_collection


def cancel_process_pool(pool, error_pid, logger=None):
    """
    Cancel all pending processes in process pool
    Return count of cancelled processes and running processes
    - pool: (Future) pool of processes
    - error_pid: (int) process ID of failed process
    - logger: (object) fancylogger object of the caller
    """
    if logger is None:
        logger = fancylogger.getLogger()

    # Count state of processes in the pool
    processes = {'cancelled': 0, 'running': 0}

    # Cancel all pending processes in the pool
    for child in pool:
        if child.cancel():
            processes['cancelled'] += 1
        elif child.running():
            processes['running'] += 1

    warnmsg = f"Process [{error_pid}] ended in error. Cancelled all {processes['cancelled']} pending processes"
    logger.warning(warnmsg)

    logger.debug(f"Waiting for {processes['running']} non-cancellable running processes")

    return True
