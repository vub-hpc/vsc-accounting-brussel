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
Library of accounting reports for vsc.accounting

@author Alex Domingo (Vrije Universiteit Brussel)
"""

import re

import numpy as np
import pandas as pd
import matplotlib.cm as cm

from vsc.utils import fancylogger
from vsc.accounting.exit import error_exit
from vsc.accounting.plotter import PlotterArea, PlotterLine, PlotterStack, PlotterPie

logger = fancylogger.getLogger()


def compute_time(ComputeTime, colorlist, savedir, plotformat, csv=False):
    """
    Overall compute time in period of time
    Plot upper limit is maximum total capacity in the period
    - ComputeTime: (ComputeTimeFrame) source data for the plot
    - colorlist: (dict) colors for each plot stack
    - savedir: (string) path of directory to store output
    - plotformat: (string) image format of the plot
    - csv: (boolean) save data used for the plot in CSV format
    """
    logger.info("Generating accounting report on absolute compute time...")
    plot = dict()

    # Add total capacity per time interval
    ComputeTime.aggregate_perdate('GlobalStats', 'capacity')

    # Full data table for the plot
    table_columns = ['compute_time', 'capacity', 'total_capacity']
    table = ComputeTime.GlobalStats.loc[:, table_columns]
    units = ComputeTime.compute_units['normname']

    # Format columns in the table and set index date frequency
    table = table.rename(columns=simple_names_units(table_columns, units))
    table.index.levels[0].freq = ComputeTime.dates.freq
    logger.debug("Data included in the report: %s", ", ".join(table.columns))

    # Data selection for the plot
    plot['table'] = table.loc[:, [f"Compute Time ({units})"]]
    logger.debug("Data used in the plot: %s", ", ".join(plot['table'].columns))

    total_capacity_colname = f"Total Capacity ({units})"
    plot['ymax'] = table.loc[:, total_capacity_colname].max()
    logger.debug("Maximum value of the plot: %s %s", '{:.2f}'.format(plot['ymax']), plot['table'].columns[0])

    # Set colors for each nodegroup in the stack plot
    plot['colors'] = [colorlist[ng] for ng in ComputeTime.GlobalStats.index.unique(level='nodegroup')]

    # Plot title
    plot['title'] = "Compute Time"

    # Render plot
    stackplot = PlotterStack(**plot)

    # Mark 80% of total capacity
    stackplot.draw_hline(0.8)

    # Output: file paths
    stackplot.set_output_paths(savedir)
    # Output: render HTML document including plot data table
    if plotformat == 'html':
        table_title = "{} compute stats per nodegroup".format(stackplot.xfreq.capitalize())
        stackplot.html_makepage()
        stackplot.html_addtable(table, table_title)
    # Output: save files
    stackplot.save_plot(plotformat)
    if csv:
        stackplot.output_csv(table)


def compute_percent(ComputeTime, colorlist, savedir, plotformat, csv=False):
    """
    Overall compute time as percentage of total capacity
    Plot upper limit is 100% total capacity in the period
    - ComputeTime: (ComputeTimeFrame) source data for the plot
    - colorlist: (dict) colors for each plot stack
    - savedir: (string) path of directory to store output
    - plotformat: (string) image format of the plot
    - csv: (boolean) save data used for the plot in CSV format
    """
    logger.info("Generating accounting report on compute time as percentage of total capacity...")
    plot = dict()

    # Calculate percent compute time per time period
    ComputeTime.aggregate_perdate('GlobalStats', 'capacity')
    ComputeTime.add_percentage('GlobalStats', 'compute_time', 'capacity')

    ComputeTime.aggregate_perdate('GlobalStats', 'compute_time')
    ComputeTime.add_percentage('GlobalStats', 'compute_time', 'total_capacity')
    ComputeTime.add_percentage('GlobalStats', 'total_compute_time', 'total_capacity', 'global_percent_capacity')

    # Full data table for the plot
    table_columns = ['compute_time', 'capacity', 'percent_capacity']
    table_columns.extend(['total_capacity', 'percent_total_capacity', 'global_percent_capacity'])
    table = ComputeTime.GlobalStats.loc[:, table_columns]

    # Format columns in the table and set index date frequency
    th = {
        'compute_time': f"Compute Time ({ComputeTime.compute_units['normname']})",
        'capacity': f"Capacity ({ComputeTime.compute_units['normname']})",
        'percent_capacity': "Capacity Used (%)",
        'total_capacity': f"Total Capacity ({ComputeTime.compute_units['normname']})",
        'percent_total_capacity': "Total Capacity Used (%)",
        'global_percent_capacity': "Total Capacity Used Globally (%)",
    }
    table = table.rename(columns=th)
    table.index.levels[0].freq = ComputeTime.dates.freq
    logger.debug("Data included in the report: %s", ", ".join(table.columns))

    # Data selection for the plot
    plot['table'] = table.loc[:, ["Total Capacity Used (%)"]]
    logger.debug("Data used in the plot: %s", ", ".join(plot['table'].columns))

    plot['ymax'] = 1
    logger.debug("Maximum value of the plot: %s %s", '{:.2%}'.format(plot['ymax']), plot['table'].columns[0])

    # Set colors for each nodegroup in the stack plot
    plot['colors'] = [colorlist[ng] for ng in ComputeTime.GlobalStats.index.unique(level='nodegroup')]

    # Plot title
    plot['title'] = "Relative Compute Time"

    # Render plot
    stackplot = PlotterStack(**plot)

    # Mark 80% of total capacity
    stackplot.draw_hline(0.8)

    # Output: file paths
    stackplot.set_output_paths(savedir)
    # Output: render HTML document including plot data table
    if plotformat == 'html':
        table_title = "{} compute stats per nodegroup".format(stackplot.xfreq.capitalize())
        stackplot.html_makepage()
        stackplot.html_addtable(table, table_title)
    # Output: save files
    stackplot.save_plot(plotformat)
    if csv:
        stackplot.output_csv(table)


def global_measure(ComputeTime, selection, colorlist, savedir, plotformat, csv=False):
    """
    Number of total measures in GlobalStats in the given period
    Plot upper limit is maximum total 'measures' in the period
    - ComputeTime: (ComputeTimeFrame) source data for the plot
    - selection: (string) matching name of column to be plotted
    - colorlist: (dict) colors for each plot stack
    - savedir: (string) path of directory to store output
    - plotformat: (string) image format of the plot
    - csv: (boolean) save data used for the plot in CSV format
    """
    logger.info("Generating accounting report on %s...", selection.replace('_', ' '))
    plot = dict()

    # Names of selection has to be capitalized
    selection = selection.title()

    # Sum jobs and users per time period
    ComputeTime.aggregate_perdate('GlobalStats', 'running_jobs')
    ComputeTime.aggregate_perdate('GlobalStats', 'unique_users')

    # Full data table for the plot
    table_columns = ['compute_time', 'running_jobs', 'total_running_jobs', 'unique_users', 'total_unique_users']
    table = ComputeTime.GlobalStats.loc[:, table_columns]

    # Format columns in the table and set index date frequency
    units = [ComputeTime.compute_units['normname'], 'jobs/day', 'jobs/day', 'users/day', 'users/day']
    table = table.rename(columns=simple_names_units(table_columns, units))
    table.index.levels[0].freq = ComputeTime.dates.freq
    logger.debug("Data included in the report: %s", ", ".join(table.columns))

    # Data selection for the plot
    plot_data = [column for column in table.columns if re.match(selection, column)]
    try:
        plot['table'] = table.loc[:, plot_data]
    except KeyError:
        error_exit(f"Data column for '{selection}' not found in GlobalStats")
    else:
        logger.debug("Data used in the plot: %s", ", ".join(plot['table'].columns))

        plot['ymax'] = max(table.loc[:, plot_data[0]].groupby('date').sum())
        logger.debug("Maximum value of the plot: %s %s", '{:.2f}'.format(plot['ymax']), plot['table'].columns[0])

    # Set colors for each nodegroup in the stack plot
    plot['colors'] = [colorlist[ng] for ng in ComputeTime.GlobalStats.index.unique(level='nodegroup')]

    # Plot title: first column name without units
    plot['title'] = re.sub('\((.*?)\)', '', plot['table'].columns[0]).rstrip()

    # Render plot
    stackplot = PlotterStack(**plot)

    # Output: file paths
    stackplot.set_output_paths(savedir)
    # Output: render HTML document including plot data table
    if plotformat == 'html':
        table_title = "{} stats per nodegroup".format(stackplot.xfreq.capitalize())
        stackplot.html_makepage()
        stackplot.html_addtable(table, table_title)
    # Output: save files
    stackplot.save_plot(plotformat)
    if csv:
        stackplot.output_csv(table)


def aggregates(ComputeTime, aggregate, selection, percent, colorlist, savedir, plotformat, csv=False):
    """
    Compute time used by each entity in the chosen aggregate during the time period
    Gives insight on resources used by each entity
    Plot upper limit is maximum compute time of entity over all nodegroups
    - ComputeTime: (ComputeTimeFrame) source data for the plot
    - aggregate: (string) name of the aggregate data
    - selection: (string) name of the accounted data
    - percent: (boolean) plot percentual compute time
    - colorlist: (dict) colors for each plot stack
    - savedir: (string) path of directory to store output
    - plotformat: (string) image format of the plot
    - csv: (boolean) save data used for the plot in CSV format
    """
    # Names of aggregate and selection have to be capitalized
    aggregate = aggregate.title()
    selection = selection.title()

    # Source data for selected accounting and aggregate
    try:
        sources = source_data(selection, aggregate, ComputeTime.compute_units['normname'])
    except AttributeError as err:
        error_exit(logger, err)

    # List of entities in this aggregation
    aggregate_list = sorted(ComputeTime.getattr(aggregate + 'List'))
    # Add total compute time per time interval
    ComputeTime.aggregate_perdate('GlobalStats', sources['reference'], sources['aggregate'])
    # Calculate percentage compute time per entity
    for entity in aggregate_list:
        ComputeTime.add_percentage(sources['aggregate'], entity, sources['total'], f"{entity} - percent")
    # Grab stats for this aggregate
    AggregateStats = ComputeTime.getattr(sources['aggregate'])

    # Render plots for each entity
    plot = dict()

    # Set colors for each nodegroup in the stack plot
    plot['colors'] = [colorlist[ng] for ng in ComputeTime.GlobalStats.index.unique(level='nodegroup')]

    # Iterate over each entity
    for entity in aggregate_list:
        logger.info("Generating accounting report on %s by %s: %s...", selection, aggregate, entity)

        # Full data table for the plot
        entity_perc = f"{entity} - percent"
        table = AggregateStats.loc[:, [entity, entity_perc, sources['total']]]

        # Format columns in tables and set index date frequency
        counter_name = sources['reference'].replace('_', ' ').title()
        column_names = {
            entity: f"{counter_name} of {entity} ({sources['units']})",
            entity_perc: f"{counter_name} of {entity} (%)",
            sources['total']: f"Total {counter_name} ({sources['units']})",
        }
        table = table.rename(columns=column_names)
        table.index.levels[0].freq = ComputeTime.dates.freq
        logger.debug("Data included in the report: %s", ", ".join(table.columns))

        # Plot title and data selection
        if percent:
            plot['title'] = f"Relative {counter_name} of {entity}"
            plot['table'] = table.loc[:, [column_names[entity_perc]]]
        else:
            plot['title'] = f"{counter_name} of {entity}"
            plot['table'] = table.loc[:, [column_names[entity]]]
        logger.debug("Data used in the plot: %s", ", ".join(plot['table'].columns))

        # Max value is set to max in the plot to avoid empty plots due to exagerated scales
        plot['ymax'] = plot['table'].iloc[:, 0].groupby('date').sum().max()
        ymax_fmt = '{:.2%}' if percent else '{:.2f}'
        logger.debug("Maximum value of the plot: %s %s", ymax_fmt.format(plot['ymax']), plot['table'].columns[0])

        # Render plot
        stackplot = PlotterStack(**plot)

        # Output: file paths
        stackplot.set_output_paths(savedir)
        # Output: render HTML document including plot data table
        if plotformat == 'html':
            table_title = "{} stats per nodegroup".format(stackplot.xfreq.capitalize())
            stackplot.html_makepage()
            stackplot.html_addtable(table, table_title)
        # Output: save files
        stackplot.save_plot(plotformat)
        if csv:
            # Report data table
            stackplot.output_csv(table)
            # Unstacked data table with absolute usage by entity per date and per nodegroup
            entity_use = table.loc[:, column_names[entity]].unstack()
            entity_use['Total'] = entity_use.sum(axis=1)
            mean_freq = entity_use.index.to_series().diff().mean().days
            entity_use = entity_use.multiply(mean_freq, axis=1)  # calculate absolute compute time
            multicol = [(column_names[entity].replace('/day', ''), col) for col in entity_use.columns]
            entity_use.columns = pd.MultiIndex.from_tuples(multicol)  # add column header with units
            entity_use_file = stackplot.id.replace('-of-', '-absolute-of-')
            stackplot.output_csv(table=entity_use, filename=entity_use_file)


def pie_compute(ranking, max_top, plot_title, compute_units):
    """
    Plot a pie chart from a ranking of top entities
    Returns plot object
    - ranking: (DataFrame) ordered table of entities (can be generated with ComputeTime.rank_aggregate())
    - max_top: (integer) maximum extension of the top list
    - plot_title: (string) main title of the pie chart
    - compute_units: (tuple of strings) long and short name of units used for mean compute time
    """
    pie = dict()

    # Slice ranking by compute percent or maximum number of top entities. Do not plot any segment below 1%
    pie_num = len(ranking.loc[ranking['compute_percent'] > 0.01].index)
    pie_num = min(max_top, pie_num)
    pie['table'] = ranking.iloc[:pie_num].loc[:, ['compute_percent']]

    # Add "Others" summing up remaining elements in ranking
    if len(ranking.index) > len(pie['table'].index):
        others_slice = pd.Series(
            # Calculate remaining percentage beyond pie_num to reach 100%
            {'compute_percent': 1 - ranking['compute_percent'].iloc[:pie_num].sum()},
            name='Others',
        )
        pie['table'] = pie['table'].append(others_slice)

        dbgmsg = "Grouped the %s smallest segments of the pie chart into 'Others'"
        logger.debug(dbgmsg, len(ranking.iloc[pie_num:].index))

    logger.debug("Data segments included in the pie chart: %s", ", ".join(pie['table'].index))

    # Legend: format names including the mean compute time
    lgd_cddfmt = fixed_length_format(ranking['compute_average'][0], '.1f', ' ')  # fixed length from top element
    lgd_templ = f"[{{compute_average{lgd_cddfmt}}} {compute_units[1]}] {{index}}"
    # Legend: list elements in the pie chart
    lgd_entries = ranking.iloc[:pie_num].loc[:, ['compute_average']].reset_index().to_dict('records')
    pie['legend'] = [lgd_templ.format(**lgd) for lgd in lgd_entries]
    # Legend: always add 'Others', it will only be printed if data table contains data for 'Others'
    pie['legend'].append('Others')

    # Format column in the ranking table
    th = {'compute_percent': 'Total Compute Used (%)'}
    pie['table'] = pie['table'].rename(columns=th)
    logger.debug("Data used in the pie chart: %s", ", ".join(pie['table'].columns))

    # Set plot title
    pie['title'] = plot_title

    # Render pie chart of top entities
    pieplot = PlotterPie(**pie)

    # Annotation under pie chart legend
    pieplot.add_annotation(f"{compute_units[1]} = mean {compute_units[0]}", (0.92, 0.00), align='left')

    return pieplot


def top_users(ComputeTime, percent, savedir, plotformat, csv=False):
    """
    Plots displaying compute use by top users during the provided time frame
    > Pie chart of top users per used compute time
    > Area plot of compute time usage by top users distributed in percentiles
      Plot upper limit is maximum compute time used in the period (absolute or percentage)
    - ComputeTime: (ComputeTimeFrame) source data for the plot
    - percent: (boolean) use percentage data instead of absolute values
    - savedir: (string) path of directory to store output
    - plotformat: (string) image format of the plot
    - csv: (boolean) save data used for the plot in CSV format
    """
    logger.info("Generating accounting report on top users by compute time...")

    # Ranking of top users
    top_users = ComputeTime.rank_aggregate('User')

    # Date range of the data
    plot_idxs = ComputeTime.unpack_indexes('UserCompute')
    plot_daterange = (plot_idxs['date'][0], plot_idxs['date'][-1])
    logger.debug("Top users report covering from %s to %s", *plot_daterange)

    # Compute units for mean compute time in pie chart
    pie_mc_units = (ComputeTime.compute_units['normname'], ComputeTime.compute_units['shortname'])

    # PIE CHART OF TOP USERS
    pie_chart = pie_compute(top_users, 15, 'Top Users by Compute Time', pie_mc_units)
    pie_chart.datelim = plot_daterange
    pie_chart.set_id()

    # AREA PLOT OVER TIME
    plot = dict()
    plot['title'] = "Activity of Top Percentiles of Users"
    # Header titles of columns in level 0 and level 1
    column_title = ("Compute Time", "Top {:.0%} Users")

    # Distribute users in percentiles
    user_percentile = [
        {'thrs': 0.20, 'color': '#845ec2'},
        {'thrs': 0.10, 'color': '#009efa'},
        {'thrs': 0.05, 'color': '#00d2fc'},
        {'thrs': 0.01, 'color': '#4ffbdf'},
    ]

    # Generate percentile stacks for the area plot
    plot_stacks = dict()

    # Calculate users in each percentile
    for pctl in user_percentile:
        pctl['user_num'] = int(np.around(len(top_users.index) * pctl['thrs'], 0))

    # Remove unpopulated percentiles
    user_percentile = [pctl for pctl in user_percentile if pctl['user_num'] > 0]

    # Calculate compute stats per percentile
    for pctl in user_percentile:
        pctl['name'] = (column_title[0], column_title[1].format(pctl['thrs']))
        # Add total percentage compute for this percentile of users (used in legend)
        pctl_user_list = top_users.iloc[: pctl['user_num']]
        pctl['compute_used'] = pctl_user_list['compute_percentile'][-1]
        # Calculate total compute time used by users in this percentile
        pctl_compute = ComputeTime.UserCompute.loc[:, pctl_user_list.index].groupby('date').sum().sum(axis=1)
        plot_stacks.update({pctl['name']: pctl_compute})
        logger.debug("Completed stats of %s users in the %s percentile", pctl['user_num'], pctl['name'])

    plot['table'] = pd.DataFrame(plot_stacks)

    # Total compute per time period
    ComputeTime.aggregate_perdate('GlobalStats', 'compute_time')
    plot_totals = ComputeTime.GlobalStats.loc[:, 'total_compute_time']
    # Pick first series of total compute, all columns have same data
    plot_totals = plot_totals.unstack().iloc[:, 0]

    if percent:
        plot_ylabel = "Use of Compute Time"
        plot_units = '%'
        plot['table'] = plot['table'].divide(plot_totals, axis=0)
        plot['ymax'] = 1
    else:
        plot_ylabel = column_title[0]
        plot_units = ComputeTime.compute_units['normname']
        plot['ymax'] = max(plot_totals)

    # Add units to main column and set index date frequency
    main_column = "{} ({})".format(plot_ylabel, plot_units)
    plot['table'].columns = plot['table'].columns.set_levels([main_column], level=0)
    plot['table'].index.freq = ComputeTime.dates.freq

    logger.debug("Data used in the area plot: %s", ", ".join(plot['table'].columns.get_level_values(1)))
    ymax_fmt = '{:.2%}' if percent else '{:.2f}'
    ymax_msg = '{} {}'.format(ymax_fmt.format(plot['ymax']), plot['table'].columns.get_level_values(0)[0])
    logger.debug("Maximum value of the area plot: %s", ymax_msg)

    # Area plot legend
    legend_msg = 'Top {thrs:.0%}\n{user_num} users contributed {compute_used:.2%} compute'
    plot['legend'] = [legend_msg.format(**pctl) for pctl in user_percentile]
    plot['colors'] = [pc['color'] for pc in user_percentile]

    # Render area plot
    areaplot = PlotterArea(**plot)

    # Format ranking of top users
    ranking_units = (ComputeTime.compute_units['normname'], ComputeTime.compute_units['name'])
    top_users = format_ranking_table(top_users, *ranking_units)
    top_users.index.name = 'User'
    logger.debug("Data in the ranking table: %s", ", ".join(top_users.columns))

    # Notes with list of nodes and range of dates
    note_nodelist = 'nodes: {}'.format(', '.join([ng for ng in plot_idxs['nodegroup']]))
    note_daterange = 'from {} to {}'.format(*plot_daterange)

    # Output: file paths
    pie_chart.set_output_paths(savedir)
    areaplot.set_output_paths(savedir)

    # Output: render HTML documents
    if plotformat == 'html':
        # Pie chart: make HTML document including ranking
        pie_title = "Data from {} to {}".format(*plot_daterange)
        pie_chart.html_makepage(pie_title, [note_nodelist])
        pie_chart.html_addtable(top_users)
        # Area plot: make HTML document including plot, data table and ranking of users
        areaplot.html_makepage(plot_notes=[note_nodelist])
        areaplot.html_addtable(plot['table'], "{} stats".format(areaplot.xfreq.capitalize()))
        areaplot.html_addtable(top_users, "Ranking of top users")
    else:
        # Pie chart: add notes to plot image
        note_height = 0.98
        for note in [note_nodelist, note_daterange]:
            pie_chart.add_annotation(note, (0.50, note_height), align='center')
            note_height -= 0.02
        # Area plot: add note with list of nodes
        areaplot.add_annotation(note_nodelist, (0.50, 1.03), align='center')

    # Output: save files
    pie_chart.save_plot(plotformat)
    areaplot.save_plot(plotformat)
    if csv:
        pie_chart.output_csv(top_users)
        # save original table because table in Plotter object uses matplotlib's date format
        areaplot.output_csv(plot['table'])


def top_fields(ComputeTime, percent, savedir, plotformat, csv=False):
    """
    Plots displaying compute use by top research fields during the provided time frame
    > Pie chart of top research fields per used compute time
    > Stacked area plot of compute time usage by top research fields distributed in percentiles
      Plot upper limit is maximum compute time used in the period (absolute or percentage)
    - ComputeTime: (ComputeTimeFrame) source data for the plot
    - percent: (boolean) use percentage data instead of absolute values
    - savedir: (string) path of directory to store output
    - plotformat: (string) image format of the plot
    - csv: (boolean) save data used for the plot in CSV format
    """
    logger.info("Generating accounting report on top research fields by compute time...")

    # Ranking of top research fields
    top_fields = ComputeTime.rank_aggregate('Field')
    # Limit full names to 32 characters
    top_fields_names = top_fields.index.to_list()
    top_fields_abbrv = [clean_cut_names(field_name, (',', 'and'), 32) for field_name in top_fields_names]
    top_fields = top_fields.rename(index=dict(zip(top_fields_names, top_fields_abbrv)))
    field_names_cut = len(top_fields_names) - len(set(top_fields_names) & set(top_fields.index))
    logger.debug("Shortened the name of %s research fields", field_names_cut)

    # Date range of the data
    plot_idxs = ComputeTime.unpack_indexes('FieldCompute')
    plot_daterange = (plot_idxs['date'][0], plot_idxs['date'][-1])
    logger.debug("Top fields report covering from %s to %s", *plot_daterange)

    # Compute units for mean compute time in pie chart
    pie_mc_units = (ComputeTime.compute_units['normname'], ComputeTime.compute_units['shortname'])

    # PIE CHART OF TOP FIELDS
    pie_chart = pie_compute(top_fields, 15, 'Top Research Fields by Compute Time', pie_mc_units)
    pie_chart.datelim = plot_daterange
    pie_chart.set_id()

    # AREA PLOT OVER TIME
    plot = dict()
    plot['title'] = "Activity of Top Research Fields"

    # Select top 10 from research fields ranking
    plot_num = min(len(top_fields.index), 10)
    plot_fields = top_fields_names[:plot_num]

    # Stack data per date and per research field
    plot_stacks = ComputeTime.FieldCompute.loc[:, plot_fields].groupby('date').sum().stack()
    plot_stacks = plot_stacks.rename('compute_time')
    plot_stacks.index.set_names(['date', 'field'], inplace=True)

    plot['table'] = pd.DataFrame(plot_stacks)

    # Total compute per time period
    ComputeTime.aggregate_perdate('GlobalStats', 'compute_time')
    plot_totals = ComputeTime.GlobalStats.loc[:, 'total_compute_time']
    # Pick first series of total compute, all columns have same data
    plot_totals = plot_totals.unstack().iloc[:, 0]

    if percent:
        plot_units = '%'
        plot['table'] = plot['table'].rename(columns={'compute_time': 'use_of_compute_time'})
        plot['table'] = plot['table'].divide(plot_totals, axis=0, level=0)
        plot['ymax'] = 1
    else:
        plot_units = ComputeTime.compute_units['normname']
        plot['ymax'] = max(plot_totals)

    # Add units to main column and set index date frequency
    th = simple_names_units(plot['table'].columns, plot_units)
    plot['table'] = plot['table'].rename(columns=th)
    plot['table'].index.levels[0].freq = ComputeTime.dates.freq

    logger.debug("Data used in the stack plot: %s", ", ".join(plot['table'].columns))
    ymax_fmt = '{:.2%}' if percent else '{:.2f}'
    ymax_msg = '{} {}'.format(ymax_fmt.format(plot['ymax']), plot['table'].columns[0])
    logger.debug("Maximum value of the stack plot: %s", ymax_msg)

    # Area plot legend
    legend_msg = '{compute_percent:.2%} {index}'
    legend_rank = top_fields.iloc[:plot_num].loc[:, ['compute_percent']].reset_index().to_dict('records')
    plot['legend'] = [legend_msg.format(**lgd) for lgd in legend_rank]
    plot['colors'] = cm.tab10(np.arange(10))

    # Render area plot
    stackplot = PlotterStack(**plot)

    # Format ranking of top fields
    ranking_units = (ComputeTime.compute_units['normname'], ComputeTime.compute_units['name'])
    top_fields = format_ranking_table(top_fields, *ranking_units)
    top_fields.index.name = 'Field'
    logger.debug("Data in the ranking table: %s", ", ".join(top_fields.columns))

    # Notes with list of nodes and range of dates
    note_nodelist = 'nodes: {}'.format(', '.join([ng for ng in plot_idxs['nodegroup']]))
    note_daterange = 'from {} to {}'.format(*plot_daterange)

    # Output: file paths
    pie_chart.set_output_paths(savedir)
    stackplot.set_output_paths(savedir)

    # Output: render HTML documents
    if plotformat == 'html':
        # Pie chart: make HTML document including ranking
        pie_title = "Data from {} to {}".format(*plot_daterange)
        pie_chart.html_makepage(pie_title, [note_nodelist])
        pie_chart.html_addtable(top_fields)
        # Area plot: make HTML document including plot, data table and ranking of users
        unstack_title = "{} stats per research field".format(stackplot.xfreq.capitalize())
        stackplot.html_makepage(plot_notes=[note_nodelist])
        stackplot.html_addtable(plot['table'].unstack(), unstack_title)
        stackplot.html_addtable(top_fields, "Ranking of top research fields")
    else:
        # Pie chart: add notes to plot image
        note_height = 0.98
        for note in [note_nodelist, note_daterange]:
            pie_chart.add_annotation(note, (0.50, note_height), align='center')
            note_height -= 0.02
        # Area plot: add note with list of nodes
        stackplot.add_annotation(note_nodelist, (0.50, 1.03), align='center')

    # Output: save files
    pie_chart.save_plot(plotformat)
    stackplot.save_plot(plotformat)
    if csv:
        pie_chart.output_csv(top_fields)
        # save original table because table in Plotter object uses matplotlib's date format
        stackplot.output_csv(plot['table'])


def top_sites(ComputeTime, percent, savedir, plotformat, csv=False):
    """
    Plots displaying compute use by top research sites during the provided time frame
    > Pie chart of top sites per used compute time
    > Stacked area plot of compute time usage by top sites distributed in percentiles
      Plot upper limit is maximum compute time used in the period (absolute or percentage)
    - ComputeTime: (ComputeTimeFrame) source data for the plot
    - percent: (boolean) use percentage data instead of absolute values
    - savedir: (string) path of directory to store output
    - plotformat: (string) image format of the plot
    - csv: (boolean) save data used for the plot in CSV format
    """
    logger.info("Generating accounting report on top research sites by compute time...")

    # Ranking of top sites
    top_sites = ComputeTime.rank_aggregate('Site')

    # Date range of the data
    plot_idxs = ComputeTime.unpack_indexes('FieldCompute')
    plot_daterange = (plot_idxs['date'][0], plot_idxs['date'][-1])
    logger.debug("Top sites report covering from %s to %s", *plot_daterange)

    # Compute units for mean compute time in pie chart
    pie_mc_units = (ComputeTime.compute_units['normname'], ComputeTime.compute_units['shortname'])

    # PIE CHART OF TOP SITES
    pie_chart = pie_compute(top_sites, 15, 'Top Research Sites by Compute Time', pie_mc_units)
    pie_chart.datelim = plot_daterange
    pie_chart.set_id()

    # LINE PLOT OVER TIME
    plot = dict()
    plot['title'] = "Activity of Research Sites"

    # Group data per site
    plot_sites = top_sites.index
    plot_num = len(plot_sites)
    plot['table'] = ComputeTime.SiteCompute.loc[:, plot_sites].groupby('date').sum()

    # Total compute per time period
    ComputeTime.aggregate_perdate('GlobalStats', 'compute_time')
    plot_totals = ComputeTime.GlobalStats.loc[:, 'total_compute_time']
    # Pick first series of total compute, all columns have same data
    plot_totals = plot_totals.unstack().iloc[:, 0]

    if percent:
        plot_ylabel = "Use of Compute Time"
        plot_units = '%'
        plot['table'] = plot['table'].divide(plot_totals, axis=0)
        plot['ymax'] = 1
    else:
        plot_ylabel = "Compute Time"
        plot_units = ComputeTime.compute_units['normname']
        plot['ymax'] = max(plot_totals)

    # Format column headers and set index date frequency
    column_lvl = ([f"{plot_ylabel} ({plot_units})"], plot['table'].columns.to_list())
    plot['table'].columns = pd.MultiIndex.from_product(column_lvl)
    plot['table'].index.freq = ComputeTime.dates.freq

    logger.debug("Data used in the linear plot: %s", ", ".join(plot['table'].columns.get_level_values(1)))
    ymax_fmt = '{:.2%}' if percent else '{:.2f}'
    ymax_msg = '{} {}'.format(ymax_fmt.format(plot['ymax']), plot['table'].columns.get_level_values(0)[0])
    logger.debug("Maximum value of the linear plot: %s", ymax_msg)

    # Linear plot legend
    plot['legend'] = plot['table'].columns.get_level_values(1)
    plot['colors'] = cm.tab10(np.arange(10))

    # Make linear plot
    lineplot = PlotterLine(**plot)

    # Format ranking of top fields
    ranking_units = (ComputeTime.compute_units['normname'], ComputeTime.compute_units['name'])
    top_sites = format_ranking_table(top_sites, *ranking_units)
    top_sites.index.name = 'Site'
    logger.debug("Data in the ranking table: %s", ", ".join(top_sites.columns))

    # Notes with list of nodes and range of dates
    note_nodelist = 'nodes: {}'.format(', '.join([ng for ng in plot_idxs['nodegroup']]))
    note_daterange = 'from {} to {}'.format(*plot_daterange)

    # Output: file paths
    pie_chart.set_output_paths(savedir)
    lineplot.set_output_paths(savedir)

    # Output: render HTML documents
    if plotformat == 'html':
        # Pie chart: make HTML document including ranking
        pie_title = "Data from {} to {}".format(*plot_daterange)
        pie_chart.html_makepage(pie_title, [note_nodelist])
        pie_chart.html_addtable(top_sites)
        # Area plot: make HTML document including plot, data table and ranking of users
        table_title = "{} stats per research site".format(lineplot.xfreq.capitalize())
        lineplot.html_makepage(plot_notes=[note_nodelist])
        lineplot.html_addtable(plot['table'], table_title)
        lineplot.html_addtable(top_sites, "Ranking of top research sites")
    else:
        # Pie chart: add notes to plot image
        note_height = 0.98
        for note in [note_nodelist, note_daterange]:
            pie_chart.add_annotation(note, (0.50, note_height), align='center')
            note_height -= 0.02
        # Area plot: add note with list of nodes
        lineplot.add_annotation(note_nodelist, (0.50, 1.03), align='center')

    # Output: save files
    pie_chart.save_plot(plotformat)
    lineplot.save_plot(plotformat)
    if csv:
        pie_chart.output_csv(top_sites)
        # save original table because table in Plotter object uses matplotlib's date format
        lineplot.output_csv(plot['table'])


def fixed_length_format(value, initial_fmt, pad_fmt='0'):
    """
    Returns format string with a fixed length and leading zeros from given value
    - value: (number) reference value to define format
    - suffix: (initial_fmt) reference format string
    """
    # Remove ':' from format string
    if initial_fmt[0] == ':':
        initial_fmt = initial_fmt[1:]

    # Anyhing but leading '0' requires '>'
    if pad_fmt != '0':
        pad_fmt = pad_fmt + '>'

    # Calculate length in charcters with provided format string
    ref_fmt = '{:' + initial_fmt + '}'
    length_fmt = len(ref_fmt.format(value))

    # Generate format string with fixed length
    fixed_fmt = ':{}{}{}'.format(pad_fmt, length_fmt, initial_fmt)
    logger.debug("Format string '%s' converted to fixed length format '%s'", initial_fmt, fixed_fmt)

    return fixed_fmt


def clean_cut_names(sentence, delimiters, max_length):
    """
    Return string cut at latest separator that fulfils the maximum length
    - sentence: (string) string to be cut
    - delimiters: (string or tuple) characters in sentence that set cut location
    - max_length: (integer) maximum number of characters in sentence
    """
    if isinstance(delimiters, str):
        delimiters = (delimiters,)

    # Generate regex to match splitting points and keep them in the result
    delimiter_regex = ('({})'.format(sep) for sep in delimiters)
    delimiter_regex = "|".join(delimiter_regex)

    if len(sentence) > max_length:
        sentence_parts = re.split(delimiter_regex, sentence[:max_length])
        sentence_parts = [part for part in sentence_parts if part]  # remove negative matches
        if len(sentence_parts) > 1:
            del sentence_parts[-1]  # remove last segment as it is a half word
        sentence_parts[-1] = '…'
        cut_sentence = ''.join(sentence_parts)
    else:
        cut_sentence = sentence

    return cut_sentence


def simple_names_units(names, units=None):
    """
    Returns dict with each name simply formatted as titles including provided units
    - names: (list of strings) names to be formatted
    - units: (string or list of strings) units for all names or for each name
    """
    name_titles = dict()

    # Define units for each name
    if not isinstance(units, list):
        ref_unit = units
        units = list()
        # Use provided units for all names except known cases (eg. percentages)
        for name in names:
            if 'percent' in name:
                units.append('%')
            else:
                units.append(ref_unit)

    # Format list of names
    for n, name in enumerate(names):
        title = name.replace('_', ' ').title()
        if units[n]:
            title = "{} ({})".format(title, units[n])
        name_titles.update({name: title})

    return name_titles


def source_data(counter, aggregate, compute_units):
    """
    Return dict with names of objects that hold source data to generate an aggregate accounting
    - counter: (string) name of global data in the accounting
    - aggregate: (string) name of aggregate criteria
    - compute_units: (string) long name of compute units
    """
    sources = dict()

    # Source of accounting data
    if counter == 'Compute':
        sources.update({'reference': 'compute_time'})
        sources.update({'units': compute_units})
    elif counter == 'Jobs':
        sources.update({'reference': 'running_jobs'})
        sources.update({'units': 'jobs/day'})
    else:
        raise AttributeError(f"Source data of selected accounting not found: {counter}")

    # Source of aggregates for the accounting
    if aggregate in ['User', 'Field', 'Site']:
        sources.update({'aggregate': aggregate + counter})
    else:
        raise AttributeError(f"Source data of selected aggregate not found: {aggregate}")

    # Source of total data
    sources.update({'total': f"total_{sources['reference']}"})

    return sources


def format_ranking_table(ranking, average_units, compute_units):
    """
    Common formating of ranking data frame to be outputted as table (HTML or CSV)
    - table: (DataFrame) ranking table from ComputeTimeFrame.rank_aggregate()
    - average_units: (string) long name of average compute units
    - compute_units: (string) long name of total compute units
    """
    ranking = ranking.loc[:, ['compute_average', 'compute_time', 'compute_percent']]
    th = {
        'compute_average': f"Average Compute Time ({average_units})",
        'compute_time': f"Compute Time ({compute_units})",
        'compute_percent': 'Total Compute Used (%)',
    }
    ranking = ranking.rename(columns=th)

    return ranking
