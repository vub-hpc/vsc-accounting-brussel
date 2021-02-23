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
Render plots and tables in several formats for vsc.accounting

@author Alex Domingo (Vrije Universiteit Brussel)
"""

import os
import re

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.ticker as tkr
import matplotlib.dates as mdates
import matplotlib.cm as cm

from bs4 import BeautifulSoup, Doctype

from vsc.utils import fancylogger
from vsc.accounting.exit import error_exit
from vsc.accounting.filetools import check_dir, find_available_path
from vsc.accounting.config.parser import MainConf
from vsc.accounting.data.parser import DataFile


class Plotter:
    """
    Data structures and methods to make a plot from data over time in a data frame
    """

    def __init__(self, title, table, ymax=None, colors=None, legend=None):
        """
        Initialize plot including axes, labels and legend
        > Plot object (matplotlib) is accessible in self.fig and self.ax
        > HTML page (beautifulsoup) is accessible in self.html_page
        - title: (string) main title of the plot
        - table: (DataFrame) data source for the plot
        - ymax: (numeric) maximum value of the Y axis
        - colors: (list of strings) color codes for each plot element
        - legend: (list of strings) alternative text elements of the legend

        Note: No default render() function defined. It is declared on child classes depending on the plot type.
        """
        self.log = fancylogger.getLogger(name=self.__class__.__name__)

        # Plot title
        try:
            cluster_name = MainConf.get('nodegroups', 'cluster_name')
        except KeyError as err:
            error_exit(self.log, err)
        else:
            self.title = f"{cluster_name}: {title}"

        # General plot format settings
        format_configs = dict()
        for format_config in ['plot_dpi', 'plot_fontsize']:
            try:
                format_value = MainConf.get_digit('reports', format_config)
            except (KeyError, ValueError) as err:
                error_exit(self.log, err)
            else:
                format_configs.update({format_config: format_value})

        # Font sizes are relative to 'plot_fontsize' configuration
        format_fontsize_mod = {
            'axes.titlesize': 4,
            'axes.labelsize': 0,
            'xtick.labelsize': -2,
            'ytick.labelsize': -2,
            'legend.fontsize': -4,
        }
        format_params = {fp: format_configs['plot_fontsize'] + fmod for fp, fmod in format_fontsize_mod.items()}
        # Add DPI setting
        format_params.update({'figure.dpi': format_configs['plot_dpi']})
        # Apply formats globally
        plt.rcParams.update(format_params)
        self.log.debug("Plot formatting set succesfully: %s", format_params)

        # Make local copy of data for the plot
        try:
            self.check_df(table)
        except TypeError as err:
            error_exit(self.log, err)
        else:
            self.table = table.copy()
            self.log.debug("Plot data table copied succesfully")

        # Plot date range
        if 'date' in self.table.index.names:
            dateidx = self.table.index.get_level_values('date').unique()
            self.datelim = (dateidx[0].date(), dateidx[-1].date())
            self.log.debug("Plot data range: %s to %s", *self.datelim)
        else:
            self.datelim = None

        # Plot measure is first column in index level 0
        if table.columns.nlevels > 1:
            self.ylab = self.table.columns.get_level_values(0)[0]
        else:
            self.ylab = self.table.columns[0]
        # Y axis scale and labels
        self.ymax = ymax
        self.yunits = re.search(r'\((.*?)\)', self.ylab)
        if self.yunits:
            self.yunits = self.yunits.group(1)
        # X axis labels
        self.xfreq = self.date_freq()
        self.xlab = f"Date ({self.xfreq})"
        self.log.debug("Plot labels: [X] %s [Y] %s", self.xlab, self.ylab)

        # Plot legend
        self.colors = colors
        self.legend = legend

        # Set plot ID from plot title plus index interval
        self.set_id()

        # Make the plot
        self.render()
        self.set_xaxis()
        self.set_yaxis()
        self.add_legend()

    def check_df(self, table):
        """
        Validate that table-like object is a pd.DataFrame
        - table: (pd.DataFrame) object to check
        """
        if isinstance(table, pd.DataFrame):
            return True
        else:
            errmsg = "Provided table is not a DataFrame"
            raise TypeError(errmsg)

    def set_id(self, corename=None, suffix=None):
        """
        Define an ID for the plot based on its characteristics
        By default generate a 'corename' from the title and append de first and last elements in index
        Resulting ID won't be necessarily unique, but is a good effort to avoid name collision
        This ID is mainly used to determine names of output files
        - corename: (string) alternative base name of the ID
        - suffix: (string) alternative suffix of the ID
        """
        if corename is None:
            # Generate core from plot title
            id_core = re.sub('[,:\(\)/]', '', self.title)
            id_core = id_core.replace(' ', '-').lower()
        else:
            # Use provided core
            id_core = str(corename)

        if suffix is not None:
            # Use provided suffix
            id_suffix = f"{suffix}"
        elif self.datelim:
            # Use date range as suffix
            id_suffix = '{}_{}'.format(*self.datelim)
        else:
            # Use first and last element of index
            idxrange = (self.table.index.get_level_values(0)[0], self.table.index.get_level_values(0)[-1])
            id_suffix = "{}-{}".format(*idxrange)

        self.id = f"{id_core}_{id_suffix}"
        self.log.debug("Plot ID set to '%s'", self.id)

    def set_output_paths(self, savedir=None):
        """
        Set output directory and define the path of output files for this object
        Paths will be based on the object ID, which will be modified as necessary to avoid filename collisions
        WARNING: do not set these paths too much in advance of any write operation
        - savedir: (string) path to directory to save output data
        """
        # Set output directory
        if savedir is not None and check_dir(savedir):
            self.savedir = savedir
        else:
            self.savedir = os.getcwd()

        # Use ID to set default paths for each output file
        self.output_path = dict()
        output_exts = ['html', 'pdf', 'png', 'svg', 'csv']
        for ext in output_exts:
            try:
                filepath = os.path.join(self.savedir, f"{self.id}.{ext}")
                filepath = find_available_path(filepath)
            except FileExistsError as err:
                error_exit(self.log, err)
            else:
                self.output_path.update({ext: filepath})
                self.log.debug("Default output path for %s files set to %s", ext.upper(), self.output_path[ext])

    def set_xaxis(self):
        """
        Custom X axis with auto date formatting
        """

        autolocator = mdates.AutoDateLocator(minticks=3, maxticks=30)
        autoformat = mdates.ConciseDateFormatter(autolocator)
        self.ax.xaxis.set_major_locator(autolocator)
        self.ax.xaxis.set_major_formatter(autoformat)

        self.ax.xaxis.set_minor_locator(mdates.DayLocator())
        self.ax.xaxis.set_minor_formatter(tkr.NullFormatter())

        self.ax.tick_params(bottom=True, top=True, left=True, right=False)
        self.ax.tick_params(labelbottom=True, labeltop=False, labelleft=True, labelright=False)

        # Add axes labels
        self.ax.set_xlabel(self.xlab)

    def set_yaxis(self):
        """
        Custom Y axis with absolute or percentage formatting
        """
        if self.yunits == '%':
            # Set percentage scale
            self.ymax = max(self.ymax, 0.01)  # set a minimum of 1%
            note_ymax = '{:.1%}'.format(np.around(self.ymax, 3)) if self.ymax < 1 else ''
            divider = 4
            majformatter = tkr.PercentFormatter(xmax=1)
            self.log.debug("Plot Y axis set to percentual values")
        else:
            # Set absolute scale
            self.ymax = max(self.ymax, 1)  # set a minimum of 1 unit
            note_ymax = '{:.0f}'.format(np.around(self.ymax, 0))
            divider = 2 if self.ymax > 10 else 1
            majformatter = tkr.ScalarFormatter()
            self.log.debug("Plot Y axis set to absolute values")

        self.ax.set_ylim([0, self.ymax])

        self.ax.yaxis.set_minor_locator(tkr.AutoMinorLocator(divider))
        self.ax.yaxis.set_major_locator(tkr.MaxNLocator())

        self.ax.yaxis.set_minor_formatter(tkr.NullFormatter())
        self.ax.yaxis.set_major_formatter(majformatter)

        # Add axes labels
        self.ax.set_ylabel(self.ylab)
        # Add max value as annotation
        self.add_annotation(note_ymax, (0.00, 1.03), align='center')

    def draw_hline(self, yvalue, perc=True, color='grey'):
        """
        Adds horizontal line at 'yvalue'
        - perc: (boolean) 'yvalue' is interpreted as a percentage of Y axis
        - color: (string) color code for the line
        """
        if perc:
            yvalue = self.ymax * yvalue

        self.ax.axhline(y=yvalue, ls=':', lw=1, color=color)

    def add_title(self):
        """
        Adds main title to the plot
        """
        self.ax.set_title(self.title, pad=20)

    def add_legend(self):
        """
        Adds main legend to the plot
        """
        # By default make legend from level 1 index (level 0 index is the X axis)
        if self.legend is None:
            lgd_type = 'default'
            self.legend = self.table.index.get_level_values(1).unique()
        else:
            lgd_type = 'custom'

        # Draw legend
        lgd = self.ax.legend(self.legend, bbox_to_anchor=(1.01, 0.00), loc="lower left", borderaxespad=1)
        self.log.debug("Plot %s legend added succesfully", lgd_type)

        # Set linewidth of legend handles
        for handle in lgd.legendHandles:
            handle.set_linewidth(4.0)

    def add_annotation(self, note, xy, align='left', fontsize=10, fontfamily='sans-serif'):
        """
        Adds a block of text to the plot
        Coordinates are defined using the plot box as reference
        - note: (string) text of teh annotation
        - xy: (numeric tuple) pair of values with the coordinates for the annotation
        - aling: (string) justification of the text inside the annotation
        - fontsize: (integer) size of the font
        - fontfamily: (string) font family of the text
        """
        self.ax.annotate(
            note,
            xy=xy,
            xycoords=('axes fraction', 'axes fraction'),
            ha=align,
            va='top',
            size=fontsize,
            family=fontfamily,
        )

    def date_freq(self):
        """
        Return a readable label for frequency of the 'date' index in the plot's table
        """
        freq_label = None

        # Human readable names of DateOffset aliases
        offset_name = {
            'D': 'daily',
            'W-MON': 'weekly',
            'MS': 'monthly',
            'QS': 'quarterly',
            'AS': 'yearly',
        }

        if 'date' in self.table.index.names:
            # Get the offset alias from the table index
            if self.table.index.nlevels == 1:
                freq_offset = self.table.index.freqstr
            else:
                freq_offset = self.table.index.levels[0].freqstr
            # Convert to a frequency label
            if freq_offset in offset_name:
                freq_label = offset_name[freq_offset]

            self.log.debug("Plot date resolution re-determined to be %s", freq_label)

        return freq_label

    def html_makepage(self, plot_title=None, plot_notes=None):
        """
        Generate HTML document from scratch with plot image and store it in self.html_page
        - plot_title: (string) alternative title for the plot
        - plot_notes: (list of string) optional text to add below plot_title
        """
        # Path to image file of the plot
        # Use SVG file for better scaling quality
        try:
            img_source_path = self.output_path['svg']
        except AttributeError as err:
            errmsg = f"Path to plot render for HTML page not found. Method self.set_output_paths() not called yet."
            error_exit(self.log, errmsg)
        else:
            img_source_path = os.path.basename(img_source_path)

        # Main titles
        page_title = self.title

        if self.datelim and plot_title is None:
            plot_title = "Graph from {} to {}".format(*self.datelim)

        # Head and title of the HTML page
        head = "<head><meta /><title>{}</title></head>".format(page_title)
        page = BeautifulSoup(''.join(head), 'lxml')
        page.insert(0, Doctype('html'))
        page.html['lang'] = 'en'
        page.head.meta['charset'] = 'utf-8'

        # CSS style: take from file defined in configuration
        html_css_file = MainConf.get('reports', 'html_main_cssfile', fallback='html_main_style.html', mandatory=False)
        css_style = DataFile(html_css_file, mandatory=True).contents
        page.head.append(css_style.find('style'))
        self.log.debug(f"HTML page: added CSS style from file: {html_css_file}")

        # Body and main title
        newobj = page.html
        for tag in ['body', 'h1']:
            newtag = page.new_tag(tag)
            newobj.append(newtag)
            newobj = newobj.contents[-1]
        page.h1.string = page_title

        # Render plot in SVG format
        img_block = page.new_tag('div')
        img_block['class'] = 'blockcard'

        if plot_title is not None:
            img_block.append(page.new_tag('h2'))
            img_block.h2.string = plot_title
            self.log.debug("HTML page: plot sub-title added")

        if plot_notes is not None:
            if not isinstance(plot_notes, list):
                plot_notes = [plot_notes]
            for note in plot_notes:
                img_block.append(page.new_tag('p'))
                p_block = img_block.contents[-1]
                p_block.string = note
            self.log.debug("HTML page: %s notes added", len(plot_notes))

        img_block.append(page.new_tag('img'))
        img_block.img['class'] = 'plotrender'
        img_block.img['src'] = img_source_path
        img_block.img['alt'] = self.title
        page.body.append(img_block)
        self.log.info("HTML page: plot render '%s' added to report page", img_block.img['src'])

        # Render container for tables
        tables_block = page.new_tag('div')
        tables_block['id'] = 'tablescontainer'
        tables_block['class'] = 'blockcard'
        page.body.append(tables_block)

        self.html_page = page

    def html_dataframe(self, table):
        """
        Format DataFrame into an HTML table, generating a complete HTML document
        - table: (DataFrame) source data for the HTML table
        """
        # Work on a local copy of data table
        table = table.copy()

        # Format any Datetime indexes to ISO format
        for level in range(table.index.nlevels):
            idx = table.index.unique(level=level)
            if isinstance(idx, pd.DatetimeIndex):
                idx = idx.strftime('%Y-%m-%d')
                if table.index.nlevels > 1:
                    table.index = table.index.set_levels(idx, level=level)
                else:
                    table = table.set_index(idx)
                self.log.debug("HTML page: dates in index formatted in ISO format")

        # CSS style: take from file defined in configuration
        table_css_file = MainConf.get(
            'reports', 'html_table_cssfile', fallback='html_table_style.json', mandatory=False
        )
        table_css = DataFile(table_css_file, mandatory=True).contents
        self.log.debug(f"HTML page: added stylist rules to table from file: {table_css_file}")

        # CSS style: table zebra pattern
        zebra_bg = ('background', 'whitesmoke')
        if table.index.nlevels == 1:
            # Intermitent shading of single rows
            zebra_css = [{'selector': 'tbody tr:nth-of-type(odd)', 'props': [zebra_bg]}]
            self.log.debug(f"HTML page: applied zebra shading to every other row")
        else:
            # Intermitent shading of all rows beloging to each element in root index level
            rows = np.prod([len(level) for level in table.index.levels[1:]])
            zebra_css = [
                {'selector': f"tbody tr:nth-of-type({rows * 2}n-{shift})", 'props': [zebra_bg]} for shift in range(rows)
            ]
            self.log.debug("HTML page: applied zebra shading to every %s rows", rows)
        table_css.extend(zebra_css)

        # Delete names of each index level as it adds a second TH row
        table.index.names = [None for name in table.index.names]
        # Delete names of each column level as thous would be also printed along the column headers
        table.columns.names = [None for name in table.columns.names]

        # Format numbers
        table_format = dict()
        for column in table.columns:
            # Use names from all column levels
            if table.columns.nlevels > 1:
                column_name = " ".join(column)
            else:
                column_name = column

            if re.search('\(coredays.*\)', column_name):
                table_format.update({column: '{:.1f}'})
            elif re.search('\(.*%\)', column_name):
                table_format.update({column: '{:.2%}'})
            elif re.search('\(.*\)', column_name):
                # by default display data with units as integers
                table_format.update({column: '{:.0f}'})
            else:
                # data without units are treated as is
                table_format.update({column: '{}'})
        self.log.debug("HTML page: number formatting set per column of table to %s", table_format)

        # Get extra padding from configuration setting
        try:
            column_xtrlen = MainConf.get_digit('reports', 'html_table_extrapadding', fallback=2, mandatory=False)
        except (KeyError, ValueError) as err:
            error_exit(self.log, err)
        else:
            self.log.debug("HTML page: table cells extra padding set to %s", column_xtrlen)

        # Set lengths for each column based on formatted maximum value
        column_maxlen = [len(table_format[col].format(val)) for col, val in table.max(axis=0).to_dict().items()]
        column_width = [
            {'selector': f".col{col}", 'props': [('width', f"{column_maxlen[col] + column_xtrlen}em")]}
            for col in range(table.shape[1])
        ]
        table_css.extend(column_width)

        self.log.debug("HTML page: table column widths adjusted to %s", column_width)

        # Heatmap for data corresponding with the plot
        if self.yunits == '%':
            # color grade all columns with percentual data
            unitlabel = f"({self.yunits})"
            if table.columns.nlevels > 1:
                graded_cols = [col for col in table.columns if unitlabel in ''.join(col)]
            else:
                graded_cols = [col for col in table.columns if unitlabel in col]
            self.log.debug("HTML page: color graded all columns in table")
        elif self.ylab in table.columns:
            # color grade columns with data of plot
            graded_cols = [self.ylab]
            self.log.debug("HTML page: color graded column '%s'", self.ylab)
        else:
            graded_cols = None
            self.log.debug("HTML page: no color grading applied")

        # Data table printout
        table_styled = table.style.format(table_format).set_table_styles(table_css)
        self.log.debug("HTML page: table CSS style applied")

        if graded_cols:
            # Note: background_gradient accepts axis=None in pandas 0.25 and vmax in pandas 1.0
            # .background_gradient(cmap='YlGnBu', axis=None, subset=dataframe_slice, vmax=num)
            table_styled = table_styled.background_gradient(cmap='YlGnBu', axis='index', subset=graded_cols)
            self.log.debug("HTML page: table color gradient applied")

        table_html = table_styled.render()

        # Parse table html
        table_soup = BeautifulSoup(table_html, 'lxml')

        # Fusion cells with equal total values for all nodegroups
        th0 = table_soup.tbody.select('th.row_heading.level0')
        rowspan = int(th0[0]['rowspan']) if th0[0].has_attr('rowspan') else 1
        ngtotals = [f"col{col}" for col, name in enumerate(table.columns) if 'Total' in name]
        # Only proceed if level 0 index has rowspan and columns named 'Total' exist
        if rowspan > 1 and len(ngtotals) > 0:
            for ngtotal in ngtotals:
                column_total = table_soup.tbody.find_all('td', ngtotal)
                # Check if values in first group of rows are equal (assumes same topology accross the column)
                firstrow = [cell.string for cell in column_total[0:rowspan]]
                if all(cell == firstrow[0] for cell in firstrow):
                    # Add rowspan to each top cell
                    for row in range(0, len(column_total), rowspan):
                        column_total[row]['rowspan'] = rowspan
                        # Delete redundant cells
                        for span in range(1, rowspan):
                            column_total[row + span].decompose()
                    self.log.debug("HTML page: cells in column '%s' fusioned succesfully", ngtotal[3:])

        return table_soup

    def html_addtable(self, table_data, table_title=None):
        """
        Convert DataFrame into an HTML table and add it to self.html_page
        - table_data: (DataFrame) source data for the HTML table
        - table_title: (string) title for the table in the HTML document
        """
        # Check provided data table
        try:
            self.check_df(table_data)
        except TypeError as err:
            error_exit(self.log, err)

        # Convert DataFrame to HTML
        table_page = self.html_dataframe(table_data)

        # Add CSS to HTML document
        table_css = table_page.head.style
        self.html_page.head.append(table_css)

        # Add table to HTML document
        table_container = self.html_page.find(id='tablescontainer')

        table_block = table_page.new_tag('div')
        table_block['class'] = 'tablecard'

        if table_title:
            table_block.append(table_page.new_tag('h2'))
            table_block.h2.string = table_title

        table_block.append(table_page.body.table)
        table_container.append(table_block)
        self.log.info("HTML page: data table added to report page")

    def output_img(self, imgfmt='svg'):
        """
        Save plot image in 'imgfmt' format to the default output path
        Matplotlib object is closed after save as it is no longer needed and
        there is a limit of plot objects that can be open at the same time
        - imgfmt: (string) file format of the image
        """
        # Work with lowercase format extensions
        imgfmt = imgfmt.lower()

        # Save image file
        try:
            self.fig.savefig(self.output_path[imgfmt], format=imgfmt, bbox_inches='tight')
        except PermissionError:
            error_exit(f"Permission denied to save plot render: {self.output_path[imgfmt]}")
        else:
            self.log.info(f"Report for '{self.title}' saved in {imgfmt.upper()} format to {self.output_path[imgfmt]}")

        # Delete plot render
        plt.close(self.fig)

    def output_html(self):
        """
        Save HTML document in self.html_page into a file
        """
        try:
            with open(self.output_path['html'], 'w') as htmlfile:
                htmlfile.write(self.html_page.prettify())
        except PermissionError as err:
            error_exit(f"Permission denied to write HTML file: {self.output_path['html']}")
        else:
            self.log.info(f"Report for '{self.title}' saved in HTML format to {self.output_path['html']}")

    def output_csv(self, table=None, filename=None):
        """
        Save data frame in CSV format to 'csvfile'
        - table: (DataFrame or Series) alternative source data to save in the CSV
        - filename: (string) alternative name of the CSV file
        """
        if not isinstance(table, pd.DataFrame) and not isinstance(table, pd.Series):
            table = self.table.copy()

        if filename is None:
            csvpath = self.output_path['csv']
        else:
            try:
                csvpath = os.path.join(self.savedir, f"{filename}.csv")
                csvpath = find_available_path(csvpath)
            except FileExistsError as err:
                error_exit(self.log, err)
            else:
                self.log.debug("Using alternative path for CSV file output: %s", csvpath)

        # Add index names to the header row
        index_header = [idx.replace('_', ' ').title() for idx in table.index.names if idx]

        # Output data to CSV
        try:
            table.to_csv(csvpath, float_format='%.2f', header=True, index_label=index_header)
        except PermissionError:
            error_exit(f"Permission denied to save data in CSV format to {csvpath}")
        else:
            self.log.info(f"Data for '{self.title}' saved in CSV format to {csvpath}")

    def save_plot(self, plotformat):
        """
        Save render of plot in requested format
        - plotformat: (string) image format of the plot
        """

        if plotformat == 'html':
            # Make minimal plot for HTML page
            self.output_img('svg')
            # Generate HTML
            self.output_html()
        else:
            # Save plot images
            self.add_title()
            self.output_img(plotformat)


class PlotterStack(Plotter):
    """
    Plotter child object to make a stacked area plot.
    """

    def render(self):
        # Unstack multiindex DataFrame for the plot
        table = self.table.unstack()
        # Formar dates in index to matplotlib format
        table.index = mdates.date2num(table.index)
        # Draw stacked plot
        self.ax = table.plot(kind='area', stacked=True, color=self.colors, legend=False, figsize=(16, 8))
        self.fig = self.ax.get_figure()
        self.log.info("Stack plot '%s' rendered succesfully", self.title)


class PlotterArea(Plotter):
    """
    Plotter child object to make a non-stacked area plot.
    """

    def render(self):
        # Formar dates in index to matplotlib format
        self.table.index = mdates.date2num(self.table.index)
        # Draw non-stacked area plot
        self.ax = self.table.plot(
            kind='area', stacked=False, color=self.colors, alpha=0.8, legend=False, figsize=(16, 8)
        )
        self.fig = self.ax.get_figure()
        self.log.info("Area plot '%s' rendered succesfully", self.title)


class PlotterLine(Plotter):
    """
    Plotter child object to make a line plot.
    """

    def render(self):
        # Formar dates in index to matplotlib format
        self.table.index = mdates.date2num(self.table.index)
        # Draw linear plot
        self.ax = self.table.plot(kind='line', linewidth=3, color=self.colors, legend=False, figsize=(16, 8))
        self.fig = self.ax.get_figure()
        self.log.info("Linear plot '%s' rendered succesfully", self.title)


class PlotterPie(Plotter):
    """
    Plotter child object to make a pie chart.
    """

    def render(self):
        # Convert first column of dataframe to list
        pie_data = self.table.iloc[:, 0].to_list()

        # Draw pie chart
        self.fig, self.ax = plt.subplots(figsize=(12, 12), subplot_kw=dict(aspect="equal"))
        cs = cm.tab20c(np.arange(20))  # set color palette with a 20 different colors
        self.ax.pie(
            pie_data, colors=cs, autopct='%1.2f%%', pctdistance=0.8, textprops={'color': '#333333', 'weight': 'bold'}
        )
        self.log.info("Pie chart '%s' rendered succesfully", self.title)

    def add_legend(self):
        """
        Adds main legend to pie chart
        """
        # Default legend lists items in pie chart
        if self.legend is None:
            lgd_type = 'default'
            self.legend = self.table.index
        else:
            lgd_type = 'custom'

        # Add percentage values of pie chart to legend
        pie_data = self.table.iloc[:, 0].to_list()
        lgd_fmt = '{:06.2%} {}'
        self.legend = [lgd_fmt.format(percent, name) for (percent, name) in zip(pie_data, self.legend)]

        # Draw legend
        lgd = self.ax.legend(self.legend, bbox_to_anchor=(0.90, 0.00), loc="lower left", borderaxespad=1)
        self.log.debug("Plot %s legend added succesfully", lgd_type)

        # Set linewidth of legend handles
        for handle in lgd.legendHandles:
            handle.set_linewidth(4.0)

    def add_title(self):
        """
        Adds main title to the plot
        Optionally adds provided notes as subtitles
        """
        self.ax.set_title(self.title, pad=20)

    def set_xaxis(self):
        """
        Disable set_xaxis for pie charts
        """
        return True

    def set_yaxis(self, percent=False):
        """
        Add Y label as subtitle of the pie chart
        """
        # Add plot subtitle
        self.add_annotation(self.ylab, (0.50, 1.01), align='center', fontsize=12)
