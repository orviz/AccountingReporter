#!/usr/bin/env python

import cairosvg
import contextlib
import logging
import MySQLdb as mdb
import pygal
import sys

from functools import wraps
from pyPdf import PdfFileWriter, PdfFileReader
from tempfile import NamedTemporaryFile

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class GEConnectorException(Exception):
    pass

class GEConnector:
    """
    Retrieves accounting data from a GridEngine system through SQL.
    """
    QUERIES_BY_PARAMETER = {
        "cpu_time": "SELECT ge_group, SUM(ge_cpu) FROM ge_jobs",
        "wall_clock": "SELECT ge_group, SUM(ge_ru_wallclock), ge_slots FROM ge_jobs",
    }

    GROUPS_TO_INFRASTRUCTURE = {
        "astro": "local",
        "biomed": "grid",
        "cms": "grid",
        "cmspj": "grid",
        "cmsprd": "grid",
        "computacio": "local",
        "dteam": "grid",
        "hidra": "local",
        "ops": "grid",
        "opssgm": "grid",
    }

    DEFAULT_CONDITIONS = [
        "ge_slots>=1",
        "ge_ru_wallclock>=0",
        "ge_submission_time<=ge_start_time",
        "ge_start_time<=ge_end_time",
    ]

    def __init__(self, dbserver, dbuser, dbpasswd, dbname):
        self.dbserver = dbserver
        self.dbuser   = dbuser
        self.dbpasswd = dbpasswd
        self.dbname   = dbname

    def _format_query(self, *args):
        """
        Adds the default WHERE conditions as well as the group by statement.
        """
        return " ".join(["%s WHERE", " AND ".join([cond for cond in self.DEFAULT_CONDITIONS]),
                         "GROUP BY %s" ]) % args

    def _format_value(self, *args):
        """
        Transforms 'v' value to float needed to render the graph.
        """
        import decimal
        l = []
        for arg in args:
            if type(arg) in [decimal.Decimal, long, int, float]:
                l.append(float(arg))
        return l

    def _format_result(self, data):
        """
        Aggregates the tuples retrieved by:
            t[0], t[1:],
        converting the numeric values to float. Finally, returns a list
        of these tuples.

        Note: cannot convert to dictionary since it can happen that the
              same index (t[0]) appears multiple times.
        """
        return [ (item[0], self._format_value(*item[1:])) for item in data ]

    def _group_by_infrastructure(self, data):
        """
        Organizes data (summing) by the infrastructure type.
        """
        d = {}
        for group, value in data.iteritems():
            try:
                d[self.GROUPS_TO_INFRASTRUCTURE[group]] += value
            except KeyError:
                d[self.GROUPS_TO_INFRASTRUCTURE[group]] = value

        return d

    def _group_by(self, d, group_by="group"):
        """
        Organizes the data according to the given group.
        """
        GROUP_FUNCS = {
            "infrastructure": self._group_by_infrastructure,
        }
        try:
            return GROUP_FUNCS[group_by](d)
        except KeyError:
            return d

    def query(self, parameter, group_by="ge_group"):
        """
        Performs a SQL query based on the parameter requested.
        """
        conn = mdb.connect(self.dbserver, self.dbuser, self.dbpasswd, self.dbname);
        with contextlib.closing(conn):
            curs = conn.cursor()
            cmd = self._format_query(self.QUERIES_BY_PARAMETER[parameter], group_by)
            logger.debug("MySQL command: `%s`" % cmd)
            curs.execute(cmd)
            res = self._format_result(curs.fetchall())
            return res

    def group(func):
        @wraps(func)
        def _group(self, *args, **kw):
            try:
                post_key = kw["group_by"]
                if kw["group_by"] == "infrastructure":
                    kw["group_by"] = "ge_group"
            except KeyError:
                post_key = "group"
            output = func(self, *args, **kw)
            return self._group_by(output, post_key)
        return _group

    @group
    def get_cpu_time(self, group_by="ge_group"):
        """
        Retrieves the CPU time grouped by 'ge_group'.
        """
        d = {}
        for item in self.query("cpu_time", group_by=group_by):
            index, values = item
            cpu_time = values[0]
            try:
                d[index] += cpu_time
            except KeyError:
                d[index] = cpu_time
        return d

    @group
    def get_wall_clock(self, group_by="ge_group"):
        """
        Retrieves the WALLCLOCK time grouped by 'ge_group'.
            Number of slots being used must be taken into account.
        """
        d = {}
        for item in self.query("wall_clock", group_by="ge_slots,%s" % group_by):
            index, values = item
            wall_clock, slots = values
            try:
                d[index] += wall_clock * slots
            except KeyError:
                d[index] = wall_clock * slots
        return d

    def get_efficiency(self, group_by="ge_group"):
        """
        Retrieves the CPU time grouped by 'ge_group'.
        """
        d = {}
        d_cpu  = self.get_cpu_time()
        d_wall = self.get_wall_clock()
        if len(d_cpu.keys()) != len(d_wall.keys()):
            raise GEConnectorException("Cannot compute efficiency. Groups do not match!")
        for k,v in d_cpu.iteritems():
            try:
                d[k] = d_cpu[k]/d_wall[k]
            except ZeroDivisionError:
                d[k] = 0
        return d

    def get(self, metric, **kw):
        METRICS = {
            "cpu": self.get_cpu_time,
            "efficiency": self.get_efficiency,
        }
        return METRICS[metric](**kw)


##################
## -- Charts -- ##
##################

class Chart:
    """
    Generates charts using PyGal.
    """
    def __init__(self, d, title='', type="pie", filename=None):
        """
        Expects a dictionary with k,v pairs to be plotted.

        If filename is defined, it assumes that the chart has to
        renderized in this file.
        """
        self.d = d
        self.title = title
        self.type = type
        self.filename = filename

    def render(self):
        """
        Renders to a pygal's chart.
        """
        chart_types = {
            "pie": pygal.Pie(),
            "horizontal_bar": pygal.HorizontalBar()
        }
        chart = chart_types[self.type]
        chart.title = self.title
        for k,v in self.d.iteritems():
            chart.add(k,v)
        if self.filename:
            chart.render_to_file(filename=self.filename)
        else:
            chart.render()

#####################
## -- Renderers -- ##
#####################

class PDFRenderer:
    """
    Generates a report with the chart content.
    """
    def __init__(self, filename, objs=[], **kw):
        """
        A PDF report is comprised of several objects. These objects
        must have a render() method.
            filename: name of the PDF file.
            objs: objects to be renderized in the document.
        """
        self.filename = filename
        self.objs = objs
        self.pdf_list = []

    def append(self, obj):
        """
        Appends an object to the report.
        """
        self.objs.append(obj)

    def render(self):
        """
        Generates the PDF report by:
            1) Calling object's render() method (creates SVG file).
            2) Transforms SVG content to PDF content.
            3) Merges the resultant PDF files into the final report.
        """
        for obj in self.objs:
            # Temporary SVG
            chart_svg_file = NamedTemporaryFile()
            obj.filename = chart_svg_file.name
            obj.render()
            logger.debug(("'%s' graph has been generated as SVG under"
                          "'%s'"
                          % (obj.title, obj.filename)))
            # Temporary PDF
            chart_pdf_file = NamedTemporaryFile()
            chart_pdf_file.write(cairosvg.svg2pdf(url=obj.filename))
            logger.debug(("'%s' graph has been generated as PDF under"
                          "'%s'"
                          % (obj.title, chart_pdf_file.name)))
            self.pdf_list.append(chart_pdf_file)
            chart_svg_file.close()
        # PDF
        output = PdfFileWriter()
        for pdf in self.pdf_list:
            input1 = PdfFileReader(pdf)
            output.addPage(input1.getPage(0))
        outputStream = file(self.filename, "wb")
        output.write(outputStream)
        outputStream.close()
        logger.debug("Result PDF created under '%s'" % self.filename)


#####################
## -- Renderers -- ##
#####################

class Report:
    """
    Main class, triggers reports based on the input given.
    """
    RENDERERS = {
        "pdf": PDFRenderer,
    }
    CONNECTORS = {
        "ge": GEConnector('localhost', 'root', '*******', 'ge_accounting'),
    }

    def __init__(self, renderer, task, **kw):
        """
        renderer: type of report (supported: RENDERERS.keys())
        task: dictionary with the metrics to be gathered.
        """
        logging.debug("New report requested (TYPE <%s>)" % renderer)
        logging.debug("TASKs: %s ; KEYWORD ARGUMENTS: %s"
                      % (task, kw))
        self.renderer = self.RENDERERS[renderer](**kw)
        self.task     = task

    def collect(self):
        """
        Gathers metric data.
        """
        for title, conf in self.task.iteritems():
            logging.info("Gathering data from metric '%s'" % title)

            self.conn = self.CONNECTORS[conf["connector"]]
            logging.debug("(Connector: %s, Metric: %s)"
                          % (conf["connector"], conf["metric"]))
            d = self.conn.get(conf["metric"], **{ "group_by": conf["group_by"] })
            logging.debug("Result from connector: '%s'" % d)

            try:
                chart = Chart(d, title, type=conf["chart"])
                logging.debug("Metric will be displayed as a chart, type <%s>"
                              % conf["chart"])
                self.renderer.append(chart)
                logging.debug(("Chart appended to report's list of "
                               "objects-to-be-rendered"))
            except KeyError:
                logging.debug("Metric is not set to be displayed as a chart. Note that no other format is supported. Doing nothing")

    def generate(self):
        """
        Triggers the report rendering.
        """
        self.renderer.render()
