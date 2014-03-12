#!/usr/bin/env python

import logging
import pygal
import sys
import cairosvg

from pyPdf import PdfFileWriter, PdfFileReader
from tempfile import NamedTemporaryFile

import achus.exception
import achus.collector.gridengine

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


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


######################
## -- Main Class -- ##
######################

class Report:
    """
    Main class, triggers reports based on the input given.
    """
    RENDERERS = {
        "pdf": PDFRenderer,
    }
    CONNECTORS = {
        "ge": achus.collector.gridengine.GECollector('localhost', 'root', '******', 'ge_accounting'),
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

    def _get_connector_kwargs(self, d):
        """
        Fills the keyword argumnents that will be passed to the
        connector method.
        """
        CONNECTOR_KWARGS = [
            # GEConnector
            "group_by", "start_time", "end_time",
        ]
        d_kwargs = {}
        for k in d.keys():
            if k in CONNECTOR_KWARGS:
                d_kwargs[k] = d[k]
        return d_kwargs

    def collect(self):
        """
        Gathers metric data.
        """
        for title, conf in self.task.iteritems():
            logging.info("Gathering data from metric '%s'" % title)

            self.conn = self.CONNECTORS[conf["connector"]]
            logging.debug("(Connector: %s, Metric: %s)"
                          % (conf["connector"], conf["metric"]))
            d = self.conn.get(conf["metric"], **self._get_connector_kwargs(conf))
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
