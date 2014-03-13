import logging
import StringIO

import cairosvg
import PyPDF2
from oslo.config import cfg

# FIXME(aloga): this should be configurable and we should take
# the class from the chart renderer
from achus import exception
import achus.renderer.chart

CONF = cfg.CONF
CONF.import_opt('output_file', 'achus.renderer', group="renderer")

import achus.renderer.base

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class PDFChart(achus.renderer.base.Renderer):
    """ Generates PDF report containing charts."""

    def __init__(self):
        self.metrics = []

    def append_metric(self, title, metric, metric_definition):
        """Appends a metric to be graphed in the report."""
        self.metrics.append((title, metric, metric_definition))

    def render(self, filename=None):
        """
        Generates the PDF report by.

        This method will render each of the metrics stored in the
        self.metrics list into a graph, that will be then joined
        into a PDF file.

        If filename is set the PDF file will be written into that file.
        If filename is not set, a string containing the file will be
        returned.
        """
        pdf_charts = []
        for title, metric, metric_definition in self.metrics:
            try:
                chart = achus.renderer.chart.Chart(metric,
                                                   title,
                                                   metric_definition["chart"])
            except exception.UnknownChartType:
                logging.debug("Metric is not set to be displayed as "
                              "a chart. Note that no other format is "
                              "supported. Doing nothing.")
            else:
                svg = chart.render()
                pdf_charts.append(cairosvg.svg2pdf(bytestring=svg))

        output = PyPDF2.PdfFileWriter()
        for pdf in pdf_charts:
            input1 = PyPDF2.PdfFileReader(StringIO.StringIO(pdf))
            output.addPage(input1.getPage(0))

        if filename:
            output_stream = file(filename, "wb")
            output.write(output_stream)
            output_stream.close()
            logger.debug("Result PDF created under '%s'" % filename)
            return None
        else:
            output_stream = StringIO.StringIO()
            output.write(output_stream)
            return output_stream.read()

    def render_to_file(self, filename=CONF.renderer.output_file):
        """Write the PDF report into filename"""
        return self.render(filename=filename)
