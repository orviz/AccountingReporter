import logging
import StringIO

import cairosvg
from oslo.config import cfg
import PyPDF2

import achus.renderer.chart

CONF = cfg.CONF
CONF.import_opt('output_file', 'achus.renderer', group="renderer")

import achus.renderer.base

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class PDFChart(achus.renderer.base.Renderer):
    """Generates PDF report containing charts.

    The metrics added using the append_metric method
    will be rendered into a PDF report containing charts.
    """

    def __init__(self):
        super(PDFChart, self).__init__()
        self.chart = achus.renderer.chart.Chart()

    def append_metric(self, title, metric, metric_definition):
        self.chart.append_metric(title, metric, metric_definition)

    def _generate_pdf(self):
        pdf_charts = []
        for chart in self.chart.render():
            pdf_charts.append(cairosvg.svg2pdf(bytestring=chart))

        output = PyPDF2.PdfFileWriter()
        for pdf in pdf_charts:
            input1 = PyPDF2.PdfFileReader(StringIO.StringIO(pdf))
            output.addPage(input1.getPage(0))

        return output

    def render(self):
        """Generates the PDF report.

        This method will render each of the metrics that have been added
        into charts, that will be then joined into a PDF file.

        If filename is set the PDF file will be written into that file.
        If filename is not set, a string containing the file contents will
        be returned.
        """

        output = self._generate_pdf()
        output_stream = StringIO.StringIO()
        output.write(output_stream)
        yield output_stream.getvalue()

    def render_to_file(self, filename=CONF.renderer.output_file):
        """Write the PDF report into filename."""
        output = self._generate_pdf()
        output_stream = file(filename, "wb")
        output.write(output_stream)
        output_stream.close()
        logger.debug("Result PDF created under '%s'" % filename)
