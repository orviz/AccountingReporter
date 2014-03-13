import logging

import cairosvg
from pyPdf import PdfFileWriter, PdfFileReader
from tempfile import NamedTemporaryFile

import achus.renderer.base

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class PDFChart(achus.renderer.base.Renderer):
    """ Generates PDF report containing charts."""

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
