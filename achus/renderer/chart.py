import pygal

from oslo.config import cfg

from achus import exception
import achus.renderer.base

CONF = cfg.CONF
CONF.import_opt('output_file', 'achus.renderer', group="renderer")


class Chart(achus.renderer.base.Renderer):
    """Generates a chart report using PyGal."""

    def __init__(self, data, chart_title='', chart_type='pie', filename=None):
        """
        Expects a dictionary with k,v pairs to be plotted.

        If filename is defined, it assumes that the chart has to
        renderized in this file.
        """
        chart_types = {
            "pie": pygal.Pie(),
            "horizontal_bar": pygal.HorizontalBar()
        }

        if chart_type not in chart_types:
            raise exception.UnknownChartType('Chart "%s" is unknown' %
                                             chart_type)


        self.chart = chart_types[chart_type]
        self.chart.title = chart_title
        for k, v in data.iteritems():
            self.chart.add(k, v)

    def render(self):
        """ Renders to a pygal's chart. """
        return self.chart.render()

    def render_to_file(self, filename=CONF.renderer.output_file):
        self.chart.render_to_file(filename=filename)
