import logging

import pygal

from oslo.config import cfg

from achus import exception
import achus.renderer.base

CONF = cfg.CONF
CONF.import_opt('output_file', 'achus.renderer', group="renderer")

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class Chart(achus.renderer.base.Renderer):
    """Generates a chart report using PyGal.

    The metrics added using the append_metric method
    will be rendered into chart reports.
    """

    def __init__(self):
        super(Chart, self).__init__()

        self.chart_types = {
            "pie": pygal.Pie(),
            "horizontal_bar": pygal.HorizontalBar()
        }

    def append_metric(self, title, metric, metric_definition):
        if "chart" not in metric_definition:
            logging.debug("Not charting metric %s (no chart definition found)")
            return

        if metric_definition["chart"] not in self.chart_types:
            raise exception.UnknownChartType('Chart "%s" is unknown' %
                                             metric_definition["chart"])
        self.metrics.append((title, metric, metric_definition))

    def _generate_charts(self):
        for chart_title, metric, metric_definition in self.metrics:
            chart_type = metric_definition["chart"]
            chart = self.chart_types[chart_type]
            chart.title = chart_title
            for k, v in metric.iteritems():
                chart.add(k, v)
            yield chart

    def render(self):
        """Render the metrics into several SVG charts.

        This method is a generator that yields a chart for each of the
        metrics stored.
        """
        for chart in self._generate_charts():
            yield chart.render()

    def render_to_file(self, filename=CONF.renderer.output_file):
        for chart in self.render():
            chart.render_to_file(filename=filename)
