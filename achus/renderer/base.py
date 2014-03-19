import abc

from oslo.config import cfg
CONF = cfg.CONF
CONF.import_opt('output_file', 'achus.renderer', group="renderer")


class Renderer(object):
    """Base class for all renderers.

    A render accepts metric using the append_metric method. Those metrics
    will be then rendered into another format.
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.metrics = []

    @abc.abstractmethod
    def append_metric(self, title, metric, metric_definition):
        """Add a metric for being renderered."""

    @abc.abstractmethod
    def render(self):
        """Generate a RAW report.

        This function must be a generator that returns the RAW reports
        in the expected format for the renderer.
        """

    @abc.abstractmethod
    def render_to_file(self, filename=CONF.renderer.output_file):
        """Generate a report and write it into a file."""
