import abc

from oslo.config import cfg
CONF = cfg.CONF
CONF.import_opt('output_file', 'achus.renderer', group="renderer")


class Renderer(object):
    """ Base class for all renderers."""
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def render(self):
        """
        Generate a RAW report.

        This function should return a RAW report in the expected format
        for the renderer.
        """

    @abc.abstractmethod
    def render_to_file(self, filename=CONF.renderer.output_file):
        """Generate a report and write it into a file."""
