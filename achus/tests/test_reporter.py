import fixtures
import mock
from oslo.config import cfg

from achus import exception
import achus.renderer.chart
import achus.renderer.pdf
from achus import reporter
from achus import test
import achus.tests.test_reporter

CONF = cfg.CONF


class ReporterTest(test.TestCase):
    def setUp(self):
        super(ReporterTest, self).setUp()

        self.reporter = reporter.Report

    def test_load_pdf_reporter(self):
        CONF.renderer.renderer_class = "achus.renderer.pdf.PDFChart"
        rep = self.reporter()
        self.assertIsInstance(rep.renderer,
                              achus.renderer.pdf.PDFChart)

    def test_load_chart_reporter(self):
        CONF.renderer.renderer_class = "achus.renderer.chart.Chart"
        rep = self.reporter()
        self.assertIsInstance(rep.renderer,
                              achus.renderer.chart.Chart)

    def test_generate_calls_render_to_file(self):
        rep = self.reporter()
        with mock.patch.object(rep.renderer, 'render_to_file') as mock_method:
            mock_method.return_value = None
            rep.generate()
        mock_method.assert_called()

    def test_get_unknown_collector(self):
        rep = self.reporter()
        rep.metric = {"foometric": {"collector": "FooCollector"}}
        self.assertRaises(exception.CollectorNotFound,
                          rep._get_collectors)

    def test_get_collectors(self):
        class FakeCollector(object):
            pass
        rep = self.reporter()
        rep.available_collectors = [FakeCollector]
        rep.metric = {"foometric": {"collector": "FakeCollector"}}

        self.assertIn("FakeCollector", rep._get_collectors())
