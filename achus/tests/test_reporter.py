import copy
import StringIO

import mock
from oslo.config import cfg
import yaml

from achus import exception
import achus.renderer.chart
import achus.renderer.pdf
from achus import reporter
from achus import test
from achus.tests import fixtures
import achus.tests.test_reporter

CONF = cfg.CONF


class ReporterTest(test.TestCase):
    def setUp(self):
        super(ReporterTest, self).setUp()

        self.report_def = copy.deepcopy(fixtures.metric)

    def _mock_open_and_assert(self, yaml, assert_method, *args, **kwargs):
        y = StringIO.StringIO(yaml)
        with mock.patch('__builtin__.open') as my_mock:
            my_mock.return_value.__enter__ = lambda x: y
            my_mock.return_value.__exit__ = mock.Mock()
            assert_method(*args, **kwargs)

        my_mock.assert_called_once()

    @mock.patch.object(reporter.Report, "_report_from_yaml")
    def test_load_pdf_reporter(self, mock_yaml):
        CONF.renderer.renderer_class = "achus.renderer.pdf.PDFChart"
        rep = reporter.Report()
        self.assertIsInstance(rep.renderer,
                              achus.renderer.pdf.PDFChart)

    @mock.patch.object(reporter.Report, "_report_from_yaml")
    def test_load_chart_reporter(self, mock_yaml):
        CONF.renderer.renderer_class = "achus.renderer.chart.Chart"
        rep = achus.reporter.Report()
        self.assertIsInstance(rep.renderer,
                              achus.renderer.chart.Chart)

    @mock.patch.object(reporter.Report, "_report_from_yaml")
    def test_generate_calls_render_to_file(self, mock_yaml):
        rep = reporter.Report()
        with mock.patch.object(rep.renderer, 'render_to_file') as mock_method:
            rep.generate()
        mock_method.assert_called()

    @mock.patch.object(reporter.Report, "_report_from_yaml")
    def test_get_unknown_collector(self, mock_yaml):
        rep = reporter.Report()
        rep.metric = {"foometric": {"collector": "FooCollector"}}
        self.assertRaises(exception.CollectorNotFound,
                          rep._get_collectors)

    @mock.patch.object(reporter.Report, "_report_from_yaml")
    def test_get_collectors(self, mock_yaml):
        class FakeCollector(object):
            pass
        rep = reporter.Report()
        rep.available_collectors = [FakeCollector]
        rep.metric = {"foometric": {"collector": "FakeCollector"}}

        self.assertIn("FakeCollector", rep._get_collectors())

    def test_load_yaml_no_aggregate(self):
        del self.report_def["aggregate"]
        y = yaml.safe_dump(self.report_def)
        self._mock_open_and_assert(y,
                                   self.assertRaisesRegexp,
                                   exception.InvalidReportDefinition,
                                   "No 'aggregate' section",
                                   reporter.Report)

    def test_load_yaml_no_metric(self):
        del self.report_def["metric"]
        y = yaml.safe_dump(self.report_def)
        self._mock_open_and_assert(y,
                                   self.assertRaisesRegexp,
                                   exception.InvalidReportDefinition,
                                   "No 'metric' section",
                                   reporter.Report)

    def test_load_yaml_metric_with_missing_aggregate(self):
        del self.report_def["metric"]["foo"]["aggregate"]
        y = yaml.safe_dump(self.report_def)
        self._mock_open_and_assert(y,
                                   self.assertRaisesRegexp,
                                   exception.InvalidReportDefinition,
                                   "Missing 'aggregate'.*metric 'foo'$",
                                   reporter.Report)

    def test_load_yaml_metric_with_missing_metric(self):
        del self.report_def["metric"]["foo"]["metric"]
        y = yaml.safe_dump(self.report_def)
        self._mock_open_and_assert(y,
                                   self.assertRaisesRegexp,
                                   exception.InvalidReportDefinition,
                                   "Missing 'metric'.*metric 'foo'$",
                                   reporter.Report)

    def test_load_yaml_metric_with_missing_collector(self):
        del self.report_def["metric"]["foo"]["collector"]
        y = yaml.safe_dump(self.report_def)
        self._mock_open_and_assert(y,
                                   self.assertRaisesRegexp,
                                   exception.InvalidReportDefinition,
                                   "Missing 'collector'.*metric 'foo'$",
                                   reporter.Report)

    def test_load_yaml_metric_without_aggregate(self):
        del self.report_def["aggregate"]["foo"]
        y = yaml.safe_dump(self.report_def)
        self._mock_open_and_assert(y,
                                   self.assertRaisesRegexp,
                                   exception.InvalidReportDefinition,
                                   "Aggregate 'foo'.*not found$",
                                   reporter.Report)

    def test_load_yaml_metric_with_several_aggregates(self):
        self.report_def["metric"]["foo"]["aggregate"] = ["foo", "bar"]
        y = yaml.safe_dump(self.report_def)
        self._mock_open_and_assert(y,
                                   self.assertRaisesRegexp,
                                   exception.InvalidReportDefinition,
                                   "More than",
                                   reporter.Report)

    def test_load_good_metric_with_several_aggregates(self):
        y = yaml.safe_dump(self.report_def)
        self._mock_open_and_assert(y,
                                   self.assertIsInstance,
                                   reporter.Report(),
                                   achus.reporter.Report)
