import types

import mock
import pygal
from oslo.config import cfg

from achus import exception
import achus.renderer
import achus.renderer.chart
import achus.renderer.pdf
from achus import test

CONF = cfg.CONF


class RendererTest(test.TestCase):
    def test_abc(self):
        self.assertRaises(TypeError,
                          achus.renderer.base.Renderer)

    def test_default_renderer(self):
        cls_ = achus.renderer.Renderer()
        cls_fqn = "%s.%s" % (cls_.__class__.__module__,
                             cls_.__class__.__name__)
        self.assertEqual(CONF.renderer.renderer_class,
                         cls_fqn)


class BaseRendererTest(object):
    def setUp(self):
        super(BaseRendererTest, self).setUp()

    def test_render_is_generator(self):
        self.assertIsInstance(self.renderer.render(),
                              types.GeneratorType)

    def test_render_file_is_none(self):
        self.assertIsNone(self.renderer.render_to_file())


class ChartRendererTest(test.TestCase, BaseRendererTest):
    chart_types = {
        "pie": pygal.Pie,
        "horizontal_bar": pygal.HorizontalBar
    }

    def setUp(self):
        super(ChartRendererTest, self).setUp()

        self.renderer = achus.renderer.chart.Chart()

    def test_chart_type_unknown(self):
        self.assertRaises(exception.UnknownChartType,
                          self.renderer.append_metric,
                          "foo",
                          {},
                          {"chart": "fake chart"})

    def test_no_chart_type(self):
        self.assertIsNone(self.renderer.append_metric("foo",
                                                      {},
                                                      {}))

    def test_append_works(self):
        self.assertIsNone(self.renderer.append_metric("bar",
                                                      {},
                                                      {"chart": "pie"}))
        self.assertIsNone(self.renderer.append_metric("foo",
                                                      {},
                                                      {}))
        self.assertIsNotNone(self.renderer.metrics)

    def test_known_charts_are_what_we_expect(self):
        for type_name, type_ in self.chart_types.iteritems():
            self.renderer.metrics = []
            self.renderer.append_metric(
                "test %s" % type_name,
                {"foo": 1, "bar": 2},
                {"chart": type_name})

            for c in self.renderer._generate_charts():
                self.assertIsInstance(c, self.chart_types[type_name])

    def test_known_charts_are_rendered(self):
        for type_name, type_ in self.chart_types.iteritems():
            self.renderer.append_metric(
                "test %s" % type_name,
                {"foo": 1, "bar": 2},
                {"chart": type_name})

        for c in self.renderer.render():
            self.assertTrue(c.startswith("<?xml version='1.0' "
                                         "encoding='utf-8'?>"))


class PDFChartRendererTest(test.TestCase, BaseRendererTest):
    def setUp(self):
        super(PDFChartRendererTest, self).setUp()

        self.renderer = achus.renderer.pdf.PDFChart()

    def test_append_metrics(self):
        title = "title"
        metric = {"foo": 1, "bar": 2}
        metric_def = {"chart": "pie"}
        with mock.patch.object(self.renderer.chart,
                               'append_metric') as mock_method:
            mock_method.return_value = None
            self.renderer.append_metric(title, metric, metric_def)
        mock_method.assert_called()

    def test_chart(self):
        title = "title"
        metric = {"foo": 1, "bar": 2}
        metric_def = {"chart": "pie"}
        self.renderer.append_metric(title, metric, metric_def)
        self.assertEqual('%PDF-1.3', self.renderer.render().next()[:8])
