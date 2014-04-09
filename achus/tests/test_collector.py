from achus import collector
from achus import exception
from achus import test
from achus.tests import fixtures

ALL_COLLECTORS = ['GECollector']


class CollectorTest(test.TestCase):
    def setUp(self):
        super(CollectorTest, self).setUp()

        self.valid_filters = fixtures.filter_map
        self.collector = collector.BaseCollector()

    def test_base_collector_get_cpu_time_not_implemented(self):
        self.assertRaises(NotImplementedError,
                          self.collector.get_cpu_time)

    def test_base_collector_get_efficiencynot_implemented(self):
        self.assertRaises(NotImplementedError,
                          self.collector.get_efficiency)

    #def test_expand_wilcards_in(self):
    #    for value, expected_result in self.valid_filters:
    #        self.assertEqual(expected_result,
    #                         self.collector._expand_wildcards(value))

    def test_expand_wilcards_raises(self):
        for value in (["!"], ["!foo", "foo"]):
            self.assertRaises(exception.CollectorException,
                              self.collector._expand_wildcards,
                              value)

    def test_format_wilcards_raises(self):
        self.assertRaises(exception.CollectorException,
                          self.collector._format_wildcard,
                          None,
                          None,
                          query_type="foo")

    def test_format_sql_wildcards(self):
        value_result_map = (
            (
                "foo",
                (["prj IN ('foo')"], [])
            ),
            (
                ["foo", "**"],
                (["prj IN ('foo')"], ["prj NOT IN ('foo')"])
            ),
            (
                ["foo", "bar"],
                (["prj IN ('foo', 'bar')"], [])
            ),
            (
                ["foo", "bar", "**"],
                (["prj IN ('foo', 'bar')"], ["prj NOT IN ('foo', 'bar')"])
            ),
            (
                ["foo", "!bar"],
                (["prj IN ('foo')", "prj NOT IN ('bar')"], [])
            ),
            (
                ["foo", "!bar", "**"],
                (["prj IN ('foo')", "prj NOT IN ('bar')"], ["prj IN ('bar')", "prj NOT IN ('foo')"])
            ),
            (
                ["foo*", "*bar", "!baz*"],
                (["prj LIKE '%bar'", "prj LIKE 'foo%'", "prj NOT LIKE 'baz%'"], [])
            ),
            (
                ["foo*", "*bar", "!baz*", "**"],
                (["prj LIKE '%bar'", "prj LIKE 'foo%'", "prj NOT LIKE 'baz%'"], ["prj LIKE 'baz%'", "prj NOT LIKE '%bar'", "prj NOT LIKE 'foo%'"])
            ),
            (
                "**",
                ([], [])
            ),
        )
        for value, expected_result in value_result_map:
            self.assertItemsEqual(expected_result,
                                  self.collector._format_wildcard("prj",
                                                                  value))


class CollectorHandlerTest(test.TestCase):
    def setUp(self):
        super(CollectorHandlerTest, self).setUp()

        self.collectorhandler = collector.CollectorHandler

    def test_get_all_all_collectors(self):
        ch = self.collectorhandler()
        aux = [i.__name__ for i in ch.get_all_classes()]
        self.assertEqual(ALL_COLLECTORS, aux)
