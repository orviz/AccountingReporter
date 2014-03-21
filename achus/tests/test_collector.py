from achus import collector
from achus import exception
from achus import test

ALL_COLLECTORS = ['GECollector']


class CollectorTest(test.TestCase):
    def setUp(self):
        super(CollectorTest, self).setUp()

        self.collector = collector.BaseCollector()

    def test_base_collector_get_cpu_time_not_implemented(self):
        self.assertRaises(NotImplementedError,
                          self.collector.get_cpu_time)

    def test_base_collector_get_efficiencynot_implemented(self):
        self.assertRaises(NotImplementedError,
                          self.collector.get_efficiency)

    def test_expand_wilcards_in(self):
        value_result_map = (
            (
                [],
                {}
            ),
            (
                ["foo", "bar", "baz"],
                {'IN': {'baz', 'foo', 'bar'}}
            ),
            (
                ["foo", "bar", "foo", "baz", "bar", "baz"],
                {'IN': {'baz', 'foo', 'bar'}}
            ),
            (
                ["*"],
                {'IN': {'*'}}
            ),
            (
                ["!foo", "bar", "baz"],
                {'NOT IN': {'foo'}, 'IN': {'baz', 'bar'}}
            ),
            (
                ["foo*", "*bar", "b*z"],
                {'CONTAINS': {'foo*', '*bar', 'b*z'}}
            ),
            (
                ["!foo*", "!*bar", "!b*z"],
                {'NOT CONTAINS': {'foo*', '*bar', 'b*z'}}
            ),
            (
                ["!*"],
                {'NOT CONTAINS': {'*'}}
            ),
            (
                ["foo", "!bar", "*baz", "!bazonk*"],
                {'IN': {'foo'},
                 'NOT IN': {'bar'},
                 'CONTAINS': {'*baz'}, 'NOT CONTAINS': {'bazonk*'}}
            ),
        )
        for value, expected_result in value_result_map:
            self.assertEqual(expected_result,
                             self.collector._expand_wildcards(value))

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
                ["prj IN ('foo')"]
            ),
            (
                ["foo", "bar"],
                ["prj IN ('foo', 'bar')"]
            ),
            (
                ["foo", "!bar"],
                ["prj IN ('foo')", "prj NOT IN ('bar')"]
            ),
            (
                ["foo*", "*bar", "!baz*"],
                ["prj LIKE 'foo%'", "prj LIKE '%bar'", "prj NOT LIKE 'baz%'"],
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
