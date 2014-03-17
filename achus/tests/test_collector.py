from achus import collector
from achus import test

ALL_COLLECTORS = ['GECollector']


class CollectorTest(test.TestCase):
    def setUp(self):
        super(CollectorTest, self).setUp()

        self.collector = collector.BaseCollector

    def test_seconds_to_hours_conversion(self):
        self.assertEqual(1, self.collector()._to_hours(3600))

    def test_base_collector_get_cpu_time_not_implemented(self):
        self.assertRaises(NotImplementedError,
                          self.collector().get_cpu_time)

    def test_base_collector_get_efficiencynot_implemented(self):
        self.assertRaises(NotImplementedError,
                          self.collector().get_efficiency)


class CollectorHandlerTest(test.TestCase):
    def setUp(self):
        super(CollectorHandlerTest, self).setUp()

        self.collectorhandler = collector.CollectorHandler

    def test_get_all_all_collectors(self):
        ch = self.collectorhandler()
        aux = [i.__name__ for i in ch.get_all_classes()]
        self.assertEqual(ALL_COLLECTORS, aux)
