import logging

from oslo.config import cfg
import yaml

import achus.collector
from achus import exception
import achus.renderer
import achus.renderer.chart
import achus.renderer.pdf

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

opts = [
    cfg.StrOpt('report_definition',
               default='etc/report.yaml',
               help='Report definition location.'),
]

CONF = cfg.CONF
CONF.register_opts(opts)


class Report(object):
    """Main class, triggers reports based on the input given."""

    def __init__(self):
        self.collector_handler = achus.collector.CollectorHandler()
        self.available_collectors = self.collector_handler.get_all_classes()

        self.renderer = achus.renderer.Renderer()

        report = self._report_from_yaml(CONF.report_definition)
        logger.debug("Loaded '%s' with content: %s"
                     % (CONF.report_definition, report))
        self.metric = report["metric"]
        self.aggregate = report["aggregate"]

    def _report_from_yaml(self, report_file):
        with open(CONF.report_definition, "rb") as f:
            yaml_data = yaml.safe_load(f)

        for i in ("aggregate", "metric"):
            if i not in yaml_data:
                raise exception.MissingSection(section=i)

        for name, metric in yaml_data["metric"].iteritems():
            for i in ("aggregate", "metric", "collector"):
                if i not in metric:
                    raise exception.MissingMetricFields(metric=name,
                                                        field=i)

            if not isinstance(metric["aggregate"], str):
                raise exception.DuplicatedAggregate(metric=name)

            if metric["aggregate"] not in yaml_data["aggregate"]:
                raise exception.AggregateNotFound(
                    metric=name, aggregate=metric["aggregate"])

        return yaml_data

    def _get_collector_kwargs(self, d):
        """Fills the keyword args for the collector method."""
        COLLECTOR_KWARGS = [
            "group", "project",
            "start_time", "end_time",
        ]
        d_kwargs = {}
        for k in d.keys():
            if k in COLLECTOR_KWARGS:
                d_kwargs[k] = d[k]
        return d_kwargs

    def _get_collectors(self):
        collectors = [i.get("collector") for _, i in self.metric.items()]

        cls_map = dict((cls.__name__, cls) for cls in
                       self.available_collectors)
        good_collectors = {}
        bad_collectors = []
        for name in collectors:
            if name not in cls_map:
                bad_collectors.append(name)
                continue
            good_collectors[name] = cls_map[name]

        if bad_collectors:
            msg = ", ".join(bad_collectors)
            raise exception.CollectorNotFound(collector=msg)

        return good_collectors

    def collect(self):
        """Gathers metric data."""
        collectors = self._get_collectors()
        for title, conf in self.metric.iteritems():
            logger.info("Gathering data from metric '%s'" % title)

            collector_name = conf["collector"]
            metric_name = conf["metric"]

            collector = collectors[collector_name]()
            logger.debug("(Collector: %s, Metric: %s)"
                         % (collector_name, metric_name))

            group_by_list = self.aggregate[conf["aggregate"]].keys() or []
            logger.debug("Aggregate's group_by parameters: %s" % group_by_list)

            for group_by in group_by_list:
                # Add group_by to the condition list
                d = {group_by: self.aggregate[conf["aggregate"]][group_by]}
                conf.update(d)
                kwargs = self._get_collector_kwargs(conf)
                logger.debug("Passing kwargs to the collector: %s"
                             % kwargs)
                metric = collector.get(conf["metric"], group_by, **kwargs)
                logger.debug("Result from collector: '%s'" % metric)

            self.renderer.append_metric(title, metric, conf)

    def generate(self):
        """Triggers the report rendering."""
        self.renderer.render_to_file()
