import logging

from oslo.config import cfg
import yaml

from achus import collector
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
    """
    Main class, triggers reports based on the input given.
    """

    def __init__(self):
        """
        renderer: type of report.
        """

        self.collector_handler = collector.CollectorHandler()
        self.available_collectors = self.collector_handler.get_all_classes()

        self.renderer = achus.renderer.Renderer()

        report = self._report_from_yaml(CONF.report_definition)
        logger.debug("Loaded '%s' with content: %s"
                     % (CONF.report_definition, report))
        self.metric = report["metric"]
        self.aggregate = report["aggregate"]

    def _report_from_yaml(self, report_file):
        # FIXME(aloga): We must catch exceptions here
        with open(CONF.report_definition, "rb") as f:
            return yaml.safe_load(f)

    def _get_collector_kwargs(self, d):
        """
        Fills the keyword argumnents that will be passed to the
        collector method.
        """
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
            raise exception.CollectorNotFound(msg)

        return good_collectors

    def collect(self):
        """
        Gathers metric data.
        """
        collectors = self._get_collectors()
        for title, conf in self.metric.iteritems():
            logger.info("Gathering data from metric '%s'" % title)

            collector_name = conf["collector"]
            metric_name = conf["metric"]

            self.conn = collectors[collector_name]()
            logger.debug("(Collector: %s, Metric: %s)"
                         % (collector_name, metric_name))

            try:
                group_by_list = self.aggregate[conf["aggregate"]].keys() or []
                logger.debug("Aggregate's group_by parameters: %s" %
                             group_by_list)
                # FIXME (orviz) multiple group_by in an aggregate definition
                # must be supported
                if len(group_by_list) != 1:
                    msg = ("You must define one and only one 'group_by' "
                           "(project, group) parameter")
                    raise achus.exception.AggregateException(msg)
            # FIXME (orviz) same here but in the metric definition
            except TypeError:
                msg = "You must define one and only one aggregate per metric"
                raise achus.exception.AggregateException(msg)
            except KeyError:
                msg = "Could not find '%s' aggregate definition"
                raise achus.exception.AggregateException(msg %
                                                         conf["aggregate"])

            for group_by in group_by_list:
                # Add group_by to the condition list
                d = {group_by: self.aggregate[conf["aggregate"]][group_by]}
                conf.update(d)
                kwargs = self._get_collector_kwargs(conf)
                logger.debug("Passing kwargs to the collector: %s"
                             % kwargs)
                metric = self.conn.get(conf["metric"], group_by, **kwargs)
                logger.debug("Result from collector: '%s'" % metric)

            self.renderer.append_metric(title, metric, conf)

    def generate(self):
        """
        Triggers the report rendering.
        """
        self.renderer.render_to_file()
