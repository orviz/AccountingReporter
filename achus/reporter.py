import logging

from oslo.config import cfg
import yaml

import achus.collector.gridengine
import achus.exception
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
    COLLECTORS = {
        "ge": achus.collector.gridengine.GECollector()
    }

    def __init__(self):
        """
        renderer: type of report.
        """
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

    def collect(self):
        """
        Gathers metric data.
        """
        for title, conf in self.metric.iteritems():
            logger.info("Gathering data from metric '%s'" % title)

            self.conn = self.COLLECTORS[conf["collector"]]
            logger.debug("(Collector: %s, Metric: %s)"
                          % (conf["collector"], conf["metric"]))

            try:
                group_by_list = self.aggregate[conf["aggregate"]].keys() or [] 
                logger.debug("Aggregate's group_by parameters: %s" % group_by_list)
                # FIXME (orviz) multiple group_by in an aggregate definition 
                # must be supported
                if len(group_by_list) != 1:
                    raise achus.exception.AggregateException(("You must one and only" 
                        "one 'group_by' (project, group) parameter"))
            # FIXME (orviz) same here but in the metric definition
            except TypeError:
                raise achus.exception.AggregateException(("You must define one and only"
                        "one aggregate per metric"))
            except KeyError:
                raise achus.exception.AggregateException(("Could not find"
                            "'%s' aggregate definition" % conf["aggregate"]))

            for group_by in group_by_list:
                # Add group_by to the condition list 
                conf.update({ group_by: self.aggregate[conf["aggregate"]][group_by] })
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
