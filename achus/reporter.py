import logging

from oslo.config import cfg
import yaml

import achus.collector.gridengine
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
        try:
            self.metagroup = report["metagroup"]
        except KeyError:
            self.metagroup = {}

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
            # GECollector
            "group_by", "start_time", "end_time",
        ]
        d_kwargs = {}
        for k in d.keys():
            if k in COLLECTOR_KWARGS:
                d_kwargs[k] = d[k]
        return d_kwargs

    def _group_by_metagroup(self, data, metagroup_list):
        """
        Organizes data (summing) by the infrastructure type.
        GROUP_FUNCS = {
            "infrastructure": self._group_by_infrastructure,
        }
        """
        d = {}
        for k,v in data.iteritems():
            for mg in metagroup_list:
                if k in self.metagroup[mg]:
                    try:
                        d[mg] += v
                    except KeyError:
                        d[mg] = v
        return d

    def collect(self):
        """
        Gathers metric data.
        """
        for title, conf in self.metric.iteritems():
            logger.info("Gathering data from metric '%s'" % title)

            self.conn = self.COLLECTORS[conf["collector"]]
            logger.debug("(Collector: %s, Metric: %s)"
                          % (conf["collector"], conf["metric"]))

            print ">>>>>>>>>>> CONF: ", conf 

            ## Metagroup check
            #metagroup_list = []
            #for mg in conf["group_by"]:
            #    if mg in self.metagroup.keys():
            #        metagroup_list.append(mg)
            #if metagroup_list:
            #    logger.info("Metagroup/s '%s' requested" % metagroup_list)
            #    conf["group_by"] = list(
            #            set(conf["group_by"]).difference(metagroup_list))

            ## Collector call
            #kwargs = self._get_collector_kwargs(conf)
            #logger.debug("Passing kwargs to the collector: %s"
            #              % kwargs)
            #metric = self.conn.get(conf["metric"], **kwargs)
            #logger.debug("Result from collector: '%s'" % metric)

            #if metagroup_list:
            #    metric = self._group_by_metagroup(metric, metagroup_list)
            #    logger.debug("Ordered by metagroup/s '%s': %s"
            #                  % (metagroup_list, metric))

            self.renderer.append_metric(title, metric, conf)

    def generate(self):
        """
        Triggers the report rendering.
        """
        self.renderer.render_to_file()
