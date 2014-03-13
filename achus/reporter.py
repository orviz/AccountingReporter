import logging

from oslo.config import cfg
import yaml

import achus.exception
import achus.collector.gridengine
import achus.renderer.chart
import achus.renderer.pdf

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

opts = [
    cfg.StrOpt('report_definition',
               default='etc/report.yaml',
               help='Report definition location.'),
    cfg.StrOpt('report_output',
               default='report.pdf',
               help='Report output file.'),
]

CONF = cfg.CONF
CONF.register_opts(opts)


class Report(object):
    """
    Main class, triggers reports based on the input given.
    """
    RENDERERS = {
        "pdf": achus.renderer.pdf.PDFRenderer,
    }
    COLLECTORS = {
        "ge": achus.collector.gridengine.GECollector()
    }

    def __init__(self, renderer):
        """
        renderer: type of report.
        """
        logging.debug("New report requested (TYPE <%s>)" % renderer)
        self.renderer = self.RENDERERS[renderer](CONF.report_output)

        report = self._report_from_yaml(CONF.report_definition)
        logging.debug("Loaded '%s' with content: %s"
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
            logging.info("Gathering data from metric '%s'" % title)

            self.conn = self.COLLECTORS[conf["collector"]]
            logging.debug("(Collector: %s, Metric: %s)"
                          % (conf["collector"], conf["metric"]))

            # Metagroup check
            metagroup_list = []
            for mg in conf["group_by"]:
                if mg in self.metagroup.keys():
                    metagroup_list.append(mg)
            if metagroup_list:
                logging.info("Metagroup/s '%s' requested" % metagroup_list)
                conf["group_by"] = list(
                        set(conf["group_by"]).difference(metagroup_list))

            # Collector call
            kwargs = self._get_collector_kwargs(conf)
            logging.debug("Passing kwargs to the collector: %s"
                          % kwargs)
            d = self.conn.get(conf["metric"], **kwargs)
            logging.debug("Result from collector: '%s'" % d)

            if metagroup_list:
                d = self._group_by_metagroup(d, metagroup_list)
                logging.debug("Ordered by metagroup/s '%s': %s"
                              % (metagroup_list, d))

            try:
                chart = achus.renderer.chart.Chart(d,
                                                   title,
                                                   type=conf["chart"])
                logging.debug("Metric will be displayed as a chart, type <%s>"
                              % conf["chart"])
                self.renderer.append(chart)
                logging.debug(("Chart appended to report's list of "
                               "objects-to-be-rendered"))
            except KeyError:
                logging.debug("Metric is not set to be displayed as "
                              "a chart. Note that no other format is "
                              "supported. Doing nothing.")

    def generate(self):
        """
        Triggers the report rendering.
        """
        self.renderer.render()
