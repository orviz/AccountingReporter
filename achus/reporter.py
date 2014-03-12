import logging

import achus.exception
import achus.collector.gridengine
import achus.renderer.chart
import achus.renderer.pdf

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class Report:
    """
    Main class, triggers reports based on the input given.
    """
    RENDERERS = {
        "pdf": achus.renderer.pdf.PDFRenderer,
    }
    CONNECTORS = {
        "ge": achus.collector.gridengine.GECollector('localhost',
                                                     'root',
                                                     '******',
                                                     'ge_accounting'),
    }

    #FIXME 'metagroups' must be obtained from an generic config file 
    def __init__(self, renderer, task, metagroups, **kw):
        """
        renderer: type of report (supported: RENDERERS.keys())
        task: dictionary with the metrics to be gathered.
        """
        logging.debug("New report requested (TYPE <%s>)" % renderer)
        logging.debug("TASKs: %s ; KEYWORD ARGUMENTS: %s"
                      % (task, kw))
        self.renderer = self.RENDERERS[renderer](**kw)
        self.task = task
        self.metagroups = metagroups

    def _get_connector_kwargs(self, d):
        """
        Fills the keyword argumnents that will be passed to the
        connector method.
        """
        CONNECTOR_KWARGS = [
            # GEConnector
            "group_by", "start_time", "end_time",
        ]
        d_kwargs = {}
        for k in d.keys():
            if k in CONNECTOR_KWARGS:
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
            for metagroup in metagroup_list:
                if k in self.metagroups[metagroup]:
                    try:
                        d[metagroup] += v
                    except KeyError:
                        d[metagroup] = v
        return d

    def collect(self):
        """
        Gathers metric data.
        """
        for title, conf in self.task.iteritems():
            logging.info("Gathering data from metric '%s'" % title)

            self.conn = self.CONNECTORS[conf["connector"]]
            logging.debug("(Connector: %s, Metric: %s)"
                          % (conf["connector"], conf["metric"]))

            # metagroup
            metagroup = False
            for group in conf["group_by"]:
                if group in self.metagroups.keys():
                    metagroup = conf.pop("group_by")
                    logging.debug(("Metagroup/s '%s' requested. Not passing "
                                   "'group_by' key to the connector"
                                   % metagroup))
                    break
            # connector call
            d = self.conn.get(conf["metric"],
                              **self._get_connector_kwargs(conf))
            logging.debug("Result from connector: '%s'" % d)

            if metagroup:
                d = self._group_by_metagroup(d, metagroup)
                logging.debug("Ordered by metagroup/s '%s': %s" 
                              % (metagroup, d))

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
