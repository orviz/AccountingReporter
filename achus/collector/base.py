import logging

from functools import wraps
from oslo.config import cfg

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

opts = [
    cfg.StrOpt('collector_group_by',
               default='group',
               help='Define result ordering.'),
]

CONF = cfg.CONF
CONF.register_opts(opts)

class Collector(object):
    # (orviz) FIXME this method should not be inside any collector class
    def _to_hours(self, seconds):
        return round((float(seconds) / 3600), 2)

    def get_cpu_time(self, **kw):
        raise NotImplementedError

    def get_efficiency(self, **kw):
        raise NotImplementedError

    def group(func):
        """
        Decorator method to organize both the arguments and keyword 
        arguments. Mandatory arguments will be passed as arguments while
        the optional ones as keyword arguments. 
            'group_by': mandatory            
            rest of kw: under 'conditions' kw.
        """
        @wraps(func)
        def _group(self, metric, **kw):
            logger.debug("Received keyword arguments: %s" % kw)
            l_args = []; d_kwargs = {}
            # arguments
            try:
                group_by = self.FIELD_MAPPING[kw.pop("group_by")]
            except KeyError:
                group_by = self.FIELD_MAPPING[CONF.collector_group_by]
            l_args.append(group_by)
            # keyword arguments
            d_kwargs["conditions"] = {}
            for k, v in kw.iteritems():
                try:
                    d_kwargs["conditions"].update({self.FIELD_MAPPING[k]: v})
                except KeyError:
                    logger.debug("Field '%s' not being considered" % k)
            logger.debug("Resultant arguments: %s" % l_args)
            logger.debug("Resultant keyword arguments: %s" % d_kwargs)
            logger.debug("Calling decorated function '%s' (metric: %s)" 
                         % (func.func_name, metric))
            output = func(self, metric, *l_args, **d_kwargs)
            return output
        return _group

    @group
    def get(self, metric, *args, **kw):
        METRICS = {
            "cpu": self.get_cpu_time,
            "wallclock": self.get_wall_clock,
            "efficiency": self.get_efficiency,
        }
        return METRICS[metric](*args, **kw)
