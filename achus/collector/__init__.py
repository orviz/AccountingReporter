import logging
import functools

from oslo.config import cfg

from achus import exception
from achus import loadables

opts = [
    cfg.StrOpt('collector_group_by',
               default='group',
               help='Define result ordering.'),
]

CONF = cfg.CONF
CONF.register_opts(opts)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class BaseCollector(object):
    # FIXME this method should not be inside any connector class
    def _to_hours(self, seconds):
        return round((float(seconds) / 3600), 2)

    def _expand_wildcards(self, value_list, result={}):
        """
        Analyses recursively the contents of the list of matches defined,
        building a dict with four types of matches:
            IN          : exact positive match (and '*')
            NOT IN      : exact negative match
            CONTAINS    : partial positive match
            NOT CONTAINS: partial negative match
        where
            value_list: list of matches requested in the report definition.
            result: list of SQL language equivalents to 'value_list'.
        """
        v = value_list[0]
        if v.startswith("!"):
            v = v[1:]
            if v.find('*') == -1:
                index = "NOT IN"
            else:
                index = "NOT CONTAINS"
        elif v.find('*') != -1:
            if v == '*':
                index = "IN"
            else:
                index = "CONTAINS"
        else:
            index = "IN"

        try:
            result[index].add(v)
        except KeyError:
            result[index] = set([v])

        if len(value_list) == 1:
            return result
        else:
            return self._expand_wildcards(value_list[1:], result=result)

    def _format_wildcard(self, condition, value, query_type="sql"):
        """
        Query language format of each of groups detected by the
        expand_wildcard function.
        """
        d = {
            "sql": {
                "IN": lambda param, match_list: [
                    "%s IN %s" % (param, tuple(match_list))],
                "NOT IN": lambda param, match_list: [
                    "%s NOT IN %s" % (param, tuple(match_list))],
                "CONTAINS": lambda param, match_list: [
                    "%s LIKE '%s'" % (param, match.replace('*', '%'))
                    for match in match_list],
                "NOT CONTAINS": lambda param, match_list: [
                    "%s NOT LIKE '%s'" % (param, match.replace('*', '%'))
                    for match in match_list],
            }
        }

        try:
            d[query_type]
        except KeyError:
            raise exception.CollectorException("Query language '%s' not known"
                                               % query_type)

        # _expand_wildcards iterates over a list
        if not isinstance(value, list):
            value = [value]
        d_condition = self._expand_wildcards(value)
        logger.debug("Wildcard expanding result: %s" % d_condition)

        r = []
        for mtype, mset in d_condition.iteritems():
            r.extend(d[query_type][mtype](condition, mset))
        return r

    def _format_conditions(self, **kw):
        raise NotImplementedError

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
        @functools.wraps(func)
        def _group(self, metric, group_by, **kw):
            logger.debug("Received keyword arguments: %s" % kw)
            l_args = []
            d_kwargs = {}
            ## arguments
            #try:
            #    group_by = self.FIELD_MAPPING[kw.pop("group_by")]
            #except KeyError:
            #    group_by = self.FIELD_MAPPING[CONF.collector_group_by]
            #l_args.append(group_by)
            # keyword arguments
            d_kwargs["conditions"] = {}
            for k, v in kw.iteritems():
                try:
                    d_kwargs["conditions"].update({self.FIELD_MAPPING[k]: v})
                except KeyError:
                    logger.debug("Field '%s' not being considered" % k)
            #logger.debug("Resultant arguments: %s" % l_args)
            logger.debug("Resultant keyword arguments: %s" % d_kwargs)
            logger.debug("Calling decorated function '%s' (metric: %s)"
                         % (func.func_name, metric))
            output = func(self,
                          metric,
                          self.FIELD_MAPPING[group_by],
                          **d_kwargs)
            return output
        return _group

    @group
    def get(self, metric, group_by, **kw):
        METRICS = {
            "cpu": self.get_cpu_time,
            "wallclock": self.get_wall_clock,
            "efficiency": self.get_efficiency,
        }
        return METRICS[metric](group_by, **kw)


class CollectorHandler(loadables.BaseLoader):
    def __init__(self):
        super(CollectorHandler, self).__init__(BaseCollector)
