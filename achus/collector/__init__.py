import functools
import logging

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
    def _expand_wildcards(self, value_list):
        """Expand wildcards.

        Analyses the contents of the list of matches defined, building a
        dict with four types of matches:
            IN          : exact positive match (and '*')
            NOT IN      : exact negative match
            CONTAINS    : partial positive match
            NOT CONTAINS: partial negative match
        where
            value_list: list of matches requested in the report definition.
        """
        result = {}
        do_proportion = False
        for v in set(value_list):
            if v.startswith("!"):
                v = v[1:]
                if not v:
                    # FIXME(aloga): check this message
                    raise exception.CollectorException("Cannot just negate "
                                                       "a match!")
                if "*" in v:
                    index = "NOT CONTAINS"
                else:
                    index = "NOT IN"
            elif v == "**":
                do_proportion = True
                continue
            else:
                if "*" in v and v != "*":
                    index = "CONTAINS"
                else:
                    index = "IN"

            if v in set().union(*result.values()):
                # FIXME(aloga): check this message
                raise exception.CollectorException("Duplicated rule for %s" %
                                                   v)
            result.setdefault(index, set()).add(v)
        return do_proportion, result

    def _format_wildcard(self, condition, value, query_type="sql"):
        """Format the wilcards into backend queries.

        Query language format of each of groups detected by the
        expand_wildcard function.
        """
        def _format(operator, param, match, match_replace=None):
            if match_replace:
                match = set([i.replace(*match_replace) for i in match])
            if operator in ("IN", "NOT IN"):
                match_str = "(%s)" % ", ".join(["'%s'" % i for i in match])
                ret = ["%s %s %s" % (param, operator, match_str)]
            else:
                ret = ["%s %s '%s'" % (param, operator, i) for i in match]
            return ret

        def _format_in(param, match):
            return _format("IN", param, match)

        def _format_not_in(param, match):
            return _format("NOT IN", param, match)

        def _format_like(param, match):
            return _format("LIKE", param, match, match_replace=('*', '%'))

        def _format_not_like(param, match):
            return _format("NOT LIKE", param, match, match_replace=('*', '%'))

        d = {
            "sql": {
                "IN": _format_in,
                "NOT IN": _format_not_in,
                "CONTAINS": _format_like,
                "NOT CONTAINS": _format_not_like,
            }
        }

        d_negate = {
            "sql": {
                "IN": "NOT IN",
                "NOT IN": "IN",
                "CONTAINS": "NOT CONTAINS",
                "NOT CONTAINS": "CONTAINS",
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
        do_proportion, d_condition = self._expand_wildcards(value)
        logger.debug("Wildcard expanding result: %s" % d_condition)

        r = []
        r_negate = []
        for mtype, mset in d_condition.iteritems():
            r.extend(d[query_type][mtype](condition, mset))
            if do_proportion:
                aux = d[query_type][d_negate[query_type][mtype]](condition,
                                                                 mset)
                r_negate.extend(aux)
        # Sort the results to get an expected output (unittest)
        r.sort()
        r_negate.sort()
        return r, r_negate

    def _format_conditions(self, **kw):
        raise NotImplementedError

    def get_cpu_time(self, **kw):
        raise NotImplementedError

    def get_efficiency(self, **kw):
        raise NotImplementedError

    def group(func):
        """Decorator to organize args and kwargs.

        Mandatory arguments will be passed as arguments while
        the optional ones as keyword arguments.
            'group_by': mandatory
            rest of kw: under 'conditions' kw.
        """
        @functools.wraps(func)
        def _group(self, metric, group_by, **kw):
            logger.debug("Received keyword arguments: %s" % kw)
            #l_args = []
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
