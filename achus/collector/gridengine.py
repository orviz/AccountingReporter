import contextlib
import logging

import MySQLdb as mdb
import _mysql_exceptions
from oslo.config import cfg

from achus import collector
from achus import exception
from achus import utils

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

opts = [
    cfg.StrOpt('host',
               default='localhost',
               help='MySQL host where GE accounting database is located.'),
    cfg.IntOpt('port',
               default=3306,
               help='Port number of MySQL host.'),
    cfg.StrOpt('user',
               default='ge_accounting',
               help='User to authenticate as.'),
    cfg.StrOpt('password',
               default='',
               help='Password to authenticate with.'),
    cfg.StrOpt('dbname',
               default='ge_accounting',
               help='Name of the accounting database.'),
]

CONF = cfg.CONF
CONF.register_opts(opts, group="gecollector")


class GECollector(collector.BaseCollector):
    """Retrieves accounting data from a GridEngine system through SQL."""
    DEFAULT_CONDITIONS = [
        "ge_slots>=1",
        "ge_ru_wallclock>=0",
        "ge_submission_time<=ge_start_time",
        "ge_start_time<=ge_end_time",
    ]

    FIELD_MAPPING = {
        "wall_clock": "ge_ru_wallclock",
        "cpu_time": "ge_cpu",
        "start_time": "ge_start_time",
        "end_time": "ge_end_time",
        "group": "ge_group",
        "project": "ge_project",
    }

    def _format_result(self, *args):
        """Transforms 'v' value to float needed to render the graph."""
        import decimal
        l = []
        for arg in args:
            l_sub = []
            for i in arg: 
                if type(i) in [decimal.Decimal, long, int, float]:
                    i = float(i)
                l_sub.append(i)
            l.append(l_sub)
        return l

    def _format_conditions(self, **kw):
        """Adds the given conditions (default+requested) to the SQL query."""
        CONDITION_OPERATORS = {
            "ge_start_time": ">=",
            "ge_end_time": "<=",
        }

        condition_list = [cond for cond in self.DEFAULT_CONDITIONS]
        condition_wildcard_list = []

        do_proportion = False
        for k, v in kw.iteritems():
            logger.debug("Analysing condition (%s, %s)" % (k, v))
            if k in CONDITION_OPERATORS.keys():
                logger.debug(("Condition '%s' not going through wilcard "
                              "expansion" % k))
                aux = "%s %s '%s'" % (k, CONDITION_OPERATORS[k], v)
            else:
                logger.debug(("Condition '%s' going through wildcard "
                              "expansion" % k))

                l, l_negate = self._format_wildcard(k, v, query_type="sql")
                if l:
                    aux = "".join(['(', " OR ".join(l), ')'])
                    condition_wildcard_list.append(aux)
                    logging.debug("Wildcard condition formatted to: %s" % aux)
                if l_negate:
                    aux_negate = "".join(['(', " OR ".join(l_negate), ')'])
                    condition_wildcard_list.append(aux_negate)
                    logging.debug("Negative wildcard condition formatted to: %s"
                                  % aux_negate)

        l = []
        if condition_wildcard_list:
            for condition_wildcard in condition_wildcard_list:
                l.append(" ".join(["WHERE", " AND ".join([condition_wildcard] + condition_list)]))
        else:
            l.append(" ".join(["WHERE", " AND ".join(condition_list)]))
        logger.debug(l)

        return l

    def query(self, parameter, group_by, conditions=None):
        """Performs a SQL query based on the parameter requested.

        'group_by': in case of multiple group, the order of this string
                    is important for the rest of the code flow. The first
                    element must always be (group, project) and then the
                    rest (e.g. "ge_group,ge_slots")
        """
        try:
            conn = mdb.connect(CONF.gecollector.host,
                               CONF.gecollector.user,
                               CONF.gecollector.password,
                               CONF.gecollector.dbname,
                               CONF.gecollector.port)
        except _mysql_exceptions.OperationalError, e:
            raise exception.MySQLBackendException(str(e))

        with contextlib.closing(conn):
            curs = conn.cursor() 
            l = self._format_conditions(**conditions)

            for c in l:
                # If list -> contains negation (aka proportion)
                if isinstance(c, list):
                    cond, cond_negate = c
                else:
                    cond, cond_negate = (c, '')

                cmd = ("SELECT %s, SUM(%s) FROM ge_jobs %s GROUP BY %s"
                       % (','.join(group_by),
                          self.FIELD_MAPPING[parameter],
                          cond,
                          ','.join(group_by)))
                logger.debug("MySQL command: `%s`" % cmd)
                curs.execute(cmd)
                res = self._format_result(*curs.fetchall())
                logger.debug("MySQL query (formatted) result: %s" % res)
            
                if cond_negate:
                    cmd = ("SELECT %s, SUM(%s) FROM ge_jobs %s"
                            % (','.join(group_by[1:]),
                              self.FIELD_MAPPING[parameter],
                              cond_negate))
                    if group_by[1:]:
                        cmd = ' '.join([' '.join([cmd, "GROUP BY "])]+group_by[1:])
                    logger.debug("Proportion MySQL command: `%s`" % cmd)
                    curs.execute(cmd)
                    leftover = self._format_result(*[("leftover",)+r for r in curs.fetchall()])
                    logger.debug("Proportion leftover: %s" % leftover)
                    res.extend(leftover)

            return res

    def get_cpu_time(self, group_by, conditions=None):
        """Computes the CPU time grouped by 'ge_group' in hours.

        conditions: extra conditions to be added to the SQL query.
        """
        d = {}
        for item in self.query("cpu_time",
                               [group_by],
                               conditions=conditions):
            index, values = item
            cpu_time = utils.to_hours(values)
            try:
                d[index] += cpu_time
            except KeyError:
                d[index] = cpu_time
        return d

    def get_wall_clock(self, group_by, conditions=None):
        """Retrieves the WALLCLOCK time grouped by 'ge_group' in hours.

        Number of slots being used must be taken into account.
        conditions: extra conditions to be added to the SQL query.
        """
        d = {}
        for item in self.query("wall_clock", 
                               [group_by, "ge_slots"],
                               conditions=conditions):
            index, slots, values = item
            wall_clock = utils.to_hours(values)
            try:
                d[index] += wall_clock * slots
            except KeyError:
                d[index] = wall_clock * slots
        return d

    def get_efficiency(self, group_by, conditions=None):
        """Retrieves the CPU time grouped by 'ge_group'.

        conditions: extra conditions to be added to the SQL query.
        """
        d = {}
        d_cpu = self.get_cpu_time(group_by,
                                  conditions=conditions)
        d_wall = self.get_wall_clock(group_by,
                                     conditions=conditions)
        if len(d_cpu.keys()) != len(d_wall.keys()):
            raise exception.CollectorException("Cannot compute efficiency. "
                                               "Groups do not match!")
        for k, v in d_cpu.iteritems():
            try:
                d[k] = round(((d_cpu[k]/d_wall[k])*100), 2)
            except ZeroDivisionError:
                d[k] = 0
        return d
