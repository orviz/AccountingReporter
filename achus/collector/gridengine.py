import contextlib
import logging

import MySQLdb as mdb
from oslo.config import cfg

from achus.collector import base
from achus import exception

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
    cfg.StrOpt('group_by',
               default='group',
               help='Define result ordering.'),
]

CONF = cfg.CONF
CONF.register_opts(opts, group="gecollector")


class GECollector(base.Collector):
    """
    Retrieves accounting data from a GridEngine system through SQL.
    """
    DEFAULT_CONDITIONS = [
        "ge_slots>=1",
        "ge_ru_wallclock>=0",
        "ge_submission_time<=ge_start_time",
        "ge_start_time<=ge_end_time",
    ]

    FIELD_MAPPING = {
        "wall_clock": "ge_ru_wallclock",
        "cpu_time"  : "ge_cpu",
        "start_time": "ge_start_time",
        "end_time"  : "ge_end_time",
        "group"     : "ge_group",
        "project"   : "ge_project",
    }

    def _format_value(self, *args):
        """
        Transforms 'v' value to float needed to render the graph.
        """
        import decimal
        l = []
        for arg in args:
            if type(arg) in [decimal.Decimal, long, int, float]:
                arg = float(arg)
            l.append(arg)
        return l

    def _format_result(self, data):
        """
        Aggregates the tuples retrieved by:
            t[0], t[1:],
        converting the numeric values to float. Finally, returns a list
        of these tuples.

        Note: cannot convert to dictionary since it can happen that the
              same index (t[0]) appears multiple times.
        """
        return [(item[0], self._format_value(*item[1:])) for item in data]

    def _format_conditions(self, **kw):
        """
        Adds the given conditions (default+requested) to the SQL query.
        """
        CONDITION_OPERATORS = {
            "ge_start_time": ">=",
            "ge_end_time": "<=",
        }
        condition_list = [cond for cond in self.DEFAULT_CONDITIONS]
        for k,v in kw.iteritems():
            aux = (k, CONDITION_OPERATORS[k], "'%s'" % v)
            condition_list.extend([" ".join(aux)])
        if condition_list:
            return " ".join(["WHERE", " AND ".join(condition_list)]) 

    def query(self, parameter, group_by, conditions=None):
        """
        Performs a SQL query based on the parameter requested.
            'group_by': in case of multiple group, the order of this string
                        is important for the rest of the code flow. The first
                        element must always be (group, project) and then the
                        rest (e.g. "ge_group,ge_slots")
        """
        conn = mdb.connect(CONF.gecollector.host,
                           CONF.gecollector.user,
                           CONF.gecollector.password,
                           CONF.gecollector.dbname,
                           CONF.gecollector.port)

        with contextlib.closing(conn):
            curs = conn.cursor()
            cmd = "SELECT %s,SUM(%s) FROM ge_jobs %s GROUP BY %s" % (group_by, self.FIELD_MAPPING[parameter], self._format_conditions(**conditions), group_by)
            logger.debug("MySQL command: `%s`" % cmd)
            curs.execute(cmd)
            res = self._format_result(curs.fetchall())
            return res

    def get_cpu_time(self, group_by, conditions=None):
        """
        Computes the CPU time grouped by 'ge_group' in hours.
            conditions: extra conditions to be added to the SQL query.
        """
        d = {}
        for item in self.query("cpu_time",
                               group_by=group_by,
                               conditions=conditions):
            index, values = item
            cpu_time = self._to_hours(values[0])
            try:
                d[index] += cpu_time
            except KeyError:
                d[index] = cpu_time
        return d

    def get_wall_clock(self, group_by, conditions=None):
        """
        Retrieves the WALLCLOCK time grouped by 'ge_group' in hours.
            Number of slots being used must be taken into account.
            conditions: extra conditions to be added to the SQL query.
        """
        d = {}
        for item in self.query("wall_clock", "%s,ge_slots" % group_by,
                               conditions=conditions):
            index, values = item
            slots, wall_clock = values
            wall_clock = self._to_hours(wall_clock)
            try:
                d[index] += wall_clock * slots
            except KeyError:
                d[index] = wall_clock * slots
        return d

    def get_efficiency(self, group_by, conditions=None):
        """
        Retrieves the CPU time grouped by 'ge_group'.
            conditions: extra conditions to be added to the SQL query.
        """
        d = {}
        d_cpu = self.get_cpu_time(group_by, conditions=conditions)
        d_wall = self.get_wall_clock(group_by, conditions=conditions)
        if len(d_cpu.keys()) != len(d_wall.keys()):
            raise exception.ConnectorException("Cannot compute efficiency. "
                                               "Groups do not match!")
        for k, v in d_cpu.iteritems():
            try:
                d[k] = round(((d_cpu[k]/d_wall[k])*100), 2)
            except ZeroDivisionError:
                d[k] = 0
        return d
