import contextlib
from functools import wraps
import logging

import MySQLdb as mdb

from achus.collector import base
import achus.exception

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class GECollector(base.Collector):
    """
    Retrieves accounting data from a GridEngine system through SQL.
    """
    QUERIES_BY_PARAMETER = {
        "cpu_time": "SELECT ge_group, SUM(ge_cpu) FROM ge_jobs",
        "wall_clock": ("SELECT ge_group, SUM(ge_ru_wallclock), "
                       "ge_slots FROM ge_jobs"),
    }

    DEFAULT_CONDITIONS = [
        "ge_slots>=1",
        "ge_ru_wallclock>=0",
        "ge_submission_time<=ge_start_time",
        "ge_start_time<=ge_end_time",
    ]

    FIELD_MAPPING = {
        "start_time": "ge_start_time",
        "end_time": "ge_end_time",
    }

    def __init__(self, dbserver, dbuser, dbpasswd, dbname):
        self.dbserver = dbserver
        self.dbuser = dbuser
        self.dbpasswd = dbpasswd
        self.dbname = dbname

    def _format_value(self, *args):
        """
        Transforms 'v' value to float needed to render the graph.
        """
        import decimal
        l = []
        for arg in args:
            if type(arg) in [decimal.Decimal, long, int, float]:
                l.append(float(arg))
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

    def _format_query(self, *args, **kw):
        """
        Adds the default WHERE conditions as well as the group by statement.
        """
        CONDITION_OPERATORS = {
            "ge_start_time": ">=",
            "ge_end_time": "<=",
        }
        condition_list = [cond for cond in self.DEFAULT_CONDITIONS]
        for k, v in kw.iteritems():
            aux = (k, CONDITION_OPERATORS[k], "'%s'" % v)
            condition_list.extend([" ".join(aux)])

        return " ".join(["%s WHERE", " AND ".join(condition_list),
                         "GROUP BY %s"]) % args

    def query(self, parameter, group_by="ge_group", conditions=None):
        """
        Performs a SQL query based on the parameter requested.
        """
        conn = mdb.connect(self.dbserver,
                           self.dbuser,
                           self.dbpasswd,
                           self.dbname)

        with contextlib.closing(conn):
            curs = conn.cursor()
            cmd = self._format_query(self.QUERIES_BY_PARAMETER[parameter],
                                     group_by, **conditions)
            logger.debug("MySQL command: `%s`" % cmd)
            curs.execute(cmd)
            res = self._format_result(curs.fetchall())
            return res

    def group(func):
        """
        Decorator method to organize the keyword arguments.
            'group_by': if other than 'group', compute it as
                        'group' and post-group the result as
                        the initial value (aka post_grouping).
            rest of kw: under 'conditions' kw.
        """
        @wraps(func)
        def _group(self, *args, **kw):
            logging.debug("Received keyword arguments: %s" % kw)
            d = {}
            post_grouping = "group"
            try:
                d["group_by"] = kw.pop("group_by")
            except KeyError:
                pass
            d["group_by"] = "ge_group"
            d["conditions"] = {}
            for k, v in kw.iteritems():
                try:
                    d["conditions"].update({self.FIELD_MAPPING[k]: v})
                except KeyError:
                    logging.debug("Field '%s' not being considered" % k)
            logging.debug("Resultant keyword arguments: %s" % d)
            logging.debug("Calling decorated function '%s'" % func.func_name)
            output = func(self, *args, **d)
            return output
        return _group

    @group
    def get_cpu_time(self, group_by="ge_group", conditions=None):
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

    @group
    def get_wall_clock(self, group_by="ge_group", conditions=None):
        """
        Retrieves the WALLCLOCK time grouped by 'ge_group' in hours.
            Number of slots being used must be taken into account.
            conditions: extra conditions to be added to the SQL query.
        """
        d = {}
        for item in self.query("wall_clock",
                               group_by="ge_slots,%s" % group_by,
                               conditions=conditions):
            index, values = item
            wall_clock, slots = values
            wall_clock = self._to_hours(wall_clock)
            try:
                d[index] += wall_clock * slots
            except KeyError:
                d[index] = wall_clock * slots
        return d

    def get_efficiency(self, group_by="ge_group", conditions=None):
        """
        Retrieves the CPU time grouped by 'ge_group'.
            conditions: extra conditions to be added to the SQL query.
        """
        d = {}
        d_cpu = self.get_cpu_time(group_by=group_by,
                                  conditions=conditions)
        d_wall = self.get_wall_clock(group_by=group_by, conditions=conditions)
        if len(d_cpu.keys()) != len(d_wall.keys()):
            raise achus.exception.ConnectorException(
                "Cannot compute efficiency. Groups do not match!")
        for k, v in d_cpu.iteritems():
            try:
                d[k] = round(((d_cpu[k]/d_wall[k])*100), 2)
            except ZeroDivisionError:
                d[k] = 0
        return d
