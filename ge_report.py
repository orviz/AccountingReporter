#!/usr/bin/env python

import contextlib
import MySQLdb as mdb
import pygal
import sys

from functools import wraps

class GEConnectorException(Exception):
    pass

class GEConnector:
    """
    Retrieves accounting data from a GridEngine system through SQL.
    """
    QUERIES_BY_PARAMETER = {
        "cpu_time": "SELECT ge_group, SUM(ge_cpu) FROM ge_jobs",
        "wall_clock": "SELECT ge_group, SUM(ge_ru_wallclock), ge_slots FROM ge_jobs",
    }

    GROUPS_TO_INFRASTRUCTURE = {
        "astro": "local",
        "biomed": "grid",
        "cms": "grid",
        "hidra": "local",
    }

    DEFAULT_CONDITIONS = [
        "ge_slots>=1",
        "ge_ru_wallclock>=0",
        "ge_submission_time<=ge_start_time",
        "ge_start_time<=ge_end_time",
    ]

    def __init__(self, dbhost, dbuser, dbpasswd, dbname):
        self.dbhost   = dbhost
        self.dbuser   = dbuser
        self.dbpasswd = dbpasswd
        self.dbname   = dbname

    def _format_query(self, *args):
        """
        Adds the default WHERE conditions as well as the group by statement.
        """
        return " ".join(["%s WHERE", " AND ".join([cond for cond in self.DEFAULT_CONDITIONS]),
                         "GROUP BY %s" ]) % args

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
        return [ (item[0], self._format_value(*item[1:])) for item in data ]

    def _group_by_infrastructure(self, data):
        """
        Organizes data (summing) by the infrastructure type.
        """
        d = {}
        for group, value in data.iteritems():
            try:
                d[self.GROUPS_TO_INFRASTRUCTURE[group]] += value
            except KeyError:
                d[self.GROUPS_TO_INFRASTRUCTURE[group]] = value

        return d

    def _group_by(self, d, group_by="group"):
        """
        Organizes the data according to the given group.
        """
        GROUP_FUNCS = {
            "infrastructure": self._group_by_infrastructure,
        }
        try:
            return GROUP_FUNCS[group_by](d)
        except KeyError:
            return d

    def query(self, parameter, group_by="ge_group"):
        """
        Performs a SQL query based on the parameter requested.
        """
        conn = mdb.connect(self.dbserver, self.dbuser, self.dbpasswd, self.dbname);
        with contextlib.closing(conn):
            curs = conn.cursor()
            curs.execute(self._format_query(self.QUERIES_BY_PARAMETER[parameter], group_by))
            res = self._format_result(curs.fetchall())
            return res

    def group(func):
        @wraps(func)
        def _group(self, *args, **kw):
            post_key = kw["group_by"]
            if kw["group_by"] == "infrastructure":
                kw["group_by"] = "ge_group"
            output = func(self, *args, **kw)
            return self._group_by(output, post_key)
        return _group

    @group
    def get_cpu_time(self, group_by="ge_group"):
        """
        Retrieves the CPU time grouped by 'ge_group'.
        """
        d = {}
        for item in self.query("cpu_time", group_by=group_by):
            index, values = item
            cpu_time = values[0]
            try:
                d[index] += cpu_time
            except KeyError:
                d[index] = cpu_time
        return d

    @group
    def get_wall_clock(self, group_by="ge_group"):
        """
        Retrieves the WALLCLOCK time grouped by 'ge_group'.
            Number of slots being used must be taken into account.
        """
        d = {}
        for item in self.query("wall_clock", group_by="ge_slots,%s" % group_by):
            index, values = item
            wall_clock, slots = values
            try:
                d[index] += wall_clock * slots
            except KeyError:
                d[index] = wall_clock * slots
        return d

    def compute_efficiency(self, group_by="ge_group"):
        """
        Retrieves the CPU time grouped by 'ge_group'.
        """
        d = {}
        d_cpu  = self.get_cpu_time()
        d_wall = self.get_wall_clock()
        if len(d_cpu.keys()) != len(d_wall.keys()):
            raise GEConnectorException("Cannot compute efficiency. Groups do not match!")
        for k,v in d_cpu.iteritems():
            try:
                d[k] = d_cpu[k]/d_wall[k]
            except ZeroDivisionError:
                d[k] = 0
        return d


def render_chart(d, title, filename, type="pie"):
    """
    Renders to 'filename' a pygal's pie chart.
        d: a dictionary with (k,v) data.
        title: title of the chart.
        filename: file location (full path) for the graph.
    """
    chart_types = {
        "pie": pygal.Pie(),
        "horizontal_bar": pygal.HorizontalBar()
    }
    chart = chart_types[type]
    chart.title = title
    for k,v in d.iteritems():
        chart.add(k,v)
    chart.render_to_png(filename=filename, dpi=72)

