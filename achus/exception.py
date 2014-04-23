class CollectorException(Exception):
    pass


class UnknownChartType(Exception):
    pass


class ClassNotFound(Exception):
    pass


class CollectorNotFound(Exception):
    pass


class InvalidReportDefinition(Exception):
    pass


class MissingMetricFields(InvalidReportDefinition):
    pass


class AggregateNotFound(InvalidReportDefinition):
    pass


class MySQLBackendException(Exception):
    pass
