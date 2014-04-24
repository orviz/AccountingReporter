import logging
import sys

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class AchusException(Exception):
    msg_fmt = "An unknown exception occurred."

    def __init__(self, message=None, **kwargs):
        self.kwargs = kwargs

        if not message:
            try:
                message = self.msg_fmt % kwargs
            except Exception:
                exc_info = sys.exc_info()
                # kwargs doesn't match a variable in the message
                # log the issue and the kwargs
                logger.exception('Exception in string format operation')
                for name, value in kwargs.iteritems():
                    logger.error("%s: %s" % (name, value))
                raise exc_info[0], exc_info[1], exc_info[2]

        super(AchusException, self).__init__(message)


class CollectorException(AchusException):
    msg_fmt = "An unknown exception occurred in the collector."


class DuplicatedRule(CollectorException):
    msg_fmt = "Duplicated rule for %(rule)s."


class UnknownQueryLanguaje(CollectorException):
    msg_fmt = "Query lang %(lang)s not know."


class CannotComputeEfficiency(CollectorException):
    msg_fmt = "Cannot compute efficiency: Groups do not match!"


class UnknownChartType(AchusException):
    msg_fmt = "Unknown chart type '%(chart)s'."


class ClassNotFound(AchusException):
    msg_fmt = "Class %(class_name)s could not be found: %(exception)s."


class CollectorNotFound(AchusException):
    msg_fmt = "Cannot find collector '%(collector)s'."


class InvalidReportDefinition(AchusException):
    msg_fmt = "Invalid report definition."


class MissingSection(InvalidReportDefinition):
    msg_fmt = "Missing '%(section)s' section in YAML report."


class DuplicatedAggregate(InvalidReportDefinition):
    msg_fmt = "More than one aggregate defined for metric '%(metric)s'."


class MissingMetricFields(InvalidReportDefinition):
    msg_fmt = "Missing field '%(field)s' in metric '%(metric)s'."


class AggregateNotFound(InvalidReportDefinition):
    msg_fmt = "Cannot find aggregate '%(aggregate)s' for metric '%(metric)s'."


class MySQLBackendException(AchusException):
    pass
