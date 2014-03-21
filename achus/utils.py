import sys
import traceback

from achus import exception


def to_hours(seconds):
    return round((float(seconds) / 3600), 2)


def import_class(import_str):
    """Returns a class from a string including module and class."""
    mod_str, _sep, class_str = import_str.rpartition('.')
    try:
        __import__(mod_str)
        return getattr(sys.modules[mod_str], class_str)
    except (ValueError, AttributeError):
        raise exception.ClassNotFound(
            'Class %s cannot be found (%s)' %
            (class_str, traceback.format_exception(*sys.exc_info()))
        )


def import_module(import_str):
    """Import a module."""
    __import__(import_str)
    return sys.modules[import_str]
