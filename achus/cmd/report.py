import sys

from oslo.config import cfg

import achus.reporter
import achus.config

CONF = cfg.CONF


def main():
    achus.config.parse_args(sys.argv)
    report = achus.reporter.Report()
    report.collect()
    report.generate()


if __name__ == "__main__":
    main()
