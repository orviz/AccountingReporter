from oslo.config import cfg


def parse_args(argv, default_config_files=None):
    cfg.CONF(argv[1:],
             project='achus',
             default_config_files=default_config_files)
