from oslo.config import cfg

import achus.utils

CONF = cfg.CONF

renderer_opts = [
    cfg.StrOpt('renderer_class',
               default='achus.renderer.pdf.PDFChart',
               help='The full class name of the '
               'renderer to use'),
    cfg.StrOpt('output_file',
               default='report.pdf',
               help='Report output file.'),
]

CONF.register_opts(renderer_opts, group="renderer")

def Renderer():
    import_class = achus.utils.import_class
    cls = import_class(CONF.renderer.renderer_class)
    return cls()
