import pygal


class Chart:
    """
    Generates charts using PyGal.
    """
    def __init__(self, d, title='', type="pie", filename=None):
        """
        Expects a dictionary with k,v pairs to be plotted.

        If filename is defined, it assumes that the chart has to
        renderized in this file.
        """
        self.d = d
        self.title = title
        self.type = type
        self.filename = filename

    def render(self):
        """
        Renders to a pygal's chart.
        """
        chart_types = {
            "pie": pygal.Pie(),
            "horizontal_bar": pygal.HorizontalBar()
        }
        chart = chart_types[self.type]
        chart.title = self.title
        for k, v in self.d.iteritems():
            chart.add(k, v)
        if self.filename:
            chart.render_to_file(filename=self.filename)
        else:
            chart.render()
