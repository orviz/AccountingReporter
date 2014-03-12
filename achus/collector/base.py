class Collector:
    # FIXME this method should not be inside any connector class
    def _to_hours(self, seconds):
        return round((float(seconds) / 3600), 2)

    def get_cpu_time(self, **kw):
        raise NotImplementedError

    def get_efficiency(self, **kw):
        raise NotImplementedError

    def get(self, metric, **kw):
        METRICS = {
            "cpu": self.get_cpu_time,
            "wallclock": self.get_wall_clock,
            "efficiency": self.get_efficiency,
        }
        return METRICS[metric](**kw)
