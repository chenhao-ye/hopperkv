import logging
from dataclasses import dataclass
from typing import List

from hdrh.histogram import HdrHistogram


class LatencyHistMgr:
    @dataclass
    class LatencyHist:
        epoch: int
        hist: HdrHistogram

        def refresh(self, new_epoch: int):
            assert self.hist.total_count == 0
            self.epoch = new_epoch

        def flush(self, f_hist, epoch_duration: int):
            assert self.epoch >= 0
            hist_blob: str = self.hist.encode().decode("utf-8")
            f_hist.write(f"{self.epoch * epoch_duration:d},{hist_blob}\n")
            self.hist.reset()
            self.refresh(-1)

    def __init__(self, num_hist: int, f_hist, epoch_duration: int):
        self.hist_list: List[HdrHistogram] = [
            self.LatencyHist(-1, HdrHistogram(1, 1000_000, 3)) for _ in range(num_hist)
        ]
        self.max_epoch_flushed = -1
        self.f_hist = f_hist
        self.epoch_duration = epoch_duration
        self.refresh_epoch(0)

    def get_hist(self, epoch: int) -> LatencyHist:
        return self.hist_list[epoch % len(self.hist_list)]

    def record_latency(self, latency: float):
        self.curr_hist.hist.record_value(latency)

    def refresh_epoch(self, new_epoch: int):
        self.curr_hist = self.get_hist(new_epoch)
        if self.curr_hist.epoch >= 0:
            # flush all histograms until the one previously occupying curr_hist
            # usually only one, unless there is no refresh happen for a histogram
            self.flush_until(self.curr_hist.epoch)
        self.curr_hist.refresh(new_epoch)

    def flush_until(self, until_epoch: int):
        flush_begin = self.max_epoch_flushed + 1
        flush_end = until_epoch + 1
        self.max_epoch_flushed = until_epoch
        for flush_epoch in range(flush_begin, flush_end):
            latency_hist = self.get_hist(flush_epoch)
            if latency_hist.epoch < 0:  # nothing to flush
                continue
            latency_hist.flush(self.f_hist, self.epoch_duration)
            logging.debug(f"Flush latency histogram of epoch {flush_epoch}")


class EpochMgr:
    def __init__(
        self,
        lat_hist_mgr: LatencyHistMgr,
        f_data,
        epoch_duration: int,
        count: int | None,
        duration: int,
    ):
        self.lat_hist_mgr = lat_hist_mgr
        self.f_data = f_data
        self.epoch_duration = epoch_duration
        self.epoch = 0
        self.num_ops = 0
        self.num_ops_last_epoch = 0
        self.count = count
        self.duration = duration
        self.elapsed_last_reported = 0
        self.num_ops_last_reported = 0

    def add_ops(self, num_ops: int = 1):
        self.num_ops += num_ops

    def refresh(self, elapsed: float) -> bool:  # return is_done
        new_epoch = int(elapsed / self.epoch_duration)
        if new_epoch > self.epoch:
            self.flush(elapsed)
            self.num_ops_last_epoch = self.num_ops
            self.epoch = new_epoch
            if self.count is not None and self.num_ops >= self.count:
                return True
            if self.duration > 0 and elapsed >= self.duration:
                return True
            self.lat_hist_mgr.refresh_epoch(new_epoch)
        return False

    def flush(self, elapsed: float):
        tput = (self.num_ops - self.num_ops_last_epoch) / self.epoch_duration
        lh = self.lat_hist_mgr.curr_hist.hist
        # format:
        #   timestamp,elapsed,tput,
        #   lat_mean,lat_min,lat_max,
        #   p10,p20,p30,p40,p50,p60,p70,p80,p90,p99,p999
        # note "elapsed" in data.csv actually means the time window
        # started at the n-th second; do not confuse with the local
        # variable "elapsed" here
        self.f_data.write(
            f"{elapsed:.3f},{self.epoch * self.epoch_duration:d},{tput:g},"
            f"{lh.get_mean_value():.0f},"
            f"{lh.get_min_value():.0f},"
            f"{lh.get_max_value():.0f},"
            f"{lh.get_value_at_percentile(10)},"
            f"{lh.get_value_at_percentile(20)},"
            f"{lh.get_value_at_percentile(30)},"
            f"{lh.get_value_at_percentile(40)},"
            f"{lh.get_value_at_percentile(50)},"
            f"{lh.get_value_at_percentile(60)},"
            f"{lh.get_value_at_percentile(70)},"
            f"{lh.get_value_at_percentile(80)},"
            f"{lh.get_value_at_percentile(90)},"
            f"{lh.get_value_at_percentile(99)},"
            f"{lh.get_value_at_percentile(99.9)}\n"
        )

    def report_tput(self, elapsed: float):  # tput since last time reported
        tput = (self.num_ops - self.num_ops_last_reported) / (
            elapsed - self.elapsed_last_reported
        )
        self.elapsed_last_reported = elapsed
        self.num_ops_last_reported = self.num_ops
        return tput
