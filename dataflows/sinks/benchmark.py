import time
from datetime import timedelta

from bytewax.outputs import StatelessSinkPartition, DynamicSink


class BenchmarkSinkPartition(StatelessSinkPartition):
    rows = 0
    start_time = time.time()
    log_every = 1000
    log_counter = 0
    last_log_time = start_time

    def write_batch(self, items: list) -> None:
        self.rows += len(items)
        self.log_counter += len(items)
        if self.log_counter >= self.log_every:
            current_time = time.time()
            dur = timedelta(seconds=current_time - self.start_time)
            rps_avg = int(self.rows / dur.total_seconds())
            dur_chunk = timedelta(seconds=current_time - self.last_log_time)
            rps_chunk = int(self.log_counter / dur_chunk.total_seconds())
            print(f"Time: {dur}   Rows: {self.rows:,}   Rows/sec avg: {rps_avg:,}   Rows/sec chunk: {rps_chunk:,}")
            self.log_counter = 0
            self.last_log_time = current_time


class BenchmarkSink(DynamicSink):

    def build(self, step_id: str, worker_index: int, worker_count: int) -> BenchmarkSinkPartition:
        return BenchmarkSinkPartition()
