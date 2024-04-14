import csv

from bytewax.outputs import StatelessSinkPartition, DynamicSink


class CSVSinkPartition(StatelessSinkPartition):
    def __init__(self, file_path):
        self.file_path = file_path

    def write_batch(self, items: list) -> None:
        with open(self.file_path, 'a', newline='') as f:
            writer = csv.writer(f)
            for item in items:
                print(item)
                # line = str(item)
                # writer.writerow([line])


class CSVSink(DynamicSink):
    def __init__(self, file_path):
        self.file_path = file_path

    def build(self, step_id: str, worker_index: int, worker_count: int) -> CSVSinkPartition:
        return CSVSinkPartition(self.file_path)
