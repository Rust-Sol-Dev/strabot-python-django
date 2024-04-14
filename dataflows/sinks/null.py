from bytewax.outputs import StatelessSinkPartition, DynamicSink


class NullSinkPartition(StatelessSinkPartition):
    def write_batch(self, items: list) -> None:
        pass


class NullSink(DynamicSink):
    def build(self, step_id: str, worker_index: int, worker_count: int) -> NullSinkPartition:
        return NullSinkPartition()
