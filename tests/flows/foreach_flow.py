"""Foreach Metaflow flow: start → foreach_step → process_item → join_step → end."""
from metaflow import FlowSpec, step


class ForeachFlow(FlowSpec):
    """A flow with a foreach fan-out and join, for testing dynamic parallelism."""

    @step
    def start(self):
        self.items = [1, 2, 3]
        self.next(self.foreach_step, foreach="items")

    @step
    def foreach_step(self):
        self.item_result = self.input * 10
        self.next(self.join_step)

    @step
    def join_step(self, inputs):
        self.results = [i.item_result for i in inputs]
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ForeachFlow()
