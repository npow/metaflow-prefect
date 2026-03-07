"""MidForeachFlow: start (linear) → foreach_mid → body → join → end.

Tests that a foreach step that is NOT the start step is handled correctly
in both graph analysis and code generation.
"""
from metaflow import FlowSpec, step


class MidForeachFlow(FlowSpec):
    """A flow where the foreach fan-out happens at a non-start step."""

    @step
    def start(self):
        self.items = [1, 2, 3]
        self.next(self.foreach_mid)

    @step
    def foreach_mid(self):
        self.next(self.body, foreach="items")

    @step
    def body(self):
        self.result = self.input * 2
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = [i.result for i in inputs]
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    MidForeachFlow()
