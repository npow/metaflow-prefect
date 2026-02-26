"""Simple linear Metaflow flow: start → process → end."""
from metaflow import FlowSpec, step


class SimpleFlow(FlowSpec):
    """A minimal two-step linear flow for testing."""

    @step
    def start(self):
        self.value = 42
        self.next(self.process)

    @step
    def process(self):
        self.result = self.value * 2
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    SimpleFlow()
