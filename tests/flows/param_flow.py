"""Parametrised Metaflow flow for testing parameter passing."""
from metaflow import FlowSpec, Parameter, step


class ParamFlow(FlowSpec):
    """A flow that takes a string and an int parameter."""

    message = Parameter("message", default="hello", help="A message to echo.")
    count = Parameter("count", default=3, type=int, help="Number of repetitions.")

    @step
    def start(self):
        self.output = self.message * self.count
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ParamFlow()
