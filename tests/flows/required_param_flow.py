"""Flow with a required parameter (no default) — used to test required param handling."""
from metaflow import FlowSpec, Parameter, step


class RequiredParamFlow(FlowSpec):
    """A flow with one required and one optional parameter."""

    message = Parameter("message", type=str, required=True, help="Required message")
    count = Parameter("count", type=int, default=5, help="Optional count")

    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    RequiredParamFlow()
