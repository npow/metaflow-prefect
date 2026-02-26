"""Flow with @retry, @timeout, and @environment decorators — used in test suite."""
from metaflow import FlowSpec, environment, retry, step, timeout


class DecoratorFlow(FlowSpec):
    """A flow whose steps carry @retry, @timeout, and @environment decorators."""

    @retry(times=2, minutes_between_retries=1)
    @timeout(seconds=300)
    @environment(vars={"MY_VAR": "hello", "OTHER": "world"})
    @step
    def start(self):
        self.value = 1
        self.next(self.end)

    @timeout(minutes=5)
    @step
    def end(self):
        pass


if __name__ == "__main__":
    DecoratorFlow()
