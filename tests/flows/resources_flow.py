"""Flow with @resources decorators — used to test resource extraction."""
from metaflow import FlowSpec, resources, step


class ResourcesFlow(FlowSpec):
    """A flow whose steps carry @resources decorators."""

    @resources(cpu=4, memory=8192)
    @step
    def start(self):
        self.next(self.end)

    @resources(gpu=1, memory=16384)
    @step
    def end(self):
        pass


if __name__ == "__main__":
    ResourcesFlow()
