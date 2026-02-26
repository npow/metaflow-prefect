"""Branch/join Metaflow flow: start → split → [branch_a, branch_b] → join → end."""
from metaflow import FlowSpec, step


class BranchFlow(FlowSpec):
    """A flow with a static split and join, for testing branch topology."""

    @step
    def start(self):
        self.base = 10
        self.next(self.branch_a, self.branch_b)

    @step
    def branch_a(self):
        self.result_a = self.base + 1
        self.next(self.join)

    @step
    def branch_b(self):
        self.result_b = self.base + 2
        self.next(self.join)

    @step
    def join(self, inputs):
        self.result = inputs.branch_a.result_a + inputs.branch_b.result_b
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    BranchFlow()
