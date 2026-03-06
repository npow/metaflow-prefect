"""3-level nested foreach flow for testing arbitrary nesting depth."""

from metaflow import FlowSpec, step


class TripleForeachFlow(FlowSpec):
    """Three levels of nested foreach: outer → middle → inner."""

    @step
    def start(self):
        self.groups = ["a", "b"]
        self.next(self.outer_step, foreach="groups")

    @step
    def outer_step(self):
        self.sub_groups = [1, 2]
        self.next(self.middle_step, foreach="sub_groups")

    @step
    def middle_step(self):
        self.items = [10, 20, 30]
        self.next(self.inner_step, foreach="items")

    @step
    def inner_step(self):
        self.result = self.input * 2
        self.next(self.inner_join)

    @step
    def inner_join(self, inputs):
        self.results = [i.result for i in inputs]
        self.next(self.middle_join)

    @step
    def middle_join(self, inputs):
        self.all_results = [r for i in inputs for r in i.results]
        self.next(self.outer_join)

    @step
    def outer_join(self, inputs):
        self.final = [r for i in inputs for r in i.all_results]
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    TripleForeachFlow()
