import os
import sys

from luigi import Parameter, run, Task
from reclada.coordinator.tasks.extractor import K8sExtractor, DominoExtractor


class All(Task):
    src = Parameter()
    run_id = Parameter(default="")
    run_type = Parameter(default="domino")

    def requires(self):
        run_id = self.run_id or os.getenv("DOMINO_RUN_ID")
        if not run_id:
            raise ValueError("No run id provided")
        if self.run_type == "k8s":
            return K8sExtractor(src=self.src, run_id=run_id)
        else:
            return DominoExtractor(src=self.src, run_id=run_id)


def main():
    if not run():
        sys.exit(1)
