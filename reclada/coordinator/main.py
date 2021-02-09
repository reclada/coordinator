import os
import sys

from luigi import Parameter, run, Task
from reclada.coordinator.tasks.extractor import K8sExtractor, DominoExtractor
from .base import create_domino


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
            domino = create_domino()
            run = domino.run_status(
                os.getenv("DOMINO_PROJECT_OWNER"),
                os.getenv("DOMINO_PROJECT_NAME"),
                run_id,
            )
            return DominoExtractor(src=self.src, run_id=run_id, run_prefix=f"{run.title}:{run.number}:")


def main():
    if not run():
        sys.exit(1)
