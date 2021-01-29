import os

from luigi import Task, Parameter

from .extractor import K8sExtractor


class All(Task):
    src = Parameter()
    run_type = Parameter(default="domino")

    def requires(self):
        if self.run_type == "k8s":
            return K8sExtractor(self.src, os.getenv())
