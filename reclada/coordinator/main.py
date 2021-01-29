import os

from luigi import Parameter, run, Task
from reclada.coordinator.tasks.extractor import K8sExtractor, DominoExtractor


# class Tables(SimpleDominoTask):
#     pdf = Parameter()
#
#     owner = configs.OWNER
#     project = "reclada_badgerdoc"
#
#     input_path = None
#     output_path = f"results/output.json/{configs.RUN_ID}.pdf/json_out.json"
#     step_id = "tables_extraction"
#
#     @property
#     def command(self):
#         my_input_path = f"/mnt/andrey_tikhonov/reclada_coordinator/{self.pdf}"
#         my_output_path = f"/mnt/andrey_tikhonov/reclada_coordinator/results/output.json"
#         return [f"python -m badgerdoc.pipeline full {shlex.quote(my_input_path)} {shlex.quote(my_output_path)}"]


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


if __name__ == "__main__":
    run()
