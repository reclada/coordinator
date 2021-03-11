import os
from typing import List, Callable

from luigi import Task
from luigi.util import inherits
from reclada.coordinator.base import S3Target, K8sTask, DocumentTask, SimpleDominoTask

from .converter import K8sConverter, DominoConverter
from .initial import UploadDocument


class BadgerdocMixin:
    input: Callable[[], S3Target]
    clone: Callable[..., Task]
    run_id: str
    src: str

    @property
    def command(self) -> List[str]:
        from_s3 = self.input().path
        _, ext = os.path.splitext(from_s3)
        to_s3 = self.output().path
        local_src_path = f"/tmp/input{ext}"
        local_dest_dir = "/tmp/output"
        local_dest_path = f"{local_dest_dir}/input{ext}/document.json"

        _, ext = os.path.splitext(self.src)
        if ext in (".xlsx",):
            return [
                "reclada-run.sh",
                "--download", from_s3, local_src_path,
                "--upload", local_dest_path, to_s3,
                "python3", "-m",
                "table_extractor.excel_run",
                local_src_path, local_dest_dir,
            ]
        else:
            return [
                "reclada-run.sh",
                "--download", from_s3, local_src_path,
                "--upload", local_dest_path, to_s3,
                "python3", "-m",
                "table_extractor.run", "run-sequentially",
                local_src_path,
                local_dest_dir,
                "--verbose=true",
                "--paddle_on=true",
            ]

    def requires(self):
        from_s3 = self.src
        _, ext = os.path.splitext(from_s3)
        if ext in (".pdf", ".xlsx"):
            return self.clone(UploadDocument)
        else:
            return self.requires_converter()

    def requires_converter(self) -> Task:
        raise NotImplementedError

    def output(self):
        return S3Target(f"results/{self.run_id}/tables.json")


@inherits(DocumentTask)
class K8sBadgerdoc(BadgerdocMixin, K8sTask):
    image = "badgerdoc"

    def requires_converter(self):
        return self.clone(K8sConverter)


@inherits(DocumentTask)
class DominoBadgerdoc(BadgerdocMixin, SimpleDominoTask):
    is_direct_command = False
    project = "reclada_badgerdoc"

    def requires_converter(self):
        return self.clone(DominoConverter)
