import os
from typing import List, Callable

from luigi import Task
from luigi.util import inherits
from reclada.coordinator.base import S3Target, K8sTask, DocumentTask, SimpleDominoTask

from .initial import UploadDocument


class ParserMixin:
    input: Callable[[], S3Target]
    clone: Callable[..., Task]
    run_id: str

    @property
    def command(self) -> List[str]:
        from_s3 = self.input().path
        _, ext = os.path.splitext(from_s3)
        to_s3 = self.output().path
        local_src_path = f"/tmp/input{ext}"
        local_dest_path = "/tmp/tables.json"

        return [
            "reclada-run.sh",
            "--download", from_s3, local_src_path,
            "--upload", local_dest_path, to_s3,
            "reclada-csv-parser", local_src_path, local_dest_path,
        ]

    def requires(self):
        return self.clone(UploadDocument)

    def output(self):
        return S3Target(f"results/{self.run_id}/tables.json")


@inherits(DocumentTask)
class K8sParser(ParserMixin, K8sTask):
    image = "parser"


@inherits(DocumentTask)
class DominoParser(ParserMixin, SimpleDominoTask):
    is_direct_command = False
    project = "reclada_parser"
