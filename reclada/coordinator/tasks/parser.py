import os
from typing import List, Callable

from luigi import Parameter
from reclada.coordinator.base import S3Target, K8sTask, DominoTask

from .initial import UploadDocument


class ParserMixin:
    src: str = Parameter()
    run_id: str = Parameter()
    input: Callable[[], S3Target]

    @property
    def command(self) -> List[str]:
        from_s3 = self.input().path
        _, ext = os.path.splitext(from_s3)
        to_s3 = self.output().path
        local_src_path = f"/tmp/input{ext}"
        local_dest_path = "/tmp/output/tables.json"

        return [
            "reclada-run.sh",
            "--download", from_s3, local_src_path,
            "--upload", local_dest_path, to_s3,
            "reclada-csv-parser", local_src_path, local_dest_path,
        ]

    def requires(self):
        return UploadDocument(self.src, self.run_id)

    def output(self):
        return S3Target(f"results/{self.run_id}/tables.json")


class K8sParser(K8sTask, ParserMixin):
    image = "reclada-converter"


class DominoParser(DominoTask, ParserMixin):
    is_direct_command = False
    project = "reclada-converter"
