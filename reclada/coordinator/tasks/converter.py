import os
from typing import List, Callable

from luigi import Task
from luigi.util import inherits
from reclada.coordinator.base import S3Target, K8sTask, DocumentTask, SimpleDominoTask

from .initial import UploadDocument


class ConverterMixin:
    input: Callable[[], S3Target]
    src: str
    run_id: str
    clone: Callable[..., Task]

    @property
    def command(self) -> List[str]:
        from_s3 = self.input().path
        _, ext = os.path.splitext(from_s3)
        to_s3 = self.output().path
        local_src_path = f"/tmp/input/document{ext}"
        local_dest_path = "/tmp/output"

        return [
            "reclada-run.sh",
            "--download", from_s3, local_src_path,
            "--upload", f"{local_dest_path}/document.pdf", to_s3,
            "libreoffice",
            "--headless",
            "--convert-to", "pdf", local_src_path,
            "--outdir", local_dest_path,
        ]

    def requires(self):
        return self.clone(UploadDocument)

    def output(self):
        return S3Target(f"results/{self.run_id}/document.pdf")


@inherits(DocumentTask)
class K8sConverter(ConverterMixin, K8sTask):
    image = "reclada_converter"


@inherits(DocumentTask)
class DominoConverter(ConverterMixin, SimpleDominoTask):
    is_direct_command = False
    project = "reclada_converter"
