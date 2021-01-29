import os
from typing import List, Callable

from luigi import Parameter
from reclada.coordinator.base import S3Target, K8sTask, DominoTask

from .initial import UploadDocument


class ConverterMixin:
    src: str = Parameter()
    run_id: str = Parameter()
    input: Callable[[], S3Target]

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
            "--upload", to_s3, f"{local_dest_path}/document.pdf",
            "--from-dir", local_dest_path,
            "libreoffice",
            "--headless",
            "--convert-to", "pdf", local_src_path,
            "--outdir", local_dest_path,
        ]

    def requires(self):
        return UploadDocument(self.src, self.run_id)

    def output(self):
        return S3Target(f"results/{self.run_id}/document.pdf")


class K8sConverter(K8sTask, ConverterMixin):
    image = "reclada_converter"


class DominoConverter(DominoTask, ConverterMixin):
    is_direct_command = False
    project = "reclada_converter"
