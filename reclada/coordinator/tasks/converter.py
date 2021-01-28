import shlex
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
        to_s3 = self.output().path
        local_src_path = "/tmp/input"
        local_dest_path = "/tmp/output"

        return [
            "reclada-run.sh",
            "--from-s3", shlex.quote(from_s3),
            "--to-dir", shlex.quote(local_src_path),
            "--to-s3", shlex.quote(to_s3),
            "--from-dir", shlex.quote(local_dest_path),
            "libreoffice",
            "--headless",
            "--convert-to pdf",
            shlex.quote(local_src_path),
            "--outdir",
            shlex.quote(local_dest_path),
        ]

    def requires(self):
        return UploadDocument(self.src, self.run_id)

    def output(self):
        return S3Target(f"results/{self.run_id}/converted.pdf")


class K8sConverter(K8sTask, ConverterMixin):
    image = "reclada-converter"


class DominoConverter(DominoTask, ConverterMixin):
    project = "reclada-converter"
