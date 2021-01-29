import os
from typing import List, Callable

from luigi import Task
from luigi.util import inherits
from reclada.coordinator.base import S3Target, K8sTask, DominoTask, DocumentTask

from .converter import K8sConverter, DominoConverter
from .initial import UploadDocument


class BadgerdocMixin:
    input: Callable[[], S3Target]
    clone: Callable[..., Task]

    @property
    def command(self) -> List[str]:
        from_s3 = self.input().path
        _, ext = os.path.splitext(from_s3)
        to_s3 = self.output().path
        local_src_path = f"/tmp/input{ext}"
        local_dest_dir = "/tmp/output"
        local_dest_path = f"{local_dest_dir}/tables.json"

        # TODO
        return [
            "reclada-run.sh",
            "--download", from_s3, local_src_path,
            "--upload", local_dest_path, to_s3,
            "python", "-m", "badgerdoc.pipeline", "full",
            local_src_path, local_dest_dir,
        ]

    def requires(self):
        from_s3 = self.input().path
        _, ext = os.path.splitext(from_s3)
        if ext in (".pdf",):
            return self.clone(UploadDocument)
        else:
            return self.requires_converter()

    def requires_converter(self) -> Task:
        raise NotImplementedError

    def output(self):
        return S3Target(f"results/{self.run_id}/tables.json")


@inherits(DocumentTask)
class K8sBadgerdoc(K8sTask, BadgerdocMixin):
    image = "reclada_badgerdoc"

    def requires_converter(self):
        return self.clone(K8sConverter)


@inherits(DocumentTask)
class DominoBadgerdoc(DominoTask, BadgerdocMixin):
    is_direct_command = False
    project = "reclada_badgerdoc"

    def requires_converter(self):
        return self.clone(DominoConverter)
