import json
import os
import shutil

from luigi import Task, LocalTarget
from luigi.contrib.s3 import S3Target as LuigiS3Target
from luigi.format import Nop
from luigi.util import inherits
from reclada.coordinator.base import S3Target, DocumentTask
from reclada.coordinator.db import Db


def document_id(src) -> int:
    with src.open() as f:
        return json.load(f)["result"]["id"]


@inherits(DocumentTask)
class UploadDocument(Task):
    def input(self):
        return LocalTarget(self.src, format=Nop)

    def run(self):
        if self.src.startswith("s3://"):
            return
        with self.input().open() as src:
            with self.output().open("w") as dest:
                shutil.copyfileobj(src, dest)

    def output(self):
        if self.src.startswith("s3://"):
            return LuigiS3Target(self.src)
        _, ext = os.path.splitext(self.src)
        return S3Target(f"results/{self.run_id}/document{ext}")


@inherits(DocumentTask)
class InitDbDocument(Task):
    def run(self):
        with Db() as c:
            res = c.add_document(
                os.path.basename(self.src),
                self.input().path
            )
        with self.output().open("w") as f:
            json.dump(res, f)

    def requires(self):
        return UploadDocument(self.src, self.run_id)

    def output(self):
        return LocalTarget(f"results/{self.run_id}/doc.json")
