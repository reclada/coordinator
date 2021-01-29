import json
import os
import shutil

from luigi import Task, LocalTarget
from luigi.util import inherits
from reclada.coordinator.base import S3Target, DocumentTask
from reclada.coordinator.db import Db


def document_id(src):
    with src.open() as f:
        return json.load(f)["result"]["id"]


@inherits(DocumentTask)
class UploadDocument(Task):
    def input(self):
        return LocalTarget(self.src)

    def run(self):
        with self.input().open() as src:
            with self.output().open("w") as dest:
                shutil.copyfileobj(src, dest)

    def output(self):
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
