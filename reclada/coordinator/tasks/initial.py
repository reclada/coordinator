import json
import os
import shutil

from luigi import Task, Parameter, LocalTarget
from reclada.coordinator.base import S3Target
from reclada.coordinator.db import Db


def document_id(src):
    with src.open() as f:
        return json.load(f)["result"]["id"]


class UploadDocument(Task):
    src: str = Parameter()
    run_id: str = Parameter()

    def input(self):
        return LocalTarget(self.src)

    def run(self):
        with self.input().open() as src:
            with self.output().open("w") as dest:
                shutil.copyfileobj(src, dest)

    def output(self):
        return S3Target(f"results/{self.run_id}/doc.json")


class InitDbDocument(Task):
    src: str = Parameter()
    run_id: str = Parameter()

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
