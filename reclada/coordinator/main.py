import json
import os
import shutil
import time
from io import BytesIO
from logging import getLogger
from typing import List
from uuid import uuid4

from luigi import Task, LocalTarget, run, Parameter, IntParameter, build
from luigi.format import Nop
from reclada.connector import PgConnector as Connector

from . import configs
from .mydomino import Domino
from .utils import PocS3Target, PocS3FileUploader

domino = Domino(configs.DOMINO_KEY, base_url=f"{configs.DOMINO_URL}/v1/")

logger = getLogger("luigi-interface")


class DominoTask(Task):
    document_id = IntParameter()

    project: str
    owner: str

    step_id: str

    def _upload(self, input, target_path):
        if input:
            logger.info("Upload file to path `%s`", target_path)
            with input.open("r") as f:
                domino.upload(self.owner, self.project, target_path, f.read())

    def _run_until_complete(self, command):
        task = domino.run(
            self.owner, self.project,
            command,
            title=f"{configs.DOMINO_RUN_NUMBER}:{self.step_id}",
            is_direct=True,
        )
        task_id = task["runId"]
        logger.info("%s: job started with id=%s", self.step_id, task_id)
        is_completed = False
        task_status = None
        while not is_completed:
            task_status = domino.run_status(self.owner, self.project, task_id)
            is_completed = task_status["isCompleted"]
            logger.debug("%s: waiting job, current status=%s", self.step_id, task_status.get("status"))
            time.sleep(5)
        return task_status

    def _download_result(self, output_commit, output_path, output):
        files = domino.files(self.owner, self.project, output_commit, output_path)["data"]
        if not files:
            text = f"Not file found at path `{output_path}` in commit {output_commit}"
            logger.warning(text)
            raise RuntimeError(text)

        logger.info("Download file `%s` from commit `%s`", output_path, output_commit)
        io = BytesIO()
        blob_connection = domino.file(files[0]["url"])
        shutil.copyfileobj(blob_connection, io)
        with output.open("w") as f:
            io.seek(0)
            shutil.copyfileobj(io, f)
        with Connector(configs.DB_URI) as c:
            io.seek(0)
            c.call_func(
                "add_job_result",
                json.dumps({"document_id": self.document_id, "job_id": self.step_id, "urls": [output.path]}),
            )

    def output_prefix(self):
        return f"results/{configs.RUN_ID}/{self.step_id}"


class SimpleDominoTask(DominoTask):
    command: List[str]
    input_path: str
    output_path: str

    def run(self):
        self._upload(self.input(), self.input_path)
        task_status = self._run_until_complete(self.command)
        output_commit = task_status.get("outputCommitId")
        self._download_result(output_commit, self.output_path, self.output())


class CsvTables(SimpleDominoTask):
    pdf = Parameter()

    owner = configs.OWNER
    project = "reclada_parser"
    output_path = f"results/csv_tables{configs.RUN_ID}.json"

    step_id = "csv_tables"

    @property
    def input_path(self):
        name, ext = os.path.splitext(self.pdf)
        return f"input/{configs.RUN_ID}.{ext}"

    @property
    def command(self):
        return [
            f"reclada-csv-parser",
            self.input_path,
            self.output_path
        ]

    def input(self):
        return LocalTarget(self.pdf, format=Nop)

    def output(self):
        return PocS3Target(f"{self.output_prefix()}/text.json")


class Tables(SimpleDominoTask):
    pdf = Parameter()

    owner = configs.OWNER
    project = "tables_extraction"
    input_path = f"input/{configs.RUN_ID}.pdf"
    output_path = f"results/output.json/{configs.RUN_ID}.pdf/json_out.json"
    command = [
        f"{configs.REPO_DIR}/run_job.sh",
        f"{configs.REPO_DIR}/tables_extraction",
        input_path,
    ]
    step_id = "tables_extraction"

    def requires(self):
        return DocumentConverter(self.document_id, self.pdf)

    def output(self):
        return PocS3Target(f"{self.output_prefix()}/boxes.json")


class DictExtractor(DominoTask):
    pdf: str = Parameter()

    owner = configs.OWNER
    project = "reclada_extractor"
    text_input_path = f"input/{configs.RUN_ID}/dicts_extractor/text.json"
    tables_input_path = f"input/{configs.RUN_ID}/dicts_extractor/tables.json"
    output_path = f"results/stdout.txt"
    step_id = "dicts_extractor"

    @property
    def command(self):
        return [
            f"reclada-dicts-extractor",
            str(self.document_id),
            self.text_input_path,
            self.tables_input_path,
        ]

    @property
    def is_csv(self):
        return self.pdf.endswith(".csv") or self.pdf.endswith(".txt") or self.pdf.endswith(".tsv")

    def output(self):
        return PocS3Target(f"{self.output_prefix()}/dicts_extractor.txt")

    def run(self):
        if self.is_csv:
            tables_step = CsvTables(self.document_id, self.pdf)
        else:
            tables_step = Tables(self.document_id, self.pdf)

        yield tables_step
        self._upload(tables_step.output(), self.tables_input_path)
        task_status = self._run_until_complete(self.command)
        output_commit = task_status.get("outputCommitId")
        self._download_result(output_commit, self.output_path, self.output())


class DocumentConverter(DominoTask):
    pdf = Parameter()

    owner = configs.OWNER
    project = "converter"
    step_id = "converter"

    @property
    def is_pdf(self):
        return self.pdf.endswith(".pdf")

    @property
    def doc_input_path(self):
        name, ext = os.path.splitext(self.pdf)
        return f"input/{configs.RUN_ID}/converter/file{ext}"

    @property
    def doc_output_path(self):
        return f"file.pdf"

    def input(self):
        return LocalTarget(self.pdf, format=Nop)

    @property
    def command(self):
        return [
            f"{configs.REPO_DIR}/run_job.sh",
            f"{configs.REPO_DIR}/converter",
            self.doc_input_path,
        ]

    def output(self):
        return LocalTarget(f"{self.output_prefix()}/document.pdf", format=Nop)

    def run(self):
        if self.is_pdf:
            with self.input().open() as fin:
                with self.output().open("w") as fout:
                    fout.write(fin.read())
            return

        text = self.input()
        self._upload(text, self.doc_input_path)
        task_status = self._run_until_complete(self.command)
        output_commit = task_status.get("outputCommitId")
        self._download_result(output_commit, self.doc_output_path, self.output())


class InitDbDocument(Task):
    pdf = Parameter()

    def run(self):
        s3_url = PocS3FileUploader(self.pdf, f"{uuid4()}.pdf").upload_local_to_s3()

        with Connector(configs.DB_URI) as c:
            res = c.call_func(
                "add_document",
                json.dumps({"name": os.path.basename(self.pdf), "url": s3_url}),
            )[0][0]
        with self.output().open("w") as f:
            json.dump(res, f)

    def output(self):
        return LocalTarget(f"results/{configs.RUN_ID}/doc.json")
        # return PocS3Target(f"{configs.RUN_ID}/doc.json")


class All(Task):
    pdf = Parameter()

    def run(self):
        prerequisite = InitDbDocument(self.pdf)
        build([prerequisite], local_scheduler=True)

        doc_id = int(json.load(prerequisite.output().open())["result"]["id"])
        yield DictExtractor(doc_id, self.pdf)
        with Connector(configs.DB_URI) as c:
            c.call_func(
                "set_document_status",
                json.dumps({"id": doc_id, "status": "done"}),
            )


if __name__ == "__main__":
    run()
