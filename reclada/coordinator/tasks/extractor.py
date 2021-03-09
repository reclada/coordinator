import os
from typing import List, Callable

from luigi import Task, LocalTarget

from .badgerdoc import K8sBadgerdoc, DominoBadgerdoc
from .initial import InitDbDocument, document_id
from .parser import K8sParser, DominoParser
from ..base import S3Target, K8sTask, SimpleDominoTask


class ExtractorMixin:
    input: Callable[[], S3Target]
    clone: Callable[..., Task]
    run_id: str
    src: str

    @property
    def command(self) -> List[str]:
        s3_doc, db_doc = self.input()
        from_s3 = s3_doc.path
        doc_id = document_id(db_doc)
        _, ext = os.path.splitext(from_s3)
        local_src_path = f"/tmp/tables.json"

        return [
            "reclada-run.sh",
            "--download", from_s3, local_src_path,
            "reclada-dicts-extractor", str(doc_id), local_src_path, local_src_path
        ]

    def requires(self):
        from_s3 = self.src
        _, ext = os.path.splitext(from_s3)
        if ext in (".csv", ".txt", ".tsv"):
            yield self.requires_parser()
        else:
            yield self.requires_badgerdoc()
        yield self.clone(InitDbDocument)

    def requires_badgerdoc(self) -> Task:
        raise NotImplementedError

    def requires_parser(self) -> Task:
        raise NotImplementedError

    def on_finished(self):
        with self.output().open("w") as f:
            f.write('{"status": "done"}')

    def output(self):
        return LocalTarget(f"results/{self.run_id}/extractor.json")


class K8sExtractor(ExtractorMixin, K8sTask):
    image = "extractor"

    def requires_badgerdoc(self) -> Task:
        return self.clone(K8sBadgerdoc)

    def requires_parser(self) -> Task:
        return self.clone(K8sParser)


class DominoExtractor(ExtractorMixin, SimpleDominoTask):
    is_direct_command = False
    project = "reclada_extractor"

    def requires_badgerdoc(self) -> Task:
        return self.clone(DominoBadgerdoc)

    def requires_parser(self) -> Task:
        return self.clone(DominoParser)
