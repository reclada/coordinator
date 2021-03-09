import logging
import os
import time
from typing import List

from luigi import Parameter, Task
from luigi.contrib.kubernetes import KubernetesJobTask
from luigi.contrib.s3 import S3Target as LuigiS3Target
from luigi.format import Nop
from luigi.util import inherits
from reclada.devops import domino

logger = logging.getLogger("luigi-interface." + __name__)


def create_domino() -> domino.Domino:
    return domino.Domino(
        token=os.getenv("DOMINO_USER_API_KEY"),
        base_url=os.getenv("DOMINO_URL", "https://try.dominodatalab.com"),
    )


class S3Target(LuigiS3Target):
    base_path: str = os.getenv("S3_DEFAULT_PATH").rstrip("/")

    def __init__(self, path):
        super().__init__(path=f"{self.base_path}/{path}", format=Nop)


class DocumentTask(Task):
    src: str = Parameter()
    run_id: str = Parameter()
    run_prefix: str = Parameter(default="")


@inherits(DocumentTask)
class DominoTask(Task):
    is_direct_command: bool = True
    project: str
    owner: str = os.getenv("DOMINO_PROJECT_OWNER")

    _domino: domino.Domino

    @property
    def domino(self) -> domino.Domino:
        if not hasattr(self, "_domino"):
            self._domino = create_domino()
        return self._domino

    def _run_title(self):
        return f"{self.run_prefix}{self.__class__.__name__}"

    def _run_until_complete(self, command: List[str]):
        title = self._run_title()
        task = self.domino.start_run(
            self.owner, self.project,
            command,
            title=self._run_title(),
            is_direct=self.is_direct_command,
        )
        task_id = task.run_id
        logger.info("%s: job started with id=%s", title, task_id)
        logger.info("Domino job url: %sjobs/%s/%s/%s", self.domino.base_url, self.owner,
                    self.project, task_id)
        is_completed = False
        task_status = None
        while not is_completed:
            task_status = self.domino.run_status(self.owner, self.project, task_id)
            is_completed = task_status.is_completed
            logger.debug("%s: waiting job, current status=%s", title, task_status.status)
            time.sleep(5)
        return task_status


class SimpleDominoTask(DominoTask):
    command: List[str]

    def run(self):
        self._run_until_complete(self.command)
        if hasattr(self, "on_finished"):
            self.on_finished()


K8S_ENV_KEYS = [
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_DEFAULT_PATH", "S3_BUCKET",
    "S3_REGION", "DB_URI"
]


def k8s_envs():
    return [
        {"name": k, "value": os.getenv(k)}
        for k in K8S_ENV_KEYS
        if k in os.environ
    ]


@inherits(DocumentTask)
class K8sTask(KubernetesJobTask):
    run_prefix: str = Parameter(default="")
    command: List[str]
    image: str
    auth_method = "service-account"

    @property
    def name(self):
        return f"{self.run_prefix}{self.__class__.__name__.lower()}"

    @property
    def image_repo(self):
        repo = os.getenv("K8S_IMAGE_REPO")
        if repo:
            return repo.rstrip("/") + "/"
        return ""

    @property
    def spec_schema(self):
        return {
            "containers": [{
                "name": self.name,
                "image": self.image_repo + self.image,
                "command": self.command,
                "env": k8s_envs(),
            }],
        }

    def run(self):
        super().run()
        if hasattr(self, "on_finished"):
            self.on_finished()
