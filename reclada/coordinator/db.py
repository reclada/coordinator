import json
import os

from reclada.connector import PgConnector as Connector


class Db(Connector):
    def __init__(self, database_url=None):
        if not database_url:
            database_url = os.getenv("DB_URI")
        super().__init__(database_url)

    def add_document(self, name, url) -> dict:
        return self.call_func(
            "add_document",
            json.dumps({"name": name, "url": url}),
        )[0][0]
