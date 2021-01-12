import os

from luigi.contrib.s3 import S3Target, S3Client
from luigi.format import Nop

from . import configs

S3_CLIENT = S3Client()


class PocS3Target(S3Target):
    def __init__(self, path):
        path = os.path.join(configs.S3_DEFAULT_PATH, path)
        format = Nop
        client = S3_CLIENT

        super().__init__(path=path, format=format, client=client)


class PocS3FileUploader():
    def __init__(self, local_path, target_path):
        self.local_path = local_path
        self.destination_s3_path = os.path.join(configs.S3_DEFAULT_PATH, target_path)
        self.client = S3_CLIENT

    def upload_local_to_s3(self):
        self.client.put(self.local_path, self.destination_s3_path)

        return self.destination_s3_path
