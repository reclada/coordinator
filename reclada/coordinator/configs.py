import os

DB_URI = os.getenv("DB_URI")
RUN_ID = os.getenv("DOMINO_RUN_ID", "run_id")
DOMINO_RUN_NUMBER = os.getenv("DOMINO_RUN_NUMBER", "0")
OWNER = os.getenv("DOMINO_PROJECT_OWNER")
DOMINO_URL = os.getenv("DOMINO_URL", "https://try.dominodatalab.com")
DOMINO_KEY = os.getenv("DOMINO_USER_API_KEY")
REPO_DIR = os.getenv("REPO_DIR", "/repos/pdf-parser")

S3_DEFAULT_PATH = os.getenv("S3_DEFAULT_PATH")
