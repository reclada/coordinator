## Reclada parser pipeline coordinator

Currently supports only running in [Domino Data Lab](https://dominodatalab.com) environment

Environment variables:

* `DB_URI` - psotgresql database uri. E.g. `postgres://postgres:password@127.0.0.1`
* `DOMINO_URL` - base url of domino server. E.g. `https://try.dominodatalab.com`
* `REPO_DIR` - directory where git repository attached in domino. E.g. `/repos/pdf-parser"`
* `S3_DEFAULT_PATH` - path to s3 bucket and dir inside it. E.g. `s3://poc-parser-test-data/`

Automatically set by domino:

* `DOMINO_USER_API_KEY` - access key for domino api
* `DOMINO_PROJECT_OWNER` - coordinator project owner. All projects bust be owned by same account.
* `DOMINO_PROJECT_NAME` - coordinator project name
* `DOMINO_RUN_ID` - current run identifier 
* `DOMINO_RUN_NUMBER` - current run incremental number

Running:
```bash
reclada-coordinator All --All-pdf filename.pdf --local-scheduler 
```