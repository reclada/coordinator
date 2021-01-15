## Reclada parser pipeline coordinator

Currently, supports only running subprojects in [Domino Data Lab](https://dominodatalab.com) environment

Environment variables:

* `DB_URI` - postgresql database uri. E.g. `postgres://postgres:password@127.0.0.1`
* `DOMINO_URL` - base url of domino server. E.g. `https://try.dominodatalab.com`
* `REPO_DIR` - directory where git repository attached in domino. Required for some subprojects. E.g. `/repos/reclada/"`
* `S3_DEFAULT_PATH` - path to s3 bucket and dir inside it. E.g. `s3://reclada-test-data/`
* `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` - keys to access AWS S3 

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

### Running in Domino

1. Configure environment variables in project (except automatic ones)
2. Upload contents of domino directory to project root
3. Create Launcher with one parameter of type `File`. Correct command to be `main.sh ${parameter1}`