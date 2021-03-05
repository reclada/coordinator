FROM python:3.6

WORKDIR /app

ENV S3_FILENAME="" \
    AWS_ACCESS_KEY_ID="" \
    AWS_SECRET_ACCESS_KEY="" \
    S3_DEFAULT_PATH="" \
    DB_URI=""

RUN pip install -U pip
COPY domino/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN pip install .[k8s]

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD [ "python", "job" ]
