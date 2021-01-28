#!/usr/bin/env bash

set -x

UPLOAD_FILE=()
UPLOAD_S3=()
while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
  --download)
    FROM_S3="$2"
    TO_DIR="$3"
    shift
    shift
    shift
    echo "Dowloading: ${FROM_S3} -> ${TO_DIR}"
        aws s3 cp "${FROM_S3}" "${TO_DIR}" --recursive
    ;;
  --upload)
    UPLOAD_FILE+=("$2")
    UPLOAD_S3+=("$3")
    shift
    shift
    shift
    ;;
  *) # unknown option
    break
    ;;
  esac
done

echo "Running app" "$@"
"$@"

for idx in "${!UPLOAD_FILE[@]}"; do
  FROM_FILE=${UPLOAD_FILE[$idx]}
  TO_S3=${UPLOAD_S3[$idx]}
  aws s3 cp "${FROM_FILE}" "${TO_S3}"
done
