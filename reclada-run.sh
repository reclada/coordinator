#!/usr/bin/env bash

while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
  --from-s3)
    FROM_S3="$2"
    shift # past argument
    shift # past value
    ;;
  --to-s3)
    TO_S3="$2"
    shift # past argument
    shift # past value
    ;;
  --from-dir)
    FROM_DIR="$2"
    shift # past argument
    shift # past value
    ;;
  --to-dir)
    TO_DIR="$2"
    shift # past argument
    shift # past value
    ;;
  *) # unknown option
    break
    ;;
  esac
done

# aws s3 cp /tmp/foo/ s3://bucket/ --recursive --exclude “*” --include “*.jpg”
echo "Dowloading: ${FROM_S3} -> ${TO_DIR}"
aws s3 cp "${FROM_S3}" "${TO_DIR}" --recursive
echo "Running app" "$@"
"$@"
echo "Uploading files: ${FROM_DIR} -> ${TO_S3}"
aws s3 cp "${FROM_DIR}" "${TO_S3}" --recursive
