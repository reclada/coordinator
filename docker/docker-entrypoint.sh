#!/usr/bin/env bash

set -e

if [ "$1" = 'job' ]; then
  exec reclada-coordinator All --All-src $S3_FILENAME --All-run_type=k8s --local-scheduler
fi

exec "$@"
