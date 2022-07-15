#!/bin/bash

set -euxo pipefail

image=391090383831.dkr.ecr.us-east-1.amazonaws.com/threat-analysis/opencti/platform
tag=latest

docker build -t "${image}:${tag}" .
docker push "${image}:${tag}"
