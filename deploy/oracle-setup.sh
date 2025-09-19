#!/usr/bin/env bash
set -e
# This script runs on the target server to install Docker and required tools
sudo apt-get update -y
sudo apt-get install -y ca-certificates curl gnupg lsb-release
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo apt-get install -y docker-compose-plugin
sudo usermod -aG docker ubuntu || true
# create directories used by compose
mkdir -p /home/ubuntu/scm-pipeline/logs /home/ubuntu/scm-pipeline/output
# ensure docker network exists
docker network ls --format '{{.Name}}' | grep -q '^scm_pipeline_default$' || docker network create scm_pipeline_default || true

echo "oracle setup complete"
