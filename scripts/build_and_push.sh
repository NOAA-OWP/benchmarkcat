#!/bin/bash
set -e
set -o pipefail

# ---------------------------------------------------------------------------
# Benchmarkcat — Build Docker image and push to ECR
#
# Usage:
#   ./scripts/build_and_push.sh             # main ingest image (Dockerfile)
#   ./scripts/build_and_push.sh ripple      # ripple worker image (Dockerfile.ripple)
#
# Reads aws_account_id, aws_region, aws_profile from terraform outputs.
# Falls back to AWS_ACCOUNT_ID, AWS_REGION, AWS_PROFILE env vars.
# Override ECR URL with ECR_REPO env var, flows2fim version with FLOWS2FIM_VERSION.
# ---------------------------------------------------------------------------

TARGET="${1:-main}"
if [ "$TARGET" = "ripple" ]; then
  DOCKERFILE="Dockerfile.ripple"
  TF_OUTPUT_KEY="ripple_ecr_repository_url"
else
  DOCKERFILE="Dockerfile"
  TF_OUTPUT_KEY="ecr_repository_url"
fi

# Read from terraform outputs first, then env vars (no hardcoded defaults)
if [ -d "terraform" ] && command -v terraform &>/dev/null; then
  _tf_account=$(cd terraform && terraform output -raw aws_account_id 2>/dev/null) || true
  _tf_region=$(cd terraform && terraform output -raw aws_region 2>/dev/null) || true
  _tf_profile=$(cd terraform && terraform output -raw aws_profile 2>/dev/null) || true
  [ -n "$_tf_account" ] && AWS_ACCOUNT_ID="$_tf_account"
  [ -n "$_tf_region" ] && AWS_REGION="$_tf_region"
  [ -n "$_tf_profile" ] && AWS_PROFILE="$_tf_profile"
fi

# Require terraform or env vars
missing=""
[ -z "$AWS_ACCOUNT_ID" ] && missing="${missing}AWS_ACCOUNT_ID "
[ -z "$AWS_REGION" ] && missing="${missing}AWS_REGION "
[ -z "$AWS_PROFILE" ] && missing="${missing}AWS_PROFILE "
if [ -n "$missing" ]; then
  echo "ERROR: Missing: $missing" >&2
  echo "Run 'cd terraform && terraform init && terraform apply' or set env vars." >&2
  exit 1
fi

# Get ECR repo URL from terraform or env var
if [ -n "$ECR_REPO" ]; then
  echo "Using ECR_REPO from environment: $ECR_REPO"
elif [ -d "terraform" ] && command -v terraform &>/dev/null; then
  ECR_REPO=$(cd terraform && terraform output -raw "$TF_OUTPUT_KEY" 2>/dev/null) || true
fi

if [ -z "$ECR_REPO" ]; then
  echo "ERROR: Could not determine ECR repository URL." >&2
  echo "Run 'cd terraform && terraform apply' or set ECR_REPO env var." >&2
  exit 1
fi
echo "Using ECR URL: $ECR_REPO"

# Extract project name from ECR URL for local docker tag
PROJECT_NAME=$(basename "$ECR_REPO")

# 1. Login to ECR
echo "Logging in to ECR..."
aws ecr get-login-password \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE" \
  | docker login --username AWS --password-stdin \
    "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# 2. Build
BUILD_ARGS=()
if [ "$TARGET" = "ripple" ]; then
  BUILD_ARGS+=(--build-arg "FLOWS2FIM_VERSION=${FLOWS2FIM_VERSION:-0.5.0}")
  echo "Using flows2fim version: ${FLOWS2FIM_VERSION:-0.5.0}"
fi
echo "Building Docker image (linux/amd64) from ${DOCKERFILE}..."
docker build --platform linux/amd64 -f "$DOCKERFILE" "${BUILD_ARGS[@]}" -t "${PROJECT_NAME}" .

# 3. Tag
docker tag "${PROJECT_NAME}:latest" "${ECR_REPO}:latest"

# 4. Push
echo "Pushing to ECR..."
docker push "${ECR_REPO}:latest"

echo ""
echo "Done. Image pushed to: ${ECR_REPO}:latest"
