#!/bin/bash
set -e

# Get the root directory of the project
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Output directory relative to project root
OUTPUT_DIR="${PROJECT_ROOT}/tests/serialization_test_data/java_generated_files"
mkdir -p "$OUTPUT_DIR"

echo "Building Docker image..."
docker build -t datasketches-java-gen -f "${PROJECT_ROOT}/tools/java-generated-files.dockerfile" "${PROJECT_ROOT}/tools"

echo "Running Docker container to generate files..."
docker run --rm \
    -v "${OUTPUT_DIR}:/output" \
    datasketches-java-gen

echo "Files generated in ${OUTPUT_DIR}"

