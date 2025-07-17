#!/bin/bash
#
# S3 Compatibility Test Script
#
# This script performs a series of basic tests on an S3-compatible object storage
# service using the AWS CLI. It covers creating buckets, uploading, listing,
# downloading, and deleting objects, and finally, cleaning up the bucket.
#
# It accepts the S3 endpoint, access key, and secret key as command-line options.
#
# IMPORTANT:
# 1. You must have the AWS CLI installed (https://aws.amazon.com/cli/).

# --- Helper Functions ---
function usage() {
    echo "Usage: $0 -e <S3_ENDPOINT_URL> -a <S3_ACCESS_KEY_ID> -s <S3_SECRET_ACCESS_KEY> -b <BUCKET>"
    echo ""
    echo "Options:"
    echo "  -e, --endpoint   S3 endpoint URL (e.g., 'http://127.0.0.1:7070')"
    echo "  -a, --access-key S3 access key ID"
    echo "  -s, --secret-key S3 secret access key"
    echo "  -b, --bucket     Bucket name"
    echo "  -h, --help       Display this help and exit"
    exit 1
}

header_count=0
function print_header() {
    echo "======================================================================"
    echo "=> ${header_count}. $1"
    echo "======================================================================"
    ((header_count++))
}

function print_success() {
    echo "[SUCCESS] $1"
}

function print_error() {
    echo "[ERROR] $1" >&2
    # In case of an error, attempt to clean up before exiting.
    cleanup
    exit 1
}

function cleanup() {
    # Check if AWS_CMD is set to avoid errors if script fails before it's defined
    if [ -z "${AWS_CMD}" ]; then
        echo "Skipping S3 cleanup as connection was not configured."
        return
    fi
    print_header "Performing Cleanup"
    echo "Deleting local test directories and files..."
    rm -rf "${TEST_DIR}" "${DOWNLOAD_DIR}"

    # Check if bucket exists before trying to delete objects and the bucket itself
    ${AWS_CMD} ls "s3://${BUCKET_NAME}" &>/dev/null
    if [ $? -eq 0 ]; then
        echo "Deleting all objects from bucket: ${BUCKET_NAME}"
        ${AWS_CMD} rm "s3://${BUCKET_NAME}" --recursive
        if [ $? -ne 0 ]; then
            echo "[WARNING] Failed to delete objects from bucket. Manual cleanup may be required."
        fi

        echo "Deleting bucket: ${BUCKET_NAME}"
        ${AWS_CMD} rb "s3://${BUCKET_NAME}"
        if [ $? -ne 0 ]; then
            echo "[WARNING] Failed to delete bucket. Manual cleanup may be required."
        fi
    else
        echo "Bucket ${BUCKET_NAME} does not exist or was already deleted. Skipping cleanup."
    fi
    print_success "Cleanup complete."
}

# --- Main Test Execution ---

while [[ "$#" -gt 0 ]]; do
    case $1 in
    -e | --endpoint)
        S3_ENDPOINT_URL="$2"
        shift
        ;;
    -a | --access-key)
        S3_ACCESS_KEY_ID="$2"
        shift
        ;;
    -s | --secret-key)
        S3_SECRET_ACCESS_KEY="$2"
        shift
        ;;
    -b | --bucket)
        BUCKET_NAME="$2"
        shift
        ;;
    -h | --help) usage ;;
    *)
        echo "Unknown parameter passed: $1"
        usage
        ;;
    esac
    shift
done

# Check if all required options are provided
if [ -z "${S3_ENDPOINT_URL}" ] || [ -z "${S3_ACCESS_KEY_ID}" ] || [ -z "${S3_SECRET_ACCESS_KEY}" ] || [ -z "${BUCKET_NAME}" ]; then
    echo "Error: Missing required arguments."
    usage
fi

# --- Script Variables ---
# A unique bucket name is generated using the date and a random number to avoid conflicts.
#BUCKET_NAME="s3-test-bucket-$(date +%s)-${RANDOM}"
TEST_DIR=$(mktemp -d)
DOWNLOAD_DIR="s3_test_download_data"
TEST_FILE_1="test_file_1.txt"
TEST_FILE_2="test_file_2.txt"
TEST_FILE_1_CONTENT="This is the first test file for S3."
TEST_FILE_2_CONTENT="This is the second test file, which will be in a subdirectory."
SUBDIR="subdir"

# Set credentials as environment variables for the AWS CLI
export AWS_ACCESS_KEY_ID=${S3_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${S3_SECRET_ACCESS_KEY}
export AWS_REGION=us-east-1
# Define the base AWS CLI command with the specified endpoint
AWS_CMD="aws --endpoint-url ${S3_ENDPOINT_URL} s3"

# TEST: Verify AWS CLI installation
print_header "Verifying AWS CLI Installation"
if ! command -v aws &> /dev/null
then
    print_error "AWS CLI could not be found. Please install it to continue (see: https://aws.amazon.com/cli/)"
fi
print_success "AWS CLI is installed."

# Trap SIGINT (Ctrl+C) and SIGTERM to ensure cleanup runs
trap cleanup SIGINT SIGTERM

# Prepare environment for test
if ! eoss3-cli get-bucket "${BUCKET_NAME}" &> /dev/null; then
    # The bucket doesn't exist. We try to create it.
    print_error "Bucket ${BUCKET_NAME} does not exist, create it with \"eoss3-cli create-bucket --name ${BUCKET_NAME} --owner <owner> --path <path>\""
fi

eoss3-cli purge-bucket "${BUCKET_NAME}"

# TEST: Prepare local test files
print_header "Preparing Local Test Files"
mkdir -p "${TEST_DIR}/${SUBDIR}"
echo "${TEST_FILE_1_CONTENT}" >"${TEST_DIR}/${TEST_FILE_1}"
echo "${TEST_FILE_2_CONTENT}" >"${TEST_DIR}/${SUBDIR}/${TEST_FILE_2}"
ls -R "${TEST_DIR}"
print_success "Local test files and directory created."

# TEST: Upload a single file
print_header "Testing Single File Upload (cp)"
${AWS_CMD} cp "${TEST_DIR}/${TEST_FILE_1}" "s3://${BUCKET_NAME}/"
if [ $? -ne 0 ]; then
    print_error "Failed to upload ${TEST_FILE_1}."
fi
print_success "${TEST_FILE_1} uploaded."

# TEST: Sync a directory
print_header "Testing Directory Sync (sync)"
${AWS_CMD} sync "${TEST_DIR}/" "s3://${BUCKET_NAME}/"
if [ $? -ne 0 ]; then
    print_error "Failed to sync directory ${TEST_DIR}."
fi
print_success "Directory ${TEST_DIR} synced to bucket."

# TEST: List objects in the bucket
print_header "Testing Object Listing (ls)"
echo "Listing all objects in the bucket:"
${AWS_CMD} ls "s3://${BUCKET_NAME}/" --recursive
# Verification
OBJECT_COUNT=$(${AWS_CMD} ls "s3://${BUCKET_NAME}/" --recursive | wc -l)
if [ "${OBJECT_COUNT}" -ne 2 ]; then
    print_error "Object listing failed. Expected 2 objects, found ${OBJECT_COUNT}."
fi
print_success "Objects listed successfully."

# TEST: List objects by prefix in the bucket
print_header "Testing listing by prefix"
echo "Listing objects by prefix in the bucket:"
${AWS_CMD} ls "s3://${BUCKET_NAME}/sub"
# Verification
OBJECT_COUNT=$(${AWS_CMD} ls "s3://${BUCKET_NAME}/sub" | wc -l)
if [ "${OBJECT_COUNT}" -ne 1 ]; then
    print_error "Object listing by prefix failed. Expected 1 element, found ${OBJECT_COUNT}."
fi
print_success "Objects listing by prefix successfully."

# TEST: List objects in "directory" (i.e. prefix + "/" delimiter) 
print_header "Testing directory in bucket"
echo "Listing objects in directory in the bucket:"
${AWS_CMD} ls "s3://${BUCKET_NAME}/subdir/"
# Verification
OBJECT_COUNT=$(${AWS_CMD} ls "s3://${BUCKET_NAME}/subdir/" | wc -l)
if [ "${OBJECT_COUNT}" -ne 1 ]; then
    print_error "Directory listing failed. Expected 1 element, found ${OBJECT_COUNT}"
fi
print_success "Directory listing successfully."

# TEST: Download a single file
print_header "Testing Single File Download (cp)"
mkdir -p "${DOWNLOAD_DIR}"
${AWS_CMD} cp "s3://${BUCKET_NAME}/${TEST_FILE_1}" "${DOWNLOAD_DIR}/"
if [ ! -f "${DOWNLOAD_DIR}/${TEST_FILE_1}" ]; then
    print_error "Failed to download ${TEST_FILE_1}."
fi
# Verify content
DOWNLOADED_CONTENT=$(cat "${DOWNLOAD_DIR}/${TEST_FILE_1}")
if [ "${DOWNLOADED_CONTENT}" != "${TEST_FILE_1_CONTENT}" ]; then
    print_error "Content of downloaded file ${TEST_FILE_1} does not match original."
fi
print_success "${TEST_FILE_1} downloaded and verified."

# TEST: Delete a single object
print_header "Testing Single Object Deletion (rm)"
${AWS_CMD} rm "s3://${BUCKET_NAME}/${TEST_FILE_1}"
if [ $? -ne 0 ]; then
    print_error "Failed to delete object ${TEST_FILE_1}."
fi
print_success "Object ${TEST_FILE_1} deleted."

# Final cleanup
cleanup

print_header "All tests passed successfully!"
exit 0
