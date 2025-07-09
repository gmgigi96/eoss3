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
    echo "Usage: $0 -e <S3_ENDPOINT_URL> -a <S3_ACCESS_KEY_ID> -s <S3_SECRET_ACCESS_KEY>"
    echo ""
    echo "Options:"
    echo "  -e, --endpoint   S3 endpoint URL (e.g., 'https://s3.your-provider.com')"
    echo "  -a, --access-key S3 access key ID"
    echo "  -s, --secret-key S3 secret access key"
    echo "  -h, --help         Display this help and exit"
    exit 1
}

function print_header() {
    echo "======================================================================"
    echo "=> $1"
    echo "======================================================================"
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
    -h | --help) usage ;;
    *)
        echo "Unknown parameter passed: $1"
        usage
        ;;
    esac
    shift
done

# Check if all required options are provided
if [ -z "${S3_ENDPOINT_URL}" ] || [ -z "${S3_ACCESS_KEY_ID}" ] || [ -z "${S3_SECRET_ACCESS_KEY}" ]; then
    echo "Error: Missing required arguments."
    usage
fi

# --- Script Variables ---
# A unique bucket name is generated using the date and a random number to avoid conflicts.
#BUCKET_NAME="s3-test-bucket-$(date +%s)-${RANDOM}"
BUCKET_NAME="gdelmont_personal"
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

# 0. Verify AWS CLI installation
print_header "0. Verifying AWS CLI Installation"
if ! command -v aws &> /dev/null
then
    print_error "AWS CLI could not be found. Please install it to continue (see: https://aws.amazon.com/cli/)"
fi
print_success "AWS CLI is installed."

# Trap SIGINT (Ctrl+C) and SIGTERM to ensure cleanup runs
trap cleanup SIGINT SIGTERM

# 1. Prepare local test files
print_header "1. Preparing Local Test Files"
mkdir -p "${TEST_DIR}/${SUBDIR}"
echo "${TEST_FILE_1_CONTENT}" >"${TEST_DIR}/${TEST_FILE_1}"
echo "${TEST_FILE_2_CONTENT}" >"${TEST_DIR}/${SUBDIR}/${TEST_FILE_2}"
ls -R "${TEST_DIR}"
print_success "Local test files and directory created."

# # 2. Create S3 Bucket
# print_header "2. Testing Bucket Creation (mb)"
# ${AWS_CMD} mb "s3://${BUCKET_NAME}"
# if [ $? -ne 0 ]; then
#     print_error "Failed to create bucket: ${BUCKET_NAME}"
# fi
# print_success "Bucket '${BUCKET_NAME}' created."

# 3. Upload a single file
print_header "3. Testing Single File Upload (cp)"
${AWS_CMD} cp "${TEST_DIR}/${TEST_FILE_1}" "s3://${BUCKET_NAME}/"
if [ $? -ne 0 ]; then
    print_error "Failed to upload ${TEST_FILE_1}."
fi
print_success "${TEST_FILE_1} uploaded."

# 4. Sync a directory
print_header "4. Testing Directory Sync (sync)"
${AWS_CMD} sync "${TEST_DIR}/" "s3://${BUCKET_NAME}/"
if [ $? -ne 0 ]; then
    print_error "Failed to sync directory ${TEST_DIR}."
fi
print_success "Directory ${TEST_DIR} synced to bucket."

# 5. List objects in the bucket
print_header "5. Testing Object Listing (ls)"
echo "Listing all objects in the bucket:"
${AWS_CMD} ls "s3://${BUCKET_NAME}/" --recursive
# Verification
OBJECT_COUNT=$(${AWS_CMD} ls "s3://${BUCKET_NAME}/" --recursive | wc -l)
if [ "${OBJECT_COUNT}" -lt 2 ]; then
    print_error "Object listing failed. Expected at least 2 objects, found ${OBJECT_COUNT}."
fi
print_success "Objects listed successfully."

# 6. Download a single file
print_header "6. Testing Single File Download (cp)"
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

# 7. Sync from S3 to local
print_header "7. Testing S3 to Local Sync (sync)"
${AWS_CMD} sync "s3://${BUCKET_NAME}/" "${DOWNLOAD_DIR}/"
if [ ! -f "${DOWNLOAD_DIR}/${SUBDIR}/${TEST_FILE_2}" ]; then
    print_error "Failed to sync from S3 to local directory."
fi
print_success "Sync from S3 to local directory completed."
echo "Downloaded files:"
ls -R "${DOWNLOAD_DIR}"

# 8. Delete a single object
print_header "8. Testing Single Object Deletion (rm)"
${AWS_CMD} rm "s3://${BUCKET_NAME}/${TEST_FILE_1}"
if [ $? -ne 0 ]; then
    print_error "Failed to delete object ${TEST_FILE_1}."
fi
print_success "Object ${TEST_FILE_1} deleted."

# Final cleanup
cleanup

print_header "All tests passed successfully!"
exit 0
