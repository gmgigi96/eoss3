#!/bin/bash
#
# S3 Compatibility Test Script - Expanded
#
# This script performs a comprehensive series of tests on an S3-compatible
# object storage service using the AWS CLI.
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
TEST_DIR=$(mktemp -d)
DOWNLOAD_DIR="s3_test_download_data"

# Set credentials as environment variables for the AWS CLI
export AWS_ACCESS_KEY_ID=${S3_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${S3_SECRET_ACCESS_KEY}
export AWS_REGION=us-east-1
# Define the base AWS CLI command with the specified endpoint
AWS_CMD="aws --endpoint-url ${S3_ENDPOINT_URL} s3"

#####################################################################
# TEST: Verify AWS CLI installation
#####################################################################
print_header "Verifying AWS CLI Installation"
if ! command -v aws &> /dev/null
then
    print_error "AWS CLI could not be found. Please install it to continue (see: https://aws.amazon.com/cli/)"
fi
print_success "AWS CLI is installed."

# Cleanup bucket before starting the tests
if ! eoss3-cli get-bucket "${BUCKET_NAME}"; then
    print_error "Bucket ${BUCKET_NAME} does not exist. Create it using the command 'eoss3-cli create-bucket --name ${BUCKET_NAME} --owner <owner> --path <path>'."
fi

if ! eoss3-cli purge-bucket "${BUCKET_NAME}"; then
    print_error "Failed to purge the bucket"
fi

# Trap SIGINT (Ctrl+C) and SIGTERM to ensure cleanup runs
trap cleanup SIGINT SIGTERM

# Prepare local test files with a more complex structure
print_header "Preparing Local Test Directory"
mkdir -p "${TEST_DIR}/documents/reports"
mkdir -p "${TEST_DIR}/images/vacation"
mkdir -p "${TEST_DIR}/logs"

# Create a variety of files
echo "This is a root file." > "${TEST_DIR}/root_file.txt"
echo "Project plan document." > "${TEST_DIR}/documents/plan.txt"
echo "Quarterly financial report." > "${TEST_DIR}/documents/reports/q1_report.txt"
echo "Company logo." > "${TEST_DIR}/images/logo.png"
echo "Beach photo." > "${TEST_DIR}/images/vacation/beach.jpg"
echo "Application log data." > "${TEST_DIR}/logs/app.log"
echo "Empty file." > "${TEST_DIR}/logs/empty.log"
head -c 1K </dev/urandom > "${TEST_DIR}/documents/reports/binary_data.dat"

# Calculate the total number of files for later verification
TOTAL_FILES=$(find "${TEST_DIR}" -type f | wc -l)

echo "Created the following directory structure:"
ls -R "${TEST_DIR}"
print_success "Local test directory created with ${TOTAL_FILES} files."

#####################################################################
# TEST: Sync the entire directory structure to S3
#####################################################################
print_header "Testing Directory Upload (sync)"
${AWS_CMD} sync "${TEST_DIR}/" "s3://${BUCKET_NAME}/"
if [ $? -ne 0 ]; then
    print_error "Failed to sync directory ${TEST_DIR}."
fi
print_success "Directory ${TEST_DIR} synced to bucket."

#####################################################################
# TEST: List all objects recursively and verify the count
#####################################################################
print_header "Testing Recursive Object Listing (ls --recursive)"
echo "Listing all objects in the bucket:"
${AWS_CMD} ls "s3://${BUCKET_NAME}/" --recursive

# Verification
OBJECT_COUNT=$(${AWS_CMD} ls "s3://${BUCKET_NAME}/" --recursive | wc -l)
if [ "${OBJECT_COUNT}" -ne ${TOTAL_FILES} ]; then
    print_error "Recursive object listing failed. Expected ${TOTAL_FILES} objects, found ${OBJECT_COUNT}."
fi
print_success "Found all ${TOTAL_FILES} objects in the bucket."

#####################################################################
# TEST: List objects within a "subdirectory" (prefix)
#####################################################################
print_header "Testing Subdirectory Object Listing (ls on a prefix)"
echo "Listing objects in 's3://${BUCKET_NAME}/documents/reports/':"
${AWS_CMD} ls "s3://${BUCKET_NAME}/documents/reports/"

# Verification for a subdirectory
SUBDIR_OBJECT_COUNT=$(${AWS_CMD} ls "s3://${BUCKET_NAME}/documents/reports/" | wc -l)
EXPECTED_SUBDIR_COUNT=$(find "${TEST_DIR}/documents/reports" -type f | wc -l)
if [ "${SUBDIR_OBJECT_COUNT}" -ne "${EXPECTED_SUBDIR_COUNT}" ]; then
    print_error "Subdirectory listing failed. Expected ${EXPECTED_SUBDIR_COUNT} objects, found ${SUBDIR_OBJECT_COUNT}."
fi
print_success "Subdirectory listing for 'documents/reports/' is correct."

#####################################################################
# TEST: List object by a prefix (not subdirectory)
#####################################################################
print_header "Testing Object Listing by prefix without delimiter"
if ${AWS_CMD} ls "s3://${BUCKET_NAME}/lo"; then
    print_error "Listing by prefix without delimiter didn't fail. Expected to fail,"
fi
print_success "Listing by prefix without delimiter is correct."

#####################################################################
# TEST: Download the entire bucket content
#####################################################################
print_header "Testing Directory Download (sync)"
mkdir -p "${DOWNLOAD_DIR}"
${AWS_CMD} sync "s3://${BUCKET_NAME}/" "${DOWNLOAD_DIR}/"
if [ $? -ne 0 ]; then
    print_error "Failed to download bucket content."
fi
print_success "Bucket content synced to ${DOWNLOAD_DIR}."

#####################################################################
# TEST: Verify data integrity of the downloaded structure
#####################################################################
print_header "Verifying Data Integrity of Downloaded Files"
diff -r "${TEST_DIR}" "${DOWNLOAD_DIR}"
if [ $? -ne 0 ]; then
    print_error "Data integrity check failed. Original and downloaded directories differ."
fi
print_success "Downloaded files are identical to the original files."

#####################################################################
# TEST: Delete a single object and verify
#####################################################################
print_header "Testing Single Object Deletion (rm)"
OBJECT_TO_DELETE="logs/app.log"
${AWS_CMD} rm "s3://${BUCKET_NAME}/${OBJECT_TO_DELETE}"
if [ $? -ne 0 ]; then
    print_error "Failed to delete object ${OBJECT_TO_DELETE}."
fi
# Verify it's gone
${AWS_CMD} ls "s3://${BUCKET_NAME}/${OBJECT_TO_DELETE}" &>/dev/null
if [ $? -eq 0 ]; then
    print_error "Object ${OBJECT_TO_DELETE} still exists after deletion."
fi
print_success "Object ${OBJECT_TO_DELETE} deleted successfully."

#####################################################################
# TEST: Delete a whole "subdirectory" (prefix) recursively
#####################################################################
print_header "Testing Recursive Deletion (rm --recursive)"
PREFIX_TO_DELETE="images/"
${AWS_CMD} rm "s3://${BUCKET_NAME}/${PREFIX_TO_DELETE}" --recursive
if [ $? -ne 0 ]; then
    print_error "Failed to recursively delete objects with prefix ${PREFIX_TO_DELETE}."
fi

# Verify they're gone
REMAINING_IMAGES=$(${AWS_CMD} ls "s3://${BUCKET_NAME}/${PREFIX_TO_DELETE}" --recursive | wc -l)
if [ "${REMAINING_IMAGES}" -ne 0 ]; then
    print_error "Objects with prefix ${PREFIX_TO_DELETE} still exist after recursive delete."
fi
print_success "Recursively deleted all objects under prefix '${PREFIX_TO_DELETE}'."

cleanup

print_success "All tests passed successfully!"
exit 0