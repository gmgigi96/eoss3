#!/bin/bash
#
# S3 Compatibility Test Script
#
# This script performs a comprehensive series of tests on an S3-compatible
# object storage service using the AWS CLI.

# --- Helper Functions ---
function list_available_steps() {
    local indent="${1:-  - }"
    grep -o 'should_run "[^"]*"' "$0" | cut -d'"' -f2 | while read -r step; do
        echo "${indent}${step}"
    done
}

function usage() {
    echo "Usage: $0 -e <S3_ENDPOINT_URL> -a <S3_ACCESS_KEY_ID> -s <S3_SECRET_ACCESS_KEY> -b <BUCKET> [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -e, --endpoint     S3 endpoint URL (e.g., 'http://127.0.0.1:7070')"
    echo "  -a, --access-key   S3 access key ID"
    echo "  -s, --secret-key   S3 secret access key"
    echo "  -b, --bucket       Bucket name"
    echo "      --step         Run a specific test step. Available steps:"
    list_available_steps "                       - "
    echo "      --list-steps   List all available test steps and exit"
    echo "      --no-cleanup   Do not run the cleanup phase at the end"
    echo "      --cleanup-only Only run the cleanup phase and exit immediately"
    echo "      --no-color     Disable colors printing"
    echo "  -h, --help         Display this help and exit"
    exit 1
}

# Color definitions
COLOR_GREEN='\033[0;32m'
COLOR_RED='\033[0;31m'
COLOR_YELLOW='\033[0;33m'
COLOR_RESET='\033[0m' # No Color

header_count=1
function print_header() {
    echo "======================================================================"
    echo "=> ${header_count}. $1"
    echo "======================================================================"
    ((header_count++))
}

function print_success() {
    if [ "$USE_COLOR" == true ]; then
        echo -e "${COLOR_GREEN}[SUCCESS] $1${COLOR_RESET}"
    else
        echo "[SUCCESS] $1"
    fi
}

function print_error() {
    if [ "$USE_COLOR" == true ]; then
        echo -e "${COLOR_RED}[ERROR] $1${COLOR_RESET}"
    else
        echo "[ERROR] $1"
    fi
    cleanup
    exit 1
}

function print_warning() {
    if [ "$USE_COLOR" == true ]; then
        echo -e "${COLOR_YELLOW}[WARNING] $1${COLOR_RESET}"
    else
        echo "[WARNING] $1"
    fi
}

function cleanup() {
    if [ "${DO_CLEANUP}" == false ] && [ "${CLEANUP_ONLY}" == false ]; then
        print_warning "Skipping cleanup as --no-cleanup was specified."
        return
    fi

    if [ -z "${AWS_CMD}" ]; then
        echo "Skipping S3 cleanup as connection was not configured."
        return
    fi
    
    print_header "Performing Cleanup"
    echo "Deleting local test directories and files..."
    rm -rf "${TEST_DIR}" "${DOWNLOAD_DIR}"

    ${AWS_CMD} ls "s3://${BUCKET_NAME}" &>/dev/null
    if [ $? -eq 0 ]; then
        echo "Deleting all objects from bucket: ${BUCKET_NAME}"
        
        echo "Aborting any incomplete multipart uploads..."
        aws configure set default.s3.multipart_threshold 8MB
        ${AWS_API_CMD} list-multipart-uploads --bucket "${BUCKET_NAME}" --query 'Uploads[*].[Key,UploadId]' --output text | while read key id; do
            if [ "$key" != "None" ] && [ -n "$key" ]; then
                ${AWS_API_CMD} abort-multipart-upload --bucket "${BUCKET_NAME}" --key "$key" --upload-id "$id"
            fi
        done

        ${AWS_CMD} rm "s3://${BUCKET_NAME}" --recursive
        if [ $? -ne 0 ]; then
            print_warning "Failed to delete objects from bucket. Manual cleanup may be required."
        fi
    else
        echo "Bucket ${BUCKET_NAME} does not exist or was already deleted. Skipping bucket cleanup."
    fi
    
    aws configure set default.s3.multipart_threshold 8MB
    aws configure set default.s3.multipart_chunksize 8MB
    
    print_success "Cleanup complete."
}

function should_run() {
    if [ -z "${RUN_STEP}" ]; then
        return 0
    elif [ "${RUN_STEP}" == "$1" ]; then
        return 0
    else
        return 1
    fi
}

# --- Configuration defaults ---
USE_COLOR=true
DO_CLEANUP=true
CLEANUP_ONLY=false
RUN_STEP=""

while [[ "$#" -gt 0 ]]; do
    case $1 in
    -e | --endpoint)
        S3_ENDPOINT_URL="$2"
        shift 2
        ;;
    -a | --access-key)
        S3_ACCESS_KEY_ID="$2"
        shift 2
        ;;
    -s | --secret-key)
        S3_SECRET_ACCESS_KEY="$2"
        shift 2
        ;;
    -b | --bucket)
        BUCKET_NAME="$2"
        shift 2
        ;;
    --step)
        RUN_STEP="$2"
        shift 2
        ;;
    --list-steps)
        echo "Available test steps:"
        list_available_steps "  - "
        exit 0
        ;;
    --no-cleanup)
        DO_CLEANUP=false
        shift
        ;;
    --cleanup-only)
        CLEANUP_ONLY=true
        shift
        ;;
    --no-color)
        USE_COLOR=false
        shift
        ;;
    -h | --help) usage ;;
    *)
        echo "Unknown parameter passed: $1"
        usage
        ;;
    esac
done

if [ "${CLEANUP_ONLY}" == false ]; then
    if [ -z "${S3_ENDPOINT_URL}" ] || [ -z "${S3_ACCESS_KEY_ID}" ] || [ -z "${S3_SECRET_ACCESS_KEY}" ] || [ -z "${BUCKET_NAME}" ]; then
        echo "Error: Missing required S3 connection arguments."
        usage
    fi
fi

# --- Script Variables ---
TEST_DIR=$(mktemp -d)
DOWNLOAD_DIR="s3_test_download_data"

export AWS_ACCESS_KEY_ID=${S3_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${S3_SECRET_ACCESS_KEY}
export AWS_REGION=us-east-1

AWS_CMD="aws --endpoint-url ${S3_ENDPOINT_URL} s3"
AWS_API_CMD="aws --endpoint-url ${S3_ENDPOINT_URL} s3api"

trap cleanup SIGINT SIGTERM

#####################################################################
# SHORT-CIRCUIT: Cleanup Only
#####################################################################
if [ "${CLEANUP_ONLY}" == true ]; then
    cleanup
    exit 0
fi

#####################################################################
# PRE-REQUISITES: Verify AWS CLI & Prepare Local Files
#####################################################################
print_header "Verifying AWS CLI Installation"
if ! command -v aws &> /dev/null
then
    print_error "AWS CLI could not be found. Please install it to continue."
fi
print_success "AWS CLI is installed."

print_header "Preparing Local Test Directory"
mkdir -p "${TEST_DIR}/documents/reports"
mkdir -p "${TEST_DIR}/images/vacation"
mkdir -p "${TEST_DIR}/logs"

echo "This is a root file." > "${TEST_DIR}/root_file.txt"
echo "Project plan document." > "${TEST_DIR}/documents/plan.txt"
echo "Quarterly financial report." > "${TEST_DIR}/documents/reports/q1_report.txt"
echo "Company logo." > "${TEST_DIR}/images/logo.png"
echo "Beach photo." > "${TEST_DIR}/images/vacation/beach.jpg"
echo "Application log data." > "${TEST_DIR}/logs/app.log"
echo "Empty file." > "${TEST_DIR}/logs/empty.log"
head -c 1K </dev/urandom > "${TEST_DIR}/documents/reports/binary_data.dat"

TOTAL_FILES=$(find "${TEST_DIR}" -type f | wc -l)
print_success "Local test directory created with ${TOTAL_FILES} files."

#####################################################################
# TEST: Sync directory to S3
#####################################################################
if should_run "sync_up"; then
    print_header "[sync_up] Testing Directory Upload (sync)"
    ${AWS_CMD} sync "${TEST_DIR}/" "s3://${BUCKET_NAME}/"
    if [ $? -ne 0 ]; then
        print_error "Failed to sync directory ${TEST_DIR}."
    fi
    print_success "Directory ${TEST_DIR} synced to bucket."
fi

#####################################################################
# TEST: Recursive Object Listing
#####################################################################
if should_run "ls_recursive"; then
    print_header "[ls_recursive] Testing Recursive Object Listing (ls --recursive)"
    OBJECT_COUNT=$(${AWS_CMD} ls "s3://${BUCKET_NAME}/" --recursive | wc -l)
    if [ "${OBJECT_COUNT}" -ne ${TOTAL_FILES} ]; then
        print_error "Recursive object listing failed. Expected ${TOTAL_FILES} objects, found ${OBJECT_COUNT}."
    fi
    print_success "Found all ${TOTAL_FILES} objects in the bucket."
fi

#####################################################################
# TEST: Subdirectory Object Listing
#####################################################################
if should_run "ls_subdir"; then
    print_header "[ls_subdir] Testing Subdirectory Object Listing (ls on a prefix)"
    SUBDIR_OBJECT_COUNT=$(${AWS_CMD} ls "s3://${BUCKET_NAME}/documents/reports/" | wc -l)
    EXPECTED_SUBDIR_COUNT=$(find "${TEST_DIR}/documents/reports" -type f | wc -l)
    if [ "${SUBDIR_OBJECT_COUNT}" -ne "${EXPECTED_SUBDIR_COUNT}" ]; then
        print_error "Subdirectory listing failed. Expected ${EXPECTED_SUBDIR_COUNT} objects, found ${SUBDIR_OBJECT_COUNT}."
    fi
    print_success "Subdirectory listing for 'documents/reports/' is correct."
fi

#####################################################################
# TEST: Object Listing by prefix (not subdirectory)
#####################################################################
if should_run "ls_prefix"; then
    print_header "[ls_prefix] Testing Object Listing by prefix without delimiter"
    if ${AWS_CMD} ls "s3://${BUCKET_NAME}/lo"; then
        print_error "Listing by prefix without delimiter didn't fail. Expected to fail."
    fi
    print_success "Listing by prefix without delimiter is correct."
fi

#####################################################################
# TEST: Multipart Upload
#####################################################################
if should_run "multipart"; then
    print_header "[multipart] Testing Multipart Upload"
    LARGE_FILE="${TEST_DIR}/large_multipart_file.dat"
    LARGE_FILE_KEY="multipart/large_multipart_file.dat"

    echo "Generating a 20MB dummy file for multipart upload testing..."
    dd if=/dev/urandom of="${LARGE_FILE}" bs=1M count=20 status=none

    aws configure set default.s3.multipart_threshold 5MB
    aws configure set default.s3.multipart_chunksize 5MB

    echo "Uploading 20MB file (should split into 4 parts)..."
    ${AWS_CMD} cp "${LARGE_FILE}" "s3://${BUCKET_NAME}/${LARGE_FILE_KEY}"
    if [ $? -ne 0 ]; then
        print_error "Failed to upload large file via multipart."
    fi

    UPLOADED_SIZE=$(${AWS_API_CMD} head-object --bucket "${BUCKET_NAME}" --key "${LARGE_FILE_KEY}" --query 'ContentLength' --output text)
    LOCAL_SIZE=$(stat -c%s "${LARGE_FILE}" 2>/dev/null || stat -f%z "${LARGE_FILE}")

    if [ "${UPLOADED_SIZE}" -ne "${LOCAL_SIZE}" ]; then
        print_error "Multipart upload failed size verification. Expected ${LOCAL_SIZE} bytes, got ${UPLOADED_SIZE} bytes."
    fi

    ETAG=$(${AWS_API_CMD} head-object --bucket "${BUCKET_NAME}" --key "${LARGE_FILE_KEY}" --query 'ETag' --output text)
    if [[ "${ETAG}" != *-*\" ]]; then
        print_warning "Upload succeeded, but ETag (${ETAG}) does not look like a standard multipart ETag."
    else
        print_success "ETag (${ETAG}) confirms standard multipart format."
    fi
    print_success "Multipart upload completed and verified."
fi

#####################################################################
# TEST: Directory Download
#####################################################################
if should_run "sync_down"; then
    print_header "[sync_down] Testing Directory Download & Integrity Check"
    mkdir -p "${DOWNLOAD_DIR}"
    ${AWS_CMD} sync "s3://${BUCKET_NAME}/" "${DOWNLOAD_DIR}/"
    if [ $? -ne 0 ]; then
        print_error "Failed to download bucket content."
    fi
    
    rm -f "${TEST_DIR}/large_multipart_file.dat"
    rm -f "${DOWNLOAD_DIR}/multipart/large_multipart_file.dat"
    rmdir "${DOWNLOAD_DIR}/multipart" 2>/dev/null

    diff -r "${TEST_DIR}" "${DOWNLOAD_DIR}"
    if [ $? -ne 0 ]; then
        print_error "Data integrity check failed. Original and downloaded directories differ."
    fi
    print_success "Downloaded files are identical to the original files."
fi

#####################################################################
# TEST: Single Object Deletion
#####################################################################
if should_run "rm_single"; then
    print_header "[rm_single] Testing Single Object Deletion (rm)"
    OBJECT_TO_DELETE="logs/app.log"
    ${AWS_CMD} rm "s3://${BUCKET_NAME}/${OBJECT_TO_DELETE}"
    if [ $? -ne 0 ]; then
        print_error "Failed to delete object ${OBJECT_TO_DELETE}."
    fi
    ${AWS_CMD} ls "s3://${BUCKET_NAME}/${OBJECT_TO_DELETE}" &>/dev/null
    if [ $? -eq 0 ]; then
        print_error "Object ${OBJECT_TO_DELETE} still exists after deletion."
    fi
    print_success "Object ${OBJECT_TO_DELETE} deleted successfully."
fi

#####################################################################
# TEST: Recursive Deletion
#####################################################################
if should_run "rm_recursive"; then
    print_header "[rm_recursive] Testing Recursive Deletion (rm --recursive)"
    PREFIX_TO_DELETE="images/"
    ${AWS_CMD} rm "s3://${BUCKET_NAME}/${PREFIX_TO_DELETE}" --recursive
    if [ $? -ne 0 ]; then
        print_error "Failed to recursively delete objects with prefix ${PREFIX_TO_DELETE}."
    fi
    REMAINING_IMAGES=$(${AWS_CMD} ls "s3://${BUCKET_NAME}/${PREFIX_TO_DELETE}" --recursive | wc -l)
    if [ "${REMAINING_IMAGES}" -ne 0 ]; then
        print_error "Objects with prefix ${PREFIX_TO_DELETE} still exist after recursive delete."
    fi
    print_success "Recursively deleted all objects under prefix '${PREFIX_TO_DELETE}'."
fi

cleanup

if [ -z "${RUN_STEP}" ]; then
    print_success "All tests passed successfully!"
else
    print_success "Test step '${RUN_STEP}' passed successfully!"
fi

exit 0