package eoss3

import (
	"bufio"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
	"github.com/versity/versitygw/s3select"
)

type BackendUnsupported struct{}

func (BackendUnsupported) Shutdown() {}
func (BackendUnsupported) String() string {
	return "Unsupported"
}
func (BackendUnsupported) ListBuckets(context.Context, s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	fmt.Println("ListBuckets")
	return s3response.ListAllMyBucketsResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) HeadBucket(context.Context, *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	fmt.Println("HeadBucket")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) GetBucketAcl(context.Context, *s3.GetBucketAclInput) ([]byte, error) {
	fmt.Println("GetBucketAcl")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) CreateBucket(context.Context, *s3.CreateBucketInput, []byte) error {
	fmt.Println("CreateBucket")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) PutBucketAcl(_ context.Context, bucket string, data []byte) error {
	fmt.Println("PutBucketAcl")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) DeleteBucket(_ context.Context, bucket string) error {
	fmt.Println("DeleteBucket")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) PutBucketVersioning(_ context.Context, bucket string, status types.BucketVersioningStatus) error {
	fmt.Println("PutBucketVersioning")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) GetBucketVersioning(_ context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	fmt.Println("GetBucketVersioning")
	return s3response.GetBucketVersioningOutput{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) PutBucketPolicy(_ context.Context, bucket string, policy []byte) error {
	fmt.Println("PutBucketPolicy")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) GetBucketPolicy(_ context.Context, bucket string) ([]byte, error) {
	fmt.Println("GetBucketPolicy")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) DeleteBucketPolicy(_ context.Context, bucket string) error {
	fmt.Println("DeleteBucketPolicy")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) PutBucketOwnershipControls(_ context.Context, bucket string, ownership types.ObjectOwnership) error {
	fmt.Println("PutBucketOwnershipControls")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) GetBucketOwnershipControls(_ context.Context, bucket string) (types.ObjectOwnership, error) {
	fmt.Println("GetBucketOwnershipControls")
	return types.ObjectOwnershipBucketOwnerEnforced, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) DeleteBucketOwnershipControls(_ context.Context, bucket string) error {
	fmt.Println("DeleteBucketOwnershipControls")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) PutBucketCors(context.Context, []byte) error {
	fmt.Println("PutBucketCors")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) GetBucketCors(_ context.Context, bucket string) ([]byte, error) {
	fmt.Println("GetBucketCors")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) DeleteBucketCors(_ context.Context, bucket string) error {
	fmt.Println("DeleteBucketCors")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (BackendUnsupported) CreateMultipartUpload(context.Context, s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	fmt.Println("CreateMultipartUpload")
	return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	fmt.Println("CompleteMultipartUpload")
	return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput) error {
	fmt.Println("AbortMultipartUpload")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) ListMultipartUploads(context.Context, *s3.ListMultipartUploadsInput) (s3response.ListMultipartUploadsResult, error) {
	fmt.Println("ListMultipartUploads")
	return s3response.ListMultipartUploadsResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) ListParts(context.Context, *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	fmt.Println("ListParts")
	return s3response.ListPartsResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) UploadPart(context.Context, *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	fmt.Println("UploadPart")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) UploadPartCopy(context.Context, *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	fmt.Println("UploadPartCopy")
	return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) PutObject(context.Context, s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	fmt.Println("PutObject")
	return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) HeadObject(context.Context, *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	fmt.Println("HeadObject")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) GetObject(context.Context, *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	fmt.Println("GetObject")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) GetObjectAcl(context.Context, *s3.GetObjectAclInput) (*s3.GetObjectAclOutput, error) {
	fmt.Println("GetObjectAcl")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) GetObjectAttributes(context.Context, *s3.GetObjectAttributesInput) (s3response.GetObjectAttributesResponse, error) {
	fmt.Println("GetObjectAttributes")
	return s3response.GetObjectAttributesResponse{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) CopyObject(context.Context, s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	fmt.Println("CopyObject")
	return s3response.CopyObjectOutput{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) ListObjects(context.Context, *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	fmt.Println("ListObjects")
	return s3response.ListObjectsResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) ListObjectsV2(context.Context, *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	fmt.Println("ListObjectsV2")
	return s3response.ListObjectsV2Result{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) DeleteObject(context.Context, *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	fmt.Println("DeleteObject")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) DeleteObjects(context.Context, *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	fmt.Println("DeleteObjects")
	return s3response.DeleteResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) PutObjectAcl(context.Context, *s3.PutObjectAclInput) error {
	fmt.Println("PutObjectAcl")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) RestoreObject(context.Context, *s3.RestoreObjectInput) error {
	fmt.Println("RestoreObject")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) SelectObjectContent(ctx context.Context, input *s3.SelectObjectContentInput) func(w *bufio.Writer) {
	return func(w *bufio.Writer) {
		var getProgress s3select.GetProgress
		progress := input.RequestProgress
		if progress != nil && *progress.Enabled {
			getProgress = func() (bytesScanned int64, bytesProcessed int64) {
				return -1, -1
			}
		}
		mh := s3select.NewMessageHandler(ctx, w, getProgress)
		apiErr := s3err.GetAPIError(s3err.ErrNotImplemented)
		mh.FinishWithError(apiErr.Code, apiErr.Description)
	}
}

func (BackendUnsupported) ListObjectVersions(context.Context, *s3.ListObjectVersionsInput) (s3response.ListVersionsResult, error) {
	fmt.Println("ListObjectVersions")
	return s3response.ListVersionsResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) GetBucketTagging(_ context.Context, bucket string) (map[string]string, error) {
	fmt.Println("GetBucketTagging")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) PutBucketTagging(_ context.Context, bucket string, tags map[string]string) error {
	fmt.Println("PutBucketTagging")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) DeleteBucketTagging(_ context.Context, bucket string) error {
	fmt.Println("DeleteBucketTagging")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) GetObjectTagging(_ context.Context, bucket, object string) (map[string]string, error) {
	fmt.Println("GetObjectTagging")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) PutObjectTagging(_ context.Context, bucket, object string, tags map[string]string) error {
	fmt.Println("PutObjectTagging")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) DeleteObjectTagging(_ context.Context, bucket, object string) error {
	fmt.Println("DeleteObjectTagging")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) PutObjectLockConfiguration(_ context.Context, bucket string, config []byte) error {
	fmt.Println("PutObjectLockConfiguration")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) GetObjectLockConfiguration(_ context.Context, bucket string) ([]byte, error) {
	fmt.Println("GetObjectLockConfiguration")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) PutObjectRetention(_ context.Context, bucket, object, versionId string, bypass bool, retention []byte) error {
	fmt.Println("PutObjectRetention")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) GetObjectRetention(_ context.Context, bucket, object, versionId string) ([]byte, error) {
	fmt.Println("GetObjectRetention")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) PutObjectLegalHold(_ context.Context, bucket, object, versionId string, status bool) error {
	fmt.Println("PutObjectLegalHold")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) GetObjectLegalHold(_ context.Context, bucket, object, versionId string) (*bool, error) {
	fmt.Println("GetObjectLegalHold")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (BackendUnsupported) ChangeBucketOwner(_ context.Context, bucket string, acl []byte) error {
	fmt.Println("ChangeBucketOwner")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (BackendUnsupported) ListBucketsAndOwners(context.Context) ([]s3response.Bucket, error) {
	fmt.Println("ListBucketsAndOwners")
	return []s3response.Bucket{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
