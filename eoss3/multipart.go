package eoss3

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gmgigi96/eoss3/eos"
	"github.com/gmgigi96/eoss3/meta"
	"github.com/google/uuid"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func multipartFolder(bucket *meta.Bucket, key, uploadId string) string {
	keyDir, keyLast := filepath.Split(key)
	return filepath.Join(bucket.Path, keyDir, fmt.Sprintf(".multipart.%s.%s", uploadId, keyLast))
}

func (b *EosBackend) CreateMultipartUpload(ctx context.Context, req s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	fmt.Println("CreateMultipartUpload")
	name := *req.Bucket
	key := *req.Key

	acct, ok := getLoggedAccount(ctx)
	if !ok {
		return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrAccessDenied)
	}

	bucket, err := b.meta.GetBucket(name)
	if err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}

	// generate an upload id
	uploadId := uuid.NewString()

	folder := multipartFolder(&bucket, key, uploadId)

	auth := eos.Auth{
		Uid: uint64(acct.UserID),
		Gid: uint64(acct.GroupID),
	}
	if err := b.eos.Mkdir(ctx, auth, folder, 0755); err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}

	return s3response.InitiateMultipartUploadResult{
		Bucket:   name,
		Key:      key,
		UploadId: uploadId,
	}, nil
}

func (b *EosBackend) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput) (_ s3response.CompleteMultipartUploadResult, versionid string, _ error) {
	panic("not yet implemented")
}

func (b *EosBackend) AbortMultipartUpload(ctx context.Context, req *s3.AbortMultipartUploadInput) error {
	fmt.Println("AbortMultipartUpload")
	name := *req.Bucket

	bucket, err := b.meta.GetBucket(name)
	if err != nil {
		return err
	}

	acct, ok := getLoggedAccount(ctx)
	if !ok {
		return s3err.GetAPIError(s3err.ErrAccessDenied)
	}

	auth := eos.Auth{
		Uid: uint64(acct.UserID),
		Gid: uint64(acct.GroupID),
	}

	folder := multipartFolder(&bucket, *req.Key, *req.UploadId)
	return b.eos.Rmdir(ctx, auth, folder)
}

func (b *EosBackend) ListParts(context.Context, *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	panic("not yet implemented")
}

func (b *EosBackend) UploadPart(context.Context, *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	panic("not yet implemented")
}
