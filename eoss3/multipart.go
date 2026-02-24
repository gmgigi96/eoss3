package eoss3

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	go_eosgrpc "github.com/cern-eos/go-eosgrpc"
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

func (b *EosBackend) CompleteMultipartUpload(ctx context.Context, req *s3.CompleteMultipartUploadInput) (_ s3response.CompleteMultipartUploadResult, versionId string, _ error) {
	fmt.Println("CompleteMultipartUpload")
	name := *req.Bucket

	// TODO: check that all parts have been provided
	// This implementation is very inefficient. We could use in the future
	// the clone mechanism to not actually copy the parts.

	bucket, err := b.meta.GetBucket(name)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	folder := multipartFolder(&bucket, *req.Key, *req.UploadId)

	acct, ok := getLoggedAccount(ctx)
	if !ok {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrAccessDenied)
	}

	auth := eos.Auth{
		Uid: uint64(acct.UserID),
		Gid: uint64(acct.GroupID),
	}

	tmpFile := filepath.Join(folder, "tmp")

	var offset uint64
	if err := b.eos.ListDir(ctx, auth, folder, func(m *go_eosgrpc.MDResponse) {
		// here we read the part and we inject the part into tmpFile
		if m.Type != go_eosgrpc.TYPE_FILE {
			return
		}

		src := string(m.Fmd.Path)
		data, length, err := b.eos.Download(ctx, auth, src)
		if err != nil {
			panic(err) // TODO: we need to return here and stop everything
		}
		defer data.Close()

		if err := b.eos.Upload(ctx, auth, tmpFile, data, uint64(length), &offset); err != nil {
			panic(err) // TODO: we need to return here and stop everything
		}
		offset += uint64(length)
	}, nil); err != nil {
		// TODO: should we do a cleanup?
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	if err := b.eos.Rename(ctx, auth, tmpFile, filepath.Join(bucket.Path, *req.Key)); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	err = b.eos.Rmdir(ctx, auth, folder)
	return s3response.CompleteMultipartUploadResult{
		Bucket: req.Bucket,
		Key:    req.Key,
	}, "", err
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

func (b *EosBackend) UploadPart(ctx context.Context, req *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	fmt.Println("UploadPart")
	name := *req.Bucket

	bucket, err := b.meta.GetBucket(name)
	if err != nil {
		return nil, err
	}

	acct, ok := getLoggedAccount(ctx)
	if !ok {
		return nil, s3err.GetAPIError(s3err.ErrAccessDenied)
	}

	auth := eos.Auth{
		Uid: uint64(acct.UserID),
		Gid: uint64(acct.GroupID),
	}

	// TODO: we should check if the upload id is correct
	partFile := filepath.Join(multipartFolder(&bucket, *req.Key, *req.UploadId), fmt.Sprintf("%05d", *req.PartNumber))

	err = b.eos.Upload(ctx, auth, partFile, req.Body, uint64(*req.ContentLength), nil)
	return &s3.UploadPartOutput{}, err
}
