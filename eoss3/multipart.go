package eoss3

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	go_eosgrpc "github.com/cern-eos/go-eosgrpc"
	"github.com/gmgigi96/eoss3/eos"
	"github.com/gmgigi96/eoss3/meta"
	"github.com/google/uuid"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func multipartFolder(bucket *meta.Bucket, uploadId string) string {
	return filepath.Join(bucket.Path, fmt.Sprintf(".multipart.%s", uploadId))
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

	folder := multipartFolder(&bucket, uploadId)

	auth := eos.Auth{
		Uid: uint64(acct.UserID),
		Gid: uint64(acct.GroupID),
	}
	if err := b.eos.Mkdir(ctx, auth, folder, 0755); err != nil {
		return s3response.InitiateMultipartUploadResult{}, err
	}

	if err := b.meta.StoreMultipartUpload(bucket.Name, acct.UserID, uploadId, time.Now()); err != nil {
		// TODO: cleanup directory on EOS
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

	folder := multipartFolder(&bucket, *req.UploadId)

	acct, ok := getLoggedAccount(ctx)
	if !ok {
		return s3response.CompleteMultipartUploadResult{}, "", s3err.GetAPIError(s3err.ErrAccessDenied)
	}

	auth := eos.Auth{
		Uid: uint64(acct.UserID),
		Gid: uint64(acct.GroupID),
	}

	tmpFile := filepath.Join(folder, "tmp")

	// compute total size
	var total uint64
	var count int
	if err := b.eos.ListDir(ctx, auth, folder, func(m *go_eosgrpc.MDResponse) {
		if m.Type != go_eosgrpc.TYPE_FILE || isHiddenResource(string(m.Fmd.Path)) {
			return
		}
		total += m.Fmd.Size
		count++
	}, nil); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	// We assume that all the parts have been provided
	var offset uint64
	for p := range count {
		part := filepath.Join(folder, fmt.Sprintf("%05d", p+1))
		fmt.Printf("Considering part %s\n", part)

		data, length, err := b.eos.Download(ctx, auth, part)
		if err != nil {
			panic(err)
		}

		if err := b.eos.UploadChunk(ctx, auth, tmpFile, data, uint64(length), offset, total); err != nil {
			panic(err)
		}
		offset += uint64(length)
	}
	dst := filepath.Join(bucket.Path, *req.Key)
	if err := b.eos.Rename(ctx, auth, tmpFile, dst); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	if err := b.eos.Remove(ctx, auth, folder, true); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	if err := b.meta.DeleteMultipartUpload(bucket.Name, *req.UploadId); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	// get the etag, which is the MD5 of the part
	res, err := b.eos.Stat(ctx, auth, dst)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}

	return s3response.CompleteMultipartUploadResult{
		Bucket: req.Bucket,
		Key:    req.Key,
		ETag:   getMD5(res),
	}, "", nil
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

	folder := multipartFolder(&bucket, *req.UploadId)
	if err := b.eos.Remove(ctx, auth, folder, true); err != nil {
		return err
	}
	if err := b.meta.DeleteMultipartUpload(bucket.Name, *req.UploadId); err != nil {
		return err
	}
	return nil
}

func (b *EosBackend) ListParts(ctx context.Context, req *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	fmt.Println("ListParts")
	name := *req.Bucket

	bucket, err := b.meta.GetBucket(name)
	if err != nil {
		return s3response.ListPartsResult{}, err
	}

	acct, ok := getLoggedAccount(ctx)
	if !ok {
		return s3response.ListPartsResult{}, s3err.GetAPIError(s3err.ErrAccessDenied)
	}

	auth := eos.Auth{
		Uid: uint64(acct.UserID),
		Gid: uint64(acct.GroupID),
	}

	folder := multipartFolder(&bucket, *req.UploadId)
	var parts []s3response.Part
	if err := b.eos.ListDir(ctx, auth, folder, func(m *go_eosgrpc.MDResponse) {
		if m.Type != go_eosgrpc.TYPE_FILE {
			return
		}

		// TODO: we don't have the etag yet
		partNumber, err := strconv.ParseInt(string(m.Fmd.Name), 10, 64)
		if err != nil {
			fmt.Println(err)
			return
		}
		parts = append(parts, s3response.Part{
			PartNumber:   int(partNumber),
			LastModified: time.Unix(int64(m.Fmd.Mtime.Sec), int64(m.Fmd.Mtime.NSec)),
			Size:         int64(m.Fmd.Size),
		})
	}, nil); err != nil {
		return s3response.ListPartsResult{}, err
	}

	return s3response.ListPartsResult{
		Bucket:      name,
		Key:         *req.Key,
		UploadID:    *req.UploadId,
		IsTruncated: false,
		Parts:       parts,
	}, nil
}

func getMD5(r *go_eosgrpc.MDResponse) *string {
	for _, xs := range r.Fmd.Checksums {
		if xs.Type == "md5" {
			val := string(xs.Value)
			return &val
		}
	}
	return nil
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
	partFile := filepath.Join(multipartFolder(&bucket, *req.UploadId), fmt.Sprintf("%05d", *req.PartNumber))

	if err := b.eos.Upload(ctx, auth, partFile, req.Body, uint64(*req.ContentLength)); err != nil {
		return nil, err
	}

	// get the etag, which is the MD5 of the part
	res, err := b.eos.Stat(ctx, auth, partFile)
	if err != nil {
		return nil, err
	}

	return &s3.UploadPartOutput{
		ETag: getMD5(res),
	}, err
}

func (b *EosBackend) ListMultipartUploads(ctx context.Context, req *s3.ListMultipartUploadsInput) (s3response.ListMultipartUploadsResult, error) {
	fmt.Println("ListMultipartUploads")
	name := *req.Bucket

	uploads, err := b.meta.ListMultipartUploads(name)
	if err != nil {
		return s3response.ListMultipartUploadsResult{}, err
	}

	res := s3response.ListMultipartUploadsResult{
		Bucket:      name,
		IsTruncated: false,
	}
	for _, up := range uploads {
		res.Uploads = append(res.Uploads, s3response.Upload{
			UploadID: up.UploadId,
			Initiator: s3response.Initiator{
				ID: strconv.FormatInt(int64(up.Initiator), 10),
			},
			Initiated: up.Initiated,
		})
	}
	return res, nil
}
