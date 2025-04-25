package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	erpc "github.com/cern-eos/go-eosgrpc"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
	"github.com/versity/versitygw/s3select"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	// URL of the EOS MGM GRPC server
	GrpcURL string `mapstructure:"grpc_url"`
	// HttpURL if the EOS HTTP server
	HttpURL string `mapstructure:"http_url"`
	// Authkey is the key that authorizes this client to connect to the EOS GRPC service
	Authkey string `mapstructure:"authkey"`
	// MountDir is the directory from where the s3 gateway is mounted
	MountDir string `mapstructure:"mount_dir"`

	// Uid is the user id doing the grpc request
	Uid int `mapstructure:"uid"`
	// Gid is the group id doing the grpc request
	Gid int `mapstructure:"gid"`

	Username string `mapstructure:"username"`
}

func (c *Config) Validate() error {
	if c.GrpcURL == "" {
		return errors.New("grpc_url not provided")
	}

	if c.HttpURL == "" {
		return errors.New("http_url not provided")
	}

	if c.Authkey == "" {
		return errors.New("authkey not provided")
	}

	if c.Username == "" {
		return errors.New("username not provided")
	}

	if c.MountDir == "" {
		c.MountDir = "/"
	}

	return nil
}

type EosBackend struct {
	cfg  *Config
	conn *grpc.ClientConn
	cl   erpc.EosClient
	hcl  *EOSHTTPClient
}

func New(cfg *Config) (*EosBackend, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(cfg.GrpcURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("error getting grpc client: %w", err)
	}

	cl := erpc.NewEosClient(conn)
	hcl, err := NewEOSHTTPClient(&HTTPConfig{
		ServerURL: cfg.HttpURL,
		Authkey:   cfg.Authkey,
		Uid:       cfg.Uid,
		Gid:       cfg.Gid,
		Username:  cfg.Username,
	})
	if err != nil {
		return nil, err
	}

	be := &EosBackend{
		cfg:  cfg,
		conn: conn,
		cl:   cl,
		hcl:  hcl,
	}
	return be, nil
}

func (b *EosBackend) Shutdown() {
	_ = b.conn.Close()
}

func (b *EosBackend) String() string { return "EOS" }

func isVersionFolder(name string) bool {
	return strings.HasPrefix(name, ".sys.v#.")
}

func (b *EosBackend) ListBuckets(ctx context.Context, req s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	fdrq := &erpc.FindRequest{
		Type: erpc.TYPE_LISTING,
		Id: &erpc.MDId{
			Path: []byte(b.cfg.MountDir),
		},
		Role: &erpc.RoleId{
			Uid: uint64(b.cfg.Uid),
			Gid: uint64(b.cfg.Gid),
		},
		Maxdepth: 1,
		Authkey:  b.cfg.Authkey,
	}

	res, err := b.cl.Find(ctx, fdrq)
	if err != nil {
		return s3response.ListAllMyBucketsResult{}, s3err.GetAPIError(s3err.ErrInternalError)
	}

	var listRes s3response.ListAllMyBucketsResult

	i := 0
	for {
		r, err := res.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			return s3response.ListAllMyBucketsResult{}, err
		}

		// only folders can be buckets
		if r.Type == erpc.TYPE_FILE || r.Cmd == nil {
			continue
		}

		i++
		if i == 1 {
			// first entry is the folder itself
			continue
		}

		name := string(r.Cmd.Name)
		if !strings.HasPrefix(name, req.Prefix) {
			continue
		}

		if isVersionFolder(name) {
			continue
		}

		entry := s3response.ListAllMyBucketsEntry{
			Name:         name,
			CreationDate: time.Unix(int64(r.Cmd.Ctime.Sec), int64(r.Cmd.Ctime.NSec)),
		}

		listRes.Buckets.Bucket = append(listRes.Buckets.Bucket, entry)
	}

	return listRes, nil
}

func (b *EosBackend) HeadBucket(context.Context, *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	fmt.Println("HeadBucket func")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) GetBucketAcl(ctx context.Context, req *s3.GetBucketAclInput) ([]byte, error) {
	fmt.Println("GetBucketAcl func")

	// The result is a json of the struct auth.ACL

	// r := b.newNsRequest(ctx)
	// path := path.Join(b.cfg.MountDir, *req.Bucket)

	// r.Command = &erpc.NSRequest_Acl{
	// 	Acl: &erpc.NSRequest_AclRequest{
	// 		Id: &erpc.MDId{
	// 			Path: []byte(path),
	// 		},
	// 		Cmd:  erpc.NSRequest_AclRequest_LIST,
	// 		Type: erpc.NSRequest_AclRequest_SYS_ACL,
	// 	},
	// }

	// res, err := b.cl.Exec(ctx, r)
	// if err != nil {
	// 	return nil, err
	// }

	// res.Acl.

	return nil, nil
}

func (b *EosBackend) newNsRequest(_ context.Context) *erpc.NSRequest {
	return &erpc.NSRequest{
		Role: &erpc.RoleId{
			Uid: uint64(b.cfg.Uid),
			Gid: uint64(b.cfg.Gid),
		},
		Authkey: b.cfg.Authkey,
	}
}

func (b *EosBackend) CreateBucket(ctx context.Context, req *s3.CreateBucketInput, acl []byte) error {
	fmt.Println("CreateBucket func")
	r := b.newNsRequest(ctx)

	path := path.Join(b.cfg.MountDir, *req.Bucket)

	r.Command = &erpc.NSRequest_Mkdir{
		Mkdir: &erpc.NSRequest_MkdirRequest{
			Id: &erpc.MDId{
				Path: []byte(path),
			},
			Recursive: true,
			Mode:      0750,
		},
	}

	res, err := b.cl.Exec(ctx, r)
	if err != nil {
		return err
	}

	if res.Error != nil && res.Error.Code != 0 {
		// TODO: check error code
		fmt.Println(res.Error)
		return s3err.GetAPIError(s3err.ErrInternalError)
	}

	return nil
}

func (b *EosBackend) PutBucketAcl(_ context.Context, bucket string, data []byte) error {
	fmt.Println("PutBucketAcl func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) DeleteBucket(ctx context.Context, bucket string) error {
	fmt.Println("DeleteBucket func")
	r := b.newNsRequest(ctx)
	path := path.Join(b.cfg.MountDir, bucket)

	r.Command = &erpc.NSRequest_Rmdir{
		Rmdir: &erpc.NSRequest_RmdirRequest{
			Id: &erpc.MDId{
				Path: []byte(path),
			},
		},
	}

	res, err := b.cl.Exec(ctx, r)
	if err != nil {
		return err
	}

	if res.Error != nil && res.Error.Code != 0 {
		fmt.Println(res.Error)
		return s3err.GetAPIError(s3err.ErrInternalError)
	}

	return nil
}

func (b *EosBackend) PutBucketVersioning(_ context.Context, bucket string, status types.BucketVersioningStatus) error {
	fmt.Println("PutBucketVersioning func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) GetBucketVersioning(_ context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	fmt.Println("GetBucketVersioning func")
	return s3response.GetBucketVersioningOutput{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) PutBucketPolicy(_ context.Context, bucket string, policy []byte) error {
	fmt.Println("PutBucketPolicy func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) GetBucketPolicy(_ context.Context, bucket string) ([]byte, error) {
	fmt.Println("GetBucketPolicy func")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) DeleteBucketPolicy(_ context.Context, bucket string) error {
	fmt.Println("DeleteBucketPolicy func")
	return nil
}

func (b *EosBackend) PutBucketOwnershipControls(_ context.Context, bucket string, ownership types.ObjectOwnership) error {
	fmt.Println("PutBucketOwnershipControls func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) GetBucketOwnershipControls(_ context.Context, bucket string) (types.ObjectOwnership, error) {
	fmt.Println("GetBucketOwnershipControls func")
	return types.ObjectOwnershipBucketOwnerEnforced, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) DeleteBucketOwnershipControls(_ context.Context, bucket string) error {
	fmt.Println("DeleteBucketOwnershipControls func")
	return nil
}

func (b *EosBackend) PutBucketCors(context.Context, []byte) error {
	fmt.Println("PutBucketCors func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) GetBucketCors(_ context.Context, bucket string) ([]byte, error) {
	fmt.Println("GetBucketCors func")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) DeleteBucketCors(_ context.Context, bucket string) error {
	fmt.Println("DeleteBucketCors func")
	return nil
}

func (b *EosBackend) CreateMultipartUpload(context.Context, s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	fmt.Println("CreateMultipartUpload func")
	return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
	fmt.Println("CompleteMultipartUpload func")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput) error {
	fmt.Println("AbortMultipartUpload func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) ListMultipartUploads(context.Context, *s3.ListMultipartUploadsInput) (s3response.ListMultipartUploadsResult, error) {
	fmt.Println("ListMultipartUploads func")
	return s3response.ListMultipartUploadsResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) ListParts(context.Context, *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	fmt.Println("ListParts func")
	return s3response.ListPartsResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) UploadPart(context.Context, *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	fmt.Println("UploadPart func")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) UploadPartCopy(context.Context, *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	fmt.Println("UploadPartCopy func")
	return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) PutObject(ctx context.Context, po s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	fmt.Println("PutObject func")
	return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) stat(ctx context.Context, path string, bucket bool) (*erpc.MDResponse, error) {
	req := &erpc.MDRequest{
		Type: erpc.TYPE_STAT,
		Id: &erpc.MDId{
			Path: []byte(path),
		},
		Authkey: b.cfg.Authkey,
		Role: &erpc.RoleId{
			Uid: uint64(b.cfg.Uid),
			Gid: uint64(b.cfg.Gid),
		},
	}
	res, err := b.cl.MD(ctx, req)
	if err != nil {
		return nil, err
	}

	r, err := res.Recv()
	if err != nil {
		// TODO: this is very bad, but there is no other way
		// to get this error if the entry does not exist
		if bucket {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchBucket)
		}
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	return r, nil
}

func (b *EosBackend) HeadObject(ctx context.Context, req *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	fmt.Println("HeadObject func")
	bucket := *req.Bucket
	key := *req.Key

	p := filepath.Join(b.cfg.MountDir, bucket, key)

	r, err := b.stat(ctx, p, false)
	if err != nil {
		return nil, err
	}

	if r.Type == erpc.TYPE_CONTAINER || r.Fmd == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	return &s3.HeadObjectOutput{
		ContentLength: Ptr(int64(r.Fmd.Size)),
		ETag:          &r.Fmd.Etag,
		LastModified:  Ptr(time.Unix(int64(r.Fmd.Mtime.Sec), int64(r.Fmd.Mtime.NSec))),
	}, nil
}

func (b *EosBackend) GetObject(ctx context.Context, req *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	fmt.Println("GetObject func")

	bucket := *req.Bucket
	key := *req.Key

	p := filepath.Join(b.cfg.MountDir, bucket, key)

	r, err := b.stat(ctx, p, false)
	if err != nil {
		return nil, err
	}

	if r.Fmd == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	file, err := b.hcl.Get(ctx, p)
	if err != nil {
		return nil, err
	}

	return &s3.GetObjectOutput{
		Body:          file,
		ContentLength: Ptr(int64(r.Fmd.Size)),
	}, nil
}

func (b *EosBackend) GetObjectAcl(context.Context, *s3.GetObjectAclInput) (*s3.GetObjectAclOutput, error) {
	fmt.Println("GetObjectAcl func")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) GetObjectAttributes(context.Context, *s3.GetObjectAttributesInput) (s3response.GetObjectAttributesResponse, error) {
	fmt.Println("GetObjectAttributes func")
	return s3response.GetObjectAttributesResponse{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) CopyObject(context.Context, s3response.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	fmt.Println("CopyObject func")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) ListObjects(ctx context.Context, req *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	fmt.Println("ListObjects func")

	bucket := *req.Bucket
	p := filepath.Join(b.cfg.MountDir, bucket)

	// check if the bucket exists
	_, err := b.stat(ctx, p, true)
	if err != nil {
		return s3response.ListObjectsResult{}, err
	}

	// list the content
	// TODO: consider the option passed, for now we list everything
	// without any filtering, max entries, ...

	findReq := &erpc.FindRequest{
		Type: erpc.TYPE_LISTING,
		Id: &erpc.MDId{
			Path: []byte(p),
		},
		Role: &erpc.RoleId{
			Uid: uint64(b.cfg.Uid),
			Gid: uint64(b.cfg.Gid),
		},
		Authkey:  b.cfg.Authkey,
		Maxdepth: math.MaxUint64, // FIXME: very bad this one
	}
	findRes, err := b.cl.Find(ctx, findReq)
	if err != nil {
		return s3response.ListObjectsResult{}, err
	}

	listRes := s3response.ListObjectsResult{
		Name:        &bucket,
		IsTruncated: Ptr(false),
	}

	for {
		r, err := findRes.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return s3response.ListObjectsResult{}, err
		}

		if r.Type == erpc.TYPE_CONTAINER || r.Fmd == nil {
			continue
		}

		key, _ := filepath.Rel(p, string(r.Fmd.Path))

		listRes.Contents = append(listRes.Contents, s3response.Object{
			ETag:         &r.Fmd.Etag,
			StorageClass: types.ObjectStorageClassStandard,
			LastModified: Ptr(time.Unix(int64(r.Fmd.Mtime.Sec), int64(r.Fmd.Mtime.NSec))),
			Key:          &key,
			Size:         Ptr(int64(r.Fmd.Size)),
			Owner: &types.Owner{
				// TODO: check this
				ID: Ptr(strconv.FormatUint(r.Fmd.Uid, 10)),
			},
		})
	}

	return listRes, nil
}

func Ptr[T any](v T) *T {
	return &v
}

func (b *EosBackend) ListObjectsV2(context.Context, *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	fmt.Println("ListObjectsV2 func")
	return s3response.ListObjectsV2Result{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) DeleteObject(ctx context.Context, req *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	fmt.Println("DeleteObject func")
	bucket := *req.Bucket
	key := *req.Key

	p := filepath.Join(b.cfg.MountDir, bucket, key)

	// we have to check that this is an actual file on EOS
	_, err := b.stat(ctx, p, false)
	if err != nil {
		return nil, err
	}

	r := b.newNsRequest(ctx)
	r.Command = &erpc.NSRequest_Rm{
		Rm: &erpc.NSRequest_RmRequest{
			Id: &erpc.MDId{
				Path: []byte(p),
			},
		},
	}
	res, err := b.cl.Exec(ctx, r)
	if err != nil {
		return nil, err
	}

	if res.Error.Code != 0 {
		return nil, fmt.Errorf("got error (%d): %s", res.Error.Code, res.Error.Msg)
	}

	return &s3.DeleteObjectOutput{}, nil
}

func (b *EosBackend) DeleteObjects(context.Context, *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	fmt.Println("DeleteObjects func")
	return s3response.DeleteResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) PutObjectAcl(context.Context, *s3.PutObjectAclInput) error {
	fmt.Println("PutObjectAcl func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) RestoreObject(context.Context, *s3.RestoreObjectInput) error {
	fmt.Println("RestoreObject func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) SelectObjectContent(ctx context.Context, input *s3.SelectObjectContentInput) func(w *bufio.Writer) {
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

func (b *EosBackend) ListObjectVersions(context.Context, *s3.ListObjectVersionsInput) (s3response.ListVersionsResult, error) {
	fmt.Println("ListObjectVersions func")
	return s3response.ListVersionsResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) GetBucketTagging(_ context.Context, bucket string) (map[string]string, error) {
	fmt.Println("GetBucketTagging func")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) PutBucketTagging(_ context.Context, bucket string, tags map[string]string) error {
	fmt.Println("PutBucketTagging func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) DeleteBucketTagging(_ context.Context, bucket string) error {
	fmt.Println("DeleteBucketTagging func")
	return nil
}

func (b *EosBackend) GetObjectTagging(_ context.Context, bucket, object string) (map[string]string, error) {
	fmt.Println("GetObjectTagging func")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) PutObjectTagging(_ context.Context, bucket, object string, tags map[string]string) error {
	fmt.Println("PutObjectTagging func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) DeleteObjectTagging(_ context.Context, bucket, object string) error {
	fmt.Println("DeleteObjectTagging func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) PutObjectLockConfiguration(_ context.Context, bucket string, config []byte) error {
	fmt.Println("PutObjectLockConfiguration func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) GetObjectLockConfiguration(_ context.Context, bucket string) ([]byte, error) {
	fmt.Println("GetObjectLockConfiguration func")
	return []byte("{}"), nil // TODO
}

func (b *EosBackend) PutObjectRetention(_ context.Context, bucket, object, versionId string, bypass bool, retention []byte) error {
	fmt.Println("PutObjectRetention func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) GetObjectRetention(_ context.Context, bucket, object, versionId string) ([]byte, error) {
	fmt.Println("GetObjectRetention func")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) PutObjectLegalHold(_ context.Context, bucket, object, versionId string, status bool) error {
	fmt.Println("PutObjectLegalHold func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) GetObjectLegalHold(_ context.Context, bucket, object, versionId string) (*bool, error) {
	fmt.Println("GetObjectLegalHold func")
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) ChangeBucketOwner(_ context.Context, bucket string, acl []byte) error {
	fmt.Println("ChangeBucketOwner func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) ListBucketsAndOwners(context.Context) ([]s3response.Bucket, error) {
	fmt.Println("ListBucketsAndOwners func")
	return []s3response.Bucket{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
