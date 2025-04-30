package eoss3

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	erpc "github.com/cern-eos/go-eosgrpc"
	"github.com/gmgigi96/eoss3/eos"
	"github.com/gmgigi96/eoss3/meta"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
	"github.com/versity/versitygw/s3select"
)

type Config struct {
	// URL of the EOS MGM GRPC server
	GrpcURL string `mapstructure:"grpc_url"`
	// HttpURL if the EOS HTTP server
	HttpURL string `mapstructure:"http_url"`
	// Authkey is the key that authorizes this client to connect to the EOS GRPC service
	Authkey string `mapstructure:"authkey"`

	// ComputeMD5 on put so the client will be happy
	// Once EOS will support storing the MD5, this can be retrieved
	// directly from it.
	ComputeMD5 bool `mapstructure:"compute_md5"`
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

	return nil
}

type EosBackend struct {
	cfg *Config

	eos  *eos.Client
	meta meta.BucketStorer
}

func New(cfg *Config, meta meta.BucketStorer) (*EosBackend, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	eosCl, err := eos.NewClient(eos.Config{
		GrpcURL: cfg.GrpcURL,
		HttpURL: cfg.HttpURL,
		AuthKey: cfg.Authkey,
	})
	if err != nil {
		return nil, err
	}

	be := &EosBackend{
		cfg:  cfg,
		eos:  eosCl,
		meta: meta,
	}
	return be, nil
}

func (b *EosBackend) Shutdown() { _ = b.eos.Close() }

func (b *EosBackend) String() string { return "EOS" }

func isHiddenResource(path string) bool {
	return eos.IsVersionFolder(path) || eos.IsAtomicFile(path)
}

func prepareListBucketResult(buckets []meta.Bucket, prefix string, tkn string, max int32) (entries []s3response.ListAllMyBucketsEntry, ctoken string) {
	// TODO: prefix, continuation token and max entries can be moved later to the registry

	i := slices.IndexFunc(buckets, func(bucket meta.Bucket) bool {
		return bucket.Name == tkn
	})
	if i >= 0 {
		buckets = buckets[i:]
	}

	entries = make([]s3response.ListAllMyBucketsEntry, 0, max)
	for i, b := range buckets {
		if i == int(max-1) {
			return entries, b.Name
		}
		if !strings.HasPrefix(b.Name, prefix) {
			continue
		}
		entries = append(entries, s3response.ListAllMyBucketsEntry{
			Name:         b.Name,
			CreationDate: b.CreatedAt,
		})
	}

	return entries, ""
}

func getLoggedAccount(ctx context.Context) (auth.Account, bool) {
	acct, ok := ctx.Value("account").(auth.Account)
	return acct, ok
}

func (b *EosBackend) ListBuckets(ctx context.Context, input s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	var buckets []s3response.ListAllMyBucketsEntry
	var ctoken string
	if input.IsAdmin {
		// returns all the buckets for admin user
		m, err := b.meta.ListBuckets()
		if err != nil {
			return s3response.ListAllMyBucketsResult{}, err
		}
		buckets, ctoken = prepareListBucketResult(m, input.Prefix, input.ContinuationToken, input.MaxBuckets)
	} else {
		acct, ok := getLoggedAccount(ctx)
		if !ok {
			// TODO: can this happen??
			return s3response.ListAllMyBucketsResult{}, errors.New("no user in request")
		}
		bs, err := b.meta.ListBucketsByUser(acct.UserID)
		if err != nil {
			return s3response.ListAllMyBucketsResult{}, err
		}
		lst := make([]meta.Bucket, 0, len(bs))
		for _, name := range bs {
			m, err := b.meta.GetBucket(name)
			if err == nil {
				lst = append(lst, m)
			}
		}
		buckets, ctoken = prepareListBucketResult(lst, input.Prefix, input.ContinuationToken, input.MaxBuckets)
	}

	return s3response.ListAllMyBucketsResult{
		Buckets: s3response.ListAllMyBucketsList{
			Bucket: buckets,
		},
		Owner: s3response.CanonicalUser{
			ID: input.Owner,
		},
		ContinuationToken: ctoken,
		Prefix:            input.Prefix,
	}, nil
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

func (b *EosBackend) CreateBucket(ctx context.Context, req *s3.CreateBucketInput, acl []byte) error {
	name := *req.Bucket

	if _, err := b.meta.GetBucket(name); err == nil {
		return s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
	}

	acct, ok := getLoggedAccount(ctx)
	if !ok {
		return s3err.GetAPIError(s3err.ErrAccessDenied)
	}

	defaultPath, err := b.meta.GetDefaultBucketPath(acct.UserID)
	if err != nil {
		return err
	}

	bucketPath := filepath.Join(defaultPath, name)

	bucket := meta.Bucket{
		Name:      name,
		Path:      bucketPath,
		CreatedAt: time.Now(),
	}
	if err := b.meta.CreateBucket(bucket); err != nil {
		return err
	}

	auth := eos.Auth{
		Uid: uint64(acct.UserID),
		Gid: uint64(acct.GroupID),
	}
	if err := b.eos.Mkdir(ctx, auth, bucketPath, 0755); err != nil {
		return err
	}

	return nil
}

func (b *EosBackend) PutBucketAcl(_ context.Context, name string, data []byte) error {
	fmt.Println("PutBucketAcl func")
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) DeleteBucket(ctx context.Context, name string) error {
	acct, ok := getLoggedAccount(ctx)
	if !ok {
		return s3err.GetAPIError(s3err.ErrAccessDenied)
	}

	bucket, err := b.meta.GetBucket(name)
	if err != nil {
		return err
	}

	auth := eos.Auth{
		Uid: uint64(acct.UserID),
		Gid: uint64(acct.GroupID),
	}
	info, err := b.eos.Stat(ctx, auth, bucket.Path)
	if err != nil {
		return err
	}

	if info.Type != erpc.TYPE_CONTAINER {
		return s3err.GetAPIError(s3err.ErrInternalError)
	}

	if info.Cmd.Containers+info.Cmd.Files != 0 {
		// There are still data inside the folder
		// Remove the bucket is then not possible.
		return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
	}

	if err := b.eos.Rmdir(ctx, auth, bucket.Path); err != nil {
		return err
	}

	return b.meta.DeleteBucket(name)
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

	name := *po.Bucket
	key := *po.Key
	length := *po.ContentLength

	bucket, err := b.meta.GetBucket(name)
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}

	acct, ok := getLoggedAccount(ctx)
	if !ok {
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrAccessDenied)
	}

	// Compute temporarily the chechsum on the gateway side
	// to make the client happy for now. Once EOS has
	// this feature available, this one will be removed from here.
	var hasher hash.Hash
	stream := po.Body
	if b.cfg.ComputeMD5 {
		hasher = md5.New()
		stream = io.TeeReader(po.Body, hasher)
	}

	auth := eos.Auth{
		Uid: uint64(acct.UserID),
		Gid: uint64(acct.GroupID),
	}

	path := filepath.Join(bucket.Path, key)

	// Create recursively all the directories
	if strings.ContainsRune(key, '/') {
		dir := filepath.Dir(path)
		if err := b.eos.Mkdir(ctx, auth, dir, 0755); err != nil {
			return s3response.PutObjectOutput{}, err
		}
	}

	if err := b.eos.Upload(ctx, auth, path, stream, uint64(length)); err != nil {
		return s3response.PutObjectOutput{}, err
	}

	out := s3response.PutObjectOutput{}
	if b.cfg.ComputeMD5 {
		out.ETag = hex.EncodeToString(hasher.Sum(nil))
	}

	return out, nil
}

func (b *EosBackend) HeadObject(ctx context.Context, req *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	name := *req.Bucket
	key := *req.Key

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

	objpath := filepath.Join(bucket.Path, key)
	info, err := b.eos.Stat(ctx, auth, objpath)
	if err != nil {
		return nil, err
	}

	if info.Type != erpc.TYPE_FILE || info.Fmd == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	return &s3.HeadObjectOutput{
		ContentLength: Ptr(int64(info.Fmd.Size)),
		ETag:          &info.Fmd.Etag, // TODO: this is actually the MD5 of the file
		LastModified:  Ptr(time.Unix(int64(info.Fmd.Mtime.Sec), int64(info.Fmd.Mtime.NSec))),
	}, nil
}

func (b *EosBackend) GetObject(ctx context.Context, req *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	name := *req.Bucket
	key := *req.Key

	acct, ok := getLoggedAccount(ctx)
	if !ok {
		return nil, s3err.GetAPIError(s3err.ErrAccessDenied)
	}

	bucket, err := b.meta.GetBucket(name)
	if err != nil {
		return nil, err
	}

	auth := eos.Auth{
		Uid: uint64(acct.UserID),
		Gid: uint64(acct.GroupID),
	}
	path := filepath.Join(bucket.Path, key)

	file, size, err := b.eos.Download(ctx, auth, path)
	if err != nil {
		return nil, err
	}

	if size < 0 {
		// The size is not available
		// A stat is requested to know the size of the file
		info, err := b.eos.Stat(ctx, auth, path)
		if err != nil {
			return nil, err
		}
		if info.Type != erpc.TYPE_FILE {
			return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
		}

		size = int64(info.Fmd.Size)
	}

	return &s3.GetObjectOutput{
		Body:          file,
		ContentLength: &size,
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

// gets the deepest directory by concatenating the bucket path with the prefix, considering
// that the last part of the prefix (after the last /), can be used to filter resources with
// a prefix inside a directory. The new returned prefix will then contain in this case
// the last part after /.
// For example:
//   - bucketpath = "/eos/user/g/gdelmont/bucket" and prefix = "obj"
//     objdir = "/eos/user/g/gdelmont/bucket" and newprefix = "obj"
//   - bucketpath = "/eos/user/g/gdelmont/bucket" and prefix = "obj/"
//     objdir = "/eos/user/g/gdelmont/bucket/obj" and newprefix = ""
//   - bucketpath = "/eos/user/g/gdelmont/bucket" and prefix = "nested/deep/obj"
//     objdir = "/eos/user/g/gdelmont/bucket/nested/deep" and newprefix = "obj"
func retrieveObjectDirectory(bucketPath, prefix string) (objdir, newprefix string) {
	objrel, newprefix := filepath.Split(prefix)
	return filepath.Join(bucketPath, objrel), newprefix
}

func (b *EosBackend) mdResponseToS3Object(bucketDir string, md *erpc.MDResponse) s3response.Object {
	var path string
	if md.Type == erpc.TYPE_CONTAINER {
		path = string(md.Cmd.Path)
	} else {
		path = string(md.Fmd.Path)
	}

	key, _ := filepath.Rel(bucketDir, path)

	var obj s3response.Object
	if md.Type == erpc.TYPE_CONTAINER {
		obj.Key = Ptr(key + "/")
		obj.LastModified = Ptr(time.Unix(int64(md.Cmd.Mtime.Sec), int64(md.Cmd.Mtime.NSec)))
		obj.Size = Ptr(int64(0))
		obj.StorageClass = types.ObjectStorageClassStandard
	} else {
		// TODO: the etag for s3 is the md5 of the resource
		obj.ETag = &md.Fmd.Etag
		obj.StorageClass = types.ObjectStorageClassStandard
		obj.LastModified = Ptr(time.Unix(int64(md.Fmd.Mtime.Sec), int64(md.Fmd.Mtime.NSec)))
		obj.Key = &key
		obj.Size = Ptr(int64(md.Fmd.Size))
		obj.Owner = &types.Owner{
			// TODO: check this
			ID: Ptr(strconv.FormatUint(uint64(md.Fmd.Uid), 10)),
		}
	}
	return obj
}

func (b *EosBackend) ListObjects(ctx context.Context, req *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	name := *req.Bucket
	prefix := *req.Prefix

	bucket, err := b.meta.GetBucket(name)
	if err != nil {
		return s3response.ListObjectsResult{}, err
	}

	objdir, fileprefix := retrieveObjectDirectory(bucket.Name, prefix)

	acct, ok := getLoggedAccount(ctx)
	if !ok {
		return s3response.ListObjectsResult{}, s3err.GetAPIError(s3err.ErrAccessDenied)
	}
	auth := eos.Auth{
		Uid: uint64(acct.UserID),
		Gid: uint64(acct.GroupID),
	}

	var objects []s3response.Object
	appendObjects := func(md *erpc.MDResponse) {
		obj := b.mdResponseToS3Object(bucket.Path, md)
		if isHiddenResource(*obj.Key) {
			return
		}
		objects = append(objects, obj)
	}

	var filters eos.ListDirFilters
	if fileprefix != "" {
		filters.Prefix = &fileprefix
	}

	if err := b.eos.ListDir(ctx, auth, objdir, appendObjects, &filters); err != nil {
		return s3response.ListObjectsResult{}, err
	}

	return s3response.ListObjectsResult{
		Name:      &name,
		Prefix:    &prefix,
		Delimiter: req.Delimiter,
		Contents:  objects,
	}, nil
}

func Ptr[T any](v T) *T {
	return &v
}

func (b *EosBackend) ListObjectsV2(context.Context, *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	fmt.Println("ListObjectsV2 func")
	return s3response.ListObjectsV2Result{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (b *EosBackend) DeleteObject(ctx context.Context, req *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	name := *req.Bucket
	key := *req.Key

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

	objpath := filepath.Join(bucket.Path, key)
	if err := b.eos.Remove(ctx, auth, objpath); err != nil {
		return nil, err
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
