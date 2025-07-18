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
	"path"
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
	BackendUnsupported
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
	fmt.Println("ListBuckets")
	fmt.Println(input.IsAdmin)

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

func (b *EosBackend) GetBucketAcl(ctx context.Context, req *s3.GetBucketAclInput) ([]byte, error) {
	fmt.Println("GetBucketAcl func")

	// The result is a json of the struct auth.ACL
	return nil, nil
}

func (b *EosBackend) CreateBucket(ctx context.Context, req *s3.CreateBucketInput, acl []byte) error {
	fmt.Println("CreateBucket")

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
	if defaultPath == "" {
		return s3err.GetAPIError(s3err.ErrInvalidQueryParams)
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

func (b *EosBackend) DeleteBucket(ctx context.Context, name string) error {
	fmt.Println("DeleteBucket")

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

func generateBucketPolicy(sid, username, effect, bucket string) string {
	s := fmt.Sprintf(
		`{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "%s",
            "Effect": "%s",
            "Principal": {
                "AWS": "%s"
            },
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::%s",
                "arn:aws:s3:::%s/*"
            ]
        }
    ]
}`, sid, effect, username, bucket, bucket)
	return s
}

func (b *EosBackend) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	fmt.Println("GetBucketPolicy func")

	acct, ok := getLoggedAccount(ctx)
	if !ok {
		return nil, s3err.GetAPIError(s3err.ErrAccessDenied)
	}

	auth := eos.Auth{
		Uid: uint64(acct.UserID),
		Gid: uint64(acct.GroupID),
	}

	var policy string
	if b.meta.IsAssigned(bucket, acct.UserID) {
		policy = generateBucketPolicy("AllowAllActionsToUser", auth.Username(), "Allow", bucket)
	} else {
		policy = generateBucketPolicy("DenyAllActionsToUser", auth.Username(), "Deny", bucket)
	}
	return []byte(policy), nil
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
	fmt.Println("HeadObject")

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
	fmt.Println("GetObject")

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
	fmt.Println("ListObjects")
	name := *req.Bucket
	prefix := *req.Prefix

	bucket, err := b.meta.GetBucket(name)
	if err != nil {
		return s3response.ListObjectsResult{}, err
	}

	objdir, fileprefix := retrieveObjectDirectory(bucket.Path, prefix)

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
		// filters.Prefix = &fileprefix
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

func eosAuthFromLoggedUser(ctx context.Context) eos.Auth {
	acct, _ := getLoggedAccount(ctx)
	return eos.Auth{
		Uid: uint64(acct.UserID),
		Gid: uint64(acct.GroupID),
	}
}

func (b *EosBackend) ListObjectsV2(ctx context.Context, req *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	fmt.Println("ListObjectsV2")

	name := *req.Bucket
	prefix := *req.Prefix
	delimiter := *req.Delimiter

	// According to the S3 specs, for directory buckets the
	// only delimiter allowed is "/". So, without a delimiter
	// we interpret the request as being "recursive".
	if delimiter != "" && delimiter != "/" {
		return s3response.ListObjectsV2Result{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}

	// For directory buckets only prefixes ending with "/" are supported
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		return s3response.ListObjectsV2Result{}, s3err.GetAPIError(s3err.ErrInvalidRequest)
	}

	recursive := false
	if delimiter == "" {
		recursive = true
	}

	bucket, err := b.meta.GetBucket(name)
	if err != nil {
		// TODO: improve this error
		return s3response.ListObjectsV2Result{}, err
	}

	folder := path.Join(bucket.Path, prefix)

	var objects []s3response.Object
	var prefixes []types.CommonPrefix
	prefixesSet := map[string]struct{}{}

	appendObjects := func(md *erpc.MDResponse) {
		obj := b.mdResponseToS3Object(bucket.Path, md)
		if isHiddenResource(*obj.Key) {
			return
		}
		if delimiter == "/" && md.Type == erpc.TYPE_CONTAINER {
			// we should group by prefix and not add this obj
			// in the list of objects
			if _, ok := prefixesSet[*obj.Key]; ok {
				return
			}

			p := types.CommonPrefix{
				Prefix: obj.Key,
			}
			prefixes = append(prefixes, p)
			prefixesSet[*obj.Key] = struct{}{}
			return
		}

		if md.Type != erpc.TYPE_CONTAINER {
			objects = append(objects, obj)
		}
	}

	filters := &eos.ListDirFilters{
		Recursive: recursive,
	}

	if err := b.eos.ListDir(ctx, eosAuthFromLoggedUser(ctx), folder, appendObjects, filters); err != nil {
		// TODO: improve this error
		return s3response.ListObjectsV2Result{}, err
	}

	return s3response.ListObjectsV2Result{
		Name:           &name,
		Prefix:         &prefix,
		Delimiter:      &delimiter,
		Contents:       objects,
		CommonPrefixes: prefixes,
	}, nil
}

func Ptr[T any](v T) *T {
	return &v
}

func (b *EosBackend) DeleteObject(ctx context.Context, req *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	fmt.Println("DeleteObject")

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
	if err := b.eos.Remove(ctx, auth, objpath, false); err != nil {
		return nil, err
	}

	return &s3.DeleteObjectOutput{}, nil
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

func (b *EosBackend) GetObjectLockConfiguration(_ context.Context, bucket string) ([]byte, error) {
	fmt.Println("GetObjectLockConfiguration")
	return []byte("{}"), nil
}
