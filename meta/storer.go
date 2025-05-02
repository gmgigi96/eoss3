package meta

import (
	"errors"
	"time"
)

// Mapping holds the information for mapping a bucket
// with the real path on EOS.
type Bucket struct {
	// Name is the name of the bucket.
	Name string
	// Path is the real path on EOS.
	Path string
	// CreatedAt is the creation time of the bucket.
	// Might be different from the actualt ctime of
	// the corresponding folder in EOS.
	CreatedAt time.Time
}

type BucketStorer interface {
	CreateBucket(bucket Bucket) error
	GetBucket(name string) (Bucket, error)
	DeleteBucket(name string) error
	ListBuckets() ([]Bucket, error)

	AssignBucket(name string, uid int) error
	IsAssigned(name string, uid int) bool
	ListBucketsByUser(uid int) ([]string, error)
	UnassignBucket(name string, uid int) error

	GetDefaultBucketPath(uid int) (string, error)
	StoreDefaultBucketPath(uid int, path string) error
}

var (
	ErrBucketAlreadyExisting = errors.New("bucket already existing")
	ErrNoSuchBucket          = errors.New("no such bucket")
)

func New(c map[string]any) (BucketStorer, error) {
	driver, ok := c["driver"]
	if !ok {
		driver = "memory"
	}

	switch driver {
	case "memory":
		return NewInMemoryBucketStorer()
	case "local":
		return NewLocalBucketStorerFromConfig(c)
	}

	return nil, errors.New("registry not found")
}
