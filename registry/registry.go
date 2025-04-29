package registry

import (
	"errors"
	"time"
)

// Mapping holds the information for mapping a bucket
// with the real path on EOS.
type Mapping struct {
	// Bucket is the name of the bucket.
	Bucket string
	// Path is the real path on EOS.
	Path string
	// CreatedAt is the creation time of the bucket.
	// Might be different from the actualt ctime of
	// the corresponding folder in EOS.
	CreatedAt time.Time
}

type Registry interface {
	CreateMapping(mapping Mapping) error
	GetMapping(bucket string) (Mapping, error)
	DeleteMapping(bucket string) error
	ListMappings() ([]Mapping, error)

	AssignBucket(bucket string, uid int) error
	ListBuckets(uid int) ([]string, error)
	UnassignBucket(bucket string, uid int) error
}

var (
	ErrMappingAlreadyExisting = errors.New("mapping already existing")
	ErrNoSuchBucket           = errors.New("no such bucket")
)

func New(c map[string]any) (Registry, error) {
	driver, ok := c["driver"]
	if !ok {
		driver = "memory"
	}

	switch driver {
	case "memory":
		return NewMemoryRegistry()
	case "local":
		return NewLocalRegistryFromConfig(c)
	}

	return nil, errors.New("registry not found")
}
