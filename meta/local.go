package meta

import (
	"os"
	"path/filepath"

	"github.com/mitchellh/mapstructure"
)

type LocalRegistry struct {
	base string
}

type Config struct {
	Folder string `mapstructure:"folder"`
}

const (
	bucketsFolder = "buckets"
	usersFolder   = "users"
)

func NewLocalRegistryFromConfig(m map[string]any) (*LocalRegistry, error) {
	var cfg Config
	if err := mapstructure.Decode(m, &cfg); err != nil {
		return nil, err
	}
	return NewLocalRegistry(cfg.Folder)
}

func NewLocalRegistry(folder string) (*LocalRegistry, error) {
	if folder == "" {
		var err error
		folder, err = os.MkdirTemp("", "eoss3")
		if err != nil {
			return nil, err
		}
	}

	r := LocalRegistry{
		base: folder,
	}
	r.init()

	return nil, nil
}

func (r *LocalRegistry) init() error {
	bucketsPath := filepath.Join(r.base, bucketsFolder)
	if err := os.MkdirAll(bucketsPath, 0755); err != nil {
		return err
	}
	usersPath := filepath.Join(r.base, usersFolder)
	if err := os.MkdirAll(usersPath, 0755); err != nil {
		return err
	}
	return nil
}

// func (r *LocalRegistry) CreateMapping(mapping Mapping) error {
// 	if _, err := r.GetMapping(mapping.Bucket); err == nil {
// 		return ErrMappingAlreadyExisting
// 	}

// 	data, err := json.Marshal(mapping)
// 	if err != nil {
// 		return err
// 	}

// 	mappingPath := filepath.Join(r.base, bucketsFolder, mapping.Bucket)
// 	return os.WriteFile(mappingPath, data, 0644)
// }

// func (r *LocalRegistry) GetMapping(bucket string) (Mapping, error) {
// 	mappingPath := filepath.Join(r.base, bucketsFolder, bucket)
// 	data, err := os.ReadFile(mappingPath)
// 	if err != nil {
// 		return Mapping{}, ErrNoSuchBucket
// 	}
// 	var m Mapping
// 	if err := json.Unmarshal(data, &m); err != nil {
// 		return Mapping{}, err
// 	}
// 	return m, nil
// }

// func (r *LocalRegistry) DeleteMapping(bucket string) error {
// 	mappingPath := filepath.Join(r.base, bucketsFolder, bucket)
// 	_ = os.Remove(mappingPath)
// 	return nil
// }

// func (r *LocalRegistry) ListMappings() ([]Mapping, error) {
// 	mappingPath := filepath.Join(r.base, bucketsFolder)
// 	entries, err := os.ReadDir(mappingPath)
// 	if err != nil {
// 		return nil, err
// 	}

// 	list := make([]Mapping, 0, len(entries))
// 	for _, entry := range entries {
// 		bucketInfoPath := filepath.Join(mappingPath, entry.Name())
// 		d, err := os.ReadFile(bucketInfoPath)
// 		if err != nil {
// 			continue
// 		}
// 		var m Mapping
// 		if err := json.Unmarshal(d, &m); err != nil {
// 			continue
// 		}
// 		list = append(list, m)
// 	}
// 	return list, nil
// }

// func (r *LocalRegistry) AssignBucket(bucket string, uid int) error {
// 	uidStr := strconv.FormatInt(int64(uid), 10)
// 	userBucketsPath := filepath.Join(r.base, usersFolder, uidStr)
// 	if err := os.MkdirAll(userBucketsPath, 0755); err != nil {
// 		return err
// 	}
// 	p := filepath.Join(userBucketsPath, bucket)
// 	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
// 	if err != nil {
// 		return err
// 	}
// 	defer f.Close()
// 	return nil
// }

// func (r *LocalRegistry) ListBuckets(uid int) ([]string, error) {
// 	uidStr := strconv.FormatInt(int64(uid), 10)
// 	userBucketsPath := filepath.Join(r.base, usersFolder, uidStr)
// 	entries, err := os.ReadDir(userBucketsPath)
// 	if err != nil {
// 		return []string{}, nil
// 	}

// 	list := make([]string, 0, len(entries))
// 	for _, entry := range entries {
// 		list = append(list, entry.Name())
// 	}
// 	return list, nil
// }

// func (r *LocalRegistry) UnassignBucket(bucket string, uid int) error {
// 	uidStr := strconv.FormatInt(int64(uid), 10)
// 	bucketPath := filepath.Join(r.base, usersFolder, uidStr, bucket)
// 	_ = os.Remove(bucketPath)
// 	return nil
// }
