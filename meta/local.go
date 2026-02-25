package meta

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/mitchellh/mapstructure"
)

type LocalBucketStorer struct {
	base string
}

type Config struct {
	Folder string `mapstructure:"folder"`
}

const (
	bucketsFolder = "buckets"
	usersFolder   = "users"
	uploadsFolder = "uploads"
	metadataFile  = ".metadata"
)

type UserMetadata struct {
	DefaultBucketPath string `json:"default_bucket_path"`
}

func NewLocalBucketStorerFromConfig(m map[string]any) (*LocalBucketStorer, error) {
	var cfg Config
	if err := mapstructure.Decode(m, &cfg); err != nil {
		return nil, err
	}
	return NewLocalBucketStorer(cfg.Folder)
}

func NewLocalBucketStorer(folder string) (*LocalBucketStorer, error) {
	if folder == "" {
		var err error
		folder, err = os.MkdirTemp("", "eoss3")
		if err != nil {
			return nil, err
		}
	}

	s := &LocalBucketStorer{
		base: folder,
	}
	s.init()

	return s, nil
}

func (s *LocalBucketStorer) init() {
	_ = os.MkdirAll(s.bucketFolder(""), 0700)
	_ = os.MkdirAll(s.userFolder(0), 0700)
	_ = os.MkdirAll(s.uploadsFolder(""), 0700)
}

func (s *LocalBucketStorer) bucketFolder(name string) string {
	return filepath.Join(s.base, bucketsFolder, name)
}

func (s *LocalBucketStorer) userFolder(uid int) string {
	var uidstr string
	if uid != 0 {
		uidstr = strconv.FormatInt(int64(uid), 10)
	}
	return filepath.Join(s.base, usersFolder, uidstr)
}

func (s *LocalBucketStorer) uploadsFolder(bucket string) string {
	return filepath.Join(s.base, uploadsFolder, bucket)
}

func (s *LocalBucketStorer) CreateBucket(bucket Bucket) error {
	if _, err := s.GetBucket(bucket.Name); err == nil {
		return ErrBucketAlreadyExisting
	}

	data, err := json.Marshal(bucket)
	if err != nil {
		return err
	}

	return os.WriteFile(s.bucketFolder(bucket.Name), data, 0600)
}

func (s *LocalBucketStorer) GetBucket(name string) (Bucket, error) {
	data, err := os.ReadFile(s.bucketFolder(name))
	if err != nil {
		if os.IsNotExist(err) {
			return Bucket{}, ErrNoSuchBucket
		}
		return Bucket{}, err
	}

	var bucket Bucket
	if err := json.Unmarshal(data, &bucket); err != nil {
		return Bucket{}, err
	}
	return bucket, nil
}

func (s *LocalBucketStorer) DeleteBucket(name string) error {
	_ = os.Remove(s.bucketFolder(name))
	return nil
}

func (s *LocalBucketStorer) ListBuckets() ([]Bucket, error) {
	entries, err := os.ReadDir(s.bucketFolder(""))
	if err != nil {
		return nil, err
	}

	buckets := make([]Bucket, 0, len(entries))
	for _, e := range entries {
		var bucket Bucket
		data, err := os.ReadFile(s.bucketFolder(e.Name()))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &bucket); err != nil {
			return nil, err
		}
		buckets = append(buckets, bucket)
	}
	return buckets, nil
}

func (s *LocalBucketStorer) AssignBucket(name string, uid int) error {
	userpath := s.userFolder(uid)
	if err := os.MkdirAll(userpath, 0700); err != nil {
		return err
	}

	f, err := os.OpenFile(filepath.Join(userpath, name), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	return nil
}

func (s *LocalBucketStorer) IsAssigned(name string, uid int) bool {
	userpath := s.userFolder(uid)

	_, err := os.Stat(filepath.Join(userpath, name))
	return !os.IsNotExist(err)
}

func (s *LocalBucketStorer) ListBucketsByUser(uid int) ([]string, error) {
	userpath := s.userFolder(uid)

	entries, err := os.ReadDir(userpath)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	buckets := make([]string, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if name == metadataFile {
			continue
		}
		buckets = append(buckets, name)
	}
	return buckets, nil
}

func (s *LocalBucketStorer) UnassignBucket(name string, uid int) error {
	userpath := s.userFolder(uid)
	_ = os.Remove(filepath.Join(userpath, name))
	return nil
}

func (s *LocalBucketStorer) metadataFile(uid int) string {
	return filepath.Join(s.userFolder(uid), metadataFile)
}

func (s *LocalBucketStorer) getUserMetadata(uid int) (*UserMetadata, error) {
	metapath := s.metadataFile(uid)
	data, err := os.ReadFile(metapath)
	if err != nil {
		if os.IsNotExist(err) {
			return &UserMetadata{}, nil
		}
		return nil, err
	}

	var meta UserMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (s *LocalBucketStorer) storeUserMetadata(uid int, meta *UserMetadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return os.WriteFile(s.metadataFile(uid), data, 0644)
}

func (s *LocalBucketStorer) GetDefaultBucketPath(uid int) (string, error) {
	meta, err := s.getUserMetadata(uid)
	if err != nil {
		return "", err
	}
	return meta.DefaultBucketPath, nil
}

func (s *LocalBucketStorer) StoreDefaultBucketPath(uid int, path string) error {
	meta, err := s.getUserMetadata(uid)
	if err != nil {
		return err
	}
	meta.DefaultBucketPath = path
	return s.storeUserMetadata(uid, meta)
}

func (s *LocalBucketStorer) StoreMultipartUpload(bucket string, initiator int, uploadId string, initiated time.Time) error {
	uploadsPath := s.uploadsFolder(bucket)
	if err := os.MkdirAll(uploadsPath, 0700); err != nil {
		return err
	}

	upload := MultipartUpload{
		Bucket:    bucket,
		UploadId:  uploadId,
		Initiator: initiator,
		Initiated: initiated,
	}
	data, err := json.Marshal(upload)
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(uploadsPath, uploadId), data, 0600)
}

func (s *LocalBucketStorer) DeleteMultipartUpload(bucket, uploadId string) error {
	_ = os.Remove(filepath.Join(s.uploadsFolder(bucket), uploadId))
	return nil
}

func (s *LocalBucketStorer) ListMultipartUploads(bucket string) ([]MultipartUpload, error) {
	uploadsPath := s.uploadsFolder(bucket)

	entries, err := os.ReadDir(uploadsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []MultipartUpload{}, nil
		}
		return nil, err
	}

	var uploads []MultipartUpload
	for _, e := range entries {
		var upload MultipartUpload
		data, err := os.ReadFile(s.uploadsFolder(e.Name()))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &upload); err != nil {
			return nil, err
		}
		uploads = append(uploads, upload)
	}
	return uploads, nil
}
