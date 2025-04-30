package meta

import (
	"slices"
	"sync"
)

type InMemoryBucketStorer struct {
	m       sync.RWMutex
	buckets map[string]Bucket // name -> bucket
	users   map[int][]string  // uid -> list of bucket name
	paths   map[int]string    // map holding for each user (uid) their default bucket path
}

func NewInMemoryBucketStorer() (*InMemoryBucketStorer, error) {
	return &InMemoryBucketStorer{
		buckets: make(map[string]Bucket),
		users:   make(map[int][]string),
	}, nil
}

func (s *InMemoryBucketStorer) CreateBucket(bucket Bucket) error {
	s.m.RLock()
	_, ok := s.buckets[bucket.Name]
	s.m.RUnlock()

	if ok {
		return ErrBucketAlreadyExisting
	}

	s.m.Lock()
	s.buckets[bucket.Name] = bucket
	s.m.Unlock()

	return nil
}

func (s *InMemoryBucketStorer) GetBucket(name string) (Bucket, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	m, ok := s.buckets[name]
	if !ok {
		return Bucket{}, ErrNoSuchBucket
	}
	return m, nil
}

func (s *InMemoryBucketStorer) DeleteBucket(name string) error {
	s.m.Lock()
	defer s.m.Unlock()

	delete(s.buckets, name)
	return nil
}

func (s *InMemoryBucketStorer) ListBuckets() ([]Bucket, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	list := make([]Bucket, 0, len(s.buckets))
	for _, m := range s.buckets {
		list = append(list, m)
	}
	return list, nil
}

func (s *InMemoryBucketStorer) AssignBucket(name string, uid int) error {
	s.m.Lock()
	defer s.m.Unlock()

	s.users[uid] = append(s.users[uid], name)
	return nil
}

func (s *InMemoryBucketStorer) ListBucketsByUser(uid int) ([]string, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	buckets := s.users[uid]
	list := make([]string, 0, len(buckets))
	copy(list, buckets)
	return list, nil
}

func (s *InMemoryBucketStorer) UnassignBucket(name string, uid int) error {
	s.m.Lock()
	defer s.m.Unlock()

	s.users[uid] = slices.DeleteFunc(s.users[uid], func(bucket string) bool {
		return bucket == name
	})

	return nil
}

func (s *InMemoryBucketStorer) GetDefaultBucketPath(uid int) (string, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.paths[uid], nil
}

func (s *InMemoryBucketStorer) StoreDefaultBucketPath(uid int, path string) error {
	s.m.Lock()
	defer s.m.Unlock()

	s.paths[uid] = path
	return nil
}
