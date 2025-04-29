package registry

import (
	"slices"
	"sync"
)

type MemoryRegistry struct {
	m       sync.RWMutex
	buckets map[string]Mapping // bucket name -> mapping
	users   map[int][]string   // uid -> list of bucket name
}

func NewMemoryRegistry() (*MemoryRegistry, error) {
	return &MemoryRegistry{
		buckets: make(map[string]Mapping),
		users:   make(map[int][]string),
	}, nil
}

func (r *MemoryRegistry) CreateMapping(mapping Mapping) error {
	r.m.RLock()
	_, ok := r.buckets[mapping.Bucket]
	r.m.RUnlock()

	if ok {
		return ErrMappingAlreadyExisting
	}

	r.m.Lock()
	r.buckets[mapping.Bucket] = mapping
	r.m.Unlock()

	return nil
}

func (r *MemoryRegistry) GetMapping(bucket string) (Mapping, error) {
	r.m.RLock()
	defer r.m.RUnlock()

	m, ok := r.buckets[bucket]
	if !ok {
		return Mapping{}, ErrNoSuchBucket
	}
	return m, nil
}

func (r *MemoryRegistry) DeleteMapping(bucket string) error {
	r.m.Lock()
	defer r.m.Unlock()

	delete(r.buckets, bucket)
	return nil
}

func (r *MemoryRegistry) ListMappings() ([]Mapping, error) {
	r.m.RLock()
	defer r.m.RUnlock()

	list := make([]Mapping, 0, len(r.buckets))
	for _, m := range r.buckets {
		list = append(list, m)
	}
	return list, nil
}

func (r *MemoryRegistry) AssignBucket(bucket string, uid int) error {
	r.m.Lock()
	defer r.m.Unlock()

	r.users[uid] = append(r.users[uid], bucket)
	return nil
}

func (r *MemoryRegistry) ListBuckets(uid int) ([]string, error) {
	r.m.RLock()
	defer r.m.RUnlock()

	buckets := r.users[uid]
	list := make([]string, 0, len(buckets))
	copy(list, buckets)
	return list, nil
}

func (r *MemoryRegistry) UnassignBucket(bucket string, uid int) error {
	r.m.Lock()
	defer r.m.Unlock()

	r.users[uid] = slices.DeleteFunc(r.users[uid], func(name string) bool {
		return bucket == name
	})
	return nil
}
