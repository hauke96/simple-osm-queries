package index

import (
	"github.com/pkg/errors"
	"math"
	"soq/feature"
	"sync"
	"time"
)

type featureCache interface {
	// has checks whether the given file is cached.
	has(filename string) bool

	// getAll returns the current entries for the given file. It returns an error when this file is not cached.
	getAll(filename string) ([]feature.Feature, error)

	// getOrInsert returns the current entries for the given file. It creates a new empty cache entry when the file has not
	// been cached yet. The boolean is true when the returned array is new in the cache.
	getOrInsert(filename string) ([]feature.Feature, bool, error)

	// insert adds the given features to the cache. When the cache is full, items might get evicted based on the cache
	// replacement policy of the concrete implementation. It returns an error when this file is already cached.
	insert(filename string, features []feature.Feature) error

	// insertOrAppend adds the given features to the cache, when the file is not caches, or it appends it, when the file
	//is already cached.
	insertOrAppend(filename string, features []feature.Feature)

	// appendAll adds the given entries to the array is the given filename. It returns an error when this file is not
	// cached.
	appendAll(filename string, features []feature.Feature) error
}

// lruFeatureCache is a simple LRU (least recently used) cache for files containing encoded features. It has an internal
// locking mechanism and can be used in concurrent goroutines. The eviction strategy uses the UTC nanoseconds as
// measurement for the recency of entries. This timestamp only gets updates when data is read, not when it's written.
type lruFeatureCache struct {
	featureCache                map[string][]feature.Feature // Filename to feature within it
	featureCacheLastAccessTimes map[string]int64             // Filename to UTC millis of last access
	featureCacheMutex           *sync.Mutex
	maxSize                     int // Maximum number of entries this cache should hold
}

func newLruCache(maxSize int) *lruFeatureCache {
	return &lruFeatureCache{
		featureCache:                map[string][]feature.Feature{},
		featureCacheLastAccessTimes: map[string]int64{},
		featureCacheMutex:           &sync.Mutex{},
		maxSize:                     maxSize,
	}
}

// has checks whether the given file is cached. This function does NOT use locking since it performs an atomic operation.
func (c lruFeatureCache) has(filename string) bool {
	_, ok := c.featureCache[filename]
	return ok
}

func (c lruFeatureCache) getAll(filename string) ([]feature.Feature, error) {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	if !c.has(filename) {
		return nil, errors.Errorf("Given filename %s is not in the cache", filename)
	}

	c.featureCacheLastAccessTimes[filename] = time.Now().UTC().UnixNano()
	features := c.featureCache[filename]

	return features, nil
}

func (c lruFeatureCache) getOrInsert(filename string) ([]feature.Feature, bool, error) {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	entryIsNew := false

	if !c.has(filename) {
		c.insertUnsafe(filename, []feature.Feature{})
		entryIsNew = true
	}

	return c.featureCache[filename], entryIsNew, nil
}

// insert adds the given features to the cache. If the cache is full, the item that hasn't been used longest will be
// evicted from the cache.
func (c lruFeatureCache) insert(filename string, features []feature.Feature) error {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	if c.has(filename) {
		return errors.Errorf("Given filename %s is already in the cache", filename)
	}

	c.insertUnsafe(filename, features)

	return nil
}

func (c lruFeatureCache) insertOrAppend(filename string, features []feature.Feature) {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	if c.has(filename) {
		c.featureCache[filename] = append(c.featureCache[filename], features...)
	} else {
		c.insertUnsafe(filename, features)
	}
}

// insertUnsafe is the core functionality of the insertion of elements. This function does NOT use locking and is meant
// for internal use only! Use insert to normally insert elements.
func (c lruFeatureCache) insertUnsafe(filename string, features []feature.Feature) {
	if len(c.featureCache) >= c.maxSize {
		// Cache is full -> evict entry that has been unused the longest
		longestUnusedFilename := c.getMinEntry()
		delete(c.featureCache, longestUnusedFilename)
		delete(c.featureCacheLastAccessTimes, longestUnusedFilename)
	}

	c.featureCacheLastAccessTimes[filename] = time.Now().UTC().UnixNano()
	c.featureCache[filename] = features
}

// getMinEntry returns the entry that hasn't been used longest. This function does NOT use locking and is meant for
// internal use only!
func (c lruFeatureCache) getMinEntry() string {
	minTimestamp := int64(math.MaxInt64)
	minFilename := ""

	for filename, timestamp := range c.featureCacheLastAccessTimes {
		if timestamp < minTimestamp {
			minTimestamp = timestamp
			minFilename = filename
		}
	}

	return minFilename
}

func (c lruFeatureCache) appendAll(filename string, additionalFeatures []feature.Feature) error {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	if !c.has(filename) {
		return errors.Errorf("Given filename %s is not in the cache", filename)
	}

	c.featureCache[filename] = append(c.featureCache[filename], additionalFeatures...)

	return nil
}
