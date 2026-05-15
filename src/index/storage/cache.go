package storage

import (
	"fmt"
	"math"
	"soq/common"
	"soq/feature"
	ownOsm "soq/osm"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type featureCache interface {
	// has checks whether the given cellExtent for the given object type is cached.
	has(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType) bool

	// getAll returns the current entries for the given cellExtent and object type. It returns an error when this cellExtent for the
	// given object type is not cached.
	getAll(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType) ([]feature.Feature, error)

	// getOrInsert returns the current entries for the given cellExtent and object type. It creates a new empty cache entry
	// when the cellExtent for the given object type has not been cached yet. The boolean is true when the returned array is
	// new in the cache.
	getOrInsert(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType) ([]feature.Feature, bool, error)

	// insert adds the given features to the cache. When the cache is full, items might get evicted based on the cache
	// replacement policy of the concrete implementation. It returns an error when this cellExtent for the given object type
	// is already cached.
	insert(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType, features []feature.Feature) error

	// insertOrAppend adds the given features to the cache, when the cellExtent for the given object type is not cached, or
	// it appends it, when the cellExtent for the given object type is already cached.
	insertOrAppend(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType, features []feature.Feature)

	// appendAll adds the given entries to the array is the given cellExtent and object type. It returns an error when the
	// cellExtent for the given object type is not cached.
	appendAll(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType, features []feature.Feature) error
}

// lruFeatureCache is a simple LRU (least recently used) cache for encoded features. It has an internal
// locking mechanism and can be used in concurrent goroutines. The eviction strategy uses the UTC nanoseconds as
// measurement for the recency of entries. This timestamp only gets updates when data is read, not when it's written.
type lruFeatureCache struct {
	featureCache                map[string][]feature.Feature // Map of cache key to feature within it
	featureCacheLastAccessTimes map[string]int64             // Map of cache key to UTC millis of last access
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

// has checks whether the given cellExtent for the given object type is cached. This function does NOT use locking since it
// performs an atomic operation.
func (c lruFeatureCache) has(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType) bool {
	cacheKey := c.getCacheKey(cellExtent, objectType)
	_, ok := c.featureCache[cacheKey]
	return ok
}

func (c lruFeatureCache) getAll(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType) ([]feature.Feature, error) {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	cacheKey := c.getCacheKey(cellExtent, objectType)

	if !c.has(cellExtent, objectType) {
		return nil, errors.Errorf("Given cache key %s is not in the cache", cacheKey)
	}

	c.featureCacheLastAccessTimes[cacheKey] = time.Now().UTC().UnixNano()
	features := c.featureCache[cacheKey]

	return features, nil
}

func (c lruFeatureCache) getOrInsert(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType) ([]feature.Feature, bool, error) {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	entryIsNew := false

	if !c.has(cellExtent, objectType) {
		c.insertUnsafe(cellExtent, objectType, []feature.Feature{})
		entryIsNew = true
	}

	cacheKey := c.getCacheKey(cellExtent, objectType)
	return c.featureCache[cacheKey], entryIsNew, nil
}

// insert adds the given features to the cache. If the cache is full, the item that hasn't been used longest will be
// evicted from the cache.
func (c lruFeatureCache) insert(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType, features []feature.Feature) error {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	if c.has(cellExtent, objectType) {
		cacheKey := c.getCacheKey(cellExtent, objectType)
		return errors.Errorf("Given cache key %s is already in the cache", cacheKey)
	}

	c.insertUnsafe(cellExtent, objectType, features)

	return nil
}

func (c lruFeatureCache) insertOrAppend(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType, features []feature.Feature) {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	if c.has(cellExtent, objectType) {
		cacheKey := c.getCacheKey(cellExtent, objectType)
		c.featureCache[cacheKey] = append(c.featureCache[cacheKey], features...)
	} else {
		c.insertUnsafe(cellExtent, objectType, features)
	}
}

// insertUnsafe is the core functionality of the insertion of elements. This function does NOT use locking and is meant
// for internal use only! Use insert to normally insert elements.
func (c lruFeatureCache) insertUnsafe(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType, features []feature.Feature) {
	if len(c.featureCache) >= c.maxSize {
		// Cache is full -> evict entry that has been unused the longest
		longestUnusedCacheKey := c.getMinEntry()
		delete(c.featureCache, longestUnusedCacheKey)
		delete(c.featureCacheLastAccessTimes, longestUnusedCacheKey)
	}

	cacheKey := c.getCacheKey(cellExtent, objectType)

	c.featureCacheLastAccessTimes[cacheKey] = time.Now().UTC().UnixNano()
	c.featureCache[cacheKey] = features
}

// getMinEntry returns the entry that hasn't been used longest. This function does NOT use locking and is meant for
// internal use only!
func (c lruFeatureCache) getMinEntry() string {
	minTimestamp := int64(math.MaxInt64)
	minCacheKey := ""

	for cacheKey, timestamp := range c.featureCacheLastAccessTimes {
		if timestamp < minTimestamp {
			minTimestamp = timestamp
			minCacheKey = cacheKey
		}
	}

	return minCacheKey
}

func (c lruFeatureCache) appendAll(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType, additionalFeatures []feature.Feature) error {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	cacheKey := c.getCacheKey(cellExtent, objectType)

	if !c.has(cellExtent, objectType) {
		return errors.Errorf("Given cache key %s is not in the cache", cacheKey)
	}

	c.featureCache[cacheKey] = append(c.featureCache[cacheKey], additionalFeatures...)

	return nil
}

func (c lruFeatureCache) getCacheKey(cellExtent common.CellExtent, objectType ownOsm.OsmObjectType) string {
	return fmt.Sprintf("%d-%d_%d-%d_%d", cellExtent.UpperRightCell().X(), cellExtent.UpperRightCell().Y(), cellExtent.LowerLeftCell().X(), cellExtent.LowerLeftCell().Y(), objectType)
}
