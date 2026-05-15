package index

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
	// has checks whether the given cell for the given object type is cached.
	has(cell common.CellIndex, objectType ownOsm.OsmObjectType) bool

	// getAll returns the current entries for the given cell and object type. It returns an error when this cell for the
	// given object type is not cached.
	getAll(cell common.CellIndex, objectType ownOsm.OsmObjectType) ([]feature.Feature, error)

	// getOrInsert returns the current entries for the given cell and object type. It creates a new empty cache entry
	// when the cell for the given object type has not been cached yet. The boolean is true when the returned array is
	// new in the cache.
	getOrInsert(cell common.CellIndex, objectType ownOsm.OsmObjectType) ([]feature.Feature, bool, error)

	// insert adds the given features to the cache. When the cache is full, items might get evicted based on the cache
	// replacement policy of the concrete implementation. It returns an error when this cell for the given object type
	// is already cached.
	insert(cell common.CellIndex, objectType ownOsm.OsmObjectType, features []feature.Feature) error

	// insertOrAppend adds the given features to the cache, when the cell for the given object type is not cached, or
	// it appends it, when the cell for the given object type is already cached.
	insertOrAppend(cell common.CellIndex, objectType ownOsm.OsmObjectType, features []feature.Feature)

	// appendAll adds the given entries to the array is the given cell and object type. It returns an error when the
	// cell for the given object type is not cached.
	appendAll(cell common.CellIndex, objectType ownOsm.OsmObjectType, features []feature.Feature) error
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

// has checks whether the given cell for the given object type is cached. This function does NOT use locking since it
// performs an atomic operation.
func (c lruFeatureCache) has(cell common.CellIndex, objectType ownOsm.OsmObjectType) bool {
	cacheKey := c.getCacheKey(cell, objectType)
	_, ok := c.featureCache[cacheKey]
	return ok
}

func (c lruFeatureCache) getAll(cell common.CellIndex, objectType ownOsm.OsmObjectType) ([]feature.Feature, error) {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	cacheKey := c.getCacheKey(cell, objectType)

	if !c.has(cell, objectType) {
		return nil, errors.Errorf("Given cache key %s is not in the cache", cacheKey)
	}

	c.featureCacheLastAccessTimes[cacheKey] = time.Now().UTC().UnixNano()
	features := c.featureCache[cacheKey]

	return features, nil
}

func (c lruFeatureCache) getOrInsert(cell common.CellIndex, objectType ownOsm.OsmObjectType) ([]feature.Feature, bool, error) {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	entryIsNew := false

	if !c.has(cell, objectType) {
		c.insertUnsafe(cell, objectType, []feature.Feature{})
		entryIsNew = true
	}

	cacheKey := c.getCacheKey(cell, objectType)
	return c.featureCache[cacheKey], entryIsNew, nil
}

// insert adds the given features to the cache. If the cache is full, the item that hasn't been used longest will be
// evicted from the cache.
func (c lruFeatureCache) insert(cell common.CellIndex, objectType ownOsm.OsmObjectType, features []feature.Feature) error {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	if c.has(cell, objectType) {
		cacheKey := c.getCacheKey(cell, objectType)
		return errors.Errorf("Given cache key %s is already in the cache", cacheKey)
	}

	c.insertUnsafe(cell, objectType, features)

	return nil
}

func (c lruFeatureCache) insertOrAppend(cell common.CellIndex, objectType ownOsm.OsmObjectType, features []feature.Feature) {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	if c.has(cell, objectType) {
		cacheKey := c.getCacheKey(cell, objectType)
		c.featureCache[cacheKey] = append(c.featureCache[cacheKey], features...)
	} else {
		c.insertUnsafe(cell, objectType, features)
	}
}

// insertUnsafe is the core functionality of the insertion of elements. This function does NOT use locking and is meant
// for internal use only! Use insert to normally insert elements.
func (c lruFeatureCache) insertUnsafe(cell common.CellIndex, objectType ownOsm.OsmObjectType, features []feature.Feature) {
	if len(c.featureCache) >= c.maxSize {
		// Cache is full -> evict entry that has been unused the longest
		longestUnusedCacheKey := c.getMinEntry()
		delete(c.featureCache, longestUnusedCacheKey)
		delete(c.featureCacheLastAccessTimes, longestUnusedCacheKey)
	}

	cacheKey := c.getCacheKey(cell, objectType)

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

func (c lruFeatureCache) appendAll(cell common.CellIndex, objectType ownOsm.OsmObjectType, additionalFeatures []feature.Feature) error {
	c.featureCacheMutex.Lock()
	defer c.featureCacheMutex.Unlock()

	cacheKey := c.getCacheKey(cell, objectType)

	if !c.has(cell, objectType) {
		return errors.Errorf("Given cache key %s is not in the cache", cacheKey)
	}

	c.featureCache[cacheKey] = append(c.featureCache[cacheKey], additionalFeatures...)

	return nil
}

func (c lruFeatureCache) getCacheKey(cell common.CellIndex, objectType ownOsm.OsmObjectType) string {
	return fmt.Sprintf("%d_%d_%d", cell.X(), cell.Y(), objectType)
}
