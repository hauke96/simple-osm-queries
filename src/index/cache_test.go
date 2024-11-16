package index

import (
	"github.com/paulmach/osm"
	"soq/common"
	"soq/feature"
	"testing"
	"time"
)

func TestLruCache_insertAndEviction(t *testing.T) {
	cache := newLruCache(3)

	filenameA := "A"
	entryA := []feature.EncodedFeature{&feature.EncodedNodeFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}
	filenameB := "B"
	entryB := []feature.EncodedFeature{&feature.EncodedWayFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}
	filenameC := "C"
	entryC := []feature.EncodedFeature{&feature.EncodedRelationFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}
	filenameD := "D"
	entryD := []feature.EncodedFeature{&feature.EncodedNodeFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}, WayIds: []osm.WayID{}}}

	common.AssertFalse(t, cache.has(filenameA))
	common.AssertFalse(t, cache.has(filenameB))
	common.AssertFalse(t, cache.has(filenameC))
	common.AssertFalse(t, cache.has(filenameD))

	// Insert A
	err := cache.insert(filenameA, entryA)
	common.AssertNil(t, err)
	common.AssertTrue(t, cache.has(filenameA))
	common.AssertFalse(t, cache.has(filenameB))
	common.AssertFalse(t, cache.has(filenameC))
	common.AssertFalse(t, cache.has(filenameD))
	time.Sleep(10 * time.Nanosecond)

	// Insert B
	err = cache.insert(filenameB, entryB)
	common.AssertNil(t, err)
	common.AssertTrue(t, cache.has(filenameA))
	common.AssertTrue(t, cache.has(filenameB))
	common.AssertFalse(t, cache.has(filenameC))
	common.AssertFalse(t, cache.has(filenameD))
	time.Sleep(10 * time.Nanosecond)

	// Insert C
	err = cache.insert(filenameC, entryC)
	common.AssertNil(t, err)
	common.AssertTrue(t, cache.has(filenameA))
	common.AssertTrue(t, cache.has(filenameB))
	common.AssertTrue(t, cache.has(filenameC))
	common.AssertFalse(t, cache.has(filenameD))
	time.Sleep(10 * time.Nanosecond)

	// Insert D
	err = cache.insert(filenameD, entryD)
	common.AssertNil(t, err)
	common.AssertFalse(t, cache.has(filenameA))
	common.AssertTrue(t, cache.has(filenameB))
	common.AssertTrue(t, cache.has(filenameC))
	common.AssertTrue(t, cache.has(filenameD))
}

func TestLruCache_insertTwice(t *testing.T) {
	// Arrange
	cache := newLruCache(3)

	filename := "A"
	entry := []feature.EncodedFeature{&feature.EncodedNodeFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}

	common.AssertFalse(t, cache.has(filename))
	err := cache.insert(filename, entry)
	common.AssertNil(t, err)
	common.AssertTrue(t, cache.has(filename))
	time.Sleep(10 * time.Nanosecond)

	// Act
	err = cache.insert(filename, entry)

	// Assert
	common.AssertNotNil(t, err)
	common.AssertTrue(t, cache.has(filename))
}

func TestLruCache_insertOrAppend(t *testing.T) {
	// Arrange
	cache := newLruCache(3)

	filename := "A"
	entry := []feature.EncodedFeature{&feature.EncodedNodeFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}

	common.AssertFalse(t, cache.has(filename))
	cache.insertOrAppend(filename, entry)
	common.AssertTrue(t, cache.has(filename))
	time.Sleep(10 * time.Nanosecond)

	// Act
	cache.insertOrAppend(filename, entry)

	// Assert
	common.AssertTrue(t, cache.has(filename))
	cachedEntries, _ := cache.getAll(filename)
	common.AssertEqual(t, 2, len(cachedEntries))
}

func TestLruCache_getAll(t *testing.T) {
	// Arrange
	cache := newLruCache(3)

	filename := "A"
	entry := []feature.EncodedFeature{&feature.EncodedNodeFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}

	err := cache.insert(filename, entry)
	common.AssertNil(t, err)
	common.AssertTrue(t, cache.has(filename))

	// Act
	allEntriesForA, err := cache.getAll(filename)

	// Assert
	common.AssertNil(t, err)
	common.AssertEqual(t, entry, allEntriesForA)
}

func TestLruCache_getAllWithoutEntries(t *testing.T) {
	// Arrange
	cache := newLruCache(3)
	filename := "A"

	// Act
	allEntriesForA, err := cache.getAll(filename)

	// Assert
	common.AssertNotNil(t, err)
	common.AssertNil(t, allEntriesForA)
}

func TestLruCache_getOrInsert(t *testing.T) {
	// Arrange
	cache := newLruCache(3)
	filename := "A"
	common.AssertFalse(t, cache.has(filename))

	// Act
	entry, entryIsNew, err := cache.getOrInsert(filename)

	// Assert
	common.AssertNil(t, err)
	common.AssertTrue(t, entryIsNew)
	common.AssertEqual(t, entry, []feature.EncodedFeature{})
}

func TestLruCache_getOrInsertFullCache(t *testing.T) {
	// Arrange
	cache := newLruCache(3)

	// Insert entries
	filenameA := "A"
	entryA := []feature.EncodedFeature{&feature.EncodedNodeFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}
	filenameB := "B"
	entryB := []feature.EncodedFeature{&feature.EncodedWayFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}
	filenameC := "C"
	entryC := []feature.EncodedFeature{&feature.EncodedRelationFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}

	err := cache.insert(filenameA, entryA)
	time.Sleep(10 * time.Nanosecond)
	err = cache.insert(filenameB, entryB)
	time.Sleep(10 * time.Nanosecond)
	err = cache.insert(filenameC, entryC)
	time.Sleep(10 * time.Nanosecond)

	filenameD := "D"

	// Act
	entry, entryIsNew, err := cache.getOrInsert(filenameD)

	// Assert
	common.AssertNil(t, err)
	common.AssertTrue(t, entryIsNew)
	common.AssertFalse(t, cache.has(filenameA))
	common.AssertTrue(t, cache.has(filenameB))
	common.AssertTrue(t, cache.has(filenameC))
	common.AssertTrue(t, cache.has(filenameD))
	common.AssertEqual(t, entry, []feature.EncodedFeature{})
}

func TestLruCache_getOrInsertTwice(t *testing.T) {
	// Arrange
	cache := newLruCache(3)
	filename := "A"
	common.AssertFalse(t, cache.has(filename))
	_, _, _ = cache.getOrInsert(filename)

	// Act
	entry, entryIsNew, err := cache.getOrInsert(filename)

	// Assert
	common.AssertNil(t, err)
	common.AssertFalse(t, entryIsNew)
	common.AssertEqual(t, entry, []feature.EncodedFeature{})
}

func TestLruCache_appendAll(t *testing.T) {
	// Arrange
	cache := newLruCache(3)

	filename := "A"
	entry := []feature.EncodedFeature{&feature.EncodedNodeFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}
	additionalFeatures := []feature.EncodedFeature{&feature.EncodedWayFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}

	err := cache.insert(filename, entry)
	common.AssertNil(t, err)
	common.AssertTrue(t, cache.has(filename))

	// Act
	err = cache.appendAll(filename, additionalFeatures)

	// Assert
	common.AssertNil(t, err)
	allEntries, _ := cache.getAll(filename)
	common.AssertEqual(t, allEntries, append(entry, additionalFeatures...))
}

func TestLruCache_appendAllNotExisting(t *testing.T) {
	// Arrange
	cache := newLruCache(3)

	filename := "A"
	entry := []feature.EncodedFeature{&feature.EncodedNodeFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}

	// Act
	err := cache.appendAll(filename, entry)

	// Assert
	common.AssertNotNil(t, err)
	common.AssertFalse(t, cache.has(filename))
}
