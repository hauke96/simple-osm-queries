package index

import (
	"github.com/paulmach/osm"
	"soq/feature"
	"soq/util"
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

	util.AssertFalse(t, cache.has(filenameA))
	util.AssertFalse(t, cache.has(filenameB))
	util.AssertFalse(t, cache.has(filenameC))
	util.AssertFalse(t, cache.has(filenameD))

	// Insert A
	err := cache.insert(filenameA, entryA)
	util.AssertNil(t, err)
	util.AssertTrue(t, cache.has(filenameA))
	util.AssertFalse(t, cache.has(filenameB))
	util.AssertFalse(t, cache.has(filenameC))
	util.AssertFalse(t, cache.has(filenameD))
	time.Sleep(10 * time.Nanosecond)

	// Insert B
	err = cache.insert(filenameB, entryB)
	util.AssertNil(t, err)
	util.AssertTrue(t, cache.has(filenameA))
	util.AssertTrue(t, cache.has(filenameB))
	util.AssertFalse(t, cache.has(filenameC))
	util.AssertFalse(t, cache.has(filenameD))
	time.Sleep(10 * time.Nanosecond)

	// Insert C
	err = cache.insert(filenameC, entryC)
	util.AssertNil(t, err)
	util.AssertTrue(t, cache.has(filenameA))
	util.AssertTrue(t, cache.has(filenameB))
	util.AssertTrue(t, cache.has(filenameC))
	util.AssertFalse(t, cache.has(filenameD))
	time.Sleep(10 * time.Nanosecond)

	// Insert D
	err = cache.insert(filenameD, entryD)
	util.AssertNil(t, err)
	util.AssertFalse(t, cache.has(filenameA))
	util.AssertTrue(t, cache.has(filenameB))
	util.AssertTrue(t, cache.has(filenameC))
	util.AssertTrue(t, cache.has(filenameD))
}

func TestLruCache_insertTwice(t *testing.T) {
	// Arrange
	cache := newLruCache(3)

	filename := "A"
	entry := []feature.EncodedFeature{&feature.EncodedNodeFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}

	util.AssertFalse(t, cache.has(filename))
	err := cache.insert(filename, entry)
	util.AssertNil(t, err)
	util.AssertTrue(t, cache.has(filename))
	time.Sleep(10 * time.Nanosecond)

	// Act
	err = cache.insert(filename, entry)

	// Assert
	util.AssertNotNil(t, err)
	util.AssertTrue(t, cache.has(filename))
}

func TestLruCache_insertOrAppend(t *testing.T) {
	// Arrange
	cache := newLruCache(3)

	filename := "A"
	entry := []feature.EncodedFeature{&feature.EncodedNodeFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}

	util.AssertFalse(t, cache.has(filename))
	cache.insertOrAppend(filename, entry)
	util.AssertTrue(t, cache.has(filename))
	time.Sleep(10 * time.Nanosecond)

	// Act
	cache.insertOrAppend(filename, entry)

	// Assert
	util.AssertTrue(t, cache.has(filename))
	cachedEntries, _ := cache.getAll(filename)
	util.AssertEqual(t, 2, len(cachedEntries))
}

func TestLruCache_getAll(t *testing.T) {
	// Arrange
	cache := newLruCache(3)

	filename := "A"
	entry := []feature.EncodedFeature{&feature.EncodedNodeFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}

	err := cache.insert(filename, entry)
	util.AssertNil(t, err)
	util.AssertTrue(t, cache.has(filename))

	// Act
	allEntriesForA, err := cache.getAll(filename)

	// Assert
	util.AssertNil(t, err)
	util.AssertEqual(t, entry, allEntriesForA)
}

func TestLruCache_getAllWithoutEntries(t *testing.T) {
	// Arrange
	cache := newLruCache(3)
	filename := "A"

	// Act
	allEntriesForA, err := cache.getAll(filename)

	// Assert
	util.AssertNotNil(t, err)
	util.AssertNil(t, allEntriesForA)
}

func TestLruCache_getOrInsert(t *testing.T) {
	// Arrange
	cache := newLruCache(3)
	filename := "A"
	util.AssertFalse(t, cache.has(filename))

	// Act
	entry, entryIsNew, err := cache.getOrInsert(filename)

	// Assert
	util.AssertNil(t, err)
	util.AssertTrue(t, entryIsNew)
	util.AssertEqual(t, entry, []feature.EncodedFeature{})
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
	util.AssertNil(t, err)
	util.AssertTrue(t, entryIsNew)
	util.AssertFalse(t, cache.has(filenameA))
	util.AssertTrue(t, cache.has(filenameB))
	util.AssertTrue(t, cache.has(filenameC))
	util.AssertTrue(t, cache.has(filenameD))
	util.AssertEqual(t, entry, []feature.EncodedFeature{})
}

func TestLruCache_getOrInsertTwice(t *testing.T) {
	// Arrange
	cache := newLruCache(3)
	filename := "A"
	util.AssertFalse(t, cache.has(filename))
	_, _, _ = cache.getOrInsert(filename)

	// Act
	entry, entryIsNew, err := cache.getOrInsert(filename)

	// Assert
	util.AssertNil(t, err)
	util.AssertFalse(t, entryIsNew)
	util.AssertEqual(t, entry, []feature.EncodedFeature{})
}

func TestLruCache_appendAll(t *testing.T) {
	// Arrange
	cache := newLruCache(3)

	filename := "A"
	entry := []feature.EncodedFeature{&feature.EncodedNodeFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}
	additionalFeatures := []feature.EncodedFeature{&feature.EncodedWayFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}

	err := cache.insert(filename, entry)
	util.AssertNil(t, err)
	util.AssertTrue(t, cache.has(filename))

	// Act
	err = cache.appendAll(filename, additionalFeatures)

	// Assert
	util.AssertNil(t, err)
	allEntries, _ := cache.getAll(filename)
	util.AssertEqual(t, allEntries, append(entry, additionalFeatures...))
}

func TestLruCache_appendAllNotExisting(t *testing.T) {
	// Arrange
	cache := newLruCache(3)

	filename := "A"
	entry := []feature.EncodedFeature{&feature.EncodedNodeFeature{AbstractEncodedFeature: feature.AbstractEncodedFeature{}}}

	// Act
	err := cache.appendAll(filename, entry)

	// Assert
	util.AssertNotNil(t, err)
	util.AssertFalse(t, cache.has(filename))
}
