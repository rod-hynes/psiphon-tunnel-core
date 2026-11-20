/*
 * Copyright (c) 2026, Psiphon Inc.
 * All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 */

// Package cache provides concurrent caches with size and expiration limits.
package cache

import (
	"iter"
	"math"
	"sync"
	"time"

	"github.com/maypok86/otter/v2"
)

const (
	// DefaultExpiration uses the default TTL specified at construction.
	DefaultExpiration time.Duration = 0

	// NoExpiration disables expiration for an entry. Size eviction still
	// applies.
	NoExpiration time.Duration = -1
)

// EvictionReason identifies why an entry was automatically removed.
type EvictionReason int

const (
	// Expired indicates that the entry's TTL elapsed.
	Expired EvictionReason = iota
	// Capacity indicates that the entry was removed to limit cache size.
	Capacity
)

// LRUCacheConfig configures an LRUCache. It is copied by NewLRUCache.
type LRUCacheConfig[K comparable, V any] struct {
	// DefaultTTL is used by writes specifying DefaultExpiration. A
	// nonpositive default disables expiration for those entries.
	DefaultTTL time.Duration

	// MaxEntries is the size limit. A nonpositive value disables the limit.
	// Entries may be evicted before the limit is reached, and concurrent
	// writes may temporarily exceed it.
	MaxEntries int

	// InsertionOrderEviction limits the cache to MaxEntries entries by
	// evicting the entry with the oldest Set or successful Add. Reads do
	// not affect which entry is evicted, and the limit is never exceeded.
	// Use it for keys that are written once and rarely read, where the
	// default policy would keep existing entries in preference to new ones.
	//
	// Limitation: an entry that is deleted, expired or replaced still
	// counts towards MaxEntries until all entries written before it are
	// gone. Writes are serialized; reads are not.
	//
	// Ignored when MaxEntries is nonpositive.
	InsertionOrderEviction bool

	// OnEvicted is called asynchronously after expiration or size eviction.
	// Calls may run concurrently and out of order, without cache locks held,
	// and may access the cache. Notifications may arrive after the key has
	// been written again, deleted, or flushed. Callers must check whether an
	// event still applies before changing other state.
	//
	// Removing or replacing a live entry does not notify OnEvicted. Removing
	// an already expired entry may report Expired. Expiration notifications
	// run during maintenance, so they are not precise deadline timers.
	OnEvicted func(key K, value V, reason EvictionReason)
}

// LRUCache is a concurrent cache supporting size limits and per-entry
// expiration.
//
// By default, it uses adaptive W-TinyLFU eviction, which considers both
// recency and frequency of access. It does not guarantee strict
// least-recently-used eviction. Note: when the cache is full, a new key
// that is not read again is evicted in preference to existing entries; see
// LRUCacheConfig.InsertionOrderEviction.
//
// Reads never extend expiration. Writes atomically publish both the value
// and its TTL. Expired entries are not returned, even before maintenance
// removes them. Values are copied by assignment. Data referenced by pointers,
// maps, or slices is shared and requires caller synchronization.
//
// TTLs are measured with the monotonic clock, so wall clock changes do not
// affect them. On platforms where the monotonic clock stops while the
// device is suspended, time spent suspended does not count towards a TTL.
//
// Use NewLRUCache to construct a cache. Its background maintenance stops
// after the cache becomes unreachable and is garbage collected.
type LRUCache[K comparable, V any] struct {
	cache      *otter.Cache[K, lruValue[V]]
	clock      lruClock
	defaultTTL time.Duration

	// The remaining fields are used only when insertionOrder is set.
	// writeMutex serializes writes and guards writes, oldestWrite and
	// nextSequence. It is not held by reads, Delete or OnEvicted calls.
	insertionOrder bool
	maxEntries     int
	onEvicted      func(key K, value V, reason EvictionReason)
	writeMutex     sync.Mutex
	writes         []lruWrite[K]
	oldestWrite    int
	nextSequence   uint64
}

type lruValue[V any] struct {
	value V

	// expiration is an lruClock time, or 0 for no expiration.
	expiration int64

	// sequence identifies the write that stored this value, or is 0
	// without InsertionOrderEviction.
	sequence uint64
}

// lruWrite records one write. LRUCache.writes holds the records in the
// order the writes were made, with the oldest at LRUCache.oldestWrite.
type lruWrite[K comparable] struct {
	key      K
	sequence uint64
}

// lruClock is the clock used by both LRUCache and its Otter backend. Entry
// deadlines are compared with Otter's entry snapshot times, so both must
// come from the same clock.
type lruClock struct {
	start time.Time
}

// Implements the otter.Clock interface. The result is the wall clock time
// at start plus the monotonic time elapsed since.
func (c lruClock) NowNano() int64 {
	return c.start.UnixNano() + int64(time.Since(c.start))
}

// time converts an lruClock time to a time.Time. The same input always
// gives the same result. The result assumes the wall clock has not been
// changed since start.
func (c lruClock) time(nano int64) time.Time {
	return c.start.Add(time.Duration(nano - c.start.UnixNano()))
}

// Implements the otter.Clock interface.
func (c lruClock) Tick(duration time.Duration) <-chan time.Time {
	return time.Tick(duration)
}

// NewLRUCache creates an empty cache with the specified configuration.
func NewLRUCache[K comparable, V any](config LRUCacheConfig[K, V]) *LRUCache[K, V] {
	clock := lruClock{start: time.Now()}
	insertionOrder := config.InsertionOrderEviction && config.MaxEntries > 0
	options := &otter.Options[K, lruValue[V]]{
		Clock: clock,
		ExpiryCalculator: otter.ExpiryWritingFunc(
			func(entry otter.Entry[K, lruValue[V]]) time.Duration {
				if entry.Value.expiration == 0 {
					return time.Duration(math.MaxInt64)
				}
				return time.Duration(max(0,
					entry.Value.expiration-entry.SnapshotAtNano))
			}),
	}
	if !insertionOrder {
		options.MaximumSize = max(0, config.MaxEntries)
	}
	if config.OnEvicted != nil {
		options.OnDeletion = func(event otter.DeletionEvent[K, lruValue[V]]) {
			switch event.Cause {
			case otter.CauseExpiration:
				config.OnEvicted(event.Key, event.Value.value, Expired)
			case otter.CauseOverflow:
				config.OnEvicted(event.Key, event.Value.value, Capacity)
			}
		}
	}

	// These options always pass Otter's validation: MaximumSize is
	// nonnegative, and no weight or initial capacity options are set.
	backend, _ := otter.New(options)
	c := &LRUCache[K, V]{
		cache:      backend,
		clock:      clock,
		defaultTTL: config.DefaultTTL,
	}
	if insertionOrder {
		c.insertionOrder = true
		c.maxEntries = config.MaxEntries
		c.onEvicted = config.OnEvicted
	}
	return c
}

// Get returns the unexpired value for key, if present.
func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	entry, ok := c.getEntry(key)
	return entry.value, ok
}

func (c *LRUCache[K, V]) getEntry(key K) (lruValue[V], bool) {
	entry, ok := c.cache.GetEntry(key)
	// Otter can update a node's expiration and value separately. Keep the
	// deadline with the value, and check that deadline before returning it.
	if !ok || (entry.Value.expiration != 0 &&
		entry.Value.expiration <= entry.SnapshotAtNano) {
		return lruValue[V]{}, false
	}
	return entry.Value, true
}

// GetWithExpiration also returns the expiration time. A zero time means
// the entry does not expire. The time is exact relative to time.Now only
// while the wall clock is unchanged; see LRUCache. All return values
// describe the same entry.
func (c *LRUCache[K, V]) GetWithExpiration(key K) (V, time.Time, bool) {
	entry, ok := c.getEntry(key)
	if !ok {
		var zero V
		return zero, time.Time{}, false
	}
	var expiration time.Time
	if entry.expiration != 0 {
		expiration = c.clock.time(entry.expiration)
	}
	return entry.value, expiration, true
}

// Set inserts or replaces key and resets its TTL. A zero TTL selects
// DefaultExpiration, and a negative TTL selects NoExpiration.
func (c *LRUCache[K, V]) Set(key K, value V, ttl time.Duration) {
	if !c.insertionOrder {
		c.cache.Set(key, c.makeValue(value, ttl, 0))
		return
	}
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()
	c.nextSequence++
	c.cache.Set(key, c.makeValue(value, ttl, c.nextSequence))
	c.recordWrite(key, c.nextSequence)
}

// Add inserts key only if it has no unexpired value. It returns the stored
// value and whether it was inserted. An existing entry's TTL is unchanged.
// TTL has the same meaning as in Set.
func (c *LRUCache[K, V]) Add(key K, value V, ttl time.Duration) (V, bool) {
	if !c.insertionOrder {
		entry, added := c.cache.SetIfAbsent(
			key, c.makeValue(value, ttl, 0))
		return entry.value, added
	}
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()
	sequence := c.nextSequence + 1
	entry, added := c.cache.SetIfAbsent(
		key, c.makeValue(value, ttl, sequence))
	if added {
		c.nextSequence = sequence
		c.recordWrite(key, sequence)
	}
	return entry.value, added
}

// recordWrite records a write, then discards the oldest records whose
// entries are gone and evicts the oldest entries while more than
// c.maxEntries records remain. Assumes c.writeMutex is locked.
func (c *LRUCache[K, V]) recordWrite(key K, sequence uint64) {
	c.writes = append(c.writes, lruWrite[K]{key: key, sequence: sequence})

	// The loop ends at the latest at the record just added, which is
	// either current or the only record left.
	now := c.clock.NowNano()
	for c.oldestWrite < len(c.writes) {
		oldest := c.writes[c.oldestWrite]

		// A record is current when its write's value is still stored.
		// Only writes change sequence, and all writes hold c.writeMutex,
		// so the check cannot go stale before Invalidate.
		entry, ok := c.cache.GetEntryQuietly(oldest.key)
		current := ok && entry.Value.sequence == oldest.sequence &&
			(entry.Value.expiration == 0 || entry.Value.expiration > now)
		if current && len(c.writes)-c.oldestWrite <= c.maxEntries {
			break
		}
		if current {
			evicted, ok := c.cache.Invalidate(oldest.key)
			if ok && c.onEvicted != nil {
				// See LRUCacheConfig.OnEvicted: calls are asynchronous.
				go c.onEvicted(oldest.key, evicted.value, Capacity)
			}
		}
		c.writes[c.oldestWrite] = lruWrite[K]{}
		c.oldestWrite++
	}

	// Release the discarded records.
	if c.oldestWrite > len(c.writes)/2 {
		c.writes = append([]lruWrite[K](nil), c.writes[c.oldestWrite:]...)
		c.oldestWrite = 0
	}
}

func (c *LRUCache[K, V]) makeValue(
	value V, ttl time.Duration, sequence uint64) lruValue[V] {

	if ttl == DefaultExpiration {
		ttl = c.defaultTTL
	}
	var expiration int64
	if ttl > 0 {
		now := c.clock.NowNano()
		expiration = now + int64(ttl)
		if expiration < now {
			expiration = math.MaxInt64
		}
	}
	return lruValue[V]{
		value: value, expiration: expiration, sequence: sequence}
}

// Delete removes key. It does not cancel an eviction notification for an
// entry that has already expired or been evicted.
func (c *LRUCache[K, V]) Delete(key K) {
	c.cache.Invalidate(key)
}

// Flush removes all entries. Concurrent writes may remain in the cache.
// It does not cancel pending eviction notifications.
func (c *LRUCache[K, V]) Flush() {
	if c.insertionOrder {
		c.writeMutex.Lock()
		defer c.writeMutex.Unlock()
		c.writes = nil
		c.oldestWrite = 0
	}
	c.cache.InvalidateAll()
}

// All iterates over unexpired entries without copying the cache. Every key
// present throughout the iteration is visited once. Concurrent changes,
// including eviction, may or may not be reflected. Iteration does not count
// as access for eviction and does not extend TTLs. The loop may modify the
// cache. Order is unspecified.
func (c *LRUCache[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for key, entry := range c.cache.All() {
			if entry.expiration != 0 &&
				entry.expiration <= c.clock.NowNano() {
				continue
			}
			if !yield(key, entry.value) {
				return
			}
		}
	}
}

// EstimatedSize returns an approximate entry count. It may include expired
// entries awaiting maintenance and may change during concurrent operations.
func (c *LRUCache[K, V]) EstimatedSize() int {
	return c.cache.EstimatedSize()
}
