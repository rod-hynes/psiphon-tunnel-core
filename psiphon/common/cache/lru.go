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
// It uses adaptive W-TinyLFU eviction, which considers both recency and
// frequency of access. It does not guarantee strict least-recently-used
// eviction.
//
// Reads never extend expiration. Writes atomically publish both the value
// and its TTL. Expired entries are not returned, even before maintenance
// removes them. Values are copied by assignment. Data referenced by pointers,
// maps, or slices is shared and requires caller synchronization.
//
// Use NewLRUCache to construct a cache. Its background maintenance stops
// after the cache becomes unreachable and is garbage collected.
type LRUCache[K comparable, V any] struct {
	cache      *otter.Cache[K, lruValue[V]]
	defaultTTL time.Duration
}

type lruValue[V any] struct {
	value      V
	expiration int64
}

// NewLRUCache creates an empty cache with the specified configuration.
func NewLRUCache[K comparable, V any](config LRUCacheConfig[K, V]) *LRUCache[K, V] {
	options := &otter.Options[K, lruValue[V]]{
		MaximumSize: max(0, config.MaxEntries),
		ExpiryCalculator: otter.ExpiryWritingFunc(
			func(entry otter.Entry[K, lruValue[V]]) time.Duration {
				if entry.Value.expiration == 0 {
					return time.Duration(math.MaxInt64)
				}
				return time.Duration(max(0,
					entry.Value.expiration-entry.SnapshotAtNano))
			}),
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
	return &LRUCache[K, V]{cache: backend, defaultTTL: config.DefaultTTL}
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
// the entry does not expire. All return values describe the same entry.
func (c *LRUCache[K, V]) GetWithExpiration(key K) (V, time.Time, bool) {
	entry, ok := c.getEntry(key)
	if !ok {
		var zero V
		return zero, time.Time{}, false
	}
	var expiration time.Time
	if entry.expiration != 0 {
		expiration = time.Unix(0, entry.expiration)
	}
	return entry.value, expiration, true
}

// Set inserts or replaces key and resets its TTL. A zero TTL selects
// DefaultExpiration, and a negative TTL selects NoExpiration.
func (c *LRUCache[K, V]) Set(key K, value V, ttl time.Duration) {
	c.cache.Set(key, c.makeValue(value, ttl))
}

// Add inserts key only if it has no unexpired value. It returns the stored
// value and whether it was inserted. An existing entry's TTL is unchanged.
// TTL has the same meaning as in Set.
func (c *LRUCache[K, V]) Add(key K, value V, ttl time.Duration) (V, bool) {
	entry, added := c.cache.SetIfAbsent(key, c.makeValue(value, ttl))
	return entry.value, added
}

func (c *LRUCache[K, V]) makeValue(value V, ttl time.Duration) lruValue[V] {
	if ttl == DefaultExpiration {
		ttl = c.defaultTTL
	}
	var expiration int64
	if ttl > 0 {
		now := time.Now().UnixNano()
		expiration = now + int64(ttl)
		if expiration < now {
			expiration = math.MaxInt64
		}
	}
	return lruValue[V]{value: value, expiration: expiration}
}

// Delete removes key. It does not cancel an eviction notification for an
// entry that has already expired or been evicted.
func (c *LRUCache[K, V]) Delete(key K) {
	c.cache.Invalidate(key)
}

// Flush removes all entries. Concurrent writes may remain in the cache.
// It does not cancel pending eviction notifications.
func (c *LRUCache[K, V]) Flush() {
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
			if entry.expiration != 0 && entry.expiration <= time.Now().UnixNano() {
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
