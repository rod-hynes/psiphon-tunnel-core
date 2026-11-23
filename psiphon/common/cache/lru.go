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
//
// LRUCache combines three parts:
//
//   - A sync.Map from key to entry, which reads use without taking a lock.
//   - A ring of entries, used to choose the entry to evict when the cache
//     is full. See LRUCache for the eviction policy.
//   - A timer wheel, lruWheel, which removes expired entries without
//     scanning the cache.
//
// An entry's value and expiration are never modified after the entry is
// stored. A write replaces the whole entry, so a read always returns a
// value together with its own expiration.
//
// LRUCache.mutex serializes writes and guards the ring, the timer wheel
// and the timer. Every entry in the sync.Map is in the ring, and both are
// changed together with LRUCache.mutex locked. OnEvicted is never called
// with LRUCache.mutex locked.
package cache

import (
	"iter"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"weak"
)

const (
	// DefaultExpiration uses the default TTL specified at construction.
	DefaultExpiration time.Duration = 0

	// NoExpiration disables expiration for an entry. Size eviction still
	// applies.
	NoExpiration time.Duration = -1
)

// lruMaxEntriesPassed is the maximum number of read entries that the hand
// passes in one write; see LRUCache. It bounds the time that one write holds
// LRUCache.mutex.
const lruMaxEntriesPassed = 1024

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

	// MaxEntries is the size limit, which is never exceeded. A nonpositive
	// value disables the limit.
	MaxEntries int

	// OnEvicted is called asynchronously after expiration or size eviction.
	// Calls are made without cache locks held, in the order the entries were
	// removed, and may access the cache. Notifications may arrive after the
	// key has been written again, deleted, or flushed. Callers must check
	// whether an event still applies before changing other state.
	//
	// Delete and Flush do not notify OnEvicted, and neither does replacing
	// an unexpired entry. Replacing an expired entry reports Expired.
	// Expiration notifications are delivered up to a few seconds after the
	// deadline, so they are not precise deadline timers.
	//
	// At most one goroutine calls OnEvicted at a time, and it exits once all
	// pending notifications are delivered.
	//
	// Limitation: the number of pending notifications is not bounded. If
	// OnEvicted blocks, notifications accumulate at the rate of expirations
	// and evictions.
	OnEvicted func(key K, value V, reason EvictionReason)
}

// LRUCache is a concurrent cache supporting a size limit and per-entry
// expiration.
//
// When the cache is full, a write of a new key first removes expired
// entries, and then, if the cache is still full, evicts one entry using the
// CLOCK policy, also known as second chance. Entries are kept in a ring, in
// the order written, and a hand marks the oldest. The entry at the hand is
// evicted, unless it has been read since the hand last passed it, in which
// case the hand moves on to the next entry, and the passed entry is then
// the last that the hand will reach again. Entries that are never read are
// therefore evicted in the order written, and an entry is kept for as long
// as it is read at least once in each turn of the hand. This approximates
// least-recently-used eviction, but is not strict. Set of an existing key
// counts as a new write.
//
// To bound the work done by one write, the hand passes at most
// lruMaxEntriesPassed read entries, and then evicts the next entry even if
// it has been read. This applies only when that many entries in a row have
// all been read.
//
// Reads do not take a lock and never extend expiration. Expired entries are
// not returned, even before they are removed. Values are copied by
// assignment. Data referenced by pointers, maps, or slices is shared and
// requires caller synchronization.
//
// TTLs are measured with lruClock, which follows the monotonic clock, so
// setting the wall clock back does not extend TTLs, and also moves forward
// when the wall clock moves ahead of the monotonic clock, so time that the
// device spends suspended counts towards TTLs. Setting the wall clock
// forward expires entries early. A wall clock change takes effect within
// lruClockCheckInterval.
//
// Use NewLRUCache to construct a cache. A cache has no long-running
// goroutine and does not need to be closed: expired entries are removed by
// a timer that is set when an entry with a TTL is stored, and that does not
// keep an unreachable cache from being garbage collected.
//
// Limitation: the timer is not stopped or set later when the entry it was
// set for is deleted or replaced. It still fires at that entry's time and
// finds nothing to remove. It is then set for the next entry with a TTL,
// or, when there is none, not set again until one is stored. This is
// intentional: the timer fires at most about once per second, while
// stopping or resetting it would add work to every such delete or
// replacement.
type LRUCache[K comparable, V any] struct {
	clock      lruClock
	defaultTTL time.Duration
	maxEntries int
	onEvicted  func(key K, value V, reason EvictionReason)

	// entries maps K to *lruEntry[K, V]. Reads do not lock mutex.
	entries sync.Map
	count   atomic.Int64

	mutex sync.Mutex

	// hand is the oldest entry in the ring, which is the next entry to
	// consider for eviction, or nil when the cache is empty. Following
	// lruEntry.ringNext from hand visits entries from the oldest to the
	// newest, which is hand.ringPrev, and then returns to hand.
	hand *lruEntry[K, V]

	wheel lruWheel[K, V]

	// timer calls expire at wakeAt, an lruClock time. wakeAt is 0 when no
	// call is pending. A call is pending whenever the timer wheel has
	// entries, and may be pending when it has none; see LRUCache.
	timer  *time.Timer
	wakeAt int64

	// expireFunc calls expire. It holds only a weak pointer to the cache,
	// so that a pending timer does not keep the cache reachable.
	expireFunc func()

	// pendingEvictions are the OnEvicted calls not yet made. notifying is
	// set while a goroutine is running notify.
	pendingEvictions []lruEviction[K, V]
	notifying        bool
}

// lruEntry is one stored write. key, value and expiration are not modified
// after the entry is stored.
type lruEntry[K comparable, V any] struct {
	key   K
	value V

	// expiration is an lruClock time, or 0 for no expiration.
	expiration int64

	// visited is set by reads and cleared when the hand passes the entry.
	visited atomic.Bool

	// The ring and timer wheel links are guarded by LRUCache.mutex.
	// ringNext and ringPrev link the ring; see LRUCache.hand. They are nil
	// when the entry is not in the ring. wheelPrev and wheelNext link a timer
	// wheel bucket, and are nil when the entry is not in the timer wheel.
	ringNext  *lruEntry[K, V]
	ringPrev  *lruEntry[K, V]
	wheelPrev *lruEntry[K, V]
	wheelNext *lruEntry[K, V]
}

func (e *lruEntry[K, V]) isExpired(now int64) bool {
	return e.expiration != 0 && e.expiration <= now
}

// lruEviction is a pending OnEvicted call.
type lruEviction[K comparable, V any] struct {
	key    K
	value  V
	reason EvictionReason
}

const (
	// lruClockJumpThreshold is the smallest change in the difference
	// between the wall clock and the monotonic clock that lruClock acts
	// on. Smaller changes accumulate until they reach it.
	lruClockJumpThreshold = int64(time.Second)

	// lruClockCheckInterval is the minimum monotonic time between
	// lruClock's readings of the wall clock. Reading only the monotonic
	// clock is cheaper, and reads of the cache read the clock.
	lruClockCheckInterval = int64(100 * time.Millisecond)
)

// lruClock is the clock for TTLs. It reports the wall clock time at start,
// plus the monotonic time elapsed since, plus adjustment.
//
// The monotonic clock is not affected by wall clock changes, but on some
// platforms it stops while the device is suspended. To count that time,
// adjustment is increased whenever the wall clock is found to have moved
// forward relative to the monotonic clock. When the wall clock moves back
// relative to the monotonic clock, adjustment is unchanged, so lruClock
// never goes back.
//
// The wall clock is compared at most once per lruClockCheckInterval, so a
// change is acted on within that much monotonic time, when the clock is
// next read.
type lruClock struct {
	start     time.Time
	startNano int64

	adjustment atomic.Int64

	// checked is the unadjusted lruClock time of the last check.
	checked atomic.Int64

	// mutex guards baseline, the wall clock time minus the unadjusted
	// lruClock time as of the last change to adjustment or baseline.
	mutex    sync.Mutex
	baseline int64

	// wallOffset is the wall clock time minus the lruClock time. It changes
	// only when the wall clock moves back relative to the monotonic clock.
	wallOffset atomic.Int64

	// adjusted, when not nil, is called after adjustment is increased. It
	// is set before the clock is first read, and must not block, since it
	// may be called with LRUCache.mutex locked.
	adjusted func()

	// testWallOffset is added to wall clock readings. It is set only by
	// tests, to simulate wall clock changes and suspended time.
	testWallOffset atomic.Int64
}

func (c *lruClock) init() {
	c.start = time.Now()
	c.startNano = c.start.UnixNano()
}

func (c *lruClock) now() int64 {
	unadjusted := c.startNano + int64(time.Since(c.start))
	if unadjusted-c.checked.Load() >= lruClockCheckInterval {
		c.check()
	}
	return unadjusted + c.adjustment.Load()
}

// check compares the wall clock with the monotonic clock and updates
// adjustment; see lruClock.
func (c *lruClock) check() {

	// When another goroutine is checking, there is nothing more to do.
	if !c.mutex.TryLock() {
		return
	}

	now := time.Now()
	unadjusted := c.startNano + int64(now.Sub(c.start))
	difference := now.UnixNano() + c.testWallOffset.Load() - unadjusted
	change := difference - c.baseline
	adjusted := false
	if change > lruClockJumpThreshold {
		c.adjustment.Add(change)
		c.baseline = difference
		adjusted = true
	} else if change < -lruClockJumpThreshold {
		c.baseline = difference
		c.wallOffset.Add(change)
	}
	c.checked.Store(unadjusted)
	c.mutex.Unlock()

	if adjusted && c.adjusted != nil {
		c.adjusted()
	}
}

// time converts an lruClock time to the wall clock time at which lruClock
// will reach it. The same input gives the same result until the wall clock
// is next set back.
//
// The result has no monotonic clock reading. With one, comparisons with
// time.Now would use it, and be wrong by adjustment.
func (c *lruClock) time(nano int64) time.Time {
	return time.Unix(0, nano+c.wallOffset.Load())
}

// NewLRUCache creates an empty cache with the specified configuration.
func NewLRUCache[K comparable, V any](config LRUCacheConfig[K, V]) *LRUCache[K, V] {
	c := &LRUCache[K, V]{
		defaultTTL: config.DefaultTTL,
		maxEntries: max(0, config.MaxEntries),
		onEvicted:  config.OnEvicted,
	}
	weakRef := weak.Make(c)
	c.expireFunc = func() {
		if c := weakRef.Value(); c != nil {
			c.expire()
		}
	}

	// After a clock adjustment, entries are due earlier than the timer,
	// which measures monotonic time, will fire. Call expire now; it sets
	// the timer again.
	expireFunc := c.expireFunc
	c.clock.adjusted = func() {
		time.AfterFunc(0, expireFunc)
	}

	c.clock.init()
	c.wheel.init(c.clock.now())
	return c
}

// Get returns the unexpired value for key, if present.
func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	entry := c.getEntry(key)
	if entry == nil {
		var zero V
		return zero, false
	}
	return entry.value, true
}

// getEntry returns the unexpired entry for key, or nil, and records the
// read for the eviction policy.
func (c *LRUCache[K, V]) getEntry(key K) *lruEntry[K, V] {
	entry := c.load(key)
	if entry == nil ||
		(entry.expiration != 0 && entry.expiration <= c.clock.now()) {
		return nil
	}

	// Check first, to avoid writing to memory shared between CPUs on every
	// read of a frequently read entry.
	if !entry.visited.Load() {
		entry.visited.Store(true)
	}
	return entry
}

func (c *LRUCache[K, V]) load(key K) *lruEntry[K, V] {
	value, ok := c.entries.Load(key)
	if !ok {
		return nil
	}
	return value.(*lruEntry[K, V])
}

// GetWithExpiration also returns the expiration time. A zero time means
// the entry does not expire. The time is the wall clock time of expiration
// if the wall clock is not changed before then; see LRUCache. All return
// values describe the same entry.
func (c *LRUCache[K, V]) GetWithExpiration(key K) (V, time.Time, bool) {
	entry := c.getEntry(key)
	if entry == nil {
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	now := c.clock.now()
	existing := c.load(key)
	if existing != nil {
		if existing.isExpired(now) {
			c.addEviction(existing, Expired)
		}
		c.unlink(existing)
	} else {
		c.makeRoom(now)
	}
	c.store(now, key, value, ttl)
	c.notifyAsync()
}

// Add inserts key only if it has no unexpired value. It returns the stored
// value and whether it was inserted. An existing entry's TTL is unchanged,
// and Add does not count as a read of it. TTL has the same meaning as in
// Set.
func (c *LRUCache[K, V]) Add(key K, value V, ttl time.Duration) (V, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	now := c.clock.now()
	existing := c.load(key)
	if existing != nil {
		if !existing.isExpired(now) {
			return existing.value, false
		}
		c.addEviction(existing, Expired)
		c.unlink(existing)
	} else {
		c.makeRoom(now)
	}
	c.store(now, key, value, ttl)
	c.notifyAsync()
	return value, true
}

// store adds a new entry for key, which must have no entry in c.entries
// that is still in the ring. Assumes c.mutex is locked.
func (c *LRUCache[K, V]) store(
	now int64, key K, value V, ttl time.Duration) {

	if ttl == DefaultExpiration {
		ttl = c.defaultTTL
	}
	var expiration int64
	if ttl > 0 {
		expiration = now + int64(ttl)
		if expiration < now {
			expiration = math.MaxInt64
		}
	}
	entry := &lruEntry[K, V]{key: key, value: value, expiration: expiration}

	// Store replaces the map's entry for a replaced key in one step, so
	// readers never find the key missing.
	c.entries.Store(key, entry)
	c.count.Add(1)
	c.enqueue(entry)

	if expiration != 0 {
		c.setTimer(now, c.wheel.schedule(now, entry))
	}
}

// enqueue adds entry to the ring as the newest entry. Assumes c.mutex is
// locked.
func (c *LRUCache[K, V]) enqueue(entry *lruEntry[K, V]) {
	if c.hand == nil {
		entry.ringNext = entry
		entry.ringPrev = entry
		c.hand = entry
		return
	}
	newest := c.hand.ringPrev
	entry.ringPrev = newest
	entry.ringNext = c.hand
	newest.ringNext = entry
	c.hand.ringPrev = entry
}

// dequeue removes entry from the ring. Assumes c.mutex is locked.
func (c *LRUCache[K, V]) dequeue(entry *lruEntry[K, V]) {
	if entry.ringNext == entry {
		c.hand = nil
	} else {
		entry.ringPrev.ringNext = entry.ringNext
		entry.ringNext.ringPrev = entry.ringPrev
		if c.hand == entry {
			c.hand = entry.ringNext
		}
	}
	entry.ringNext = nil
	entry.ringPrev = nil
}

// unlink removes entry from the ring and the timer wheel, but not from
// c.entries. Assumes c.mutex is locked.
func (c *LRUCache[K, V]) unlink(entry *lruEntry[K, V]) {
	c.dequeue(entry)
	c.wheel.deschedule(entry)
	c.count.Add(-1)
}

// remove removes entry from the cache. Assumes c.mutex is locked.
func (c *LRUCache[K, V]) remove(entry *lruEntry[K, V]) {
	c.unlink(entry)
	c.entries.Delete(entry.key)
}

// makeRoom ensures that one more entry can be stored within c.maxEntries.
// It removes expired entries before evicting any other entry. Assumes
// c.mutex is locked.
//
// Limitation: an entry that expired within the last timer wheel tick,
// about one second, and that is not at c.hand, may still be stored when
// another entry is evicted.
func (c *LRUCache[K, V]) makeRoom(now int64) {

	if c.maxEntries == 0 || int(c.count.Load()) < c.maxEntries {
		return
	}

	c.removeExpired(now)

	passed := 0
	for int(c.count.Load()) >= c.maxEntries {
		entry := c.hand
		if entry.isExpired(now) {
			c.addEviction(entry, Expired)
		} else if entry.visited.Load() && passed < lruMaxEntriesPassed {

			// Moving the hand makes entry the newest in the ring.
			entry.visited.Store(false)
			c.hand = entry.ringNext
			passed++
			continue
		} else {
			c.addEviction(entry, Capacity)
		}
		c.remove(entry)
	}
}

// removeExpired removes the expired entries that the timer wheel has
// reached. Assumes c.mutex is locked.
func (c *LRUCache[K, V]) removeExpired(now int64) {
	c.wheel.advance(now, func(entry *lruEntry[K, V]) {
		c.addEviction(entry, Expired)
		c.remove(entry)
	})
}

// addEviction adds a pending OnEvicted call. Assumes c.mutex is locked.
func (c *LRUCache[K, V]) addEviction(
	entry *lruEntry[K, V], reason EvictionReason) {

	if c.onEvicted == nil {
		return
	}
	c.pendingEvictions = append(c.pendingEvictions, lruEviction[K, V]{
		key: entry.key, value: entry.value, reason: reason})
}

// startNotify reports whether the caller must call notify: there are
// pending OnEvicted calls and no goroutine is running notify. Assumes
// c.mutex is locked.
func (c *LRUCache[K, V]) startNotify() bool {
	if len(c.pendingEvictions) == 0 || c.notifying {
		return false
	}
	c.notifying = true
	return true
}

// notifyAsync runs notify in a new goroutine, if required. The caller of a
// write may hold locks that OnEvicted takes, so OnEvicted is not called
// from the write. Assumes c.mutex is locked.
func (c *LRUCache[K, V]) notifyAsync() {
	if c.startNotify() {
		go c.notify()
	}
}

// notify makes the pending OnEvicted calls, including those added while it
// runs. It must be called only after startNotify returns true, and without
// c.mutex locked.
func (c *LRUCache[K, V]) notify() {
	for {
		c.mutex.Lock()
		evictions := c.pendingEvictions
		c.pendingEvictions = nil
		if len(evictions) == 0 {
			c.notifying = false
			c.mutex.Unlock()
			return
		}
		c.mutex.Unlock()
		for _, eviction := range evictions {
			c.onEvicted(eviction.key, eviction.value, eviction.reason)
		}
	}
}

// setTimer arranges for expire to be called at wakeAt, unless a call is
// already pending at or before that time. Assumes c.mutex is locked.
func (c *LRUCache[K, V]) setTimer(now int64, wakeAt int64) {
	if c.wakeAt != 0 && c.wakeAt <= wakeAt {
		return
	}
	c.wakeAt = wakeAt
	delay := time.Duration(max(0, wakeAt-now))
	if c.timer != nil {
		c.timer.Reset(delay)
		return
	}
	c.timer = time.AfterFunc(delay, c.expireFunc)
}

// expire is called by c.timer, and after a clock adjustment. It removes the
// expired entries that the timer wheel has reached and sets the timer for
// the next ones.
func (c *LRUCache[K, V]) expire() {
	c.mutex.Lock()
	now := c.clock.now()
	c.removeExpired(now)
	c.wakeAt = 0
	if wakeAt := c.wheel.nextWake(); wakeAt != 0 {
		c.setTimer(now, wakeAt)
	}
	notify := c.startNotify()
	c.mutex.Unlock()
	if notify {
		c.notify()
	}
}

// Delete removes key.
func (c *LRUCache[K, V]) Delete(key K) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if entry := c.load(key); entry != nil {
		c.remove(entry)
	}
}

// Flush removes all entries.
func (c *LRUCache[K, V]) Flush() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.entries.Clear()
	c.count.Store(0)
	c.hand = nil
	c.wheel.init(c.clock.now())
	if c.timer != nil {
		c.timer.Stop()
	}
	c.wakeAt = 0
}

// All iterates over unexpired entries without copying the cache. Every key
// present throughout the iteration is visited once. Concurrent changes,
// including eviction, may or may not be reflected. Iteration does not count
// as a read for eviction and does not extend TTLs. The loop may modify the
// cache. Order is unspecified.
func (c *LRUCache[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		c.entries.Range(func(_, value any) bool {
			entry := value.(*lruEntry[K, V])
			if entry.isExpired(c.clock.now()) {
				return true
			}
			return yield(entry.key, entry.value)
		})
	}
}

// EstimatedSize returns the entry count. It may include expired entries
// that have not yet been removed.
func (c *LRUCache[K, V]) EstimatedSize() int {
	return int(c.count.Load())
}
