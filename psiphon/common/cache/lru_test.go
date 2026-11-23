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

package cache

import (
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestLRUCacheExpiration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewLRUCache(LRUCacheConfig[string, int]{DefaultTTL: time.Minute})

		c.Set("default", 1, DefaultExpiration)
		c.Set("short", 2, 10*time.Second)
		c.Set("retained", 3, NoExpiration)
		c.Set("updated", 4, 10*time.Second)
		_, expiration, ok := c.GetWithExpiration("default")
		if !ok || !expiration.Equal(time.Now().Add(time.Minute)) {
			t.Fatal("default TTL was not applied")
		}
		_, expiration, ok = c.GetWithExpiration("retained")
		if !ok || !expiration.IsZero() {
			t.Fatal("unexpected expiration for retained entry")
		}

		// Advance virtual time; Wait completes any scheduled maintenance.
		time.Sleep(5 * time.Second)
		synctest.Wait()
		c.Set("updated", 5, time.Minute)
		c.Get("short")
		_, expiration, _ = c.GetWithExpiration("short")
		if !expiration.Equal(time.Now().Add(5 * time.Second)) {
			t.Fatal("read extended TTL")
		}
		if value, added := c.Add("short", 6, time.Hour); added || value != 2 {
			t.Fatal("Add replaced an existing value")
		}

		time.Sleep(6 * time.Second)
		synctest.Wait()
		if _, ok := c.Get("short"); ok {
			t.Fatal("read or Add extended TTL")
		}
		if value, added := c.Add("short", 7, time.Hour); !added || value != 7 {
			t.Fatal("Add did not replace an expired entry")
		}

		time.Sleep(50 * time.Second)
		synctest.Wait()
		if _, ok := c.Get("default"); ok {
			t.Fatal("default TTL did not expire")
		}
		if value, ok := c.Get("updated"); !ok || value != 5 {
			t.Fatal("Set did not replace the value and extend TTL")
		}
		c.Set("updated", 8, NoExpiration)
		c.Set("retained", 9, time.Second)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if _, ok := c.Get("retained"); ok {
			t.Fatal("Set did not add expiration to retained entry")
		}
		if value, expiration, ok := c.GetWithExpiration("updated"); !ok || value != 8 || !expiration.IsZero() {
			t.Fatal("Set did not remove expiration")
		}
	})
}

func TestLRUCacheExpiredBeforeMaintenance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewLRUCache(LRUCacheConfig[string, int]{})
		c.Set("expired", 1, time.Millisecond)
		time.Sleep(2 * time.Millisecond)
		synctest.Wait()
		if _, _, ok := c.GetWithExpiration("expired"); ok {
			t.Fatal("expired entry returned before periodic cleanup")
		}
		for range c.All() {
			t.Fatal("iteration returned expired entry")
		}
		if value, added := c.Add("expired", 2, NoExpiration); !added || value != 2 {
			t.Fatal("expired entry prevented insertion")
		}
	})
}

func TestLRUCacheConcurrentAdd(t *testing.T) {
	c := NewLRUCache(LRUCacheConfig[string, *int]{})
	const workers = 64
	start := make(chan struct{})
	var wait sync.WaitGroup
	var inserted atomic.Int64
	values := make([]*int, workers)
	for i := range workers {
		wait.Go(func() {
			<-start
			value := new(int)
			*value = i
			actual, added := c.Add("shared", value, DefaultExpiration)
			values[i] = actual
			if added {
				inserted.Add(1)
			}
		})
	}
	close(start)
	wait.Wait()
	if inserted.Load() != 1 {
		t.Fatal("expected exactly one insertion")
	}
	for _, value := range values {
		if value != values[0] {
			t.Fatal("Add returned different stored values")
		}
	}
}

func TestLRUCacheConcurrentExpirationSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewLRUCache(LRUCacheConfig[string, time.Duration]{})
		now := time.Now()
		var wait sync.WaitGroup
		for worker := range 8 {
			wait.Go(func() {
				for i := range 1000 {
					ttl := time.Duration(worker+i+1) * time.Hour
					c.Set("shared", ttl, ttl)
					value, expiration, ok := c.GetWithExpiration("shared")
					if !ok || !expiration.Equal(now.Add(value)) {
						t.Error("value and expiration describe different writes")
						return
					}
				}
			})
		}
		wait.Wait()
	})
}

func TestLRUCacheEvictionCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type event struct {
			key    string
			value  int
			reason EvictionReason
		}
		events := make(chan event, 8)
		var c *LRUCache[string, int]
		c = NewLRUCache(LRUCacheConfig[string, int]{
			OnEvicted: func(key string, value int, reason EvictionReason) {
				c.Set("callback", value, NoExpiration)
				c.Get("callback")
				c.Delete("callback")
				events <- event{key, value, reason}
			},
		})
		c.Set("replaced", 1, NoExpiration)
		c.Set("replaced", 2, NoExpiration)
		c.Set("deleted", 3, NoExpiration)
		c.Delete("deleted")
		c.Flush()
		synctest.Wait()
		select {
		case e := <-events:
			t.Fatalf("manual removal reported eviction: %+v", e)
		default:
		}
		c.Set("expired", 4, time.Millisecond)
		time.Sleep(2 * time.Second)
		synctest.Wait()
		select {
		case e := <-events:
			if e != (event{"expired", 4, Expired}) {
				t.Fatalf("unexpected eviction: %+v", e)
			}
		default:
			t.Fatal("missing expiration notification")
		}
	})
}

func TestLRUCacheCapacityAndIteration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var evictions atomic.Int64
		c := NewLRUCache(LRUCacheConfig[int, int]{
			MaxEntries: 10,
			OnEvicted: func(_, _ int, reason EvictionReason) {
				if reason == Capacity {
					evictions.Add(1)
				}
			},
		})
		for i := range 100 {
			c.Set(i, i, NoExpiration)
		}
		synctest.Wait()
		if size := c.EstimatedSize(); size > 10 || size == 0 {
			t.Fatalf("unexpected size after maintenance: %d", size)
		}
		if evictions.Load() == 0 {
			t.Fatal("missing capacity notifications")
		}
		seen := make(map[int]bool)
		for key, value := range c.All() {
			if seen[key] || key != value {
				t.Fatal("unexpected iteration result")
			}
			seen[key] = true
			c.Delete(key)
		}
		if c.EstimatedSize() != 0 {
			t.Fatal("iteration did not visit all entries")
		}
	})
}

func TestLRUCacheConcurrentOperations(t *testing.T) {
	c := NewLRUCache(LRUCacheConfig[int, int]{MaxEntries: 100})
	var wait sync.WaitGroup
	for worker := range 8 {
		wait.Go(func() {
			for i := range 500 {
				key := (worker + i) % 200
				c.Set(key, key, time.Minute)
				c.Add(key, key, NoExpiration)
				if value, ok := c.Get(key); ok && value != key {
					t.Error("unexpected value")
				}
				c.GetWithExpiration(key)
				if i%10 == 0 {
					for k, v := range c.All() {
						if k != v {
							t.Error("unexpected iteration value")
						}
						break
					}
				}
				c.Delete(key)
				if i%100 == 0 {
					c.Flush()
				}
			}
		})
	}
	wait.Wait()
	c.Flush()
	if c.EstimatedSize() != 0 {
		t.Fatal("Flush retained entries")
	}
}

func TestLRUCacheUnreadEvictedInWriteOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const maxEntries = 100
		type event struct {
			key    int
			reason EvictionReason
		}
		events := make(chan event, 20*maxEntries)
		c := NewLRUCache(LRUCacheConfig[int, int]{
			MaxEntries: maxEntries,
			OnEvicted: func(key, _ int, reason EvictionReason) {
				events <- event{key, reason}
			},
		})

		// Keys are written once and not read before the check.
		for i := range 10 * maxEntries {
			c.Add(i, i, NoExpiration)
			if c.EstimatedSize() > maxEntries {
				t.Fatal("limit exceeded")
			}
		}
		// All does not count as a read.
		present := make(map[int]bool)
		for key := range c.All() {
			present[key] = true
		}
		for i := range 10 * maxEntries {
			if present[i] != (i >= 9*maxEntries) {
				t.Fatalf("unexpected presence for key %d", i)
			}
		}
		synctest.Wait()
		for i := range 9 * maxEntries {
			select {
			case e := <-events:
				if e.reason != Capacity || e.key >= 9*maxEntries {
					t.Fatalf("unexpected eviction: %+v", e)
				}
			default:
				t.Fatalf("missing capacity notification %d", i)
			}
		}

		// A deleted and rewritten key is evicted by its latest write, and
		// its deleted entry does not count.
		c.Delete(9 * maxEntries)
		c.Set(9*maxEntries, 0, NoExpiration)
		for i := range maxEntries - 1 {
			c.Set(-1-i, 0, NoExpiration)
		}
		if _, ok := c.Get(9 * maxEntries); !ok {
			t.Fatal("rewritten key evicted by its earlier write")
		}
		if c.EstimatedSize() != maxEntries {
			t.Fatalf("unexpected size: %d", c.EstimatedSize())
		}

		// Expired entries do not count, and are reported once, as Expired.
		c.Flush()
		synctest.Wait()
		for len(events) > 0 {
			<-events
		}
		for i := range maxEntries {
			c.Set(i, i, time.Second)
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		for i := range maxEntries {
			c.Set(maxEntries+i, i, NoExpiration)
		}
		synctest.Wait()
		if c.EstimatedSize() != maxEntries {
			t.Fatalf("unexpected size: %d", c.EstimatedSize())
		}
		checkLRUCacheInvariants(t, c)
		if len(events) != maxEntries {
			t.Fatalf("unexpected notification count: %d", len(events))
		}
		for len(events) > 0 {
			if e := <-events; e.reason != Expired || e.key >= maxEntries {
				t.Fatalf("unexpected eviction: %+v", e)
			}
		}
	})
}

func TestLRUCacheConcurrentLimit(t *testing.T) {
	const maxEntries = 50
	c := NewLRUCache(LRUCacheConfig[int, int]{MaxEntries: maxEntries})
	var wait sync.WaitGroup
	for worker := range 8 {
		wait.Go(func() {
			for i := range 2000 {
				key := worker*10000 + i
				c.Add(key, key, time.Minute)
				c.Set(key%300, key, NoExpiration)
				c.Get(key)
				if i%7 == 0 {
					c.Delete(key)
				}
				if i%500 == 0 {
					c.Flush()
				}
			}
		})
	}
	wait.Wait()
	c.Set(-1, 0, NoExpiration)
	if size := c.EstimatedSize(); size > maxEntries {
		t.Fatalf("limit exceeded: %d", size)
	}
	checkLRUCacheInvariants(t, c)
}

// checkLRUCacheInvariants checks that the map, the ring, the timer wheel
// and the count describe the same entries.
func checkLRUCacheInvariants[K comparable, V any](
	t *testing.T, c *LRUCache[K, V]) {

	t.Helper()
	c.mutex.Lock()
	defer c.mutex.Unlock()

	queued := 0
	scheduled := 0
	for entry := c.hand; entry != nil; entry = entry.ringNext {
		if entry == c.hand && queued > 0 {
			break
		}
		queued++
		if entry.ringNext == nil || entry.ringNext.ringPrev != entry {
			t.Fatal("inconsistent ring links")
		}
		if c.load(entry.key) != entry {
			t.Fatal("ring entry is not the stored entry")
		}
		if (entry.wheelPrev != nil) != (entry.expiration != 0) {
			t.Fatal("unexpected timer wheel state")
		}
		if entry.wheelPrev != nil {
			scheduled++
		}
	}
	stored := 0
	c.entries.Range(func(_, _ any) bool {
		stored++
		return true
	})
	if queued != stored || queued != c.EstimatedSize() {
		t.Fatalf("inconsistent counts: %d, %d, %d",
			queued, stored, c.EstimatedSize())
	}
	inBuckets := 0
	for level := 0; level < lruWheelLevels; level++ {
		for _, entry := range c.wheel.buckets(level) {
			for ; entry != nil; entry = entry.wheelNext {
				inBuckets++
			}
		}
	}
	if scheduled != c.wheel.count || scheduled != inBuckets {
		t.Fatalf("inconsistent timer wheel counts: %d, %d, %d",
			scheduled, c.wheel.count, inBuckets)
	}
	if c.wakeAt == 0 && c.wheel.count != 0 {
		t.Fatal("timer not set for timer wheel entries")
	}
	if c.maxEntries > 0 && queued > c.maxEntries {
		t.Fatal("limit exceeded")
	}
}

func TestLRUCacheReadEntriesKept(t *testing.T) {
	const maxEntries = 100
	c := NewLRUCache(LRUCacheConfig[int, int]{MaxEntries: maxEntries})

	// Keys 0 to 9 are read regularly while many other keys are written
	// once.
	for i := range 10 {
		c.Set(i, i, NoExpiration)
	}
	for i := 10; i < 100*maxEntries; i++ {
		for j := range 10 {
			if _, ok := c.Get(j); !ok {
				t.Fatalf("read key %d evicted at write %d", j, i)
			}
		}
		c.Set(i, i, NoExpiration)
		if c.EstimatedSize() > maxEntries {
			t.Fatal("limit exceeded")
		}
	}

	// The remaining entries are the most recently written.
	for i := 10; i < 100*maxEntries; i++ {
		_, ok := c.Get(i)
		if ok != (i >= 99*maxEntries+10) {
			t.Fatalf("unexpected presence for key %d: %v", i, ok)
		}
	}
	checkLRUCacheInvariants(t, c)

	// Entries that are no longer read are evicted.
	for i := range 2 * maxEntries {
		c.Set(-1-i, 0, NoExpiration)
	}
	for i := range 10 {
		if _, ok := c.Get(i); ok {
			t.Fatalf("key %d not evicted", i)
		}
	}
	checkLRUCacheInvariants(t, c)
}

func TestLRUCacheExpiredRemovedBeforeEviction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const maxEntries = 100
		type event struct {
			key    int
			reason EvictionReason
		}
		events := make(chan event, 2*maxEntries)
		c := NewLRUCache(LRUCacheConfig[int, int]{
			MaxEntries: maxEntries,
			OnEvicted: func(key, _ int, reason EvictionReason) {
				events <- event{key, reason}
			},
		})

		// The older half does not expire. The newer half expires and is
		// read, which would otherwise protect it from eviction.
		for i := range maxEntries / 2 {
			c.Set(i, i, NoExpiration)
		}
		for i := maxEntries / 2; i < maxEntries; i++ {
			c.Set(i, i, time.Hour)
			c.Get(i)
		}

		// Flush and rewrite, to stop the timer from removing the expired
		// entries before the next write does.
		time.Sleep(2 * time.Hour)
		synctest.Wait()
		for len(events) > 0 {
			<-events
		}
		if c.EstimatedSize() != maxEntries/2 {
			t.Fatalf("unexpected size: %d", c.EstimatedSize())
		}
		for i := maxEntries / 2; i < maxEntries; i++ {
			c.Set(i, i, time.Minute)
			c.Get(i)
		}
		c.mutex.Lock()
		c.timer.Stop()
		c.mutex.Unlock()
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if c.EstimatedSize() != maxEntries {
			t.Fatalf("unexpected size: %d", c.EstimatedSize())
		}

		c.Set(maxEntries, 0, NoExpiration)
		synctest.Wait()
		for i := range maxEntries / 2 {
			if _, ok := c.Get(i); !ok {
				t.Fatalf("unexpired key %d evicted", i)
			}
		}
		if c.EstimatedSize() != maxEntries/2+1 {
			t.Fatalf("unexpected size: %d", c.EstimatedSize())
		}
		if len(events) != maxEntries/2 {
			t.Fatalf("unexpected notification count: %d", len(events))
		}
		for len(events) > 0 {
			if e := <-events; e.reason != Expired || e.key < maxEntries/2 {
				t.Fatalf("unexpected eviction: %+v", e)
			}
		}
		checkLRUCacheInvariants(t, c)
	})
}

func TestLRUCacheExpirationTimes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		expired := make(map[time.Duration]time.Duration)
		var mutex sync.Mutex
		start := time.Now()
		c := NewLRUCache(LRUCacheConfig[time.Duration, int]{
			OnEvicted: func(ttl time.Duration, _ int, reason EvictionReason) {
				mutex.Lock()
				defer mutex.Unlock()
				if reason != Expired {
					t.Error("unexpected reason")
				}
				if _, ok := expired[ttl]; ok {
					t.Error("duplicate notification")
				}
				expired[ttl] = time.Since(start)
			},
		})

		// TTLs cover every timer wheel level, and its boundaries.
		var ttls []time.Duration
		for shift := 0; shift < 52; shift++ {
			ttl := time.Duration(1) << shift
			ttls = append(ttls, ttl-1, ttl, ttl+1, ttl+ttl/3)
		}
		ttls = append(ttls,
			time.Second, time.Minute, time.Hour, 24*time.Hour,
			7*24*time.Hour, 30*24*time.Hour)
		slices.Sort(ttls)
		ttls = slices.Compact(ttls)
		ttls = slices.DeleteFunc(
			ttls, func(ttl time.Duration) bool { return ttl <= 0 })
		for _, ttl := range ttls {
			c.Set(ttl, 0, ttl)
		}
		count := len(ttls)
		checkLRUCacheInvariants(t, c)

		// Each entry is readable until its TTL, and not after.
		for _, ttl := range ttls {
			time.Sleep(time.Until(start.Add(ttl - 1)))
			if _, ok := c.Get(ttl); !ok {
				t.Fatalf("entry with TTL %v expired early", ttl)
			}
			time.Sleep(time.Until(start.Add(ttl)))
			if _, ok := c.Get(ttl); ok {
				t.Fatalf("entry with TTL %v not expired", ttl)
			}
		}
		time.Sleep(time.Hour)
		synctest.Wait()

		// Each entry is removed, and reported once, within two level 0
		// ticks after its TTL.
		mutex.Lock()
		defer mutex.Unlock()
		if len(expired) != count || c.EstimatedSize() != 0 {
			t.Fatalf("unexpected counts: %d, %d, %d",
				len(expired), count, c.EstimatedSize())
		}
		for ttl, elapsed := range expired {
			if elapsed < ttl || elapsed > ttl+3*time.Second {
				t.Fatalf("entry with TTL %v removed after %v", ttl, elapsed)
			}
		}
		checkLRUCacheInvariants(t, c)
	})
}

func TestLRUCacheGetDoesNotAllocate(t *testing.T) {
	c := NewLRUCache(LRUCacheConfig[string, int]{MaxEntries: 10})
	key := "key-" + time.Now().String()
	c.Set(key, 1, time.Hour)
	allocs := testing.AllocsPerRun(1000, func() {
		if _, ok := c.Get(key); !ok {
			t.Fatal("missing entry")
		}
	})
	if allocs != 0 {
		t.Fatalf("unexpected allocations per Get: %v", allocs)
	}
}

func TestLRUCacheCollectedWithPendingTimer(t *testing.T) {
	collected := make(chan struct{})
	func() {
		c := NewLRUCache(LRUCacheConfig[int, int]{})
		c.Set(1, 1, time.Hour)
		runtime.AddCleanup(c, func(struct{}) { close(collected) }, struct{}{})
	}()
	for range 100 {
		runtime.GC()
		select {
		case <-collected:
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("cache with a pending timer was not collected")
}

func TestLRUCacheNotifications(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const maxEntries = 10
		const writes = 1000
		release := make(chan struct{})
		var running, maxRunning atomic.Int64
		var keys []int
		var c *LRUCache[int, int]
		c = NewLRUCache(LRUCacheConfig[int, int]{
			MaxEntries: maxEntries,
			OnEvicted: func(key, _ int, _ EvictionReason) {
				maxRunning.Store(max(maxRunning.Load(), running.Add(1)))
				<-release
				keys = append(keys, key)
				c.Get(key)
				running.Add(-1)
			},
		})

		// OnEvicted is blocked while writes continue. Writes do not wait,
		// and one goroutine delivers all notifications, in order.
		for i := range writes {
			c.Set(i, i, NoExpiration)
		}
		synctest.Wait()
		if running.Load() != 1 {
			t.Fatalf("unexpected OnEvicted calls running: %d", running.Load())
		}
		close(release)
		synctest.Wait()
		if maxRunning.Load() != 1 {
			t.Fatal("concurrent OnEvicted calls")
		}
		if len(keys) != writes-maxEntries {
			t.Fatalf("unexpected notification count: %d", len(keys))
		}
		for i, key := range keys {
			if key != i {
				t.Fatalf("notification %d out of order: %d", i, key)
			}
		}

		// The goroutine has exited, and a later eviction starts another.
		c.mutex.Lock()
		notifying := c.notifying
		c.mutex.Unlock()
		if notifying {
			t.Fatal("notify still running")
		}
		c.Set(writes, 0, NoExpiration)
		synctest.Wait()
		if len(keys) != writes-maxEntries+1 {
			t.Fatal("missing notification after notify exited")
		}
	})
}

func TestLRUCacheTimerAfterDelete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewLRUCache(LRUCacheConfig[int, int]{})

		// See the limitation in LRUCache: the timer remains set, fires
		// once, and is then not set again.
		c.Set(1, 1, time.Hour)
		c.Delete(1)
		c.Set(2, 2, time.Hour)
		c.Set(2, 2, NoExpiration)
		checkLRUCacheInvariants(t, c)
		time.Sleep(2 * time.Hour)
		synctest.Wait()
		c.mutex.Lock()
		wakeAt := c.wakeAt
		c.mutex.Unlock()
		if wakeAt != 0 || c.EstimatedSize() != 1 {
			t.Fatal("unexpected state after timer fired")
		}

		// The timer is set again for a new entry with a TTL.
		c.Set(3, 3, time.Minute)
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if _, ok := c.Get(3); ok || c.EstimatedSize() != 1 {
			t.Fatal("entry not expired after timer was set again")
		}
		checkLRUCacheInvariants(t, c)
	})
}

// lruModel is a reference implementation of the eviction policy, as a queue
// in which a read entry at the front is moved to the back.
type lruModel struct {
	maxEntries int
	keys       []int
	visited    map[int]bool
}

func (m *lruModel) index(key int) int {
	return slices.Index(m.keys, key)
}

func (m *lruModel) get(key int) bool {
	if m.index(key) == -1 {
		return false
	}
	m.visited[key] = true
	return true
}

func (m *lruModel) delete(key int) {
	if i := m.index(key); i != -1 {
		m.keys = slices.Delete(m.keys, i, i+1)
		delete(m.visited, key)
	}
}

func (m *lruModel) set(key int) {
	if m.index(key) != -1 {
		m.delete(key)
	} else {
		passed := 0
		for len(m.keys) >= m.maxEntries {
			front := m.keys[0]
			m.keys = m.keys[1:]
			if m.visited[front] && passed < lruMaxEntriesPassed {
				m.visited[front] = false
				m.keys = append(m.keys, front)
				passed++
				continue
			}
			delete(m.visited, front)
		}
	}
	m.keys = append(m.keys, key)
}

func (m *lruModel) add(key int) bool {
	if m.index(key) != -1 {
		return false
	}
	m.set(key)
	return true
}

// checkLRUCacheModel checks that the ring, from the hand, has the same
// entries in the same order, with the same visited state, as model.
func checkLRUCacheModel(
	t *testing.T, step int, c *LRUCache[int, int], model *lruModel) {

	t.Helper()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	entry := c.hand
	for i, key := range model.keys {
		if entry == nil || entry.key != key ||
			entry.visited.Load() != model.visited[key] {
			t.Fatalf("step %d: ring differs from model at %d", step, i)
		}
		entry = entry.ringNext
	}
	if entry != c.hand || int(c.count.Load()) != len(model.keys) {
		t.Fatalf("step %d: ring length differs from model", step)
	}
}

func TestLRUCacheMatchesModel(t *testing.T) {
	for _, maxEntries := range []int{1, 2, 3, 8, 64, 2000} {
		c := NewLRUCache(LRUCacheConfig[int, int]{MaxEntries: maxEntries})
		model := &lruModel{maxEntries: maxEntries, visited: make(map[int]bool)}

		// A fixed sequence of pseudorandom operations. Keys are drawn from
		// a range larger than the cache, so that the cache stays full, and
		// reads are frequent enough for long runs of read entries.
		state := uint64(maxEntries)
		random := func(n int) int {
			state = state*6364136223846793005 + 1442695040888963407
			return int((state >> 33) % uint64(n))
		}
		for step := range 20000 {
			key := random(2 * maxEntries)
			switch op := random(100); {
			case op < 60:
				_, ok := c.Get(key)
				if ok != model.get(key) {
					t.Fatalf("step %d: Get differs from model", step)
				}
			case op < 80:
				c.Set(key, key, NoExpiration)
				model.set(key)
			case op < 95:
				_, added := c.Add(key, key, NoExpiration)
				if added != model.add(key) {
					t.Fatalf("step %d: Add differs from model", step)
				}
			default:
				c.Delete(key)
				model.delete(key)
			}
			if maxEntries <= 64 || step%500 == 0 {
				checkLRUCacheModel(t, step, c, model)
			}
		}
		checkLRUCacheModel(t, -1, c, model)
		checkLRUCacheInvariants(t, c)
	}
}

func TestLRUCacheEntriesPassedLimit(t *testing.T) {
	const maxEntries = 3 * lruMaxEntriesPassed
	c := NewLRUCache(LRUCacheConfig[int, int]{MaxEntries: maxEntries})
	for i := range maxEntries {
		c.Set(i, i, NoExpiration)
		c.Get(i)
	}

	// Every entry has been read. The hand passes lruMaxEntriesPassed
	// entries, and evicts the next.
	c.Set(-1, 0, NoExpiration)
	for key := range c.All() {
		if key == lruMaxEntriesPassed {
			t.Fatal("expected entry was not evicted")
		}
	}
	if c.EstimatedSize() != maxEntries {
		t.Fatalf("unexpected size: %d", c.EstimatedSize())
	}
	c.mutex.Lock()
	hand := c.hand.key
	passed := 0
	for i := range lruMaxEntriesPassed {
		if !c.load(i).visited.Load() {
			passed++
		}
	}
	c.mutex.Unlock()
	if hand != lruMaxEntriesPassed+1 || passed != lruMaxEntriesPassed {
		t.Fatalf("unexpected hand or passed entries: %d, %d", hand, passed)
	}

	// With fewer entries than the limit, all read, the oldest is evicted.
	small := NewLRUCache(LRUCacheConfig[int, int]{MaxEntries: 10})
	for i := range 10 {
		small.Set(i, i, NoExpiration)
		small.Get(i)
	}
	small.Set(-1, 0, NoExpiration)
	if _, ok := small.Get(0); ok {
		t.Fatal("oldest entry was not evicted")
	}
	checkLRUCacheInvariants(t, c)
	checkLRUCacheInvariants(t, small)
}

func BenchmarkLRUCacheSetAllRead(b *testing.B) {
	const maxEntries = 1000000
	c := NewLRUCache(LRUCacheConfig[int, int]{MaxEntries: maxEntries})
	for i := range maxEntries {
		c.Set(i, i, NoExpiration)
	}
	for i := 0; b.Loop(); i++ {

		// Read lruMaxEntriesPassed+1 entries from the hand, so that each
		// write passes the maximum number of entries.
		b.StopTimer()
		c.mutex.Lock()
		entry := c.hand
		for range lruMaxEntriesPassed + 1 {
			entry.visited.Store(true)
			entry = entry.ringNext
		}
		c.mutex.Unlock()
		b.StartTimer()
		c.Set(maxEntries+i, 0, NoExpiration)
	}
}

func BenchmarkLRUCacheGet(b *testing.B) {
	c := NewLRUCache(LRUCacheConfig[string, int]{MaxEntries: 1000})
	c.Set("key", 1, time.Hour)
	for b.Loop() {
		c.Get("key")
	}
}

func BenchmarkLRUCacheGetParallel(b *testing.B) {
	c := NewLRUCache(LRUCacheConfig[string, int]{MaxEntries: 1000})
	c.Set("key", 1, time.Hour)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Get("key")
		}
	})
}

func TestLRUCacheClockChanges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var expired atomic.Int64
		c := NewLRUCache(LRUCacheConfig[string, int]{
			OnEvicted: func(_ string, _ int, reason EvictionReason) {
				if reason == Expired {
					expired.Add(1)
				}
			},
		})
		present := func(key string) bool {
			_, ok := c.Get(key)
			return ok
		}

		// remaining returns the time until key expires, as a caller would
		// compute it from GetWithExpiration and the wall clock. The wall
		// clock is time.Now plus the simulated offset.
		remaining := func(key string) time.Duration {
			t.Helper()
			_, expiration, ok := c.GetWithExpiration(key)
			if !ok || expiration.IsZero() {
				t.Fatalf("missing expiration for %s", key)
			}
			if expiration != expiration.Round(0) {
				t.Fatal("expiration has a monotonic clock reading")
			}
			wallNow := time.Now().Round(0).Add(
				time.Duration(c.clock.testWallOffset.Load()))
			return expiration.Sub(wallNow)
		}
		checkRemaining := func(key string, expected time.Duration) {
			t.Helper()
			actual := remaining(key)
			if actual > expected || actual < expected-time.Second {
				t.Fatalf("unexpected time remaining for %s: %v", key, actual)
			}
		}

		// The wall clock moves ahead of the monotonic clock, as it does
		// while a device is suspended. That time counts towards TTLs, once
		// lruClockCheckInterval has passed.
		c.Set("short", 1, time.Minute)
		c.Set("long", 1, 24*time.Hour)
		c.clock.testWallOffset.Add(int64(8 * time.Hour))
		if !present("short") {
			t.Fatal("wall clock compared before lruClockCheckInterval")
		}
		time.Sleep(time.Duration(lruClockCheckInterval))
		if present("short") || !present("long") {
			t.Fatal("unexpected entries after suspended time")
		}

		// The read that found the clock change causes the expired entry to
		// be removed, with no write and without waiting for the timer,
		// which was set for the time before the change.
		synctest.Wait()
		if c.EstimatedSize() != 1 || expired.Load() != 1 {
			t.Fatalf("expired entry not removed: %d, %d",
				c.EstimatedSize(), expired.Load())
		}

		// Expiration times are correct after the change, for entries
		// written before and after it.
		checkRemaining("long", 16*time.Hour)
		c.Set("short", 1, time.Minute)
		checkRemaining("short", time.Minute)

		// The timer is set again for the changed clock: with no further
		// reads or writes, entries are removed when they expire.
		c.Set("hour", 1, time.Hour)
		c.clock.testWallOffset.Add(int64(30 * time.Minute))
		time.Sleep(time.Duration(lruClockCheckInterval))
		c.clock.check()
		synctest.Wait()
		checkRemaining("hour", 30*time.Minute)
		time.Sleep(30*time.Minute + 3*time.Second)
		synctest.Wait()
		if c.EstimatedSize() != 1 || expired.Load() != 3 {
			t.Fatalf("entries not removed on time after clock change: %d, %d",
				c.EstimatedSize(), expired.Load())
		}

		// Setting the wall clock back does not extend TTLs, expiration
		// times follow the wall clock, and time suspended after that still
		// counts in full.
		c.clock.testWallOffset.Add(-int64(time.Hour))
		c.clock.check()

		// Nine hours and three seconds of "long"'s TTL have passed. The
		// time remaining is unchanged by the wall clock being set back.
		checkRemaining("long", 15*time.Hour-3*time.Second)
		c.Set("short", 1, time.Minute)
		checkRemaining("short", time.Minute)
		time.Sleep(30 * time.Second)
		if !present("short") {
			t.Fatal("entry expired early after wall clock set back")
		}
		time.Sleep(30 * time.Second)
		if present("short") {
			t.Fatal("entry not expired after wall clock set back")
		}
		c.Set("medium", 1, 2*time.Hour)
		c.clock.testWallOffset.Add(int64(3 * time.Hour))
		c.clock.check()
		if present("medium") {
			t.Fatal("suspended time after wall clock set back not counted")
		}

		// Changes smaller than lruClockJumpThreshold are ignored until
		// they accumulate.
		c.Set("short", 1, 2*time.Second)
		c.clock.testWallOffset.Add(int64(900 * time.Millisecond))
		time.Sleep(time.Second)
		if !present("short") {
			t.Fatal("small wall clock change expired entry")
		}
		c.clock.testWallOffset.Add(int64(900 * time.Millisecond))
		c.clock.check()
		if present("short") {
			t.Fatal("accumulated wall clock change not counted")
		}

		time.Sleep(48 * time.Hour)
		synctest.Wait()
		if c.EstimatedSize() != 0 {
			t.Fatalf("unexpected size: %d", c.EstimatedSize())
		}
		checkLRUCacheInvariants(t, c)
	})
}

func TestLRUCacheClockConcurrent(t *testing.T) {
	c := NewLRUCache(LRUCacheConfig[int, int]{})
	var wait sync.WaitGroup
	for range 8 {
		wait.Go(func() {
			previous := int64(0)
			for range 20000 {
				now := c.clock.now()
				if now < previous {
					t.Error("clock went back")
					return
				}
				previous = now
			}
		})
	}
	const jumps = 100
	for i := range jumps {
		if i%10 == 9 {
			c.clock.testWallOffset.Add(-int64(time.Hour))
		} else {
			c.clock.testWallOffset.Add(int64(time.Hour))
		}
		c.clock.check()
	}
	wait.Wait()
	c.clock.check()

	// Each forward jump is counted once.
	adjustment := time.Duration(c.clock.adjustment.Load())
	expected := time.Duration(jumps-jumps/10) * time.Hour
	if adjustment < expected-time.Minute || adjustment > expected+time.Minute {
		t.Fatalf("unexpected adjustment: %v", adjustment)
	}
}

func TestLRUCacheExpirationHasNoMonotonicReading(t *testing.T) {

	// This test does not use synctest, in which time.Now has no monotonic
	// clock reading.
	if now := time.Now(); now == now.Round(0) {
		t.Skip("time.Now has no monotonic clock reading")
	}
	c := NewLRUCache(LRUCacheConfig[string, int]{})
	c.clock.testWallOffset.Add(int64(8 * time.Hour))
	c.clock.check()
	c.Set("key", 1, time.Minute)
	_, expiration, ok := c.GetWithExpiration("key")
	if !ok {
		t.Fatal("missing entry")
	}

	// With a monotonic clock reading, time.Until would use it, and would
	// be wrong by the clock adjustment.
	if expiration != expiration.Round(0) {
		t.Fatal("expiration has a monotonic clock reading")
	}
	wallNow := time.Now().Round(0).Add(8 * time.Hour)
	remaining := expiration.Sub(wallNow)
	if remaining > time.Minute || remaining < time.Minute-5*time.Second {
		t.Fatalf("unexpected time remaining: %v", remaining)
	}
}
