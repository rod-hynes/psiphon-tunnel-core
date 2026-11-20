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
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestLRUCacheExpiration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewLRUCache(LRUCacheConfig[string, int]{DefaultTTL: time.Minute})
		defer c.cache.StopAllGoroutines()

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
		defer c.cache.StopAllGoroutines()
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
	defer c.cache.StopAllGoroutines()
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
		defer c.cache.StopAllGoroutines()
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
		defer c.cache.StopAllGoroutines()
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
		defer c.cache.StopAllGoroutines()
		for i := range 100 {
			c.Set(i, i, NoExpiration)
		}
		c.cache.CleanUp()
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
	defer c.cache.StopAllGoroutines()
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

func TestLRUCacheInsertionOrderEviction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const maxEntries = 100
		type event struct {
			key    int
			reason EvictionReason
		}
		events := make(chan event, 20*maxEntries)
		c := NewLRUCache(LRUCacheConfig[int, int]{
			MaxEntries:             maxEntries,
			InsertionOrderEviction: true,
			OnEvicted: func(key, _ int, reason EvictionReason) {
				events <- event{key, reason}
			},
		})
		defer c.cache.StopAllGoroutines()

		// Keys are written once and not read before the check.
		for i := range 10 * maxEntries {
			c.Add(i, i, NoExpiration)
			if c.EstimatedSize() > maxEntries {
				t.Fatal("limit exceeded")
			}
		}
		for i := range 10 * maxEntries {
			_, ok := c.Get(i)
			if ok != (i >= 9*maxEntries) {
				t.Fatalf("unexpected presence for key %d: %v", i, ok)
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
		// its deleted entry stops counting once older entries are gone.
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
		if len(c.writes)-c.oldestWrite != maxEntries {
			t.Fatalf("unexpected record count: %d",
				len(c.writes)-c.oldestWrite)
		}
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

func TestLRUCacheInsertionOrderConcurrent(t *testing.T) {
	const maxEntries = 50
	c := NewLRUCache(LRUCacheConfig[int, int]{
		MaxEntries:             maxEntries,
		InsertionOrderEviction: true,
	})
	defer c.cache.StopAllGoroutines()
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
	if len(c.writes)-c.oldestWrite > maxEntries {
		t.Fatal("record limit exceeded")
	}
}
