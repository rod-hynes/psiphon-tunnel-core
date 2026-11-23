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

// lruWheel is based on the timer wheel in otter:
// https://github.com/maypok86/otter/blob/v2.4.0/internal/expiration/wheel.go
//
// The levels, bucket counts and bucket spans, the choice of bucket for an
// expiration, and the way advance processes buckets follow otter. The code
// was rewritten for LRUCache: it uses typed entries and bucket lists without
// sentinel nodes, and adds an entry count and nextWake.
/*
Copyright (c) 2024 Alexey Mayshev and contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// lruWheel is a hierarchical timer wheel: it keeps entries in buckets by
// expiration, so that expired entries are found without scanning all
// entries. An lruWheel is guarded by LRUCache.mutex.
//
// There are five levels. A bucket of a level covers the time span below,
// and a level holds entries that expire within all its buckets' spans from
// now. The last level has one bucket, which holds all later entries.
//
//	level   buckets   bucket span
//	0       64        1.07 seconds
//	1       64        1.14 minutes
//	2       32        1.22 hours
//	3       4         1.63 days
//	4       1         6.5 days
//
// Time is divided into ticks of the bucket span at each level, and an entry
// is placed in the bucket for the tick that contains its expiration. When
// time reaches a bucket's tick, advance moves the bucket's entries to lower
// levels. An entry is reported as expired by advance once the level 0 tick
// that contains its expiration has passed, so up to 1.07 seconds late.
type lruWheel[K comparable, V any] struct {

	// nanos is the lruClock time of the last advance, or of the last
	// schedule into an empty wheel.
	nanos int64

	// count is the number of entries in the wheel.
	count int

	// Each bucket is the first entry of a list linked with
	// lruEntry.wheelNext and lruEntry.wheelPrev, or nil. The first entry's
	// wheelPrev is itself, so that deschedule can tell that the entry is
	// first without knowing its bucket.
	level0 [64]*lruEntry[K, V]
	level1 [64]*lruEntry[K, V]
	level2 [32]*lruEntry[K, V]
	level3 [4]*lruEntry[K, V]
	level4 [1]*lruEntry[K, V]
}

const lruWheelLevels = 5

// lruWheelShifts gives each level's bucket span as a power of two
// nanoseconds, followed by the total span of level 3.
var lruWheelShifts = [lruWheelLevels + 1]uint{30, 36, 42, 47, 49, 49}

func (w *lruWheel[K, V]) init(now int64) {
	*w = lruWheel[K, V]{nanos: now}
}

func (w *lruWheel[K, V]) buckets(level int) []*lruEntry[K, V] {
	switch level {
	case 0:
		return w.level0[:]
	case 1:
		return w.level1[:]
	case 2:
		return w.level2[:]
	case 3:
		return w.level3[:]
	}
	return w.level4[:]
}

// schedule adds entry, which must have an expiration after now and not be
// in the wheel, and returns the lruClock time at which advance must next be
// called for it.
func (w *lruWheel[K, V]) schedule(now int64, entry *lruEntry[K, V]) int64 {

	// With no entries, there are no buckets for advance to process.
	if w.count == 0 && now > w.nanos {
		w.nanos = now
	}
	w.count++

	// The level is chosen relative to w.nanos, not the current time, since
	// advance processes buckets relative to w.nanos.
	duration := entry.expiration - w.nanos
	level := lruWheelLevels - 1
	for i := 0; i < lruWheelLevels-1; i++ {
		if duration < int64(1)<<lruWheelShifts[i+1] {
			level = i
			break
		}
	}

	shift := lruWheelShifts[level]
	buckets := w.buckets(level)
	tick := entry.expiration >> shift
	index := int(tick) & (len(buckets) - 1)

	first := buckets[index]
	entry.wheelNext = first
	entry.wheelPrev = entry
	if first != nil {
		first.wheelPrev = entry
	}
	buckets[index] = entry

	// At level 0, the entry is expired once its tick has passed. At other
	// levels, the entry must move to a lower level when its tick starts.
	if level == 0 {
		tick++
	}
	return saturatingShift(tick, shift)
}

// deschedule removes entry from the wheel, if it is in the wheel.
func (w *lruWheel[K, V]) deschedule(entry *lruEntry[K, V]) {
	if entry.wheelPrev == nil {
		return
	}
	if entry.wheelPrev == entry {

		// The entry is the first in its bucket. Its bucket is recomputed
		// rather than stored in each entry.
		w.removeFirst(entry)
	} else {
		entry.wheelPrev.wheelNext = entry.wheelNext
		if entry.wheelNext != nil {
			entry.wheelNext.wheelPrev = entry.wheelPrev
		}
	}
	entry.wheelPrev = nil
	entry.wheelNext = nil
	w.count--
}

func (w *lruWheel[K, V]) removeFirst(entry *lruEntry[K, V]) {
	for level := 0; level < lruWheelLevels; level++ {
		buckets := w.buckets(level)
		tick := entry.expiration >> lruWheelShifts[level]
		index := int(tick) & (len(buckets) - 1)
		if buckets[index] == entry {
			buckets[index] = entry.wheelNext
			if entry.wheelNext != nil {
				entry.wheelNext.wheelPrev = entry.wheelNext
			}
			return
		}
	}
}

// advance moves the wheel to now. It calls expired for each entry whose
// expiration is at or before now and that is in a bucket whose tick has
// been reached; expired entries are first removed from the wheel. Other
// entries in those buckets are scheduled again.
func (w *lruWheel[K, V]) advance(
	now int64, expired func(entry *lruEntry[K, V])) {

	previous := w.nanos
	if now <= previous {
		return
	}
	w.nanos = now

	for level := 0; level < lruWheelLevels; level++ {
		shift := lruWheelShifts[level]
		previousTick := previous >> shift
		tick := now >> shift
		if tick == previousTick {

			// Higher levels have longer ticks, so theirs are unchanged too.
			break
		}

		// Include the bucket for previousTick: its entries that were not
		// expired at the last advance were scheduled into it again.
		buckets := w.buckets(level)
		count := min(tick-previousTick+1, int64(len(buckets)))
		for i := int64(0); i < count; i++ {
			index := int(previousTick+i) & (len(buckets) - 1)
			entry := buckets[index]
			buckets[index] = nil
			for entry != nil {
				next := entry.wheelNext
				entry.wheelPrev = nil
				entry.wheelNext = nil
				w.count--
				if entry.expiration <= now {
					expired(entry)
				} else {
					w.schedule(now, entry)
				}
				entry = next
			}
		}
	}
}

// nextWake returns the earliest lruClock time at which advance must be
// called for the entries in the wheel, or 0 when the wheel is empty.
func (w *lruWheel[K, V]) nextWake() int64 {
	var wakeAt int64
	for level := 0; level < lruWheelLevels; level++ {
		shift := lruWheelShifts[level]
		buckets := w.buckets(level)
		tick := w.nanos >> shift

		// See schedule for when a bucket's entries need advance. At level
		// 0, the current tick's bucket may hold entries. At other levels, a
		// bucket with the current tick's index holds entries for the tick
		// one whole turn of the level later.
		first, last := int64(1), int64(len(buckets))
		if level == 0 {
			first, last = 0, int64(len(buckets))-1
		}
		for i := first; i <= last; i++ {
			index := int(tick+i) & (len(buckets) - 1)
			if buckets[index] == nil {
				continue
			}
			bucketTick := tick + i
			if level == 0 {
				bucketTick++
			}
			bucketWakeAt := saturatingShift(bucketTick, shift)
			if wakeAt == 0 || bucketWakeAt < wakeAt {
				wakeAt = bucketWakeAt
			}
			break
		}
	}
	return wakeAt
}

// saturatingShift returns tick<<shift, or the maximum int64 on overflow.
func saturatingShift(tick int64, shift uint) int64 {
	if tick > (int64(1)<<(63-shift))-1 {
		return int64(^uint64(0) >> 1)
	}
	return tick << shift
}
