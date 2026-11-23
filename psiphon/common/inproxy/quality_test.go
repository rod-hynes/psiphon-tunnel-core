/*
 * Copyright (c) 2025, Psiphon Inc.
 * All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package inproxy

import (
	"fmt"
	"testing"
	"time"

	lrucache "github.com/Psiphon-Labs/psiphon-tunnel-core/psiphon/common/cache"
	"github.com/Psiphon-Labs/psiphon-tunnel-core/psiphon/common/errors"
	"github.com/Psiphon-Labs/psiphon-tunnel-core/psiphon/internal/testutils"
)

func TestProxyQualityState(t *testing.T) {
	err := runTestProxyQualityState()
	if err != nil {
		t.Error(errors.Trace(err).Error())
	}
}

func TestProxyQualityReporter(t *testing.T) {
	err := runTestProxyQualityReporter()
	if err != nil {
		t.Error(errors.Trace(err).Error())
	}
}

func TestProxyQualityDelayedFailure(t *testing.T) {
	for _, flush := range []bool{false, true} {
		t.Run(fmt.Sprintf("flush=%t", flush), func(t *testing.T) {
			q := NewProxyQuality()
			q.SetParameters(time.Hour, time.Millisecond, 1)
			ready := make(chan struct{})
			release := make(chan struct{})
			done := make(chan struct{})
			q.pendingFailedMatches = lrucache.NewLRUCache(
				lrucache.LRUCacheConfig[string, pendingFailedMatch]{
					MaxEntries: proxyQualityMaxPendingFailedMatches,
					OnEvicted: func(key string, pending pendingFailedMatch, reason lrucache.EvictionReason) {
						close(ready)
						<-release
						q.addFailedMatch(key, pending, reason)
						close(done)
					},
				})
			defer q.pendingFailedMatches.Flush()
			defer close(release)

			proxyID, err := MakeID()
			if err != nil {
				t.Fatal(err)
			}
			proxyASN := "65537"
			proxyKey := MakeProxyQualityKey(proxyID, proxyASN)
			counts := ProxyQualityASNCounts{"65538": 1}
			q.AddQuality(proxyKey, counts)
			q.Matched(proxyID, proxyASN)
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("expiration callback did not start")
			}

			if flush {
				q.Flush()
			}
			q.AddQuality(proxyKey, counts)
			q.SetParameters(time.Hour, time.Hour, 1)
			q.Matched(proxyID, proxyASN)
			// Release the callback without closing the channel until cleanup.
			release <- struct{}{}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("expiration callback did not finish")
			}
			if !q.HasQuality(proxyID, proxyASN, "") {
				t.Fatal("old failure removed newer quality")
			}
			if _, ok := q.pendingFailedMatches.Get(string(proxyKey[:])); !ok {
				t.Fatal("old failure removed newer pending match")
			}
		})
	}
}

func TestProxyQualityMatchPreservesDeadline(t *testing.T) {
	q := NewProxyQuality()
	proxyID, err := MakeID()
	if err != nil {
		t.Fatal(err)
	}
	proxyASN := "65537"
	proxyKey := MakeProxyQualityKey(proxyID, proxyASN)
	q.AddQuality(proxyKey, ProxyQualityASNCounts{"65538": 1})
	q.Matched(proxyID, proxyASN)
	_, original, ok := q.pendingFailedMatches.GetWithExpiration(string(proxyKey[:]))
	if !ok || original.IsZero() {
		t.Fatal("missing pending match deadline")
	}
	q.SetParameters(time.Hour, time.Hour, 1)
	q.Matched(proxyID, proxyASN)
	_, expiration, ok := q.pendingFailedMatches.GetWithExpiration(string(proxyKey[:]))
	if !ok || !expiration.Equal(original) {
		t.Fatal("matching again changed the pending deadline")
	}
	q.AddQuality(proxyKey, ProxyQualityASNCounts{"65538": 1})
	if _, ok := q.pendingFailedMatches.Get(string(proxyKey[:])); ok {
		t.Fatal("quality did not cancel the pending failure")
	}
}

func runTestProxyQualityState() error {

	qualityTTL := 100 * time.Millisecond
	pendingFailedMatchDeadline := 100 * time.Millisecond
	failedMatchThreshold := 3

	q := NewProxyQuality()

	failedMatch := make(chan struct{}, 1)
	q.pendingFailedMatches = lrucache.NewLRUCache(
		lrucache.LRUCacheConfig[string, pendingFailedMatch]{
			MaxEntries: proxyQualityMaxPendingFailedMatches,
			OnEvicted: func(key string, pending pendingFailedMatch, reason lrucache.EvictionReason) {
				q.addFailedMatch(key, pending, reason)
				failedMatch <- struct{}{}
			},
		})

	q.SetParameters(
		qualityTTL, pendingFailedMatchDeadline, failedMatchThreshold)

	testProxyASN := "65537"
	testClientASN1 := "65538"
	testClientASN2 := "65539"
	testClientASN3 := "65540"

	proxyID, err := MakeID()
	if err != nil {
		return errors.Trace(err)
	}

	proxyKey := MakeProxyQualityKey(proxyID, testProxyASN)

	q.AddQuality(proxyKey, ProxyQualityASNCounts{testClientASN1: 1, testClientASN2: 2})

	// Test: HasQuality for any client ASN

	if !q.HasQuality(proxyID, testProxyASN, "") {
		return errors.TraceNew("unexpected HasQuality")
	}

	// Test: HasQuality for specific client ASN

	if !q.HasQuality(proxyID, testProxyASN, testClientASN1) {
		return errors.TraceNew("unexpected HasQuality")
	}

	if q.HasQuality(proxyID, testProxyASN, testClientASN3) {
		return errors.TraceNew("unexpected HasQuality")
	}

	// Test: TTL expires

	time.Sleep(qualityTTL + 1*time.Millisecond)

	if q.HasQuality(proxyID, testProxyASN, "") {
		return errors.TraceNew("unexpected HasQuality")
	}

	// Test: flush

	qualityTTL = proxyQualityTTL

	q.SetParameters(
		qualityTTL, pendingFailedMatchDeadline, failedMatchThreshold)

	q.AddQuality(proxyKey, ProxyQualityASNCounts{testClientASN1: 1, testClientASN2: 2})

	q.Flush()

	if q.HasQuality(proxyID, testProxyASN, "") {
		return errors.TraceNew("unexpected HasQuality")
	}

	// Test: quality removed once failed match threshold reached

	q.AddQuality(proxyKey, ProxyQualityASNCounts{testClientASN1: 1, testClientASN2: 2})

	for i := 0; i < failedMatchThreshold; i++ {

		q.Matched(proxyID, testProxyASN)

		select {
		case <-failedMatch:
		case <-time.After(5 * time.Second):
			return errors.TraceNew("failed match deadline was not processed")
		}

		expectQuality := i < failedMatchThreshold-1

		if q.HasQuality(proxyID, testProxyASN, "") != expectQuality {
			return errors.TraceNew("unexpected HasQuality")
		}
	}

	return nil
}

func runTestProxyQualityReporter() error {

	// This unit test exercises the report queue state, but not the report
	// requests. ProxyQualityReporter.requestScheduler/sendToBrokers are
	// exercised in TestInproxy.

	r, err := NewProxyQualityReporter(
		testutils.NewTestLogger(),
		nil,
		SessionPrivateKey{},
		nil,
		nil,
		nil,
		nil)
	if err != nil {
		return errors.Trace(err)
	}

	maxEntries := 10
	expectedRequestCount := 2

	r.SetRequestParameters(maxEntries, 0, 0, 0)

	var proxyKeys []ProxyQualityKey
	testProxyASN := "65537"

	for i := 0; i < 20; i++ {
		proxyID, err := MakeID()
		if err != nil {
			return errors.Trace(err)
		}
		proxyKey := MakeProxyQualityKey(proxyID, testProxyASN)
		for j := 0; j < 10; j++ {
			testClientASN := fmt.Sprintf("%d", 65538+j)
			for k := 0; k <= i; k++ {
				r.ReportQuality(
					proxyID, testProxyASN, testClientASN)
			}
		}
		proxyKeys = append(proxyKeys, proxyKey)
	}

	if r.reportQueue.Len() != len(proxyKeys) {
		return errors.TraceNew("unexpected queue size")

	}

	for count := 0; count < expectedRequestCount; count++ {

		requestCounts := r.prepareNextRequest()

		if len(requestCounts) == 0 {
			return errors.TraceNew("unexpected requestCounts")
		}

		for i := count * 10; i < count*10+10; i++ {
			counts, ok := requestCounts[proxyKeys[i]]
			if !ok {
				return errors.TraceNew("missing proxyID")
			}
			for j := 0; j < 10; j++ {
				testClientASN := fmt.Sprintf("%d", 65538+j)
				count, ok := counts[testClientASN]
				if !ok {
					return errors.TraceNew("missing client ASN")
				}
				if count != i+1 {
					return errors.Tracef("unexpected count")
				}
			}
		}

	}

	if len(r.prepareNextRequest()) != 0 {
		return errors.TraceNew("unexpected prepareNextRequest")
	}

	return nil
}
