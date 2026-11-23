/*
 * Copyright (c) 2020, Psiphon Inc.
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

package obfuscator

import (
	"encoding/hex"
	"time"

	"github.com/Psiphon-Labs/psiphon-tunnel-core/psiphon/common"
	lrucache "github.com/Psiphon-Labs/psiphon-tunnel-core/psiphon/common/cache"
)

const (
	HISTORY_SEED_TTL              = 24 * time.Hour
	HISTORY_SEED_MAX_ENTRIES      = 1000000
	HISTORY_CLIENT_IP_TTL         = 2 * time.Minute
	HISTORY_CLIENT_IP_MAX_ENTRIES = 10000
)

// TODO: rename clientIP to peerIP to reflect newer terminology in
// psiphon/server where the immediate network peer may be an in-proxy proxy,
// not the client.

// SeedHistory maintains a history of recently observed obfuscation seed values.
// This history is used to identify duplicate seed messages.
//
// As a heurististic to exclude expected duplicates, due to, for example, meek
// retries, the source client IP is retained for comparison for a short
// duration -- long enough to cover meek retries without retaining client
// IPs in memory long past a client connection lifetime.
type SeedHistory struct {
	seedToTime     *lrucache.LRUCache[string, time.Time]
	seedToClientIP *lrucache.LRUCache[string, string]
}

type SeedHistoryConfig struct {
	SeedTTL            time.Duration
	SeedMaxEntries     int
	ClientIPTTL        time.Duration
	ClientIPMaxEntries int
}

// NewSeedHistory creates a new SeedHistory. Config is optional.
func NewSeedHistory(config *SeedHistoryConfig) *SeedHistory {

	// Default TTL and MAX_ENTRIES are tuned to provide an effective history size
	// while bounding the amount of memory that will be used. While a
	// probabilistic data structure such as a Bloom filter would provide a
	// smaller memory footprint, we wish to avoid the associated risk of false
	// positives.
	//
	// Client IPs use a separate, smaller cache with a shorter TTL so they can
	// be discarded while retaining the seeds for replay detection.

	useConfig := SeedHistoryConfig{
		SeedTTL:            HISTORY_SEED_TTL,
		SeedMaxEntries:     HISTORY_SEED_MAX_ENTRIES,
		ClientIPTTL:        HISTORY_CLIENT_IP_TTL,
		ClientIPMaxEntries: HISTORY_CLIENT_IP_MAX_ENTRIES,
	}

	if config != nil {
		if config.SeedTTL != 0 {
			useConfig.SeedTTL = config.SeedTTL
		}
		if config.SeedMaxEntries != 0 {
			useConfig.SeedMaxEntries = config.SeedMaxEntries
		}
		if config.ClientIPTTL != 0 {
			useConfig.ClientIPTTL = config.ClientIPTTL
		}
		if config.ClientIPMaxEntries != 0 {
			useConfig.ClientIPMaxEntries = config.ClientIPMaxEntries
		}
	}

	// Seeds are written once and read only when a duplicate arrives, so
	// when a cache is full, the oldest seeds are evicted first.

	return &SeedHistory{
		seedToTime: lrucache.NewLRUCache(lrucache.LRUCacheConfig[string, time.Time]{
			DefaultTTL: useConfig.SeedTTL,
			MaxEntries: useConfig.SeedMaxEntries,
		}),

		seedToClientIP: lrucache.NewLRUCache(lrucache.LRUCacheConfig[string, string]{
			DefaultTTL: useConfig.ClientIPTTL,
			MaxEntries: useConfig.ClientIPMaxEntries,
		}),
	}
}

// AddNew calls AddNewWithTTL using the SeedTTL that was specified in the
// SeedHistoryConfig.
func (h *SeedHistory) AddNew(
	strictMode bool,
	clientIP string,
	seedType string,
	seed []byte) (bool, *common.LogFields) {

	return h.AddNewWithTTL(
		strictMode, clientIP, seedType, seed, lrucache.DefaultExpiration)
}

// AddNewWithTTL adds a new seed value to the history, set to expire with the
// specified TTL. If the seed value is already in the history, and an expected
// case such as a meek retry is ruled out (or strictMode is on), AddNew
// returns false.
//
// When a duplicate seed is found, a common.LogFields instance is returned,
// populated with event data. Log fields may be returned in either the false
// or true case.
func (h *SeedHistory) AddNewWithTTL(
	strictMode bool,
	clientIP string,
	seedType string,
	seed []byte,
	TTL time.Duration) (bool, *common.LogFields) {

	key := string(seed)

	now := time.Now()

	previousTime, added := h.seedToTime.Add(key, now, TTL)
	if added {
		// Seed was not already in cache
		// TODO: if TTL < SeedHistory.ClientIPTTL, use the shorter TTL here
		h.seedToClientIP.Set(key, clientIP, lrucache.DefaultExpiration)
		return true, nil
	}

	logFields := common.LogFields{
		"duplicate_seed":            hex.EncodeToString(seed),
		"duplicate_seed_type":       seedType,
		"duplicate_elapsed_time_ms": int64(time.Since(previousTime) / time.Millisecond),
	}

	previousClientIP, ok := h.seedToClientIP.Get(key)
	if ok {
		if clientIP == previousClientIP {
			logFields["duplicate_client_ip"] = "equal"
			return !strictMode, &logFields
		} else {
			logFields["duplicate_client_ip"] = "unequal"
			return false, &logFields
		}
	}

	logFields["duplicate_client_ip"] = "unknown"
	return false, &logFields
}
