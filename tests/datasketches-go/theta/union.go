/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package theta

import (
	"errors"
	"fmt"
	"slices"

	"github.com/apache/datasketches-go/internal"
)

// Union computes the union of Theta sketches.
type Union struct {
	policy    Policy
	hashtable *Hashtable
	theta     uint64
}

type unionOptions struct {
	theta     uint64
	seed      uint64
	p         float32
	lgCurSize uint8
	lgK       uint8
	rf        ResizeFactor
}

type UnionOptionFunc func(*unionOptions)

// WithUnionLgK sets log2(k), where k is a nominal number of entries in the union
func WithUnionLgK(lgK uint8) UnionOptionFunc {
	return func(opts *unionOptions) {
		opts.lgK = lgK
	}
}

// WithUnionResizeFactor sets a resize factor for the internal hash table (defaults to 8)
func WithUnionResizeFactor(rf ResizeFactor) UnionOptionFunc {
	return func(opts *unionOptions) {
		opts.rf = rf
	}
}

// WithUnionSketchP sets sampling probability (initial theta). The default is 1, so the union retains
// all entries until it reaches the limit, at which point it goes into the estimation mode
// and reduces the effective sampling probability (theta) as necessary
func WithUnionSketchP(p float32) UnionOptionFunc {
	return func(opts *unionOptions) {
		opts.p = p
	}
}

// WithUnionSeed sets the seed for the hash function. Should be used carefully if needed.
// Union produced with different seeds are not compatible
// and cannot be mixed in set operations.
func WithUnionSeed(seed uint64) UnionOptionFunc {
	return func(opts *unionOptions) {
		opts.seed = seed
	}
}

// NewUnion creates a new union with the given options
func NewUnion(opts ...UnionOptionFunc) (*Union, error) {
	options := &unionOptions{
		lgK:  DefaultLgK,
		rf:   DefaultResizeFactor,
		p:    1.0,
		seed: DefaultSeed,
	}
	for _, opt := range opts {
		opt(options)
	}

	if options.lgK < MinLgK {
		return nil, fmt.Errorf("lg_k must not be less than %d: %d", MinLgK, options.lgK)
	}
	if options.lgK > MaxLgK {
		return nil, fmt.Errorf("lg_k must not be greater than %d: %d", MaxLgK, options.lgK)
	}
	if options.p <= 0 || options.p > 1 {
		return nil, errors.New("sampling probability must be between 0 and 1")
	}

	options.lgCurSize = startingSubMultiple(options.lgK+1, MinLgK, uint8(options.rf))
	options.theta = startingThetaFromP(options.p)

	table := NewHashtable(
		options.lgCurSize, options.lgK, options.rf, options.p, options.theta, options.seed, true,
	)

	return &Union{
		hashtable: table,
		policy:    &noopPolicy{},
		theta:     table.theta,
	}, nil
}

// Update adds a sketch to the union
func (u *Union) Update(sketch Sketch) error {
	if sketch.IsEmpty() {
		return nil
	}

	seedHash, err := internal.ComputeSeedHash(int64(u.hashtable.seed))
	if err != nil {
		return err
	}
	sketchSeedHash, err := sketch.SeedHash()
	if err != nil {
		return err
	}
	if uint16(seedHash) != sketchSeedHash {
		return errors.New("seed hash mismatch")
	}

	u.hashtable.isEmpty = false
	u.theta = min(u.theta, sketch.Theta64())

	for entry := range sketch.All() {
		if entry < u.theta && entry < u.hashtable.theta {
			index, err := u.hashtable.Find(entry)
			if err != nil {
				if err == ErrKeyNotFound {
					u.hashtable.Insert(index, entry)
					continue
				}
				return err
			}

			u.policy.Apply(&u.hashtable.entries[index], entry)
		} else {
			// For ordered sketches, we can break early
			if sketch.IsOrdered() {
				break
			}
		}
	}

	u.theta = min(u.theta, u.hashtable.theta)
	return nil
}

// Result produces a copy of the current state of the Union as a compact sketch
func (u *Union) Result(ordered bool) (*CompactSketch, error) {
	if u.hashtable.isEmpty {
		seedHash, err := internal.ComputeSeedHash(int64(u.hashtable.seed))
		if err != nil {
			return nil, err
		}
		return newCompactSketchFromEntries(true, true, uint16(seedHash), u.theta, nil), nil
	}

	var entries []uint64

	theta := min(u.theta, u.hashtable.theta)
	nominalNum := uint32(1 << u.hashtable.lgNomSize)

	if u.theta >= u.hashtable.theta {
		for _, entry := range u.hashtable.entries {
			if entry != 0 {
				entries = append(entries, entry)
			}
		}
	} else {
		for _, entry := range u.hashtable.entries {
			if entry != 0 && entry < theta {
				entries = append(entries, entry)
			}
		}
	}

	if uint32(len(entries)) > nominalNum {
		internal.QuickSelect(entries, 0, len(entries)-1, int(nominalNum))
		theta = entries[nominalNum]
		entries = entries[:nominalNum]
	}

	if ordered {
		slices.Sort(entries)
	}

	seedHash, err := internal.ComputeSeedHash(int64(u.hashtable.seed))
	if err != nil {
		return nil, err
	}

	return newCompactSketchFromEntries(u.hashtable.isEmpty, ordered, uint16(seedHash), theta, entries), nil
}

// OrderedResult produces a copy of the current state of the Union
// as an ordered compact sketch
func (u *Union) OrderedResult() (*CompactSketch, error) {
	return u.Result(true)
}

// Reset resets the union to the initial empty state
func (u *Union) Reset() {
	u.hashtable.Reset()
	u.theta = u.hashtable.theta
}

// Policy returns the policy used by this union
func (u *Union) Policy() Policy {
	return u.policy
}
