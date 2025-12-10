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

package tuple

import (
	"errors"
	"math"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/theta"
)

const (
	resizeThreshold  = 0.5
	rebuildThreshold = 15.0 / 16.0
)

const (
	strideHashBits = 7
	strideMask     = (1 << strideHashBits) - 1
)

var (
	ErrKeyNotFound                = errors.New("key not found")
	ErrKeyNotFoundAndNoEmptySlots = errors.New("key not found and no empty slots")
	// ErrZeroHashValue is used to indicate that the hash value is zero.
	// Zero is a reserved value for empty slots in the hash table.
	ErrZeroHashValue    = errors.New("zero hash value")
	ErrHashExceedsTheta = errors.New("hash exceeds theta")
)

type entry[S Summary] struct {
	Hash    uint64
	Summary S
}

func (e *entry[S]) reset() {
	if e.Hash != 0 {
		e.Summary.Reset()
	}
	e.Hash = 0
}

type hashtable[S Summary] struct {
	entries       []entry[S]
	entryLessFunc func(a, b entry[S]) int
	theta         uint64
	seed          uint64
	numEntries    uint32
	p             float32
	lgCurSize     uint8
	lgNomSize     uint8
	rf            theta.ResizeFactor
	isEmpty       bool
}

func newHashtable[S Summary](lgCurSize, lgNomSize uint8, rf theta.ResizeFactor, p float32, theta, seed uint64, isEmpty bool) *hashtable[S] {
	sketch := &hashtable[S]{
		isEmpty:    isEmpty,
		lgCurSize:  lgCurSize,
		lgNomSize:  lgNomSize,
		rf:         rf,
		p:          p,
		numEntries: 0,
		theta:      theta,
		seed:       seed,
		entries:    nil,
		entryLessFunc: func(a, b entry[S]) int {
			if a.Hash < b.Hash {
				return -1
			} else if a.Hash > b.Hash {
				return 1
			}
			return 0
		},
	}

	if lgCurSize > 0 {
		size := 1 << lgCurSize
		sketch.entries = make([]entry[S], size)
	}

	return sketch
}

// HashStringAndScreen computes the hash of string and checks if it passes theta threshold
func (t *hashtable[S]) HashStringAndScreen(data string) (uint64, error) {
	t.isEmpty = false
	h1, _ := internal.HashCharSliceMurmur3([]byte(data), 0, len(data), t.seed)
	hash := h1 >> 1
	if hash >= t.theta {
		return 0, ErrHashExceedsTheta
	}
	if hash == 0 {
		return 0, ErrZeroHashValue
	}
	return hash, nil
}

// HashInt32AndScreen computes the hash of int32 and checks if it passes theta threshold
func (t *hashtable[S]) HashInt32AndScreen(data int32) (uint64, error) {
	t.isEmpty = false
	h1, _ := internal.HashInt32SliceMurmur3([]int32{data}, 0, 1, t.seed)
	hash := h1 >> 1
	if hash >= t.theta {
		return 0, ErrHashExceedsTheta
	}
	if hash == 0 {
		return 0, ErrZeroHashValue
	}
	return hash, nil
}

// HashInt64AndScreen computes the hash of int64 and checks if it passes theta threshold
func (t *hashtable[S]) HashInt64AndScreen(data int64) (uint64, error) {
	t.isEmpty = false
	h1, _ := internal.HashInt64SliceMurmur3([]int64{data}, 0, 1, t.seed)
	hash := h1 >> 1
	if hash >= t.theta {
		return 0, ErrHashExceedsTheta
	}
	if hash == 0 {
		return 0, ErrZeroHashValue
	}
	return hash, nil
}

// HashBytesAndScreen computes the hash of bytes and checks if it passes theta threshold
func (t *hashtable[S]) HashBytesAndScreen(data []byte) (uint64, error) {
	t.isEmpty = false
	h1, _ := internal.HashByteArrMurmur3(data, 0, len(data), t.seed)
	hash := h1 >> 1
	if hash >= t.theta {
		return 0, ErrHashExceedsTheta
	}
	if hash == 0 {
		return 0, ErrZeroHashValue
	}
	return hash, nil
}

// Find searches for an entry in the hash table and returns the index if found,
// or an error if not found
func (t *hashtable[S]) Find(key uint64) (int, error) {
	return find(t.entries, t.lgCurSize, key)
}

func find[S Summary](entries []entry[S], lgSize uint8, key uint64) (int, error) {
	size := uint32(1 << lgSize)
	mask := size - 1
	stride := computeStride(key, lgSize)
	index := uint32(key) & mask

	loopIndex := index
	for {
		probe := entries[index]
		if probe.Hash == 0 {
			return int(index), ErrKeyNotFound
		} else if probe.Hash == key {
			return int(index), nil
		}

		index = (index + stride) & mask
		if index == loopIndex {
			return 0, ErrKeyNotFoundAndNoEmptySlots
		}
	}
}

// computeStride computes the stride for probing
func computeStride(key uint64, lgSize uint8) uint32 {
	// odd and independent of the index assuming lg_size lowest bits of the key were used for the index
	return (2 * uint32((key>>lgSize)&strideMask)) + 1
}

// Insert inserts an entry at the given index
func (t *hashtable[S]) Insert(index int, entry entry[S]) {
	t.entries[index] = entry
	t.numEntries++

	if t.numEntries > computeCapacity(t.lgCurSize, t.lgNomSize) {
		if t.lgCurSize <= t.lgNomSize {
			t.resize()
		} else {
			t.rebuild()
		}
	}
}

func computeCapacity(lgCurSize, lgNomSize uint8) uint32 {
	var fraction float64
	if lgCurSize <= lgNomSize {
		fraction = resizeThreshold
	} else {
		fraction = rebuildThreshold
	}
	return uint32(math.Floor(fraction * float64(uint32(1)<<lgCurSize)))
}

func (t *hashtable[S]) resize() {
	oldSize := 1 << t.lgCurSize
	lgNewSize := min(t.lgCurSize+uint8(t.rf), t.lgNomSize+1)
	newSize := 1 << lgNewSize
	newEntries := make([]entry[S], newSize)

	for i := 0; i < oldSize; i++ {
		e := t.entries[i]
		if e.Hash != 0 {
			// always finds an empty slot in a larger table
			index, _ := find(newEntries, lgNewSize, e.Hash)
			newEntries[index] = e
		}
	}

	t.entries = newEntries
	t.lgCurSize = lgNewSize
}

func (t *hashtable[S]) rebuild() {
	size := 1 << t.lgCurSize
	nominalSize := 1 << t.lgNomSize

	// empty entries have uninitialized payloads
	consolidateNonEmpty(t.entries, size, int(t.numEntries))

	internal.QuickSelectFunc[entry[S]](t.entries[:t.numEntries], 0, int(t.numEntries)-1, nominalSize, t.entryLessFunc)
	t.theta = t.entries[nominalSize].Hash

	oldEntries := t.entries
	t.entries = make([]entry[S], size)
	t.numEntries = uint32(nominalSize)

	// reinsert entries below new theta
	for i := 0; i < nominalSize; i++ {
		index, _ := find(t.entries, t.lgCurSize, oldEntries[i].Hash)
		t.entries[index] = oldEntries[i]
	}
}

// Trim reduces the sketch to nominal size if needed
func (t *hashtable[S]) Trim() {
	if t.numEntries > uint32(1<<t.lgNomSize) {
		t.rebuild()
	}
}

// Reset clears the sketch
func (t *hashtable[S]) Reset() {
	startingLgSize := startingSubMultiple(t.lgNomSize+1, theta.MinLgK, uint8(t.rf))

	if startingLgSize != t.lgCurSize {
		t.lgCurSize = startingLgSize
		newSize := 1 << startingLgSize
		t.entries = make([]entry[S], newSize)
	} else {
		// just clear existing entries
		for i := range t.entries {
			t.entries[i].reset()
		}
	}

	t.numEntries = 0
	t.theta = startingThetaFromP(t.p)
	t.isEmpty = true
}

func consolidateNonEmpty[S Summary](entries []entry[S], size, num int) {
	// find the first empty slot
	i := 0
	for i < size && entries[i].Hash != 0 {
		i++
	}

	// scan the rest and move non-empty entries to the front
	for j := i + 1; j < size; j++ {
		if entries[j].Hash != 0 {
			entries[i] = entries[j]
			entries[j] = entry[S]{}
			i++
			if i == num {
				break
			}
		}
	}
}

// startingThetaFromP returns the starting theta value from probability p
// Consistent way of initializing theta from p
// Avoids multiplication if p == 1 since it might not yield MAX_THETA exactly
func startingThetaFromP(p float32) uint64 {
	if p < 1 {
		return uint64(float64(theta.MaxTheta) * float64(p))
	}
	return theta.MaxTheta
}

// startingSubMultiple calculates the starting sub-multiple
func startingSubMultiple(lgTgt, lgMin, lgRf uint8) uint8 {
	if lgTgt <= lgMin {
		return lgMin
	}
	if lgRf == 0 {
		return lgTgt
	}
	return ((lgTgt - lgMin) % lgRf) + lgMin
}
