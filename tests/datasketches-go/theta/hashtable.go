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
	"math"

	"github.com/apache/datasketches-go/internal"
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

type Hashtable struct {
	entries    []uint64
	theta      uint64
	seed       uint64
	numEntries uint32
	p          float32
	lgCurSize  uint8
	lgNomSize  uint8
	rf         ResizeFactor
	isEmpty    bool
}

// NewHashtable creates a new hash table
func NewHashtable(lgCurSize, lgNomSize uint8, rf ResizeFactor, p float32, theta, seed uint64, isEmpty bool) *Hashtable {
	sketch := &Hashtable{
		isEmpty:    isEmpty,
		lgCurSize:  lgCurSize,
		lgNomSize:  lgNomSize,
		rf:         rf,
		p:          p,
		numEntries: 0,
		theta:      theta,
		seed:       seed,
		entries:    nil,
	}

	if lgCurSize > 0 {
		size := 1 << lgCurSize
		sketch.entries = make([]uint64, size)
	}

	return sketch
}

// Copy creates a deep copy of the sketch
func (t *Hashtable) Copy() *Hashtable {
	c := &Hashtable{
		isEmpty:    t.isEmpty,
		lgCurSize:  t.lgCurSize,
		lgNomSize:  t.lgNomSize,
		rf:         t.rf,
		p:          t.p,
		numEntries: t.numEntries,
		theta:      t.theta,
		seed:       t.seed,
		entries:    nil,
	}

	if t.entries != nil {
		size := 1 << t.lgCurSize
		c.entries = make([]uint64, size)

		copy(c.entries, t.entries)
	}

	return c
}

// HashStringAndScreen computes the hash of string and checks if it passes theta threshold
func (t *Hashtable) HashStringAndScreen(data string) (uint64, error) {
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
func (t *Hashtable) HashInt32AndScreen(data int32) (uint64, error) {
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
func (t *Hashtable) HashInt64AndScreen(data int64) (uint64, error) {
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
func (t *Hashtable) HashBytesAndScreen(data []byte) (uint64, error) {
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

// Find searches for a key in the hash table and returns the index if found,
// or an error if not found
func (t *Hashtable) Find(key uint64) (int, error) {
	return find(t.entries, t.lgCurSize, key)
}

func find(entries []uint64, lgSize uint8, key uint64) (int, error) {
	size := uint32(1 << lgSize)
	mask := size - 1
	stride := computeStride(key, lgSize)
	index := uint32(key) & mask

	loopIndex := index
	for {
		probe := entries[index]
		if probe == 0 {
			return int(index), ErrKeyNotFound
		} else if probe == key {
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
func (t *Hashtable) Insert(index int, entry uint64) {
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

func (t *Hashtable) resize() {
	oldSize := 1 << t.lgCurSize
	lgNewSize := min(t.lgCurSize+uint8(t.rf), t.lgNomSize+1)
	newSize := 1 << lgNewSize
	newEntries := make([]uint64, newSize)

	for i := 0; i < oldSize; i++ {
		key := t.entries[i]
		if key != 0 {
			// always finds an empty slot in a larger table
			index, _ := find(newEntries, lgNewSize, key)
			newEntries[index] = key
		}
	}

	t.entries = newEntries
	t.lgCurSize = lgNewSize
}

func (t *Hashtable) rebuild() {
	size := 1 << t.lgCurSize
	nominalSize := 1 << t.lgNomSize

	// empty entries have uninitialized payloads
	consolidateNonEmpty(t.entries, size, int(t.numEntries))

	internal.QuickSelect(t.entries[:t.numEntries], 0, int(t.numEntries)-1, nominalSize)
	t.theta = t.entries[nominalSize]

	oldEntries := t.entries
	t.entries = make([]uint64, size)
	t.numEntries = uint32(nominalSize)

	// reinsert entries below new theta
	for i := 0; i < nominalSize; i++ {
		index, _ := find(t.entries, t.lgCurSize, oldEntries[i])
		t.entries[index] = oldEntries[i]
	}
}

// Trim reduces the sketch to nominal size if needed
func (t *Hashtable) Trim() {
	if t.numEntries > uint32(1<<t.lgNomSize) {
		t.rebuild()
	}
}

// Reset clears the sketch
func (t *Hashtable) Reset() {
	startingLgSize := startingSubMultiple(t.lgNomSize+1, MinLgK, uint8(t.rf))

	if startingLgSize != t.lgCurSize {
		t.lgCurSize = startingLgSize
		newSize := 1 << startingLgSize
		t.entries = make([]uint64, newSize)
	} else {
		// just clear existing entries
		for i := range t.entries {
			t.entries[i] = 0
		}
	}

	t.numEntries = 0
	t.theta = startingThetaFromP(t.p)
	t.isEmpty = true
}

func consolidateNonEmpty(entries []uint64, size, num int) {
	// find the first empty slot
	i := 0
	for i < size && entries[i] != 0 {
		i++
	}

	// scan the rest and move non-empty entries to the front
	for j := i + 1; j < size; j++ {
		if entries[j] != 0 {
			entries[i] = entries[j]
			entries[j] = 0
			i++
			if i == num {
				break
			}
		}
	}
}
