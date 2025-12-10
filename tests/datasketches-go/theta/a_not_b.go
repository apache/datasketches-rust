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
	"fmt"
	"slices"

	"github.com/apache/datasketches-go/internal"
)

// ANotB computes the set difference of two Theta sketches.
func ANotB(a, b Sketch, seed uint64, ordered bool) (*CompactSketch, error) {
	seedHash, err := internal.ComputeSeedHash(int64(seed))
	if err != nil {
		return nil, err
	}

	if a.IsEmpty() {
		return NewCompactSketch(a, ordered), nil
	}
	if a.NumRetained() > 0 && b.IsEmpty() {
		return NewCompactSketch(a, ordered), nil
	}

	aSeedHash, err := a.SeedHash()
	if err != nil {
		return nil, err
	}
	bSeedHash, err := b.SeedHash()
	if err != nil {
		return nil, err
	}
	if aSeedHash != uint16(seedHash) {
		return nil, fmt.Errorf("sketch A seed hash mismatch: expected %d, got %d", seedHash, aSeedHash)
	}
	if bSeedHash != uint16(seedHash) {
		return nil, fmt.Errorf("sketch B seed hash mismatch: expected %d, got %d", seedHash, bSeedHash)
	}

	theta := min(a.Theta64(), b.Theta64())
	var entries []uint64

	if b.NumRetained() == 0 {
		for entry := range a.All() {
			if entry < theta {
				entries = append(entries, entry)
			}
		}
	} else if a.IsOrdered() && b.IsOrdered() {
		entries = computeSortBased(a, b, theta)
	} else {
		var err error
		entries, err = computeHashBased(a, b, theta)
		if err != nil {
			return nil, err
		}
	}

	isEmpty := a.IsEmpty()
	if len(entries) == 0 && theta == MaxTheta {
		isEmpty = true
	}

	if ordered && !a.IsOrdered() {
		slices.Sort(entries)
	}

	return newCompactSketchFromEntries(
		isEmpty,
		a.IsOrdered() || ordered,
		uint16(seedHash),
		theta,
		entries,
	), nil
}

func computeSortBased(a, b Sketch, theta uint64) []uint64 {
	bEntries := make(map[uint64]struct{})
	for entry := range b.All() {
		bEntries[entry] = struct{}{}
	}

	var entries []uint64
	for entry := range a.All() {
		if _, ok := bEntries[entry]; ok {
			continue
		}

		if entry < theta {
			entries = append(entries, entry)
		}
	}
	return entries
}

func computeHashBased(a, b Sketch, theta uint64) ([]uint64, error) {
	lgSize := internal.LgSizeFromCount(b.NumRetained(), rebuildThreshold)

	table := NewHashtable(lgSize, lgSize, ResizeX1, 1, 0, 0, false)

	for entry := range b.All() {
		if entry < theta {
			idx, err := table.Find(entry)
			if err != nil && err == ErrKeyNotFoundAndNoEmptySlots {
				return nil, err
			}

			table.Insert(idx, entry)
		} else if b.IsOrdered() {
			break // Early stop
		}
	}

	// Scan A and look up B
	var entries []uint64
	for entry := range a.All() {
		if entry < theta {
			_, err := table.Find(entry)
			if err != nil && err == ErrKeyNotFound {
				entries = append(entries, entry)
			}
		} else if a.IsOrdered() {
			break // Early stop
		}
	}

	return entries, nil
}
