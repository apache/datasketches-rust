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

package cpc

import (
	"fmt"
	"github.com/apache/datasketches-go/internal"
	"math/bits"
)

type CpcUnion struct {
	seed uint64
	lgK  int

	// Note: at most one of bitMatrix and accumulator will be non-null at any given moment.
	// accumulator is a sketch object that is employed until it graduates out of Sparse mode.
	// At that point, it is converted into a full-sized bitMatrix, which is mathematically a sketch,
	// but doesn't maintain any of the "extra" fields of our sketch objects, so some additional work
	// is required when getResult is called at the end.
	bitMatrix   []uint64
	accumulator *CpcSketch
}

func NewCpcUnionSketch(lgK int, seed uint64) (CpcUnion, error) {
	acc, err := NewCpcSketch(lgK, seed)
	if err != nil {
		return CpcUnion{}, err
	}
	return CpcUnion{
		seed: seed,
		lgK:  lgK,
		// We begin with the accumulator holding an EMPTY_MERGED sketch object.
		// As an optimization the accumulator could start as NULL, but that would require changes elsewhere.
		accumulator: acc,
	}, nil
}

func NewCpcUnionSketchWithDefault(lgK int) (CpcUnion, error) {
	return NewCpcUnionSketch(lgK, internal.DEFAULT_UPDATE_SEED)
}

func (u *CpcUnion) GetFamilyId() int {
	return internal.FamilyEnum.CPC.Id
}

func (u *CpcUnion) Update(source *CpcSketch) error {
	if err := checkSeeds(u.seed, source.seed); err != nil {
		return err
	}

	sourceFlavorOrd := source.getFlavor()
	if sourceFlavorOrd == CpcFlavorEmpty {
		return nil
	}

	if err := u.checkUnionState(); err != nil {
		return err
	}

	// Downsample union if the source sketch has a smaller lgK.
	if source.lgK < u.lgK {
		if err := u.reduceUnionK(source.lgK); err != nil {
			return err
		}
	}

	// If the source is past SPARSE mode, ensure union is in bitMatrix mode.
	if sourceFlavorOrd > CpcFlavorSparse && u.accumulator != nil {
		bitMatrix, err := u.accumulator.bitMatrixOfSketch()
		if err != nil {
			return err
		}
		u.bitMatrix = bitMatrix
		u.accumulator = nil
	}
	state := (sourceFlavorOrd - 1) << 1
	if u.bitMatrix != nil {
		state |= 1
	}

	switch state {
	case 0: // Case A: source is SPARSE, union.accumulator valid, bitMatrix == nil.
		if u.accumulator.lgK == 0 {
			return fmt.Errorf("union accumulator cannot be nil")
		}
		// If the union is EMPTY and lgK matches, copy the source.
		if u.accumulator.getFlavor() == CpcFlavorEmpty && u.lgK == source.lgK {
			cp, err := source.Copy()
			if err != nil {
				return err
			}
			u.accumulator = cp
			break
		}
		if err := walkTableUpdatingSketch(u.accumulator, source.pairTable); err != nil {
			return err
		}
		// If accumulator has graduated beyond SPARSE, switch to bitMatrix.
		if u.accumulator.getFlavor() > CpcFlavorSparse {
			bitMatrix, err := u.accumulator.bitMatrixOfSketch()
			if err != nil {
				return err
			}
			u.bitMatrix = bitMatrix
			u.accumulator = nil
		}
	case 1: // Case B: source is SPARSE, union already in bitMatrix mode.
		u.orTableIntoMatrix(source.pairTable)
	case 3, 5: // Case C: source is HYBRID or PINNED, union in bitMatrix mode.
		if err := u.orWindowIntoMatrix(source.slidingWindow, 0, source.lgK); err != nil {
			return err
		}
		u.orTableIntoMatrix(source.pairTable)
	case 7: // Case D: source is SLIDING, union in bitMatrix mode.
		sourceMatrix, err := source.bitMatrixOfSketch()
		if err != nil {
			return err
		}
		if err := u.orMatrixIntoMatrix(sourceMatrix, source.lgK); err != nil {
			return err
		}
	default:
		return fmt.Errorf("illegal Union state: %d", state)
	}
	return nil
}

func (u *CpcUnion) GetResult() (*CpcSketch, error) {
	if err := u.checkUnionState(); err != nil {
		return nil, err
	}

	if u.accumulator != nil {
		if u.accumulator.numCoupons == 0 {
			result, err := NewCpcSketch(u.lgK, u.accumulator.seed)
			if err != nil {
				return nil, err
			}
			result.mergeFlag = true
			return result, nil
		}
		if u.accumulator.getFlavor() != CpcFlavorSparse {
			return nil, fmt.Errorf("accumulator must be SPARSE")
		}
		// Return a copy of the accumulator.
		result, err := u.accumulator.Copy()
		if err != nil {
			return nil, err
		}
		result.mergeFlag = true
		return result, nil
	}

	// Case: union contains a bitMatrix.
	matrix := u.bitMatrix
	lgK := u.lgK
	result, err := NewCpcSketch(u.lgK, u.seed)
	if err != nil {
		return nil, err
	}

	numCoupons := countBitsSetInMatrix(matrix)
	result.numCoupons = numCoupons

	flavor := determineFlavor(lgK, numCoupons)
	if flavor <= CpcFlavorSparse {
		return nil, fmt.Errorf("flavor must be greater than SPARSE")
	}

	offset := determineCorrectOffset(lgK, numCoupons)
	result.windowOffset = offset

	k := 1 << lgK
	window := make([]byte, k)
	result.slidingWindow = window

	newTableLgSize := max(lgK-4, 2)
	table, err := NewPairTable(newTableLgSize, 6+lgK)
	if err != nil {
		return nil, err
	}
	result.pairTable = table

	maskForClearingWindow := (0xFF << offset) ^ -1
	maskForFlippingEarlyZone := (1 << offset) - 1
	allSurprisesORed := uint64(0)

	for i := 0; i < k; i++ {
		pattern := matrix[i]
		window[i] = byte((pattern >> offset) & 0xFF)
		pattern &= uint64(maskForClearingWindow)
		pattern ^= uint64(maskForFlippingEarlyZone)
		allSurprisesORed |= pattern
		for pattern != 0 {
			col := bits.TrailingZeros64(pattern)
			pattern ^= 1 << col
			rowCol := (i << 6) | col
			isNovel, err := table.maybeInsert(rowCol)
			if err != nil {
				return nil, err
			}
			if !isNovel {
				return nil, fmt.Errorf("isNovel must be true")
			}
		}
	}

	result.fiCol = bits.TrailingZeros64(allSurprisesORed)
	if result.fiCol > offset {
		result.fiCol = offset
	}

	result.mergeFlag = true
	return result, nil
}

func (u *CpcUnion) checkUnionState() error {
	if u == nil {
		return fmt.Errorf("union cannot be nil")
	}
	accumulator := u.accumulator
	if (accumulator != nil) == (u.bitMatrix != nil) {
		return fmt.Errorf("accumulator and bitMatrix cannot be both valid or both nil")
	}
	if accumulator != nil {
		if accumulator.numCoupons > 0 {
			if accumulator.slidingWindow != nil || accumulator.pairTable == nil {
				return fmt.Errorf("non-empty union accumulator must be SPARSE")
			}
		}
		if u.lgK != accumulator.lgK {
			return fmt.Errorf("union LgK must equal accumulator LgK")
		}
	}
	return nil
}

func (u *CpcUnion) reduceUnionK(newLgK int) error {
	if newLgK < u.lgK {
		if u.bitMatrix != nil {
			newK := 1 << newLgK
			newMatrix := make([]uint64, newK)
			orMatrixIntoMatrix(newMatrix, newLgK, u.bitMatrix, u.lgK)
			u.bitMatrix = newMatrix
			u.lgK = newLgK
		} else {
			oldSketch := u.accumulator
			if oldSketch.numCoupons == 0 {
				acc, err := NewCpcSketch(newLgK, oldSketch.seed)
				if err != nil {
					return err
				}
				u.accumulator = acc
				u.lgK = newLgK
				return nil
			}
			newSketch, err := NewCpcSketch(newLgK, oldSketch.seed)
			if err != nil {
				return err
			}
			if err := walkTableUpdatingSketch(newSketch, oldSketch.pairTable); err != nil {
				return err
			}
			finalNewFlavor := newSketch.getFlavor()
			if finalNewFlavor == CpcFlavorSparse {
				u.accumulator = newSketch
				u.lgK = newLgK
				return nil
			}
			// The new sketch has graduated beyond sparse, so convert to bitMatrix.
			bitMatrix, err := newSketch.bitMatrixOfSketch()
			if err != nil {
				return err
			}
			u.bitMatrix = bitMatrix
			u.lgK = newLgK
			// Ensure that the accumulator is cleared.
			u.accumulator = nil
		}
	}
	return nil
}

func (u *CpcUnion) orWindowIntoMatrix(srcWindow []byte, srcOffset int, srcLgK int) error {
	//assert(destLgK <= srcLgK)
	if u.lgK > srcLgK {
		return fmt.Errorf("destLgK <= srcLgK")
	}
	destMask := (1 << u.lgK) - 1 // downsamples when destLgK < srcLgK
	srcK := 1 << srcLgK
	for srcRow := 0; srcRow < srcK; srcRow++ {
		u.bitMatrix[srcRow&destMask] |= uint64(srcWindow[srcRow]) << srcOffset
	}
	return nil
}

func (u *CpcUnion) orTableIntoMatrix(srcTable *pairTable) {
	slots := srcTable.slotsArr
	numSlots := 1 << srcTable.lgSizeInts
	destMask := (1 << u.lgK) - 1 // downsamples when destLgK < srcLgK
	for i := 0; i < numSlots; i++ {
		rowCol := slots[i]
		if rowCol != -1 {
			col := rowCol & 63
			row := rowCol >> 6
			u.bitMatrix[row&destMask] |= 1 << col // Set the bit.
		}

	}
}

func (u *CpcUnion) orMatrixIntoMatrix(srcMatrix []uint64, srcLgK int) error {
	if u.lgK > srcLgK {
		return fmt.Errorf("destLgK <= srcLgK")
	}
	destMask := (1 << u.lgK) - 1 // downsamples when destLgK < srcLgK
	srcK := 1 << srcLgK
	for srcRow := 0; srcRow < srcK; srcRow++ {
		u.bitMatrix[srcRow&destMask] |= srcMatrix[srcRow]
	}
	return nil

}

func (u *CpcUnion) getNumCoupons() uint64 {
	if u.bitMatrix != nil {
		return countBitsSetInMatrix(u.bitMatrix)
	}
	return u.accumulator.numCoupons
}

func (u *CpcUnion) GetBitMatrix() ([]uint64, error) {
	if err := u.checkUnionState(); err != nil {
		return nil, err
	}

	if u.bitMatrix != nil {
		return u.bitMatrix, nil
	}

	if u.accumulator == nil {
		return nil, fmt.Errorf("both bitMatrix and accumulator are nil, invalid union state")
	}
	bm, err := u.accumulator.bitMatrixOfSketch()
	if err != nil {
		return nil, fmt.Errorf("accumulator.bitMatrixOfSketch failed: %v", err)
	}
	return bm, nil
}
