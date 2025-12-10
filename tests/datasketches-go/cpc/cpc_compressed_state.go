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

// Constants for ptrArr indices.
const (
	NextWordIdx = 0 // ptrArr[0]: nextWordIndex
	BitBuf      = 1 // ptrArr[1]: bitBuf
	BufBits     = 2 // ptrArr[2]: bufBits
)

type CpcCompressedState struct {
	CsvIsValid    bool
	WindowIsValid bool
	LgK           int
	SeedHash      int16
	FiCol         int
	MergeFlag     bool // compliment of HIP Flag
	NumCoupons    uint64

	Kxp         float64
	HipEstAccum float64

	NumCsv        uint64
	CsvStream     []int // may be longer than required
	CsvLengthInts int
	CwStream      []int // may be longer than required
	CwLengthInts  int
}

func NewCpcCompressedState(lgK int, seedHash int16) *CpcCompressedState {
	return &CpcCompressedState{
		LgK:      lgK,
		SeedHash: seedHash,
		Kxp:      float64(int(1) << lgK),
	}
}

func NewCpcCompressedStateFromSketch(sketch *CpcSketch) (*CpcCompressedState, error) {
	seedHash, err := internal.ComputeSeedHash(int64(sketch.seed))
	if err != nil {
		return nil, err
	}
	state := NewCpcCompressedState(sketch.lgK, seedHash)
	state.FiCol = sketch.fiCol
	state.MergeFlag = sketch.mergeFlag
	state.NumCoupons = sketch.numCoupons
	state.Kxp = sketch.kxp
	state.HipEstAccum = sketch.hipEstAccum
	state.CsvIsValid = sketch.pairTable != nil
	state.WindowIsValid = sketch.slidingWindow != nil

	err = state.compress(sketch)
	return state, err
}

func (c *CpcCompressedState) getRequiredSerializedBytes() int {
	preInts := getDefinedPreInts(c.getFormat())
	return 4 * (preInts + c.CsvLengthInts + c.CwLengthInts)
}

func (c *CpcCompressedState) getWindowOffset() int {
	return determineCorrectOffset(c.LgK, c.NumCoupons)
}

func (c *CpcCompressedState) getFormat() CpcFormat {
	ordinal := 0
	if c.CwLengthInts > 0 {
		ordinal |= 4
	}
	if c.NumCsv > 0 {
		ordinal |= 2
	}
	if !c.MergeFlag {
		ordinal |= 1
	}
	return CpcFormat(ordinal)
}

func (c *CpcCompressedState) compress(src *CpcSketch) error {
	srcFlavor := src.getFlavor()
	var err error
	switch srcFlavor {
	case CpcFlavorEmpty:
		return nil
	case CpcFlavorSparse:
		err = c.compressSparseFlavor(src)
		if c.CwStream != nil {
			return fmt.Errorf("compress: sparse flavor %v CwStream not null %v", srcFlavor, c.CwStream)
		}
		if c.CsvStream == nil {
			return fmt.Errorf("compress: sparse flavor %v CsvStream is null", srcFlavor)
		}
	case CpcFlavorHybrid:
		err = c.compressHybridFlavor(src)
		if c.CwStream != nil {
			return fmt.Errorf("compress: sparse flavor %v CwStream not null %v", srcFlavor, c.CwStream)
		}
		if c.CsvStream == nil {
			return fmt.Errorf("compress: sparse flavor %v CsvStream is null", srcFlavor)
		}
	case CpcFlavorPinned:
		err = c.compressPinnedFlavor(src)
		if c.CwStream == nil {
			return fmt.Errorf("compress: sparse flavor %v CwStream is null", srcFlavor)
		}
	case CpcFlavorSliding:
		err = c.compressSlidingFlavor(src)
		if c.CwStream == nil {
			return fmt.Errorf("compress: sparse flavor %v CwStream is null", srcFlavor)
		}
	default:
		return fmt.Errorf("unable to compress flavor %v", srcFlavor)
	}
	return err
}

func (c *CpcCompressedState) uncompress(src *CpcSketch) error {
	srcFlavor := src.getFlavor()
	var err error
	switch srcFlavor {
	case CpcFlavorEmpty:
		return nil
	case CpcFlavorSparse:
		if c.CwStream != nil {
			return fmt.Errorf("uncompress: sparse flavor %v CwStream not null %v", srcFlavor, c.CwStream)
		}
		err = c.uncompressSparseFlavor(src)
	case CpcFlavorHybrid:
		err = c.uncompressHybridFlavor(src)
	case CpcFlavorPinned:
		if c.CwStream == nil {
			return fmt.Errorf("uncompress: pinned flavor %v CwStream is null", srcFlavor)
		}
		err = c.uncompressPinnedFlavor(src)
	case CpcFlavorSliding:
		err = c.uncompressSlidingFlavor(src)
	default:
		return fmt.Errorf("unable to uncompress flavor %v", srcFlavor)
	}
	return err
}

// uncompressSketch creates a new CpcSketch from the compressed state and the given seed,
// after verifying that the computed seed hash matches the source’s seed hash.
func uncompressSketch(source *CpcCompressedState, seed uint64) (*CpcSketch, error) {
	// Compute the seed hash from the provided seed.
	computedSeedHash, err := internal.ComputeSeedHash(int64(seed))
	if err != nil {
		return nil, err
	}
	// Verify that the computed seed hash matches the one stored in the source.
	if computedSeedHash != source.SeedHash {
		return nil, fmt.Errorf("seed hash mismatch: computed %d != source %d", computedSeedHash, source.SeedHash)
	}

	// Create a new sketch using the source's lgK and the given seed.
	sketch, err := NewCpcSketch(source.LgK, seed)
	if err != nil {
		return nil, err
	}

	// Populate the new sketch with fields from the compressed state.
	sketch.numCoupons = source.NumCoupons
	// Assuming source.getWindowOffset() exists and returns the correct window offset.
	sketch.windowOffset = source.getWindowOffset()
	sketch.fiCol = source.FiCol
	sketch.mergeFlag = source.MergeFlag
	sketch.kxp = source.Kxp
	sketch.hipEstAccum = source.HipEstAccum
	// Reset fields that will be filled during uncompression.
	sketch.slidingWindow = nil
	sketch.pairTable = nil

	// Uncompress the detailed data into the sketch using the existing uncompress method.
	if err := source.uncompress(sketch); err != nil {
		return nil, err
	}

	return sketch, nil
}

func (c *CpcCompressedState) compressSparseFlavor(src *CpcSketch) error {
	// There is no window to compress
	if src.slidingWindow != nil {
		return fmt.Errorf("compressSparseFlavor: expected slidingWindow to be nil")
	}
	// Get the pair table and extract its pairs.
	srcPairTable := src.pairTable
	srcNumPairs := srcPairTable.numPairs
	srcPairArr, err := srcPairTable.unwrap(srcNumPairs)
	if err != nil {
		return err
	}
	introspectiveInsertionSort(srcPairArr, 0, srcNumPairs-1)
	return compressTheSurprisingValues(c, src, srcPairArr, srcNumPairs)
}

func (c *CpcCompressedState) uncompressSparseFlavor(src *CpcSketch) error {
	if c.CwStream != nil {
		return fmt.Errorf("uncompressSparseFlavor: expected cwStream to be nil, got %v", c.CwStream)
	}
	if c.CsvStream == nil {
		return fmt.Errorf("uncompressSparseFlavor: csvStream is nil")
	}

	srcPairArr, err := uncompressTheSurprisingValues(c)
	if err != nil {
		return err
	}
	numPairs := int(c.NumCsv)
	table, err := newInstanceFromPairsArray(srcPairArr, numPairs, c.LgK)
	if err != nil {
		return err
	}
	src.pairTable = table
	return nil
}

func (c *CpcCompressedState) compressHybridFlavor(src *CpcSketch) error {
	srcK := 1 << src.lgK
	srcPairTable := src.pairTable
	srcNumPairs := srcPairTable.numPairs
	// Get and sort the pair array.
	srcPairArr, err := srcPairTable.unwrap(srcNumPairs)
	if err != nil {
		return err
	}
	introspectiveInsertionSort(srcPairArr, 0, srcNumPairs-1)
	// Retrieve sliding window and related values.
	srcSlidingWindow := src.slidingWindow
	srcWindowOffset := src.windowOffset
	srcNumCoupons := src.numCoupons
	if srcSlidingWindow == nil {
		return fmt.Errorf("compressHybridFlavor: slidingWindow is nil")
	}
	if srcWindowOffset != 0 {
		return fmt.Errorf("compressHybridFlavor: windowOffset must be 0, got %d", srcWindowOffset)
	}
	// Determine the number of pairs present in the window.
	numPairs := srcNumCoupons - uint64(srcNumPairs)
	// Check that numPairs fits in an int.
	if numPairs >= uint64(int(^uint32(0)>>1)) {
		return fmt.Errorf("compressHybridFlavor: numPairs (%d) exceeds maximum int value", numPairs)
	}
	numPairsFromArray := int(numPairs)
	// Invariant check: total pairs from array must equal numCoupons.
	if numPairsFromArray+srcNumPairs != int(srcNumCoupons) {
		return fmt.Errorf("compressHybridFlavor: invariant violation (%d + %d != %d)",
			numPairsFromArray, srcNumPairs, srcNumCoupons)
	}
	allPairs := trickyGetPairsFromWindow(srcSlidingWindow, srcK, numPairsFromArray, srcNumPairs)
	mergePairs(srcPairArr, 0, srcNumPairs, allPairs, srcNumPairs, numPairsFromArray, allPairs, 0)
	return compressTheSurprisingValues(c, src, allPairs, int(srcNumCoupons))
}

func (c *CpcCompressedState) uncompressHybridFlavor(src *CpcSketch) error {
	// Ensure that the window compression stream is nil and the CSV stream is present.
	if c.CwStream != nil {
		return fmt.Errorf("uncompressHybridFlavor: expected CwStream to be nil, got %v", c.CwStream)
	}
	if c.CsvStream == nil {
		return fmt.Errorf("uncompressHybridFlavor: CsvStream is nil")
	}

	// Uncompress the surprising values (i.e. the pairs) from the CSV stream.
	pairs, err := uncompressTheSurprisingValues(c)
	if err != nil {
		return err
	}
	numPairs := int(c.NumCsv)

	// For the hybrid flavor, some pairs belong to the sliding window.
	srcLgK := c.LgK
	k := 1 << srcLgK

	// Allocate a window of k bytes (one byte per row).
	window := make([]byte, k)

	// Separate out the pairs that belong in the window.
	// Pairs with a column index (low 6 bits) less than 8 are moved into the window.
	nextTruePair := 0
	for i := 0; i < numPairs; i++ {
		rowCol := pairs[i]
		if rowCol == -1 {
			return fmt.Errorf("uncompressHybridFlavor: invalid pair value -1 at index %d", i)
		}
		col := rowCol & 63
		if col < 8 {
			row := rowCol >> 6
			window[row] |= 1 << col // set the corresponding bit in the window
		} else {
			// Move the "true" pair down into the pairs array.
			pairs[nextTruePair] = rowCol
			nextTruePair++
		}
	}

	// The compressed state's window offset should be 0.
	if c.getWindowOffset() != 0 {
		return fmt.Errorf("uncompressHybridFlavor: expected windowOffset to be 0, got %d", c.getWindowOffset())
	}
	// Set the target sketch's windowOffset to 0.
	src.windowOffset = 0

	// Build a new pair table from the true pairs.
	table, err := newInstanceFromPairsArray(pairs, nextTruePair, srcLgK)
	if err != nil {
		return err
	}
	src.pairTable = table

	// Set the sliding window in the target sketch.
	src.slidingWindow = window

	return nil
}

func (c *CpcCompressedState) compressPinnedFlavor(src *CpcSketch) error {
	// Compress the window portion.
	if err := c.compressTheWindow(src); err != nil {
		return err
	}
	srcPairTable := src.pairTable
	numPairs := srcPairTable.numPairs
	if numPairs > 0 {
		pairs, err := srcPairTable.unwrap(numPairs)
		if err != nil {
			return err
		}
		// Subtract 8 from the column indices (stored in the low 6 bits).
		for i := 0; i < numPairs; i++ {
			// Ensure that the column (pairs[i] & 63) is at least 8.
			if (pairs[i] & 63) < 8 {
				return fmt.Errorf("compressPinnedFlavor: pair %d has column index less than 8", pairs[i])
			}
			pairs[i] -= 8
		}
		introspectiveInsertionSort(pairs, 0, numPairs-1)
		return compressTheSurprisingValues(c, src, pairs, numPairs)
	}
	return nil
}

func (c *CpcCompressedState) uncompressPinnedFlavor(src *CpcSketch) error {
	// The pinned flavor must have a non-nil cwStream.
	if c.CwStream == nil {
		return fmt.Errorf("uncompressPinnedFlavor: expected cwStream to be non-nil")
	}
	// Uncompress the window portion into the target sketch.
	if err := uncompressTheWindow(src, c); err != nil {
		return err
	}

	srcLgK := c.LgK
	numPairs := int(c.NumCsv)
	if numPairs == 0 {
		// If there are no pairs, create an empty pair table.
		pt, err := NewPairTable(2, 6+srcLgK)
		if err != nil {
			return err
		}
		src.pairTable = pt
	} else {
		// For pinned flavor, csvStream must be non-nil.
		if c.CsvStream == nil {
			return fmt.Errorf("uncompressPinnedFlavor: expected csvStream to be non-nil")
		}
		// Uncompress the surprising values.
		pairs, err := uncompressTheSurprisingValues(c)
		if err != nil {
			return err
		}
		// Undo the compressor's 8-column shift:
		// For each pair, the lower 6 bits (the column) must be less than 56.
		// Then add 8 back.
		for i := 0; i < numPairs; i++ {
			if (pairs[i] & 63) >= 56 {
				return fmt.Errorf("uncompressPinnedFlavor: invalid pair value %d at index %d", pairs[i], i)
			}
			pairs[i] += 8
		}
		// Create a new pair table from the corrected pairs array.
		table, err := newInstanceFromPairsArray(pairs, numPairs, srcLgK)
		if err != nil {
			return err
		}
		src.pairTable = table
	}
	return nil
}

func (c *CpcCompressedState) compressSlidingFlavor(src *CpcSketch) error {
	// First, compress the window.
	if err := c.compressTheWindow(src); err != nil {
		return err
	}

	srcPairTable := src.pairTable
	numPairs := srcPairTable.numPairs

	if numPairs > 0 {
		pairs, err := srcPairTable.unwrap(numPairs)
		if err != nil {
			return err
		}

		// Apply a transformation to the column indices.
		pseudoPhase := determinePseudoPhase(src.lgK, int64(src.numCoupons))
		if pseudoPhase >= 16 {
			return fmt.Errorf("compressSlidingFlavor: pseudoPhase (%d) >= 16", pseudoPhase)
		}
		permutation := columnPermutationsForEncoding[pseudoPhase]

		offset := src.windowOffset
		if offset <= 0 || offset > 56 {
			return fmt.Errorf("compressSlidingFlavor: invalid windowOffset %d", offset)
		}

		for i := 0; i < numPairs; i++ {
			rowCol := pairs[i]
			row := rowCol >> 6
			col := rowCol & 63
			// Rotate the columns into canonical configuration:
			//   new = ((old - (offset+8)) + 64) mod 64,
			// which simplifies here to:
			col = ((col + 56) - offset) & 63
			if col < 0 || col >= 56 {
				return fmt.Errorf("compressSlidingFlavor: transformed column %d out of range", col)
			}
			// Then apply the permutation.
			col = int(permutation[col])
			pairs[i] = (row << 6) | col
		}

		introspectiveInsertionSort(pairs, 0, numPairs-1)
		return compressTheSurprisingValues(c, src, pairs, numPairs)
	}
	return nil
}

func (c *CpcCompressedState) uncompressSlidingFlavor(src *CpcSketch) error {
	// Ensure that cwStream is not nil.
	if c.CwStream == nil {
		return fmt.Errorf("uncompressSlidingFlavor: expected cwStream to be non-nil")
	}
	// Uncompress the window portion.
	if err := uncompressTheWindow(src, c); err != nil {
		return err
	}

	srcLgK := c.LgK
	numPairs := int(c.NumCsv)
	if numPairs == 0 {
		// Create an empty pair table.
		pt, err := NewPairTable(2, 6+srcLgK)
		if err != nil {
			return err
		}
		src.pairTable = pt
	} else {
		// Ensure csvStream is present.
		if c.CsvStream == nil {
			return fmt.Errorf("uncompressSlidingFlavor: expected csvStream to be non-nil")
		}
		// Uncompress the surprising values.
		pairs, err := uncompressTheSurprisingValues(c)
		if err != nil {
			return err
		}

		// Determine pseudoPhase.
		pseudoPhase := determinePseudoPhase(srcLgK, int64(c.NumCoupons))
		if pseudoPhase >= 16 {
			return fmt.Errorf("uncompressSlidingFlavor: pseudoPhase %d out of range", pseudoPhase)
		}
		permutation := columnPermutationsForDecoding[pseudoPhase]

		// Get the window offset; it must be in (0, 56].
		offset := c.getWindowOffset()
		if offset <= 0 || offset > 56 {
			return fmt.Errorf("uncompressSlidingFlavor: invalid window offset %d", offset)
		}

		// For each pair, undo the permutation and rotation.
		for i := 0; i < numPairs; i++ {
			rowCol := pairs[i]
			row := rowCol >> 6
			col := rowCol & 63
			// First, undo the permutation.
			col = int(permutation[col])
			// Then, undo the rotation: old = (new + (offset+8)) mod 64.
			col = (col + (offset + 8)) & 63
			pairs[i] = (row << 6) | col
		}

		// Create a new pair table from the adjusted pairs.
		table, err := newInstanceFromPairsArray(pairs, numPairs, srcLgK)
		if err != nil {
			return err
		}
		src.pairTable = table
	}
	return nil
}

func importFromMemory(bytes []byte) (*CpcCompressedState, error) {
	if err := checkLoPreamble(bytes); err != nil {
		return nil, err
	}
	if !isCompressed(bytes) {
		return nil, fmt.Errorf("not compressed")
	}
	lgK := getLgK(bytes)
	seedHash := getSeedHash(bytes)
	state := NewCpcCompressedState(lgK, seedHash)
	fmtOrd := getFormatOrdinal(bytes)
	format := CpcFormat(fmtOrd)
	state.MergeFlag = (fmtOrd & 1) == 0
	state.CsvIsValid = (fmtOrd & 2) > 0
	state.WindowIsValid = (fmtOrd & 4) > 0

	switch format {
	case CpcFormatEmptyMerged, CpcFormatEmptyHip:
		if err := checkCapacity(len(bytes), 8); err != nil {
			return nil, err
		}
	case CpcFormatSparseHybridMerged:
		state.NumCoupons = getNumCoupons(bytes)
		state.NumCsv = state.NumCoupons
		state.CsvLengthInts = getSvLengthInts(bytes)
		if err := checkCapacity(len(bytes), state.getRequiredSerializedBytes()); err != nil {
			return nil, err
		}
		state.CsvStream = getSvStream(bytes)
	case CpcFormatSparseHybridHip:
		state.NumCoupons = getNumCoupons(bytes)
		state.NumCsv = state.NumCoupons
		state.CsvLengthInts = getSvLengthInts(bytes)
		state.Kxp = getKxP(bytes)
		state.HipEstAccum = getHipAccum(bytes)
		if err := checkCapacity(len(bytes), state.getRequiredSerializedBytes()); err != nil {
			return nil, err
		}
		state.CsvStream = getSvStream(bytes)
	case CpcFormatPinnedSlidingMergedNosv:
		state.FiCol = getFiCol(bytes)
		state.NumCoupons = getNumCoupons(bytes)
		state.CwLengthInts = getWLengthInts(bytes)
		if err := checkCapacity(len(bytes), state.getRequiredSerializedBytes()); err != nil {
			return nil, err
		}
		state.CwStream = getWStream(bytes)
	case CpcFormatPinnedSlidingHipNosv:
		state.FiCol = getFiCol(bytes)
		state.NumCoupons = getNumCoupons(bytes)
		state.CwLengthInts = getWLengthInts(bytes)
		state.Kxp = getKxP(bytes)
		state.HipEstAccum = getHipAccum(bytes)
		if err := checkCapacity(len(bytes), state.getRequiredSerializedBytes()); err != nil {
			return nil, err
		}
		state.CwStream = getWStream(bytes)
	case CpcFormatPinnedSlidingMerged:
		state.FiCol = getFiCol(bytes)
		state.NumCoupons = getNumCoupons(bytes)
		state.NumCsv = getNumSV(bytes)
		state.CsvLengthInts = getSvLengthInts(bytes)
		state.CwLengthInts = getWLengthInts(bytes)
		if err := checkCapacity(len(bytes), state.getRequiredSerializedBytes()); err != nil {
			return nil, err
		}
		state.CwStream = getWStream(bytes)
		state.CsvStream = getSvStream(bytes)
	case CpcFormatPinnedSlidingHip:
		state.FiCol = getFiCol(bytes)
		state.NumCoupons = getNumCoupons(bytes)
		state.NumCsv = getNumSV(bytes)
		state.CsvLengthInts = getSvLengthInts(bytes)
		state.CwLengthInts = getWLengthInts(bytes)
		state.Kxp = getKxP(bytes)
		state.HipEstAccum = getHipAccum(bytes)
		if err := checkCapacity(len(bytes), state.getRequiredSerializedBytes()); err != nil {
			return nil, err
		}
		state.CwStream = getWStream(bytes)
		state.CsvStream = getSvStream(bytes)
	default:
		panic("not implemented")
	}
	return state, nil
}

func (c *CpcCompressedState) exportToMemory() ([]byte, error) {
	// Determine the total number of bytes required.
	totalBytes := c.getRequiredSerializedBytes()
	// Allocate a byte slice (zero-filled by default).
	mem := make([]byte, totalBytes)

	// Determine the format of the state.
	format := c.getFormat()

	switch format {
	case CpcFormatEmptyMerged:
		if err := putEmptyMerged(mem, c.LgK, c.SeedHash); err != nil {
			return nil, err
		}
	case CpcFormatEmptyHip:
		if err := putEmptyHip(mem, c.LgK, c.SeedHash); err != nil {
			return nil, err
		}
	case CpcFormatSparseHybridMerged:
		if err := putSparseHybridMerged(mem, c.LgK, int(c.NumCoupons), c.CsvLengthInts, c.SeedHash, c.CsvStream); err != nil {
			return nil, err
		}
	case CpcFormatSparseHybridHip:
		if err := putSparseHybridHip(mem, c.LgK, int(c.NumCoupons), c.CsvLengthInts, c.Kxp, c.HipEstAccum, c.SeedHash, c.CsvStream); err != nil {
			return nil, err
		}
	case CpcFormatPinnedSlidingMergedNosv:
		if err := putPinnedSlidingMergedNoSv(mem, c.LgK, c.FiCol, int(c.NumCoupons), c.CwLengthInts, c.SeedHash, c.CwStream); err != nil {
			return nil, err
		}
	case CpcFormatPinnedSlidingHipNosv:
		if err := putPinnedSlidingHipNoSv(mem, c.LgK, c.FiCol, int(c.NumCoupons), c.CwLengthInts, c.Kxp, c.HipEstAccum, c.SeedHash, c.CwStream); err != nil {
			return nil, err
		}
	case CpcFormatPinnedSlidingMerged:
		if err := putPinnedSlidingMerged(mem, c.LgK, c.FiCol, int(c.NumCoupons), int(c.NumCsv), c.CsvLengthInts, c.CwLengthInts, c.SeedHash, c.CsvStream, c.CwStream); err != nil {
			return nil, err
		}
	case CpcFormatPinnedSlidingHip:
		if err := putPinnedSlidingHip(mem, c.LgK, c.FiCol, int(c.NumCoupons), int(c.NumCsv), c.Kxp, c.HipEstAccum, c.CsvLengthInts, c.CwLengthInts, c.SeedHash, c.CsvStream, c.CwStream); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("exportToMemory: format %v not implemented", format)
	}

	if err := checkCapacity(len(mem), totalBytes); err != nil {
		return nil, err
	}

	return mem, nil
}

func compressTheSurprisingValues(target *CpcCompressedState, source *CpcSketch, pairs []int, numPairs int) error {
	if numPairs <= 0 {
		return fmt.Errorf("compressTheSurprisingValues: numPairs must be > 0, got %d", numPairs)
	}
	// Set the number of CSV values.
	target.NumCsv = uint64(numPairs)
	// Compute srcK = 1 << source.lgK.
	srcK := 1 << source.lgK
	// Determine the number of base bits using a Golomb code decision.
	numBaseBits := golombChooseNumberOfBaseBits(srcK+numPairs, numPairs)
	// Compute an upper-bound length for the compressed pairs buffer.
	pairBufLen := safeLengthForCompressedPairBuf(srcK, numPairs, numBaseBits)
	// Allocate the buffer for compression.
	pairBuf := make([]int, pairBufLen)
	// lowLevelCompressPairs compresses 'pairs' using the chosen base bits into pairBuf.
	// It returns the number of ints that represent the compressed data.
	csvLength := lowLevelCompressPairs(pairs, numPairs, numBaseBits, pairBuf)
	target.CsvLengthInts = csvLength
	target.CsvStream = pairBuf
	return nil
}

func uncompressTheSurprisingValues(source *CpcCompressedState) ([]int, error) {
	srcK := 1 << source.LgK
	numPairs := int(source.NumCsv)
	if numPairs <= 0 {
		return nil, fmt.Errorf("uncompressTheSurprisingValues: numPairs must be > 0, got %d", numPairs)
	}
	pairs := make([]int, numPairs)
	// Determine the number of base bits using the Golomb code decision.
	numBaseBits := golombChooseNumberOfBaseBits(srcK+numPairs, numPairs)
	// lowLevelUncompressPairs fills the 'pairs' slice using the compressed CSV stream.
	if err := lowLevelUncompressPairs(pairs, numPairs, numBaseBits, source.CsvStream, source.CsvLengthInts); err != nil {
		return nil, err
	}
	return pairs, nil
}

func golombChooseNumberOfBaseBits(k, count int) int {
	if k < 1 || count < 1 {
		panic("golombChooseNumberOfBaseBits: k and count must be >= 1")
	}
	quotient := (k - count) / count
	if quotient == 0 {
		return 0
	}
	return floorLog2(uint64(quotient))
}

func floorLog2(x uint64) int {
	return bits.Len64(x) - 1
}

func safeLengthForCompressedPairBuf(k, numPairs, numBaseBits int) int {
	if numPairs <= 0 {
		panic("safeLengthForCompressedPairBuf: numPairs must be > 0")
	}
	// Compute ybits = (numPairs * (1 + numBaseBits)) + (k >>> numBaseBits)
	ybits := int64(numPairs)*(1+int64(numBaseBits)) + (int64(k) >> uint(numBaseBits))
	xbits := int64(12 * numPairs)
	padding := int64(10 - numBaseBits)
	if padding < 0 {
		padding = 0
	}
	totalBits := xbits + ybits + padding

	// Divide by 32 rounding up to get a word count.
	words := divideBy32RoundingUp(totalBits)
	// Ensure the number of words fits in a 31-bit int.
	if words >= (1 << 31) {
		panic("safeLengthForCompressedPairBuf: words too large")
	}
	return int(words)
}

func divideBy32RoundingUp(x int64) int64 {
	tmp := x >> 5 // equivalent to dividing by 32
	if tmp<<5 == x {
		return tmp
	}
	return tmp + 1
}

func lowLevelCompressPairs(pairArray []int, numPairsToEncode, numBaseBits int, compressedWords []int) int {
	nextWordIndex := 0
	var bitBuf uint64 = 0
	bufBits := 0

	// Allocate the pointer array (used for writeUnary).
	ptrArr := make([]int64, 3)

	// golombLoMask = (1L << numBaseBits) - 1
	golombLoMask := (uint64(1) << uint(numBaseBits)) - 1

	predictedRowIndex := 0
	predictedColIndex := 0

	for pairIndex := 0; pairIndex < numPairsToEncode; pairIndex++ {
		rowCol := pairArray[pairIndex]
		// Extract row index (upper bits) and column index (lower 6 bits)
		rowIndex := rowCol >> 6
		colIndex := rowCol & 0x3F // 0x3F == 63

		if rowIndex != predictedRowIndex {
			predictedColIndex = 0
		}
		if rowIndex < predictedRowIndex || colIndex < predictedColIndex {
			panic(fmt.Sprintf("lowLevelCompressPairs: assertion failed: rowIndex=%d, predictedRowIndex=%d, colIndex=%d, predictedColIndex=%d",
				rowIndex, predictedRowIndex, colIndex, predictedColIndex))
		}

		// yDelta is the difference in row indices.
		yDelta := uint64(rowIndex - predictedRowIndex)
		// xDelta is the difference in column indices.
		xDelta := colIndex - predictedColIndex

		predictedRowIndex = rowIndex
		predictedColIndex = colIndex + 1

		// Retrieve the code information from the lookup table.
		codeInfo := uint64(lengthLimitedUnaryEncodingTable65[xDelta]) & 0xFFFF
		// Lower 12 bits are the code value.
		codeVal := codeInfo & 0xFFF
		// Upper bits (shifted right 12) are the code length.
		codeLen := int(codeInfo >> 12)

		// Append the code value into the bit buffer.
		bitBuf |= codeVal << uint(bufBits)
		bufBits += codeLen
		// Flush the bit buffer if we have 32 or more bits.
		if bufBits >= 32 {
			compressedWords[nextWordIndex] = int(bitBuf & 0xFFFFFFFF)
			nextWordIndex++
			bitBuf >>= 32
			bufBits -= 32
		}

		// Process Golomb coding for yDelta.
		golombLo := yDelta & golombLoMask
		golombHi := yDelta >> uint(numBaseBits)

		// Inline WriteUnary:
		ptrArr[NextWordIdx] = int64(nextWordIndex)
		ptrArr[BitBuf] = int64(bitBuf)
		ptrArr[BufBits] = int64(bufBits)
		// Call writeUnary to output unary code for golombHi.
		writeUnary(compressedWords, ptrArr, int(golombHi))
		// Retrieve updated values.
		nextWordIndex = int(ptrArr[NextWordIdx])
		bitBuf = uint64(ptrArr[BitBuf])
		bufBits = int(ptrArr[BufBits])

		// Append the lower bits of the Golomb code.
		bitBuf |= golombLo << uint(bufBits)
		bufBits += numBaseBits
		if bufBits >= 32 {
			compressedWords[nextWordIndex] = int(bitBuf & 0xFFFFFFFF)
			nextWordIndex++
			bitBuf >>= 32
			bufBits -= 32
		}
	}

	// Pad the bitstream so that the decompressor's 12-bit peek can't overrun its input.
	padding := 10 - numBaseBits
	if padding < 0 {
		padding = 0
	}
	bufBits += padding
	if bufBits >= 32 {
		compressedWords[nextWordIndex] = int(bitBuf & 0xFFFFFFFF)
		nextWordIndex++
		bitBuf >>= 32
		bufBits -= 32
	}
	if bufBits > 0 {
		// Flush any remaining bits.
		compressedWords[nextWordIndex] = int(bitBuf & 0xFFFFFFFF)
		nextWordIndex++
	}
	return nextWordIndex
}

func lowLevelUncompressPairs(pairArray []int, numPairsToDecode, numBaseBits int, compressedWords []int, numCompressedWords int) error {
	// Output index for pairArray.
	pairIndex := 0
	ptrArr := make([]int64, 3)
	nextWordIndex := 0
	var bitBuf uint64 = 0
	bufBits := 0

	// golombLoMask = (1 << numBaseBits) - 1
	golombLoMask := (uint64(1) << uint(numBaseBits)) - 1

	predictedRowIndex := 0
	predictedColIndex := 0

	// For each pair to decode:
	for pairIndex < numPairsToDecode {
		// Ensure we have at least 12 bits in bitBuf.
		if bufBits < 12 {
			if nextWordIndex >= len(compressedWords) {
				return fmt.Errorf("lowLevelUncompressPairs: insufficient compressedWords data")
			}
			bitBuf |= (uint64(compressedWords[nextWordIndex]) & 0xFFFFFFFF) << uint(bufBits)
			nextWordIndex++
			bufBits += 32
		}

		// Peek 12 bits.
		peek12 := int(bitBuf & 0xFFF) // 0xFFF is 12 bits.
		lookup := int(lengthLimitedUnaryDecodingTable65[peek12]) & 0xFFFF
		codeWordLength := lookup >> 8
		xDelta := lookup & 0xFF

		// Consume the xDelta bits.
		bitBuf >>= uint(codeWordLength)
		bufBits -= codeWordLength

		// Inline ReadUnary:
		ptrArr[NextWordIdx] = int64(nextWordIndex)
		ptrArr[BitBuf] = int64(bitBuf)
		ptrArr[BufBits] = int64(bufBits)
		golombHi := readUnary(compressedWords, ptrArr)
		// Retrieve updated values.
		nextWordIndex = int(ptrArr[NextWordIdx])
		bitBuf = uint64(ptrArr[BitBuf])
		bufBits = int(ptrArr[BufBits])

		// Ensure at least numBaseBits in bitBuf.
		if bufBits < numBaseBits {
			if nextWordIndex >= len(compressedWords) {
				return fmt.Errorf("lowLevelUncompressPairs: insufficient compressedWords data for golombLo")
			}
			bitBuf |= (uint64(compressedWords[nextWordIndex]) & 0xFFFFFFFF) << uint(bufBits)
			nextWordIndex++
			bufBits += 32
		}

		golombLo := bitBuf & golombLoMask
		bitBuf >>= uint(numBaseBits)
		bufBits -= numBaseBits

		// yDelta is the combination of the unary high and the base bits.
		yDelta := (uint64(golombHi) << uint(numBaseBits)) | golombLo
		// Now compute the pair's row and column.
		if yDelta > 0 {
			predictedColIndex = 0
		}
		rowIndex := predictedRowIndex + int(yDelta)
		colIndex := predictedColIndex + xDelta
		rowCol := (rowIndex << 6) | colIndex
		pairArray[pairIndex] = rowCol
		pairIndex++

		predictedRowIndex = rowIndex
		predictedColIndex = colIndex + 1
	}

	if nextWordIndex > numCompressedWords {
		return fmt.Errorf("lowLevelUncompressPairs: nextWordIndex %d exceeds numCompressedWords %d", nextWordIndex, numCompressedWords)
	}
	return nil
}

func readUnary(compressedWords []int, ptrArr []int64) int64 {
	nextWordIndex := int(ptrArr[NextWordIdx])
	bitBuf := uint64(ptrArr[BitBuf])
	bufBits := int(ptrArr[BufBits])

	var subTotal int64 = 0
	var trailingZeros int

	// Loop until we get a byte that doesn't have all 8 zeros.
	for {
		// Ensure we have at least 8 bits in the bit buffer.
		if bufBits < 8 {
			if nextWordIndex >= len(compressedWords) {
				panic("readUnary: insufficient compressedWords data")
			}
			bitBuf |= (uint64(compressedWords[nextWordIndex]) & 0xFFFFFFFF) << uint(bufBits)
			nextWordIndex++
			bufBits += 32
		}

		// Peek at the lowest 8 bits.
		peek8 := int(bitBuf & 0xFF)
		// Compute the number of trailing zeros in these 8 bits.
		// bits.TrailingZeros8 returns a value between 0 and 8.
		trailingZeros = bits.TrailingZeros8(uint8(peek8))
		// If all 8 bits are zeros, the codeword is partial; add 8 to subTotal and consume 8 bits.
		if trailingZeros == 8 {
			subTotal += 8
			bufBits -= 8
			bitBuf >>= 8
			continue
		}
		break
	}

	// Consume the terminating one and the zeros.
	bufBits -= 1 + trailingZeros
	bitBuf >>= uint(1 + trailingZeros)

	// Update the pointer array.
	ptrArr[NextWordIdx] = int64(nextWordIndex)
	ptrArr[BitBuf] = int64(bitBuf)
	ptrArr[BufBits] = int64(bufBits)

	return subTotal + int64(trailingZeros)
}

func writeUnary(compressedWords []int, ptrArr []int64, theValue int) {
	nextWordIndex := int(ptrArr[NextWordIdx])
	bitBuf := uint64(ptrArr[BitBuf])
	bufBits := int(ptrArr[BufBits])

	remaining := theValue

	// Write out groups of 16 zeros.
	for remaining >= 16 {
		remaining -= 16
		bufBits += 16
		if bufBits >= 32 {
			compressedWords[nextWordIndex] = int(bitBuf & 0xFFFFFFFF)
			nextWordIndex++
			bitBuf >>= 32
			bufBits -= 32
		}
	}
	// remaining is now between 0 and 15.
	theUnaryCode := uint64(1) << uint(remaining) // a one at position 'remaining'
	bitBuf |= theUnaryCode << uint(bufBits)
	bufBits += 1 + remaining
	if bufBits >= 32 {
		compressedWords[nextWordIndex] = int(bitBuf & 0xFFFFFFFF)
		nextWordIndex++
		bitBuf >>= 32
		bufBits -= 32
	}
	ptrArr[NextWordIdx] = int64(nextWordIndex)
	ptrArr[BitBuf] = int64(bitBuf)
	ptrArr[BufBits] = int64(bufBits)
}

func trickyGetPairsFromWindow(window []byte, k, numPairsToGet, emptySpace int) []int {
	outputLength := emptySpace + numPairsToGet
	pairs := make([]int, outputLength)
	pairIndex := emptySpace

	for rowIndex := 0; rowIndex < k; rowIndex++ {
		// Treat the byte as an unsigned value.
		wByte := int(window[rowIndex]) & 0xFF
		for wByte != 0 {
			// bits.TrailingZeros8 returns the number of trailing zero bits in an uint8.
			colIndex := bits.TrailingZeros8(uint8(wByte))
			// Erase the found bit.
			wByte ^= 1 << colIndex
			// Encode the pair as (rowIndex << 6) | colIndex.
			pairs[pairIndex] = (rowIndex << 6) | colIndex
			pairIndex++
		}
	}

	if pairIndex != outputLength {
		panic(fmt.Sprintf("trickyGetPairsFromWindow: pairIndex (%d) != outputLength (%d)", pairIndex, outputLength))
	}

	return pairs
}

func (c *CpcCompressedState) compressTheWindow(src *CpcSketch) error {
	// Get the source parameters.
	srcLgK := src.lgK
	srcK := 1 << srcLgK
	// Determine the safe buffer length for compressing the window.
	windowBufLen := safeLengthForCompressedWindowBuf(int64(srcK))
	windowBuf := make([]int, windowBufLen)
	// Determine the pseudo-phase using srcLgK and the number of coupons.
	pseudoPhase := determinePseudoPhase(srcLgK, int64(src.numCoupons))
	// Compress the sliding window bytes.
	// lowLevelCompressBytes is assumed to return (cwLengthInts int, err error).
	cwLengthInts := lowLevelCompressBytes(src.slidingWindow, srcK, encodingTablesForHighEntropyByte[pseudoPhase], windowBuf)
	// Store the results into the compressed state.
	c.CwLengthInts = cwLengthInts
	c.CwStream = windowBuf

	return nil
}

func uncompressTheWindow(target *CpcSketch, source *CpcCompressedState) error {
	srcLgK := source.LgK
	srcK := 1 << srcLgK
	// Allocate a byte slice of length srcK (zeroed by default).
	window := make([]byte, srcK)

	// Ensure that target.slidingWindow is nil.
	if target.slidingWindow != nil {
		return fmt.Errorf("uncompressTheWindow: target.slidingWindow is already set")
	}
	target.slidingWindow = window

	// Determine the pseudo-phase using srcLgK and source.NumCoupons.
	pseudoPhase := determinePseudoPhase(srcLgK, int64(source.NumCoupons))
	// Ensure that source.CwStream is not nil.
	if source.CwStream == nil {
		return fmt.Errorf("uncompressTheWindow: source.CwStream is nil")
	}

	// Uncompress the window bytes into target.slidingWindow.
	return lowLevelUncompressBytes(target.slidingWindow, srcK,
		decodingTablesForHighEntropyByte[pseudoPhase],
		source.CwStream,
		source.CwLengthInts)
}

// safeLengthForCompressedWindowBuf computes the safe buffer length (in 32‐bit words)
// for compressing the window, given k (typically 1 << lgK).
func safeLengthForCompressedWindowBuf(k int64) int {
	// Compute total total_bits = (12 * k) + 11 (i.e. 12 total_bits per row plus 11 total_bits of padding).
	totalBits := (12 * k) + 11
	// Divide by 32 rounding up.
	return int(divideBy32RoundingUp(totalBits))
}

func determinePseudoPhase(lgK int, numCoupons int64) int {
	k := int64(1) << uint(lgK)
	c := numCoupons
	// Midrange logic.
	if (1000 * c) < (2375 * k) {
		if (4 * c) < (3 * k) {
			return 16 + 0
		} else if (10 * c) < (11 * k) {
			return 16 + 1
		} else if (100 * c) < (132 * k) {
			return 16 + 2
		} else if (3 * c) < (5 * k) {
			return 16 + 3
		} else if (1000 * c) < (1965 * k) {
			return 16 + 4
		} else if (1000 * c) < (2275 * k) {
			return 16 + 5
		} else {
			return 6 // steady-state table employed before its actual phase.
		}
	} else {
		// Steady-state logic.
		if lgK < 4 {
			panic("determinePseudoPhase: lgK must be at least 4")
		}
		tmp := c >> uint(lgK-4)
		phase := int(tmp & 15)
		if phase < 0 || phase >= 16 {
			panic(fmt.Sprintf("determinePseudoPhase: phase out of range: %d", phase))
		}
		return phase
	}
}

func lowLevelCompressBytes(byteArray []byte, numBytesToEncode int, encodingTable []uint16, compressedWords []int) int {
	nextWordIndex := 0
	var bitBuf uint64 = 0 // accumulator for bits
	bufBits := 0          // number of bits currently in bitBuf

	for byteIndex := 0; byteIndex < numBytesToEncode; byteIndex++ {
		// Get the byte as an unsigned value.
		theByte := int(byteArray[byteIndex]) & 0xFF
		codeInfo := uint64(encodingTable[theByte]) & 0xFFFF
		// Lower 12 bits are the code value.
		codeVal := codeInfo & 0xFFF
		// Upper bits (after shifting right by 12) give the code word length.
		codeWordLength := int(codeInfo >> 12)
		// Append the code value into bitBuf.
		bitBuf |= codeVal << uint(bufBits)
		bufBits += codeWordLength

		// Flush complete 32-bit words.
		if bufBits >= 32 {
			compressedWords[nextWordIndex] = int(bitBuf & 0xFFFFFFFF)
			nextWordIndex++
			bitBuf >>= 32
			bufBits -= 32
		}
	}

	// Pad with 11 zero-bits so that the decompressor's 12-bit peek cannot overrun.
	bufBits += 11
	if bufBits >= 32 {
		compressedWords[nextWordIndex] = int(bitBuf & 0xFFFFFFFF)
		nextWordIndex++
		bitBuf >>= 32
		bufBits -= 32
	}
	// Flush any remaining bits.
	if bufBits > 0 {
		// bufBits is guaranteed to be less than 32.
		compressedWords[nextWordIndex] = int(bitBuf & 0xFFFFFFFF)
		nextWordIndex++
	}
	return nextWordIndex
}

func lowLevelUncompressBytes(byteArray []byte, numBytesToDecode int, decodingTable []uint16, compressedWords []int, numCompressedWords int) error {
	// Precondition checks.
	if byteArray == nil {
		return fmt.Errorf("lowLevelUncompressBytes: byteArray is nil")
	}
	if decodingTable == nil {
		return fmt.Errorf("lowLevelUncompressBytes: decodingTable is nil")
	}
	if compressedWords == nil {
		return fmt.Errorf("lowLevelUncompressBytes: compressedWords is nil")
	}

	byteIndex := 0
	nextWordIndex := 0
	var bitBuf uint64 = 0
	bufBits := 0

	// Loop for each output byte.
	for byteIndex < numBytesToDecode {
		// Ensure there are at least 12 bits in bitBuf.
		if bufBits < 12 {
			if nextWordIndex >= len(compressedWords) {
				return fmt.Errorf("lowLevelUncompressBytes: insufficient compressedWords data")
			}
			// Append next 32 bits from compressedWords.
			bitBuf |= (uint64(compressedWords[nextWordIndex]) & 0xFFFFFFFF) << uint(bufBits)
			nextWordIndex++
			bufBits += 32
		}

		// Peek 12 bits.
		peek12 := int(bitBuf & 0xFFF) // 0xFFF == 12 bits.
		lookup := int(decodingTable[peek12]) & 0xFFFF
		codeWordLength := lookup >> 8
		decodedByte := byte(lookup & 0xFF)
		byteArray[byteIndex] = decodedByte
		byteIndex++

		// Consume the codeword bits.
		bitBuf >>= uint(codeWordLength)
		bufBits -= codeWordLength
	}

	// Check that we did not over-run the compressedWords array.
	if nextWordIndex > numCompressedWords {
		return fmt.Errorf("lowLevelUncompressBytes: nextWordIndex (%d) exceeds expected (%d)", nextWordIndex, numCompressedWords)
	}
	return nil
}
