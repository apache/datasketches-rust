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
	"encoding/binary"
	"fmt"
	"github.com/apache/datasketches-go/internal"
	"github.com/twmb/murmur3"
	"math"
	"math/bits"
	"unsafe"
)

const (
	minLgK                 = 4
	maxLgK                 = 26
	empiricalSizeMaxLgK    = 19
	empiricalMaxSizeFactor = 0.6 // equals 0.6 = 4.8 / 8.0
	maxPreambleSizeBytes   = 40
)

var empiricalMaxBytes = []int{
	24,     // lgK = 4
	36,     // lgK = 5
	56,     // lgK = 6
	100,    // lgK = 7
	180,    // lgK = 8
	344,    // lgK = 9
	660,    // lgK = 10
	1292,   // lgK = 11
	2540,   // lgK = 12
	5020,   // lgK = 13
	9968,   // lgK = 14
	19836,  // lgK = 15
	39532,  // lgK = 16
	78880,  // lgK = 17
	157516, // lgK = 18
	314656, // lgK = 19
}

type CpcSketch struct {
	seed uint64

	//common variables
	lgK        int
	numCoupons uint64 // The number of coupons collected so far.
	mergeFlag  bool   // Is the sketch the result of merging?
	fiCol      int    // First Interesting Column. This is part of a speed optimization.

	windowOffset  int
	slidingWindow []byte     //either null or size K bytes
	pairTable     *pairTable //for sparse and surprising values, either null or variable size

	//The following variables are only valid in HIP variants
	kxp         float64 //used with HIP
	hipEstAccum float64 //used with HIP

	scratch [8]byte
}

func NewCpcSketchWithDefault(lgK int) (*CpcSketch, error) {
	return NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
}

func NewCpcSketch(lgK int, seed uint64) (*CpcSketch, error) {
	if err := checkLgK(lgK); err != nil {
		return nil, err
	}

	return &CpcSketch{
		lgK:  lgK,
		seed: seed,
		kxp:  float64(int64(1) << lgK),
	}, nil
}

func NewCpcSketchFromSlice(bytes []byte, seed uint64) (*CpcSketch, error) {
	c, err := importFromMemory(bytes)
	if err != nil {
		return nil, err
	}

	sketch, err := NewCpcSketch(c.LgK, seed)
	if err != nil {
		return nil, err
	}
	sketch.numCoupons = c.NumCoupons
	sketch.windowOffset = c.getWindowOffset()
	sketch.fiCol = c.FiCol
	sketch.mergeFlag = c.MergeFlag
	sketch.kxp = c.Kxp
	sketch.hipEstAccum = c.HipEstAccum
	sketch.slidingWindow = nil
	sketch.pairTable = nil

	err = c.uncompress(sketch)
	return sketch, err
}

func NewCpcSketchFromSliceWithDefault(bytes []byte) (*CpcSketch, error) {
	return NewCpcSketchFromSlice(bytes, internal.DEFAULT_UPDATE_SEED)
}

func (c *CpcSketch) GetEstimate() float64 {
	if c.mergeFlag {
		return iconEstimate(c.lgK, c.numCoupons)
	}
	return c.hipEstAccum
}

func (c *CpcSketch) GetLowerBound(kappa int) float64 {
	if c.mergeFlag {
		return iconConfidenceLB(c.lgK, c.numCoupons, kappa)
	}
	return hipConfidenceLB(c.lgK, c.numCoupons, c.hipEstAccum, kappa)
}

func (c *CpcSketch) GetUpperBound(kappa int) float64 {
	if c.mergeFlag {
		return iconConfidenceUB(c.lgK, c.numCoupons, kappa)
	}
	return hipConfidenceUB(c.lgK, c.numCoupons, c.hipEstAccum, kappa)
}

func (c *CpcSketch) UpdateUint64(datum uint64) error {
	binary.LittleEndian.PutUint64(c.scratch[:], datum)
	hashLo, hashHi := hash(c.scratch[:], c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateInt64(datum int64) error {
	return c.UpdateUint64(uint64(datum))
}

func (c *CpcSketch) UpdateFloat64(datum float64) error {
	// Merge +0.0 and -0.0
	if datum == 0.0 {
		datum = 0.0 // ensures +0.0 == -0.0
	}
	datumBits := math.Float64bits(datum)

	// Canonicalize all NaN forms to 0x7ff8000000000000
	if (datumBits&0x7ff0000000000000) == 0x7ff0000000000000 && // exponent all 1's
		(datumBits&0x000fffffffffffff) != 0 { // mantissa non-zero => NaN
		datumBits = 0x7ff8000000000000
	}

	// Hash the canonicalized bits
	binary.LittleEndian.PutUint64(c.scratch[:], datumBits)
	hashLo, hashHi := hash(c.scratch[:], c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateInt64Slice(datum []int64) error {
	if len(datum) == 0 {
		return nil
	}
	hashLo, hashHi := internal.HashInt64SliceMurmur3(datum, 0, len(datum), c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateInt32Slice(datum []int32) error {
	if len(datum) == 0 {
		return nil
	}
	hashLo, hashHi := internal.HashInt32SliceMurmur3(datum, 0, len(datum), c.seed)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateByteSlice(datum []byte) error {
	if len(datum) == 0 {
		return nil
	}
	hashLo, hashHi := murmur3.SeedSum128(c.seed, c.seed, datum)
	return c.hashUpdate(hashLo, hashHi)
}

func (c *CpcSketch) UpdateString(datum string) error {
	if len(datum) == 0 {
		return nil
	}
	// get a slice to the string data (avoiding a copy to heap)
	return c.UpdateByteSlice(unsafe.Slice(unsafe.StringData(datum), len(datum)))
}

func (c *CpcSketch) hashUpdate(hash0, hash1 uint64) error {
	col := bits.LeadingZeros64(hash1)
	if col < c.fiCol {
		return nil
	}
	if col > 63 {
		col = 63
	}
	if c.numCoupons == 0 {
		if err := c.promoteEmptyToSparse(); err != nil {
			return err
		}
	}

	k := uint64(1) << c.lgK
	row := int(hash0 & (k - 1))

	// Use a 64-bit intermediate to avoid sign overflow.
	rowCol64 := (int64(row) << 6) | int64(col)

	// Only apply the hack if lgK >= 26 and rowCol64 == 0xFFFFFFFF (which is -1 in signed 64-bit).
	if c.lgK >= 26 && rowCol64 == -1 {
		rowCol64 ^= 1 << 6 // flip the LSB of row
	}

	rowCol := int(rowCol64)

	// Then proceed normally:
	if (c.numCoupons << 5) < (3 * k) {
		return c.updateSparse(rowCol)
	}
	return c.updateWindowed(rowCol)
}

func (c *CpcSketch) promoteEmptyToSparse() error {
	pairTable, err := NewPairTable(2, 6+c.lgK)
	if err != nil {
		return err
	}
	c.pairTable = pairTable
	return nil
}

func (c *CpcSketch) updateSparse(rowCol int) error {
	k := uint64(1) << c.lgK
	c32pre := c.numCoupons << 5
	if c32pre >= (3 * k) {
		// C >= 3K/32, in other words, flavor == SPARSE
		return fmt.Errorf("C >= 3K/32")
	}
	if c.pairTable == nil {
		return fmt.Errorf("pairTable is nil")
	}
	isNovel, err := c.pairTable.maybeInsert(rowCol)
	if err != nil {
		return err
	}
	if isNovel {
		c.numCoupons++
		c.updateHIP(rowCol)
		c32post := c.numCoupons << 5
		if c32post >= (3 * k) {
			err = c.promoteSparseToWindowed() // C >= 3K/32
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *CpcSketch) updateWindowed(rowCol int) error {
	if c.windowOffset < 0 || c.windowOffset > 56 {
		return fmt.Errorf("windowOffset < 0 || windowOffset > 56")
	}
	k := uint64(1) << c.lgK
	c32pre := c.numCoupons << 5
	if c32pre < (3 * k) {
		return fmt.Errorf("C < 3K/32")
	}
	c8pre := c.numCoupons << 3
	w8pre := uint64(c.windowOffset << 3)
	if c8pre >= ((uint64(27) + w8pre) * k) {
		return fmt.Errorf("C >= (K * 27/8) + (K * windowOffset)")
	}

	isNovel := false //novel if new coupon
	err := error(nil)
	col := rowCol & 63

	if col < c.windowOffset { // track the surprising 0's "before" the window
		isNovel, err = c.pairTable.maybeDelete(rowCol)
		if err != nil {
			return err
		}
	} else if col < (c.windowOffset + 8) { // track the 8 bits inside the window
		row := rowCol >> 6
		oldBits := c.slidingWindow[row]
		newBits := oldBits | (1 << (col - c.windowOffset))
		if newBits != oldBits {
			c.slidingWindow[row] = newBits
			isNovel = true
		}
	} else { // track the surprising 1's "after" the window
		isNovel, err = c.pairTable.maybeInsert(rowCol)
		if err != nil {
			return err
		}
	}

	if isNovel {
		c.numCoupons++
		c.updateHIP(rowCol)
		c8post := c.numCoupons << 3
		if c8post >= ((27 + w8pre) * k) {
			if err := c.modifyOffset(c.windowOffset + 1); err != nil {
				return err
			}
			if c.windowOffset < 1 || c.windowOffset > 56 {
				return fmt.Errorf("windowOffset < 1 || windowOffset > 56")
			}
			w8post := uint64(c.windowOffset << 3)
			if c8post >= ((uint64(27) + w8post) * k) {
				return fmt.Errorf("C < (K * 27/8) + (K * windowOffset)")
			}
		}

	}
	return nil
}

func hash(bs []byte, seed uint64) (uint64, uint64) {
	return murmur3.SeedSum128(seed, seed, bs)
}

func (c *CpcSketch) getFormat() CpcFormat {
	ordinal := 0
	f := c.getFlavor()
	if f == CpcFlavorHybrid || f == CpcFlavorSparse {
		ordinal = 2
		if !c.mergeFlag {
			ordinal |= 1
		}
	} else {
		ordinal = 0
		if c.slidingWindow != nil {
			ordinal |= 4
		}
		if c.pairTable != nil && c.pairTable.numPairs > 0 {
			ordinal |= 2
		}
		if !c.mergeFlag {
			ordinal |= 1
		}
	}
	return CpcFormat(ordinal)
}

func (c *CpcSketch) getFlavor() CpcFlavor {
	return determineFlavor(c.lgK, c.numCoupons)
}

func (c *CpcSketch) updateHIP(rowCol int) {
	k := 1 << c.lgK
	col := rowCol & 63
	oneOverP := float64(k) / c.kxp
	c.hipEstAccum += oneOverP
	kxp, _ := internal.InvPow2(col + 1)
	c.kxp -= kxp
}

func (c *CpcSketch) promoteSparseToWindowed() error {
	window := make([]byte, 1<<c.lgK)
	newTable, _ := NewPairTable(2, 6+c.lgK)
	oldTable := c.pairTable

	oldSlots := oldTable.slotsArr
	oldNumSlots := 1 << oldTable.lgSizeInts

	for i := 0; i < oldNumSlots; i++ {
		rowCol := oldSlots[i]
		if rowCol != -1 {
			col := rowCol & 63
			if col < 8 {
				row := rowCol >> 6
				window[row] |= 1 << col
			} else {
				isNovel, err := newTable.maybeInsert(rowCol)
				if err != nil {
					return fmt.Errorf("maybeInsert: %v", err)
				}
				if !isNovel {
					return fmt.Errorf("promoteSparseToWindowed: unexpected collision")
				}
			}
		}
	}

	c.slidingWindow = window
	c.pairTable = newTable

	return nil
}

func (c *CpcSketch) reset() {
	c.numCoupons = 0
	c.mergeFlag = false
	c.fiCol = 0
	c.windowOffset = 0
	c.slidingWindow = nil
	c.pairTable = nil
	c.kxp = float64(int64(1) << c.lgK)
	c.hipEstAccum = 0
}

func (c *CpcSketch) rowColUpdate(rowCol int) error {
	col := rowCol & 63
	if col < c.fiCol {
		return nil
	}
	if c.numCoupons == 0 {
		err := c.promoteEmptyToSparse()
		if err != nil {
			return err
		}
	}
	k := uint64(1) << c.lgK
	if (c.numCoupons << 5) < (3 * k) {
		return c.updateSparse(rowCol)
	}
	return c.updateWindowed(rowCol)
}

func (c *CpcSketch) modifyOffset(newOffset int) error {
	if newOffset < 0 || newOffset > 56 {
		return fmt.Errorf("newOffset < 0 || newOffset > 56")
	}
	if newOffset != (c.windowOffset + 1) {
		return fmt.Errorf("newOffset != (c.windowOffset + 1)")
	}
	if c.slidingWindow == nil || c.pairTable == nil {
		return fmt.Errorf("slidingWindow == nil || pairTable == nil")
	}
	k := 1 << c.lgK
	bitMatrix, err := c.bitMatrixOfSketch()
	if err != nil {
		return err
	}
	if (newOffset & 0x7) == 0 {
		c.refreshKXP(bitMatrix)
	}
	c.pairTable.clear()
	maskForClearingWindow := (0xFF << newOffset) ^ -1
	maskForFlippingEarlyZone := (1 << newOffset) - 1
	allSurprisesORed := uint64(0)
	for i := 0; i < k; i++ {
		pattern := bitMatrix[i]
		c.slidingWindow[i] = byte((pattern >> newOffset) & 0xFF)
		pattern &= uint64(maskForClearingWindow)
		pattern ^= uint64(maskForFlippingEarlyZone)
		allSurprisesORed |= pattern
		for pattern != 0 {
			col := bits.TrailingZeros64(pattern)
			pattern ^= 1 << col
			rowCol := (i << 6) | col
			isNovel, err := c.pairTable.maybeInsert(rowCol)
			if err != nil {
				return err
			}
			if !isNovel {
				return nil
			}
		}
	}
	c.windowOffset = newOffset
	c.fiCol = bits.TrailingZeros64(allSurprisesORed)
	if c.fiCol > newOffset {
		c.fiCol = newOffset
	}
	return nil
}

func (c *CpcSketch) refreshKXP(bitMatrix []uint64) {
	k := 1 << c.lgK
	byteSums := make([]float64, 8)
	for i := 0; i < k; i++ {
		row := bitMatrix[i]
		for j := 0; j < 8; j++ {
			byteIdx := int(row & 0xFF)
			byteSums[j] += kxpByteLookup[byteIdx]
			row >>= 8
		}
	}
	total := 0.0
	for j := 6; j >= 0; j-- {
		factor, _ := internal.InvPow2(8 * j)
		total += factor * byteSums[j]
	}
	c.kxp = total
}

func (c *CpcSketch) bitMatrixOfSketch() ([]uint64, error) {
	k := uint64(1) << c.lgK
	offset := c.windowOffset
	if offset < 0 || offset > 56 {
		return nil, fmt.Errorf("offset < 0 || offset > 56")
	}
	matrix := make([]uint64, k)
	if c.numCoupons == 0 {
		return matrix, nil // Returning a matrix of zeros rather than NULL.
	}
	//Fill the matrix with default rows in which the "early zone" is filled with ones.
	//This is essential for the routine's O(k) time cost (as opposed to O(C)).
	defaultRow := (1 << offset) - 1
	for i := range matrix {
		matrix[i] = uint64(defaultRow)
	}
	if c.slidingWindow != nil { // In other words, we are in window mode, not sparse mode.
		for i, v := range c.slidingWindow { // set the window bits, trusting the sketch's current offset.
			matrix[i] |= uint64(v) << offset
		}
	}
	table := c.pairTable
	if table == nil {
		return nil, fmt.Errorf("table == nil")
	}
	slots := table.slotsArr
	numSlots := 1 << table.lgSizeInts
	for i := 0; i < numSlots; i++ {
		rowCol := slots[i]
		if rowCol != -1 {
			col := rowCol & 63
			row := rowCol >> 6
			// Flip the specified matrix bit from its default value.
			// In the "early" zone the bit changes from 1 to 0.
			// In the "late" zone the bit changes from 0 to 1.
			matrix[row] ^= 1 << col
		}
	}
	return matrix, nil
}

// ToCompactSlice returns this sketch as a compressed byte slice.
func (c *CpcSketch) ToCompactSlice() ([]byte, error) {
	// Create the compressed state from the sketch.
	compressedState, err := NewCpcCompressedStateFromSketch(c)
	if err != nil {
		return nil, err
	}
	return compressedState.exportToMemory()
}

func (c *CpcSketch) getFamily() int {
	return internal.FamilyEnum.CPC.Id
}

// GetLgK returns the log-base-2 of K.
func (c *CpcSketch) GetLgK() int {
	return c.lgK
}

// isEmpty returns true if no coupons have been collected.
func (c *CpcSketch) isEmpty() bool {
	return c.numCoupons == 0
}

// validate recomputes the coupon count from the bit matrix and returns true if it matches the sketch's numCoupons.
func (c *CpcSketch) validate() (bool, error) {
	bitMatrix, err := c.bitMatrixOfSketch()
	if err != nil {
		return false, err
	}
	matrixCoupons := countBitsSetInMatrix(bitMatrix)
	return matrixCoupons == c.numCoupons, nil
}

// Copy creates and returns a deep copy of the CpcSketch.
func (c *CpcSketch) Copy() (*CpcSketch, error) {
	// Create a new sketch with the same lgK and seed.
	copySketch, err := NewCpcSketch(c.lgK, c.seed)
	if err != nil {
		// This should never happen if the current sketch is valid.
		return nil, err
	}
	// copy basic fields.
	copySketch.numCoupons = c.numCoupons
	copySketch.mergeFlag = c.mergeFlag
	copySketch.fiCol = c.fiCol
	copySketch.windowOffset = c.windowOffset

	// Clone the slidingWindow slice if present.
	if c.slidingWindow != nil {
		copySketch.slidingWindow = make([]byte, len(c.slidingWindow))
		copy(copySketch.slidingWindow, c.slidingWindow)
	} else {
		copySketch.slidingWindow = nil
	}

	// Copy the pair table if present.
	if c.pairTable != nil {
		copySketch.pairTable, err = c.pairTable.copy()
		if err != nil {
			copySketch.pairTable = nil
		}
	} else {
		copySketch.pairTable = nil
	}

	// copy floating-point accumulators.
	copySketch.kxp = c.kxp
	copySketch.hipEstAccum = c.hipEstAccum

	/*
		Added the copy of the scratch buffer to ensure that every field, even temporary ones, in the struct is duplicated,
		so that the copy is entirely independent of the original. Since the scratch buffer is part of the struct, we copy it too.
	*/
	copy(copySketch.scratch[:], c.scratch[:])

	return copySketch, nil
}

func (c *CpcSketch) String() string {
	mem, _ := c.ToCompactSlice()
	str, _ := CpcSketchToString(mem, false)
	return str
}

// getMaxSerializedBytes returns the estimated maximum serialized size of a sketch
// given lgK.
func getMaxSerializedBytes(lgK int) (int, error) {
	// Verify that lgK is within valid bounds.
	if err := checkLgK(lgK); err != nil {
		return 0, err
	}

	// Use the empirical array if lgK is <= empiricalSizeMaxLgK.
	if lgK <= empiricalSizeMaxLgK {
		return empiricalMaxBytes[lgK-minLgK] + maxPreambleSizeBytes, nil
	}
	// Otherwise, compute based on k = 1 << lgK.
	k := 1 << lgK
	return int(empiricalMaxSizeFactor*float64(k)) + maxPreambleSizeBytes, nil
}
