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
	"math/bits"

	"github.com/twmb/murmur3"
)

// BitMatrix is a test structure that tracks bit patterns for hashed 64-bit data.
// It is not part of the core CPC algorithm; it's just for certain tests.
type BitMatrix struct {
	lgK               int
	seed              uint64
	numCoupons        uint64
	bitMatrix         []uint64
	numCouponsInvalid bool // only used if merges are allowed (not typical in this usage)
}

// NewBitMatrixWithSeed creates a BitMatrix with the given lgK and custom seed.
func NewBitMatrixWithSeed(lgK int, seed uint64) *BitMatrix {
	size := 1 << lgK
	return &BitMatrix{
		lgK:        lgK,
		seed:       seed,
		numCoupons: 0,
		bitMatrix:  make([]uint64, size),
	}
}

// Reset clears the entire bitMatrix, setting all bits to zero,
// and resets the coupon count to zero.
func (bm *BitMatrix) Reset() {
	for i := range bm.bitMatrix {
		bm.bitMatrix[i] = 0
	}
	bm.numCoupons = 0
	bm.numCouponsInvalid = false
}

// GetNumCoupons returns the number of set bits (coupons) in the matrix.
// If numCouponsInvalid were ever set to true, it would recalculate by scanning
// the bitMatrix. By default, it’s always up to date.
func (bm *BitMatrix) GetNumCoupons() uint64 {
	if bm.numCouponsInvalid {
		bm.numCoupons = CountCoupons(bm.bitMatrix)
		bm.numCouponsInvalid = false
	}
	return bm.numCoupons
}

// GetMatrix returns the underlying array of 64-bit words storing the bits.
func (bm *BitMatrix) GetMatrix() []uint64 {
	return bm.bitMatrix
}

// Update hashes the given 64-bit datum and sets the corresponding bit
// in the matrix. If that bit was previously unset, the coupon count increments.
func (bm *BitMatrix) Update(datum int64) {
	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:], uint64(datum))
	hashLo, hashHi := murmur3.SeedSum128(bm.seed, bm.seed, scratch[:])
	bm.hashUpdate(hashLo, hashHi)
}

// hashUpdate extracts row and column from the 128-bit hash, then sets
// the appropriate bit in bm.bitMatrix[row].
func (bm *BitMatrix) hashUpdate(hash0, hash1 uint64) {
	col := bits.LeadingZeros64(hash1)
	if col > 63 {
		col = 63
	}
	kMask := (uint64(1) << bm.lgK) - 1
	row := int(hash0 & kMask)

	rowCol := (row << 6) | col
	if rowCol == -1 {
		row ^= 1
	}

	oldPattern := bm.bitMatrix[row]
	newPattern := oldPattern | (uint64(1) << col)
	if newPattern != oldPattern {
		bm.numCoupons++
		bm.bitMatrix[row] = newPattern
	}
}

// CountCoupons sums the number of set bits across all 64-bit words in bitMatrix.
func CountCoupons(bitMatrix []uint64) uint64 {
	var count uint64
	for _, word := range bitMatrix {
		count += uint64(bits.OnesCount64(word))
	}
	return count
}
