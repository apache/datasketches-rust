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
	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
	"testing"
)

// checkFirst8 verifies that the first eight bytes of the preamble in mem match
// the expected values for the given format, lgK, and fiCol.
func checkFirst8(t *testing.T, mem []byte, format CpcFormat, lgK, fiCol int) {
	t.Helper()
	if got := getFormat(mem); got != format {
		t.Errorf("getFormat: got %v, expected %v", got, format)
	}
	if got := getPreInts(mem); got != getDefinedPreInts(format) {
		t.Errorf("getPreInts: got %d, expected %d", got, getDefinedPreInts(format))
	}
	if got := getSerVer(mem); got != serVer {
		t.Errorf("getSerVer: got %d, expected %d", got, serVer)
	}
	if got := getFamilyId(mem); got != internal.FamilyEnum.CPC.Id {
		t.Errorf("getFamily: got %d, expected %d", got, internal.FamilyEnum.CPC.Id)
	}
	if got := getLgK(mem); got != lgK {
		t.Errorf("GetLgK: got %d, expected %d", got, lgK)
	}
	if got := getFiCol(mem); got != fiCol {
		t.Errorf("getFiCol: got %d, expected %d", got, fiCol)
	}
	expectedFlags := (int(format) << 2) | compressedFlagMask
	if got := getFlags(mem); got != expectedFlags {
		t.Errorf("getFlags: got %d, expected %d", got, expectedFlags)
	}
	defaultSeedHash, err := internal.ComputeSeedHash(int64(internal.DEFAULT_UPDATE_SEED))
	assert.NoError(t, err)
	if got := getSeedHash(mem); got != defaultSeedHash {
		t.Errorf("getSeedHash: got %d, expected %d", got, defaultSeedHash)
	}
}

func TestCheckFirst8(t *testing.T) {
	lgK := 12
	fiCol := 0
	// Allocate minimal memory for the low preamble
	mem := make([]byte, 8)
	defaultSeedHash, err := internal.ComputeSeedHash(int64(internal.DEFAULT_UPDATE_SEED))
	assert.NoError(t, err)
	if err := putEmptyMerged(mem, lgK, defaultSeedHash); err != nil {
		t.Fatalf("putEmptyMerged error: %v", err)
	}
	t.Logf("Memory image: %v", mem)
	checkFirst8(t, mem, CpcFormatEmptyMerged, lgK, fiCol)
}

func TestCheckNormalPutMemory(t *testing.T) {
	lgK := 12
	kxp := float64(lgK)
	hipAccum := 1005.0
	fiCol := 1
	csvStream := []int{1, 2, 3}
	numCoupons := len(csvStream)
	csvLength := len(csvStream)

	defaultSeedHash, err := internal.ComputeSeedHash(int64(internal.DEFAULT_UPDATE_SEED))
	assert.NoError(t, err)

	cwStream := []int{4, 5, 6}
	cwLength := len(cwStream)
	numSv := cwLength

	maxInts := 10 + csvLength + cwLength
	mem := make([]byte, 4*maxInts)

	// 1) EMPTY_MERGED
	format := CpcFormatEmptyMerged
	err = putEmptyMerged(mem, lgK, defaultSeedHash)
	assert.NoError(t, err, "putEmptyMerged failed")

	str, err := CpcSketchToString(mem, true)
	assert.NoError(t, err)
	t.Logf("EMPTY_MERGED (verbose): %s", str)
	checkFirst8(t, mem, CpcFormatEmptyMerged, lgK, 0)
	assert.False(t, hasHip(mem), "expected hasHip=false for empty merged")

	// 2) SPARSE_HYBRID_MERGED
	format = CpcFormatSparseHybridMerged
	err = putSparseHybridMerged(mem, lgK, numCoupons, csvLength, defaultSeedHash, csvStream)
	assert.NoError(t, err, "putSparseHybridMerged failed")

	str, err = CpcSketchToString(mem, true)
	assert.NoError(t, err)
	t.Logf("SPARSE_HYBRID_MERGED (verbose): %s", str)
	str, err = CpcSketchToString(mem, false)
	assert.NoError(t, err)
	t.Logf("SPARSE_HYBRID_MERGED (brief): %s", str)
	checkFirst8(t, mem, format, lgK, 0)
	assert.Equal(t, numCoupons, int(getNumCoupons(mem)))
	assert.Equal(t, csvLength, getSvLengthInts(mem))

	// 3) SPARSE_HYBRID_HIP
	format = CpcFormatSparseHybridHip
	err = putSparseHybridHip(mem, lgK, numCoupons, csvLength, kxp, hipAccum, defaultSeedHash, csvStream)
	assert.NoError(t, err, "putSparseHybridHip failed")

	str, err = CpcSketchToString(mem, true)
	assert.NoError(t, err)
	t.Logf("SPARSE_HYBRID_HIP (verbose): %s", str)
	str, err = CpcSketchToString(mem, false)
	assert.NoError(t, err)
	t.Logf("SPARSE_HYBRID_HIP (brief): %s", str)
	checkFirst8(t, mem, format, lgK, 0)
	assert.Equal(t, numCoupons, int(getNumCoupons(mem)))
	assert.Equal(t, csvLength, getSvLengthInts(mem))
	assert.Equal(t, kxp, getKxP(mem))
	assert.Equal(t, hipAccum, getHipAccum(mem))
	assert.True(t, hasHip(mem), "expected hasHip=true for sparse hybrid HIP")

	// 4) PINNED_SLIDING_MERGED_NOSV
	format = CpcFormatPinnedSlidingMergedNosv
	err = putPinnedSlidingMergedNoSv(mem, lgK, fiCol, numCoupons, cwLength, defaultSeedHash, cwStream)
	assert.NoError(t, err, "putPinnedSlidingMergedNoSv failed")

	str, err = CpcSketchToString(mem, true)
	assert.NoError(t, err)
	t.Logf("PINNED_SLIDING_MERGED_NOSV (verbose): %s", str)
	str, err = CpcSketchToString(mem, false)
	assert.NoError(t, err)
	t.Logf("PINNED_SLIDING_MERGED_NOSV (brief): %s", str)
	checkFirst8(t, mem, format, lgK, fiCol)
	assert.Equal(t, numCoupons, int(getNumCoupons(mem)))
	assert.Equal(t, cwLength, getWLengthInts(mem))

	// 5) PINNED_SLIDING_HIP_NOSV
	format = CpcFormatPinnedSlidingHipNosv
	err = putPinnedSlidingHipNoSv(mem, lgK, fiCol, numCoupons, cwLength, kxp, hipAccum, defaultSeedHash, cwStream)
	assert.NoError(t, err, "putPinnedSlidingHipNoSv failed")

	str, err = CpcSketchToString(mem, true)
	assert.NoError(t, err)
	t.Logf("PINNED_SLIDING_HIP_NOSV (verbose): %s", str)
	str, err = CpcSketchToString(mem, false)
	assert.NoError(t, err)
	t.Logf("PINNED_SLIDING_HIP_NOSV (brief): %s", str)
	checkFirst8(t, mem, format, lgK, fiCol)
	assert.Equal(t, numCoupons, int(getNumCoupons(mem)))
	assert.Equal(t, cwLength, getWLengthInts(mem))
	assert.Equal(t, kxp, getKxP(mem))
	assert.Equal(t, hipAccum, getHipAccum(mem))

	// 6) PINNED_SLIDING_MERGED
	format = CpcFormatPinnedSlidingMerged
	err = putPinnedSlidingMerged(mem, lgK, fiCol, numCoupons, numSv, csvLength, cwLength, defaultSeedHash, csvStream, cwStream)
	assert.NoError(t, err, "putPinnedSlidingMerged failed")

	str, err = CpcSketchToString(mem, true)
	assert.NoError(t, err)
	t.Logf("PINNED_SLIDING_MERGED (verbose): %s", str)
	str, err = CpcSketchToString(mem, false)
	assert.NoError(t, err)
	t.Logf("PINNED_SLIDING_MERGED (brief): %s", str)
	checkFirst8(t, mem, format, lgK, fiCol)
	assert.Equal(t, numCoupons, int(getNumCoupons(mem)))
	assert.Equal(t, numSv, int(getNumSV(mem)))
	assert.Equal(t, csvLength, getSvLengthInts(mem))
	assert.Equal(t, cwLength, getWLengthInts(mem))

	// 7) PINNED_SLIDING_HIP
	format = CpcFormatPinnedSlidingHip
	err = putPinnedSlidingHip(mem, lgK, fiCol, numCoupons, numSv, kxp, hipAccum, csvLength, cwLength, defaultSeedHash, csvStream, cwStream)
	assert.NoError(t, err, "putPinnedSlidingHip failed")

	str, err = CpcSketchToString(mem, true)
	assert.NoError(t, err)
	t.Logf("PINNED_SLIDING_HIP (verbose): %s", str)
	str, err = CpcSketchToString(mem, false)
	assert.NoError(t, err)
	t.Logf("PINNED_SLIDING_HIP (brief): %s", str)
	checkFirst8(t, mem, format, lgK, fiCol)
	assert.Equal(t, numCoupons, int(getNumCoupons(mem)))
	assert.Equal(t, numSv, int(getNumSV(mem)))
	assert.Equal(t, csvLength, getSvLengthInts(mem))
	assert.Equal(t, cwLength, getWLengthInts(mem))
	assert.Equal(t, kxp, getKxP(mem))
	assert.Equal(t, hipAccum, getHipAccum(mem))
}

func TestCheckEmptyMemory(t *testing.T) {
	mem := make([]byte, 4*10)
	// Set Family (byte at offset 2) to 16 (legal for CPC)
	mem[loFieldFamily] = 16
	// Set Flags (byte at offset 5) to (1 << 2), selecting NONE.
	mem[loFieldFlags] = byte(1 << 2)
	// Log output of CpcSketchToString in brief mode.
	str, err := CpcSketchToString(mem, false)
	assert.NoError(t, err)
	t.Logf("Empty Memory: %s", str)
}

func TestCheckFieldError(t *testing.T) {
	err := fieldError(CpcFormatEmptyMerged, hiFieldNumCoupons)
	assert.Error(t, err, "Expected fieldError to return an error")
}

func TestCheckCapacity(t *testing.T) {
	err := checkCapacity(100, 101)
	assert.Error(t, err, "Expected checkCapacity to return an error for insufficient capacity")
}

func TestCheckWindowOffset(t *testing.T) {
	offset := determineCorrectOffset(4, 54)
	assert.Equal(t, 1, offset, "Expected window offset to be 1")
}

func TestCheckFormatEnum(t *testing.T) {
	assert.Equal(t, CpcFormatEmptyMerged, CpcFormat(0))
	assert.Equal(t, CpcFormatEmptyHip, CpcFormat(1))
	assert.Equal(t, CpcFormatSparseHybridMerged, CpcFormat(2))
	assert.Equal(t, CpcFormatSparseHybridHip, CpcFormat(3))
	assert.Equal(t, CpcFormatPinnedSlidingMergedNosv, CpcFormat(4))
	assert.Equal(t, CpcFormatPinnedSlidingHipNosv, CpcFormat(5))
	assert.Equal(t, CpcFormatPinnedSlidingMerged, CpcFormat(6))
	assert.Equal(t, CpcFormatPinnedSlidingHip, CpcFormat(7))
}

func TestCheckFlavorEnum(t *testing.T) {
	assert.Equal(t, CpcFlavorEmpty, CpcFlavor(0))
	assert.Equal(t, CpcFlavorSparse, CpcFlavor(1))
	assert.Equal(t, CpcFlavorHybrid, CpcFlavor(2))
	assert.Equal(t, CpcFlavorPinned, CpcFlavor(3))
	assert.Equal(t, CpcFlavorSliding, CpcFlavor(4))
}

func TestCheckStreamErrors(t *testing.T) {
	// Allocate 10 integers (40 bytes)
	mem := make([]byte, 4*10)
	// For testing, call putEmptyMerged with lgK = 12 and default seed hash.
	defaultSeedHash, err := internal.ComputeSeedHash(int64(internal.DEFAULT_UPDATE_SEED))
	assert.NoError(t, err)
	// Use putEmptyMerged to write an EMPTY_MERGED preamble.
	err = putEmptyMerged(mem, 12, defaultSeedHash)
	assert.NoError(t, err)

	// Now, calling getSvStreamOffset should return an error because CSV stream is not valid.
	_, err = getSvStreamOffset(mem)
	assert.Error(t, err, "Expected getSvStreamOffset to return an error when CSV stream offset is undefined")

	// Set byte 5 (flags) to (7 << 2)
	mem[loFieldFlags] = byte(7 << 2)
	// Again, getSvStreamOffset should return an error.
	_, err = getSvStreamOffset(mem)
	assert.Error(t, err, "Expected getSvStreamOffset to return an error when flags are invalid for CSV stream")

	// Now set byte 5 to 0.
	mem[loFieldFlags] = 0
	// Now getWStreamOffset should return an error.
	_, err = getWStreamOffset(mem)
	assert.Error(t, err, "Expected getWStreamOffset to return an error when window stream offset is undefined")

	// Set byte 5 to (7 << 2) again.
	mem[loFieldFlags] = byte(7 << 2)
	// getWStreamOffset should return an error.
	_, err = getWStreamOffset(mem)
	assert.Error(t, err, "Expected getWStreamOffset to return an error when flags are invalid for window stream")
}

func TestCheckStreamErrors2(t *testing.T) {
	mem := make([]byte, 4*10)
	svStream := []int{1}
	wStream := []int{2}
	// We expect putPinnedSlidingMerged to return an error because the state is invalid.
	err := putPinnedSlidingMerged(mem, 4, 0, 1, 1, 1, 0, 0, svStream, wStream)
	assert.Error(t, err, "Expected putPinnedSlidingMerged to return an error due to state error")
	// Then, ensure that isCompressed returns true.
	assert.True(t, isCompressed(mem), "Expected memory to be marked as compressed")
}

func TestCheckHiFieldError(t *testing.T) {
	_, err := getHiFieldOffset(CpcFormatEmptyMerged, hiFieldNumCoupons)
	assert.Error(t, err, "Expected getHiFieldOffset to return an error")
}
