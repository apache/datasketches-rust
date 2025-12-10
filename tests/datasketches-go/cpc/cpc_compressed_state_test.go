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
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// TestWriteReadUnary verifies that writeUnary and readUnary are inverses.
func TestWriteReadUnary(t *testing.T) {
	compressedWords := make([]int, 256)
	ptrArr := make([]int64, 3)
	nextWordIndex := 0
	var bitBuf int64 = 0
	bufBits := 0

	// Write unary codes for values 0 to 99.
	for i := 0; i < 100; i++ {
		ptrArr[NextWordIdx] = int64(nextWordIndex)
		ptrArr[BitBuf] = bitBuf
		ptrArr[BufBits] = int64(bufBits)

		if nextWordIndex != int(ptrArr[NextWordIdx]) {
			t.Errorf("Before writeUnary: nextWordIndex %d != ptrArr[NextWordIdx] %d", nextWordIndex, ptrArr[NextWordIdx])
		}

		writeUnary(compressedWords, ptrArr, i)

		nextWordIndex = int(ptrArr[NextWordIdx])
		bitBuf = ptrArr[BitBuf]
		bufBits = int(ptrArr[BufBits])
		if nextWordIndex != int(ptrArr[NextWordIdx]) {
			t.Errorf("After writeUnary: nextWordIndex %d != ptrArr[NextWordIdx] %d", nextWordIndex, ptrArr[NextWordIdx])
		}
	}

	// Pad the bitstream so that the decompressor's 12-bit peek can't overrun.
	padding := 7
	bufBits += padding
	if bufBits >= 32 {
		compressedWords[nextWordIndex] = int(bitBuf & 0xFFFFFFFF)
		nextWordIndex++
		bitBuf >>= 32
		bufBits -= 32
	}
	if bufBits > 0 {
		if bufBits >= 32 {
			t.Errorf("bufBits should be less than 32, got %d", bufBits)
		}
		compressedWords[nextWordIndex] = int(bitBuf & 0xFFFFFFFF)
		nextWordIndex++
	}
	numWordsUsed := nextWordIndex
	t.Logf("Words used: %d", numWordsUsed)

	// Now read back the unary values.
	nextWordIndex = 0
	bitBuf = 0
	bufBits = 0
	for i := 0; i < 100; i++ {
		ptrArr[NextWordIdx] = int64(nextWordIndex)
		ptrArr[BitBuf] = bitBuf
		ptrArr[BufBits] = int64(bufBits)
		if nextWordIndex != int(ptrArr[NextWordIdx]) {
			t.Errorf("Before readUnary: nextWordIndex %d != ptrArr[NextWordIdx] %d", nextWordIndex, ptrArr[NextWordIdx])
		}
		result := readUnary(compressedWords, ptrArr)
		t.Logf("Result: %d, expected: %d", result, i)
		if result != int64(i) {
			t.Errorf("Mismatch: got %d, expected %d", result, i)
		}
		nextWordIndex = int(ptrArr[NextWordIdx])
		bitBuf = ptrArr[BitBuf]
		bufBits = int(ptrArr[BufBits])
		if nextWordIndex != int(ptrArr[NextWordIdx]) {
			t.Errorf("After readUnary: nextWordIndex %d != ptrArr[NextWordIdx] %d", nextWordIndex, ptrArr[NextWordIdx])
		}
	}
	if nextWordIndex > numWordsUsed {
		t.Errorf("nextWordIndex (%d) exceeds numWordsUsed (%d)", nextWordIndex, numWordsUsed)
	}
}

// TestWriteReadBytes tests compressing and uncompressing a 256-byte array using different encoding tables.
func TestWriteReadBytes(t *testing.T) {
	compressedWords := make([]int, 128)
	byteArray := make([]byte, 256)
	byteArray2 := make([]byte, 256)
	for i := 0; i < 256; i++ {
		byteArray[i] = byte(i)
	}
	// Loop over 22 different encoding tables.
	for j := 0; j < 22; j++ {
		numWordsWritten := lowLevelCompressBytes(byteArray, 256, encodingTablesForHighEntropyByte[j], compressedWords)
		err := lowLevelUncompressBytes(byteArray2, 256, decodingTablesForHighEntropyByte[j], compressedWords, numWordsWritten)
		if err != nil {
			t.Errorf("Error in lowLevelUncompressBytes for j=%d: %v", j, err)
		}
		t.Logf("Words used: %d", numWordsWritten)
		if !reflect.DeepEqual(byteArray2, byteArray) {
			t.Errorf("Mismatch in byte arrays for j=%d: got %v, expected %v", j, byteArray2, byteArray)
		}
	}
}

// TestWriteReadBytes65 tests compressing and uncompressing a 65-byte array using length-limited tables.
func TestWriteReadBytes65(t *testing.T) {
	size := 65
	compressedWords := make([]int, 128)
	byteArray := make([]byte, size)
	byteArray2 := make([]byte, size)
	for i := 0; i < size; i++ {
		byteArray[i] = byte(i)
	}
	numWordsWritten := lowLevelCompressBytes(byteArray, size, lengthLimitedUnaryEncodingTable65, compressedWords)
	err := lowLevelUncompressBytes(byteArray2, size, lengthLimitedUnaryDecodingTable65, compressedWords, numWordsWritten)
	if err != nil {
		t.Errorf("Error in lowLevelUncompressBytes: %v", err)
	}
	t.Logf("Words used: %d", numWordsWritten)
	if !reflect.DeepEqual(byteArray2, byteArray) {
		t.Errorf("Mismatch in byte arrays: got %v, expected %v", byteArray2, byteArray)
	}
}

// TestWriteReadPairs tests compressing and uncompressing an array of pair values.
func TestWriteReadPairs(t *testing.T) {
	rgen := rand.New(rand.NewSource(1))
	lgK := 14
	N := 3000
	MaxWords := 4000
	pairArray := make([]int, N)
	pairArray2 := make([]int, N)
	for i := 0; i < N; i++ {
		pairArray[i] = rgen.Intn(1 << (lgK + 6))
	}
	sort.Ints(pairArray)
	prev := -1
	nxt := 0
	for i := 0; i < N; i++ {
		if pairArray[i] != prev {
			prev = pairArray[i]
			pairArray[nxt] = pairArray[i]
			nxt++
		}
	}
	numPairs := nxt
	t.Logf("numCsv = %d", numPairs)

	compressedWords := make([]int, MaxWords)
	// Loop over base bits 0 to 11.
	for bb := 0; bb <= 11; bb++ {
		numWordsWritten := lowLevelCompressPairs(pairArray, numPairs, bb, compressedWords)
		t.Logf("numWordsWritten = %d, bb = %d", numWordsWritten, bb)
		err := lowLevelUncompressPairs(pairArray2, numPairs, bb, compressedWords, numWordsWritten)
		if err != nil {
			t.Errorf("Error in lowLevelUncompressPairs for bb=%d: %v", bb, err)
		}
		for i := 0; i < numPairs; i++ {
			if pairArray[i] != pairArray2[i] {
				t.Errorf("Mismatch at index %d for bb=%d: got %d, expected %d", i, bb, pairArray2[i], pairArray[i])
			}
		}
	}
}

// updateStateUnion compresses the current sketch, exports its compressed state,
// re-imports it, then creates a union (using the official union implementation),
// updates the union with the sketch, and verifies that the union’s result
// has a format that matches the sketch’s format.
func updateStateUnion(t *testing.T, sk *CpcSketch, vIn *uint64, lgK int) {
	// Compress the current sketch.
	skFmt := sk.getFormat()
	cs, err := NewCpcCompressedStateFromSketch(sk)
	if err != nil {
		t.Fatalf("Failed to compress sketch: %v", err)
	}
	if cs.getFormat() != skFmt {
		t.Errorf("Compressed state format %v != sketch format %v", cs.getFormat(), skFmt)
	}
	c := cs.NumCoupons

	// Export to memory and log the state.
	mem, err := cs.exportToMemory()
	if err != nil {
		t.Fatalf("Failed to export to memory: %v", err)
	}
	t.Logf("vIn: %8d   coupons: %8d   Format: %v", *vIn, c, cs.getFormat())

	// Re-import the state.
	cs2, err := importFromMemory(mem)
	if err != nil {
		t.Fatalf("Failed to import from memory: %v", err)
	}
	if cs2.getFormat() != skFmt {
		t.Errorf("Re-imported state format %v != sketch format %v", cs2.getFormat(), skFmt)
	}

	// --- Use the official union implementation ---
	u, err := NewCpcUnionSketchWithDefault(lgK)
	if err != nil {
		t.Fatalf("Failed to create union: %v", err)
	}
	if err = u.Update(sk); err != nil {
		t.Fatalf("Union update failed: %v", err)
	}
	sk2, err := u.GetResult()
	if err != nil {
		t.Fatalf("Union GetResult failed: %v", err)
	}
	skFmt = sk2.getFormat()
	cs, err = NewCpcCompressedStateFromSketch(sk2)
	if err != nil {
		t.Fatalf("Failed to compress union result: %v", err)
	}
	if cs.getFormat() != skFmt {
		t.Errorf("Union compressed state format %v != union sketch format %v", cs.getFormat(), skFmt)
	}
	c = cs.NumCoupons
	mem, err = cs.exportToMemory()
	if err != nil {
		t.Fatalf("Failed to export union state to memory: %v", err)
	}
	cs2, err = importFromMemory(mem)
	if err != nil {
		t.Fatalf("Failed to import union state from memory: %v", err)
	}
	if cs2.getFormat() != skFmt {
		t.Errorf("Imported union state format %v != union sketch format %v", cs2.getFormat(), skFmt)
	}
}

func TestLoadMemory(t *testing.T) {
	lgK := 10
	vIn := uint64(0)
	sk, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	if err != nil {
		t.Fatalf("Failed to create CpcSketch: %v", err)
	}
	k := 1 << lgK

	// EMPTY_MERGED (empty sketch)
	updateStateUnion(t, sk, &vIn, lgK)

	// SPARSE: update with one value.
	vIn++
	if err = sk.UpdateUint64(vIn); err != nil {
		t.Fatalf("UpdateUint64 failed: %v", err)
	}
	updateStateUnion(t, sk, &vIn, lgK)

	// HYBRID: update until (numCoupons << 5) >= (3 * k)
	for (sk.numCoupons << 5) < uint64(3*k) {
		vIn++
		if err = sk.UpdateUint64(vIn); err != nil {
			t.Fatalf("UpdateUint64 failed: %v", err)
		}
	}
	updateStateUnion(t, sk, &vIn, lgK)

	// PINNED: update until (numCoupons << 1) >= k
	for (sk.numCoupons << 1) < uint64(k) {
		vIn++
		if err = sk.UpdateUint64(vIn); err != nil {
			t.Fatalf("UpdateUint64 failed: %v", err)
		}
	}
	updateStateUnion(t, sk, &vIn, lgK)

	// SLIDING: update until (numCoupons << 3) >= (27 * k)
	for (sk.numCoupons << 3) < uint64(27*k) {
		vIn++
		if err = sk.UpdateUint64(vIn); err != nil {
			t.Fatalf("UpdateUint64 failed: %v", err)
		}
	}
	updateStateUnion(t, sk, &vIn, lgK)
}

// TestToString logs string representations of compressed states.
func TestToString(t *testing.T) {
	// Create a sketch with lgK = 10.
	sk, err := NewCpcSketch(10, internal.DEFAULT_UPDATE_SEED)
	if err != nil {
		t.Fatalf("Failed to create sketch: %v", err)
	}
	cs, err := NewCpcCompressedStateFromSketch(sk)
	if err != nil {
		t.Fatalf("Failed to compress empty sketch: %v", err)
	}
	t.Logf("Empty sketch state: %+v", cs)

	// Update with value 0.
	if err = sk.UpdateUint64(0); err != nil {
		t.Fatalf("UpdateUint64 failed: %v", err)
	}
	cs, err = NewCpcCompressedStateFromSketch(sk)
	if err != nil {
		t.Fatalf("Failed to compress sketch after update(0): %v", err)
	}
	t.Logf("After update(0): %+v", cs)

	// Update sketch with values 1 to 599.
	for i := 1; i < 600; i++ {
		if err = sk.UpdateUint64(uint64(i)); err != nil {
			t.Fatalf("UpdateUint64 failed at i=%d: %v", i, err)
		}
	}
	cs, err = NewCpcCompressedStateFromSketch(sk)
	if err != nil {
		t.Fatalf("Failed to compress sketch after 600 updates: %v", err)
	}
	t.Logf("After 600 updates: %+v", cs)
}

// TestIsCompressed corrupts the compressed state and expects an error on import.
func TestIsCompressed(t *testing.T) {
	sk, err := NewCpcSketch(10, internal.DEFAULT_UPDATE_SEED)
	if err != nil {
		t.Fatalf("Failed to create sketch: %v", err)
	}
	// Update once so that sketch is non-empty.
	if err = sk.UpdateUint64(12345); err != nil {
		t.Fatalf("UpdateUint64 failed: %v", err)
	}
	cs, err := NewCpcCompressedStateFromSketch(sk)
	if err != nil {
		t.Fatalf("Failed to compress sketch: %v", err)
	}
	mem, err := cs.exportToMemory()
	if err != nil {
		t.Fatalf("exportToMemory failed: %v", err)
	}
	// Corrupt a byte (for example, clear bit 1 at index 5).
	mem[5] = mem[5] & 0xFD

	// Try to import; we expect an error.
	_, err = importFromMemory(mem)
	if err == nil {
		t.Errorf("Expected error when importing corrupted compressed state, got nil")
	}
}

// Additional tests for pair compression and consistency remain unchanged.
func TestWriteReadPairsExtended(t *testing.T) {
	rgen := rand.New(rand.NewSource(1))
	lgK := 14
	N := 3000
	MaxWords := 4000
	pairArray := make([]int, N)
	pairArray2 := make([]int, N)
	for i := 0; i < N; i++ {
		// Generate pair values in the full range (row in [0,1<<lgK), col in [0,64))
		pairArray[i] = rgen.Intn(1 << (lgK + 6))
	}
	// Sort and remove duplicates.
	sort.Ints(pairArray)
	prev := -1
	nxt := 0
	for i := 0; i < N; i++ {
		if pairArray[i] != prev {
			prev = pairArray[i]
			pairArray[nxt] = pairArray[i]
			nxt++
		}
	}
	numPairs := nxt
	t.Logf("Number of unique pairs: %d", numPairs)

	compressedWords := make([]int, MaxWords)
	// Loop over base bits 0 to 11.
	for bb := 0; bb <= 11; bb++ {
		numWordsWritten := lowLevelCompressPairs(pairArray, numPairs, bb, compressedWords)
		t.Logf("Base bits: %d, words written: %d", bb, numWordsWritten)
		err := lowLevelUncompressPairs(pairArray2, numPairs, bb, compressedWords, numWordsWritten)
		if err != nil {
			t.Errorf("Error in lowLevelUncompressPairs for base bits %d: %v", bb, err)
		}
		for i := 0; i < numPairs; i++ {
			if pairArray[i] != pairArray2[i] {
				t.Errorf("Mismatch at index %d for base bits %d: got %d, expected %d", i, bb, pairArray2[i], pairArray[i])
			}
		}
	}
}

// TestCompressedStateConsistency ensures that two sketches updated identically yield identical compressed states.
func TestCompressedStateConsistency(t *testing.T) {
	sk1, err := NewCpcSketch(10, internal.DEFAULT_UPDATE_SEED)
	if err != nil {
		t.Fatalf("Failed to create sketch 1: %v", err)
	}
	sk2, err := NewCpcSketch(10, internal.DEFAULT_UPDATE_SEED)
	if err != nil {
		t.Fatalf("Failed to create sketch 2: %v", err)
	}
	// Update both sketches with the same values.
	for i := 0; i < 1000; i++ {
		if err = sk1.UpdateUint64(uint64(i)); err != nil {
			t.Fatalf("UpdateUint64 failed on sk1 at i=%d: %v", i, err)
		}
		if err = sk2.UpdateUint64(uint64(i)); err != nil {
			t.Fatalf("UpdateUint64 failed on sk2 at i=%d: %v", i, err)
		}
	}
	cs1, err := NewCpcCompressedStateFromSketch(sk1)
	if err != nil {
		t.Fatalf("Failed to compress sk1: %v", err)
	}
	cs2, err := NewCpcCompressedStateFromSketch(sk2)
	if err != nil {
		t.Fatalf("Failed to compress sk2: %v", err)
	}
	mem1, err := cs1.exportToMemory()
	if err != nil {
		t.Fatalf("exportToMemory failed for sk1: %v", err)
	}
	mem2, err := cs2.exportToMemory()
	if err != nil {
		t.Fatalf("exportToMemory failed for sk2: %v", err)
	}
	if !reflect.DeepEqual(mem1, mem2) {
		t.Errorf("Compressed states do not match for identical sketches")
	}
}
