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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewHashtable(t *testing.T) {
	lgCurSize := uint8(4)
	lgNomSize := uint8(4)
	rf := ResizeX1
	p := float32(1.0)
	theta := MaxTheta
	seed := DefaultSeed

	sketch := NewHashtable(lgCurSize, lgNomSize, rf, p, theta, seed, true)

	assert.NotNil(t, sketch)
	assert.True(t, sketch.isEmpty)
	assert.Equal(t, lgCurSize, sketch.lgCurSize)
	assert.Equal(t, lgNomSize, sketch.lgNomSize)
	assert.Equal(t, rf, sketch.rf)
	assert.Equal(t, p, sketch.p)
	assert.Zero(t, sketch.numEntries)
	assert.Equal(t, theta, sketch.theta)
	assert.Equal(t, seed, sketch.seed)
	assert.Equal(t, 1<<lgCurSize, len(sketch.entries))

	// Check all entries are initialized to zero
	for i, entry := range sketch.entries {
		assert.Emptyf(t, entry, "entry at index %d should be zero", i)
	}
}

func TestHashtable_Copy(t *testing.T) {
	original := NewHashtable(4, 4, ResizeX1, 1.0, MaxTheta, DefaultSeed, true)

	// Add some entries
	original.entries[0] = 12345
	original.entries[5] = 67890
	original.numEntries = 2
	original.isEmpty = false

	copied := original.Copy()

	assert.Equal(t, original, copied)
}

func TestHashtable_HashStringAndScreen(t *testing.T) {
	testCases := []struct {
		name       string
		data       string
		theta      uint64
		seed       uint64
		wantErrMsg string
	}{
		{
			name:       "normal string with max theta",
			data:       "hello world",
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "empty string",
			data:       "",
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "string with special characters",
			data:       "test@#$%^&*()",
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "unicode string",
			data:       "가나다라마바사",
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "with low theta (likely filtered)",
			data:       "test",
			theta:      1,
			seed:       DefaultSeed,
			wantErrMsg: "hash exceeds theta",
		},
		{
			name:       "different seed",
			data:       "test",
			theta:      MaxTheta,
			seed:       99999,
			wantErrMsg: "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ht := NewHashtable(4, 4, ResizeX1, 1.0, tc.theta, tc.seed, true)
			hash, err := ht.HashStringAndScreen(tc.data)

			assert.False(t, ht.isEmpty)
			if tc.wantErrMsg != "" {
				assert.ErrorContains(t, err, tc.wantErrMsg)
			} else {
				assert.NotZero(t, hash, "Expected non-zero hash for data: %s", tc.data)
			}
		})
	}
}

func TestHashtable_HashStringAndScreenConsistency(t *testing.T) {
	ht := NewHashtable(4, 4, ResizeX1, 1.0, MaxTheta, DefaultSeed, true)

	hash1, err := ht.HashStringAndScreen("test")
	assert.NoError(t, err)
	hash2, err := ht.HashStringAndScreen("test")
	assert.NoError(t, err)

	assert.Equal(t, hash1, hash2, "Same string should produce same hash")
}

func TestHashtable_HashInt32AndScreen(t *testing.T) {
	testCases := []struct {
		name       string
		data       int32
		theta      uint64
		seed       uint64
		wantErrMsg string
	}{
		{
			name:       "positive integer",
			data:       12345,
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "negative integer",
			data:       -12345,
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "zero",
			data:       0,
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "max int32",
			data:       2147483647,
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "min int32",
			data:       -2147483648,
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "with very low theta (likely filtered)",
			data:       12345,
			theta:      1,
			seed:       DefaultSeed,
			wantErrMsg: "hash exceeds theta",
		},
		{
			name:       "different seed",
			data:       12345,
			theta:      MaxTheta,
			seed:       99999,
			wantErrMsg: "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ht := NewHashtable(4, 4, ResizeX1, 1.0, tc.theta, tc.seed, true)
			hash, err := ht.HashInt32AndScreen(tc.data)

			assert.False(t, ht.isEmpty)

			if tc.wantErrMsg != "" {
				assert.ErrorContains(t, err, tc.wantErrMsg)
			} else {
				assert.NotZero(t, hash, "Expected non-zero hash for data: %d", tc.data)
			}
		})
	}
}

func TestHashtable_HashInt32AndScreenConsistency(t *testing.T) {
	ht := NewHashtable(4, 4, ResizeX1, 1.0, MaxTheta, DefaultSeed, true)
	hash1, err := ht.HashInt32AndScreen(42)
	assert.NoError(t, err)
	hash2, err := ht.HashInt32AndScreen(42)
	assert.NoError(t, err)
	assert.Equal(t, hash1, hash2, "Same int32 should produce same hash")
}

func TestHashtable_HashInt64AndScreen(t *testing.T) {
	testCases := []struct {
		name       string
		data       int64
		theta      uint64
		seed       uint64
		wantErrMsg string
	}{
		{
			name:       "positive integer",
			data:       1234567890,
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "negative integer",
			data:       -1234567890,
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "zero",
			data:       0,
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "max int64",
			data:       9223372036854775807,
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "min int64",
			data:       -9223372036854775808,
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "large positive value",
			data:       9876543210123456,
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "with very low theta (likely filtered)",
			data:       1234567890,
			theta:      1,
			seed:       DefaultSeed,
			wantErrMsg: "hash exceeds theta",
		},
		{
			name:       "different seed",
			data:       1234567890,
			theta:      MaxTheta,
			seed:       55555,
			wantErrMsg: "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ht := NewHashtable(4, 4, ResizeX1, 1.0, tc.theta, tc.seed, true)
			hash, err := ht.HashInt64AndScreen(tc.data)

			assert.False(t, ht.isEmpty)

			if tc.wantErrMsg != "" {
				assert.ErrorContains(t, err, tc.wantErrMsg)
			} else {
				assert.NotZero(t, hash, "Expected non-zero hash for data: %d", tc.data)
			}
		})
	}
}

func TestHashtable_HashInt64AndScreenConsistency(t *testing.T) {
	ht := NewHashtable(4, 4, ResizeX1, 1.0, MaxTheta, DefaultSeed, true)
	hash1, err := ht.HashInt64AndScreen(123456789)
	assert.NoError(t, err)
	hash2, err := ht.HashInt64AndScreen(123456789)
	assert.NoError(t, err)
	assert.Equal(t, hash1, hash2, "Same int64 should produce same hash")
}

func TestHashtable_HashBytesAndScreen(t *testing.T) {
	testCases := []struct {
		name       string
		data       []byte
		theta      uint64
		seed       uint64
		wantErrMsg string
	}{
		{
			name:       "normal byte array",
			data:       []byte{1, 2, 3, 4, 5},
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "empty byte array",
			data:       []byte{},
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "single byte",
			data:       []byte{42},
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "byte array from string",
			data:       []byte("hello world"),
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "byte array with zeros",
			data:       []byte{0, 0, 0, 0},
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "byte array with max values",
			data:       []byte{255, 255, 255, 255},
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "large byte array",
			data:       make([]byte, 1000),
			theta:      MaxTheta,
			seed:       DefaultSeed,
			wantErrMsg: "",
		},
		{
			name:       "with very low theta (likely filtered)",
			data:       []byte{1, 2, 3, 4, 5},
			theta:      100,
			seed:       DefaultSeed,
			wantErrMsg: "hash exceeds theta",
		},
		{
			name:       "different seed",
			data:       []byte{1, 2, 3, 4, 5},
			theta:      MaxTheta,
			seed:       77777,
			wantErrMsg: "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ht := NewHashtable(4, 4, ResizeX1, 1.0, tc.theta, tc.seed, true)
			hash, err := ht.HashBytesAndScreen(tc.data)

			assert.False(t, ht.isEmpty)

			if tc.wantErrMsg != "" {
				assert.ErrorContains(t, err, tc.wantErrMsg)
			} else {
				assert.NotZero(t, hash, "Expected non-zero hash for data: %v", tc.data)
			}
		})
	}
}

func TestHashtable_HashBytesAndScreenConsistency(t *testing.T) {
	ht := NewHashtable(4, 4, ResizeX1, 1.0, MaxTheta, DefaultSeed, true)
	hash1, err := ht.HashBytesAndScreen([]byte{1, 2, 3, 4, 5})
	assert.NoError(t, err)
	hash2, err := ht.HashBytesAndScreen([]byte{1, 2, 3, 4, 5})
	assert.NoError(t, err)
	assert.Equal(t, hash1, hash2, "Same byte array should produce same hash")
}

func TestHashTable_Find(t *testing.T) {
	sketch := NewHashtable(2, 4, ResizeX1, 1.0, MaxTheta, DefaultSeed, true)

	key := uint64(12345)

	// Find an empty table
	index, err := sketch.Find(key)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	sketch.entries[index] = key
	sketch.numEntries++

	// Find the inserted key
	index2, err := sketch.Find(key)
	assert.NoError(t, err)
	assert.Equal(t, index, index2)

	// Table is full
	size := 1 << sketch.lgCurSize
	for i := 0; i < size; i++ {
		sketch.entries[i] = uint64(i + 1000)
	}
	sketch.numEntries = uint32(size)

	index, err = sketch.Find(key)
	assert.ErrorIs(t, err, ErrKeyNotFoundAndNoEmptySlots)
}

func TestHashtable_Insert(t *testing.T) {
	sketch := NewHashtable(4, 4, ResizeX1, 1.0, MaxTheta, DefaultSeed, true)

	key := uint64(12345)
	index, err := sketch.Find(key)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	sketch.Insert(index, key)
	assert.Equal(t, 1, int(sketch.numEntries))

	index2, err := sketch.Find(key)
	assert.NoError(t, err)
	assert.Equal(t, sketch.entries[index2], key)
}

func TestHashtable_InsertWithResize(t *testing.T) {
	lgCurSize := uint8(2)
	lgNomSize := uint8(4)
	sketch := NewHashtable(lgCurSize, lgNomSize, ResizeX2, 1.0, MaxTheta, DefaultSeed, true)

	initialSize := sketch.lgCurSize

	insertedKeys := make([]uint64, 0)
	numToInsert := 10 // Insert enough to trigger resize
	for i := 0; i < numToInsert; i++ {
		key := uint64(i + 1000)
		index, err := sketch.Find(key)
		if err == nil {
			continue
		}

		sketch.Insert(index, key)
		insertedKeys = append(insertedKeys, key)
	}

	assert.Greater(t, sketch.lgCurSize, initialSize, "Table should have been resized")
	assert.Equal(t, numToInsert, len(insertedKeys), "Should have inserted all keys")

	for _, key := range insertedKeys {
		index, err := sketch.Find(key)
		assert.NoError(t, err)
		assert.Equal(t, key, sketch.entries[index], "Key value should match")
	}
}

func TestHashtable_InsertWithRebuild(t *testing.T) {
	lgNomSize := uint8(3)
	lgCurSize := uint8(4)
	sketch := NewHashtable(lgCurSize, lgNomSize, ResizeX2, 1.0, MaxTheta, DefaultSeed, true)

	numToInsert := 100
	insertedKeys := make([]uint64, 0)
	rebuildOccurred := false

	for i := 0; i < numToInsert; i++ {
		key := uint64(i + 1000)
		index, err := sketch.Find(key)
		if err == nil {
			continue
		}
		if index == -1 {
			// Table is full, cannot insert more
			break
		}

		prevTheta := sketch.theta
		sketch.Insert(index, key)
		insertedKeys = append(insertedKeys, key)

		// Rebuild is detected when theta decreases
		if sketch.theta < prevTheta {
			rebuildOccurred = true
			nominalSize := uint32(1 << lgNomSize)
			assert.Equal(t, nominalSize, sketch.numEntries, "After rebuild, entries should equal nominal size")
			assert.Less(t, sketch.theta, MaxTheta, "Theta should decrease after rebuild")
			break
		}
	}

	assert.True(t, rebuildOccurred, "Rebuild should have occurred")

	foundCount := 0
	for _, key := range insertedKeys {
		index, err := sketch.Find(key)
		if err == nil && index >= 0 && sketch.entries[index] == key {
			foundCount++
		}
	}

	assert.Greater(t, foundCount, 0, "Some entries should still be accessible after rebuild")
}

func TestHashtable_Trim(t *testing.T) {
	lgNomSize := uint8(3)
	lgCurSize := uint8(5)
	sketch := NewHashtable(lgCurSize, lgNomSize, ResizeX2, 1.0, MaxTheta, DefaultSeed, true)

	// Insert entries exceeding nominal size
	numToInsert := 20
	for i := 0; i < numToInsert; i++ {
		key := uint64(i + 5000)
		index, err := sketch.Find(key)
		if err == nil {
			continue
		}

		sketch.entries[index] = key
		sketch.numEntries++
	}

	initialNumEntries := sketch.numEntries
	nominalSize := uint32(1 << lgNomSize)

	assert.Greater(t, initialNumEntries, nominalSize, "numEntries should exceed nominal size before Trim")

	sketch.Trim()

	assert.Equal(t, nominalSize, sketch.numEntries, "After Trim, numEntries should equal nominal size")
	assert.Less(t, sketch.theta, MaxTheta, "Theta should decrease after Trim")
}

func TestHashtable_TrimNoOp(t *testing.T) {
	lgNomSize := uint8(4)
	lgCurSize := uint8(4)
	sketch := NewHashtable(lgCurSize, lgNomSize, ResizeX2, 1.0, MaxTheta, DefaultSeed, true)

	// Insert fewer entries than the nominal size
	numToInsert := 5
	for i := 0; i < numToInsert; i++ {
		key := uint64(i + 6000)
		index, err := sketch.Find(key)
		if err == nil {
			continue
		}

		sketch.entries[index] = key
		sketch.numEntries++
	}

	initialNumEntries := sketch.numEntries
	initialTheta := sketch.theta
	nominalSize := uint32(1 << lgNomSize)

	assert.Less(t, initialNumEntries, nominalSize, "numEntries should be less than nominal size")

	sketch.Trim()

	assert.Equal(t, initialNumEntries, sketch.numEntries, "numEntries should not change when less than nominal size")
	assert.Equal(t, initialTheta, sketch.theta, "Theta should not change when entries <= nominal size")
}

func TestHashtable_Reset(t *testing.T) {
	sketch := NewHashtable(4, 4, ResizeX1, 0.5, MaxTheta, DefaultSeed, false)

	sketch.entries[0] = 100
	sketch.entries[5] = 200
	sketch.numEntries = 2
	sketch.isEmpty = false

	sketch.Reset()

	assert.True(t, sketch.isEmpty)
	assert.Zero(t, sketch.numEntries)
	// Verify all entries are zero
	for i, entry := range sketch.entries {
		assert.Zero(t, entry, "entry at index %d should be zero after reset", i)
	}

	expectedTheta := startingThetaFromP(sketch.p)
	assert.Equal(t, expectedTheta, sketch.theta, "theta should be %d after reset", expectedTheta)
}
