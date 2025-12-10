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
	"math"
	"testing"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCPCCheckUpdatesEstimate(t *testing.T) {
	sk, err := NewCpcSketch(10, 0)
	assert.NoError(t, err)
	assert.Equal(t, sk.getFormat(), CpcFormatEmptyHip)
	err = sk.UpdateUint64(1)
	assert.NoError(t, err)
	err = sk.UpdateFloat64(2.0)
	assert.NoError(t, err)
	err = sk.UpdateString("3")
	assert.NoError(t, err)
	bytes := []byte{4, 4}
	err = sk.UpdateByteSlice(bytes)
	assert.NoError(t, err)
	bytes2 := []byte{4}
	err = sk.UpdateByteSlice(bytes2)
	assert.NoError(t, err)
	err = sk.UpdateByteSlice([]byte{5})
	assert.NoError(t, err)
	err = sk.UpdateInt32Slice([]int32{6})
	assert.NoError(t, err)
	err = sk.UpdateInt64Slice([]int64{7})
	assert.NoError(t, err)
	est := sk.GetEstimate()
	lb := sk.GetLowerBound(2)
	ub := sk.GetUpperBound(2)
	assert.True(t, lb >= 0)
	assert.True(t, lb <= est)
	assert.True(t, est <= ub)
	assert.Equal(t, sk.getFlavor(), CpcFlavorSparse)
	assert.Equal(t, sk.getFormat(), CpcFormatSparseHybridHip)
}

func TestCPCCheckEstimatesWithMerge(t *testing.T) {
	lgk := 4
	sk1, err := NewCpcSketch(lgk, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	sk2, err := NewCpcSketch(lgk, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	n := 1 << lgk
	for i := 0; i < n; i++ {
		err = sk1.UpdateUint64(uint64(i))
		assert.NoError(t, err)
		err = sk2.UpdateUint64(uint64(i + n))
		assert.NoError(t, err)
	}
	union, err := NewCpcUnionSketchWithDefault(lgk)
	assert.NoError(t, err)
	err = union.Update(sk1)
	assert.NoError(t, err)
	err = union.Update(sk2)
	assert.NoError(t, err)
	result, err := union.GetResult()
	assert.NoError(t, err)
	est := result.GetEstimate()
	lb := result.GetLowerBound(2)
	ub := result.GetUpperBound(2)
	assert.True(t, lb >= 0)
	assert.True(t, lb <= est)
	assert.True(t, est <= ub)
}

func TestCPCCheckCornerCaseUpdates(t *testing.T) {
	lgK := 4
	sk, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	err = sk.UpdateFloat64(0.0)
	assert.NoError(t, err)
	err = sk.UpdateFloat64(-0.0)
	assert.NoError(t, err)
	assert.Equal(t, sk.GetEstimate(), float64(1))
	err = sk.UpdateString("")
	assert.NoError(t, err)
	assert.Equal(t, sk.GetEstimate(), float64(1))

	err = sk.UpdateByteSlice(nil)
	assert.NoError(t, err)
	assert.Equal(t, sk.GetEstimate(), float64(1))
	emptySlice := make([]byte, 0)
	err = sk.UpdateByteSlice(emptySlice)
	assert.NoError(t, err)
	assert.Equal(t, sk.GetEstimate(), float64(1))

	err = sk.UpdateInt32Slice(nil)
	assert.NoError(t, err)
	assert.Equal(t, sk.GetEstimate(), float64(1))
	emptyInt32Slice := make([]int32, 0)
	err = sk.UpdateInt32Slice(emptyInt32Slice)
	assert.NoError(t, err)

	err = sk.UpdateInt64Slice(nil)
	assert.NoError(t, err)
	assert.Equal(t, sk.GetEstimate(), float64(1))
	emptyInt64Slice := make([]int64, 0)
	err = sk.UpdateInt64Slice(emptyInt64Slice)
	assert.NoError(t, err)
}

func TestCPCCheckCornerHashUpdates(t *testing.T) {
	sk, err := NewCpcSketch(26, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	hash0 := ^uint64(0)
	hash1 := uint64(0)
	err = sk.hashUpdate(hash0, hash1)
	assert.NoError(t, err)
	assert.NotNil(t, sk.pairTable)
}

// TestCPCCheckCopyWithWindow tests the copy() method and then refreshes KXP.
func TestCPCCheckCopyWithWindow(t *testing.T) {
	lgK := 4
	sk, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	sk2, err := sk.Copy()
	assert.NoError(t, err)
	n := 1 << lgK
	for i := 0; i < n; i++ {
		err = sk.UpdateUint64(uint64(i))
		assert.NoError(t, err)
	}
	sk2, err = sk.Copy()
	assert.NoError(t, err)
	bitMatrix, err := sk.bitMatrixOfSketch()
	assert.NoError(t, err)
	sk.refreshKXP(bitMatrix)
	assert.True(t, specialEquals(sk2, sk, false, false))
}

// TestCPCCheckFamily verifies that GetFamily returns the CPC family enum.
func TestCPCCheckFamily(t *testing.T) {
	sk, err := NewCpcSketch(10, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)

	family := sk.getFamily()

	assert.Equal(t, family, internal.FamilyEnum.CPC.Id)
}

func TestCPCCheckLgK(t *testing.T) {
	sk, err := NewCpcSketch(10, 0)
	assert.NoError(t, err)
	assert.Equal(t, sk.lgK, 10)
	_, err = NewCpcSketch(3, 0)
	assert.Error(t, err)
	sk, err = NewCpcSketchWithDefault(defaultLgK)
	assert.NoError(t, err)
	assert.Equal(t, sk.lgK, defaultLgK)
	assert.Equal(t, sk.seed, internal.DEFAULT_UPDATE_SEED)
}

func TestCPCCheckIconHipUBLBLg15(t *testing.T) {
	iconConfidenceUB(15, 1, 2)
	iconConfidenceLB(15, 1, 2)
	hipConfidenceUB(15, 1, 1.0, 2)
	hipConfidenceLB(15, 1, 1.0, 2)
}

// TestCPCCheckRowColUpdate tests that rowColUpdate properly updates the sketch.
func TestCPCCheckRowColUpdate(t *testing.T) {
	lgK := 10
	sk, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	err = sk.rowColUpdate(0)
	assert.NoError(t, err)
	assert.Equal(t, CpcFlavorSparse, sk.getFlavor())
}

// TestCPCCheckGetMaxSize verifies the maximum serialized size calculations.
func TestCPCCheckGetMaxSize(t *testing.T) {
	size4, err := getMaxSerializedBytes(4)
	assert.NoError(t, err)
	size26, err := getMaxSerializedBytes(26)
	assert.NoError(t, err)
	assert.Equal(t, 24+40, size4)

	expectedFloat := 0.6 * float64(1<<26)
	expected := int(expectedFloat) + 40
	assert.Equal(t, expected, size26)
}

func TestAddIntSlice(t *testing.T) {
	sk, err := NewCpcSketch(10, internal.DEFAULT_UPDATE_SEED)
	require.NoError(t, err)

	// Add 100 different slices, each containing a single unique value
	for i := 0; i < 100; i++ {
		slice := []int64{int64(i)}
		require.NoError(t, sk.UpdateInt64Slice(slice))
	}

	estimate := sk.GetEstimate()
	require.InDelta(t, float64(100), estimate, 5.0)

	// Test that identical slices are counted as a single value
	sk2, err := NewCpcSketch(10, internal.DEFAULT_UPDATE_SEED)
	require.NoError(t, err)

	sameSlice := []int64{42, 43, 44}
	for i := 0; i < 10; i++ {
		require.NoError(t, sk2.UpdateInt64Slice(sameSlice))
	}

	estimate = sk2.GetEstimate()
	require.InDelta(t, float64(1), estimate, 0.01)
}

func TestEdgeCases(t *testing.T) {
	sk, err := NewCpcSketch(10, internal.DEFAULT_UPDATE_SEED)
	require.NoError(t, err)

	// Test extreme values
	require.NoError(t, sk.UpdateInt64Slice([]int64{math.MinInt64}))
	require.NoError(t, sk.UpdateInt64Slice([]int64{math.MaxInt64}))
	require.NoError(t, sk.UpdateFloat64(math.SmallestNonzeroFloat64))
	require.NoError(t, sk.UpdateFloat64(math.MaxFloat64))

	// Empty string and empty bytes
	require.NoError(t, sk.UpdateString(""))
	require.NoError(t, sk.UpdateByteSlice([]byte{}))

	// All these represent different values, so estimate should be > 0
	require.Greater(t, sk.GetEstimate(), float64(0))
}

func TestLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	const numValues = 1000000

	// Test with different lgK values
	for _, lgK := range []int{8, 10, 12, 14} {
		t.Run(fmt.Sprintf("lgK=%d", lgK), func(t *testing.T) {
			sk, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
			require.NoError(t, err)

			for i := 0; i < numValues; i++ {
				require.NoError(t, sk.UpdateUint64(uint64(i)))
			}

			estimate := sk.GetEstimate()
			expected := float64(numValues)

			// Calculate theoretical error bounds
			// Use 3 std deviations for approximately 99.7% confidence
			stdDevs := 3.0

			// Get empirical relative error factor for low lgK values (≤14) or use theoretical for larger lgK
			var relErrorFactor float64
			if lgK <= 14 && lgK >= 4 {
				// Index into hipHighSideData array (for 3 std deviations)
				idx := (3 * (lgK - 4)) + 2 // +2 for 3 std deviations (3rd column in the data)
				relErrorFactor = float64(hipHighSideData[idx]) / 10000.0
			} else {
				// Use theoretical formula for lgK > 14
				relErrorFactor = hipErrorConstant
			}

			// Calculate relative error based on the formula: rel = factor / sqrt(2^lgK)
			relError := relErrorFactor / math.Sqrt(float64(uint64(1)<<lgK))

			// Multiply by standard deviations for confidence interval
			maxRelError := stdDevs * relError

			// Calculate allowed absolute error
			maxAbsError := expected * maxRelError

			// Actual difference between estimate and expected
			absDiff := math.Abs(estimate - expected)

			// Check if within error bounds
			if absDiff > maxAbsError {
				t.Errorf("lgK=%d: estimate=%f, expected=%f\n"+
					"absDiff=%f exceeds theoretical max=%f\n"+
					"relError=%f (%.2f%%)",
					lgK, estimate, expected,
					absDiff, maxAbsError,
					maxRelError, maxRelError*100.0)
			} else {
				t.Logf("lgK=%d: estimate within %.2f%% error (theory allows %.2f%%)",
					lgK, (absDiff/expected)*100.0, maxRelError*100.0)
			}
		})
	}
}

func TestSketchWithDifferentLgK(t *testing.T) {
	// Test different lgK values
	for _, lgK := range []int{4, 8, 12, 16} {
		sk, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
		require.NoError(t, err)

		for i := 0; i < 1000; i++ {
			require.NoError(t, sk.UpdateUint64(uint64(i)))
		}

		estimate := sk.GetEstimate()
		expected := float64(1000)

		// Calculate theoretical error bounds
		// Standard deviations for confidence level
		stdDevs := 3.0 // Corresponds to ~99.7% confidence level

		// Get empirical relative error factor for low lgK values (≤14) or use theoretical for larger lgK
		var relErrorFactor float64
		if lgK <= 14 && lgK >= 4 {
			// Index into hipHighSideData array (for 3 std deviations)
			idx := (3 * (lgK - 4)) + 2 // +2 for 3 std deviations
			relErrorFactor = float64(hipHighSideData[idx]) / 10000.0
		} else {
			// Use theoretical formula for lgK > 14
			relErrorFactor = hipErrorConstant // approximately 0.589
		}

		// Calculate relative error based on the formula: rel = factor / sqrt(2^lgK)
		relError := relErrorFactor / math.Sqrt(float64(uint64(1)<<lgK))

		// Multiply by standard deviations for confidence interval
		maxRelError := stdDevs * relError

		// Calculate allowed absolute error
		maxAbsError := expected * maxRelError

		// Actual difference between estimate and expected
		absDiff := math.Abs(estimate - expected)

		// Check if within error bounds and print detailed information
		if absDiff > maxAbsError {
			t.Errorf("For lgK=%d, estimate=%f, expected=%f\n"+
				"absDiff=%f exceeds theoretical max=%f\n"+
				"relError=%f (%.2f%%)",
				lgK, estimate, expected,
				absDiff, maxAbsError,
				maxRelError, maxRelError*100.0)
		} else {
			t.Logf("lgK=%d: estimate within %.2f%% error (theory allows %.2f%%)",
				lgK, (absDiff/expected)*100.0, maxRelError*100.0)
		}
	}
}

// Benchmark adding integers to sketch
func BenchmarkAddInt(b *testing.B) {
	sk, _ := NewCpcSketch(10, internal.DEFAULT_UPDATE_SEED)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = sk.UpdateUint64(uint64(i))
	}
}

// Benchmark merging sketches
func BenchmarkMerge(b *testing.B) {
	sk1, _ := NewCpcSketch(10, internal.DEFAULT_UPDATE_SEED)
	sk2, _ := NewCpcSketch(10, internal.DEFAULT_UPDATE_SEED)

	// Add some values to sketch2
	for i := 0; i < 1000; i++ {
		_ = sk2.UpdateUint64(uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		union, _ := NewCpcUnionSketchWithDefault(10)
		_ = union.Update(sk1)
		_ = union.Update(sk2)
		_, _ = union.GetResult()
	}
}
