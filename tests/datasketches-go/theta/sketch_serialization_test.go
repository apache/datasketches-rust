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
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
)

func TestGenerateGoBinariesForCompatibilityTestingThetaSketch(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	t.Run("theta sketch generate", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			sketch, err := NewQuickSelectUpdateSketch()
			assert.NoError(t, err)
			for i := 0; i < n; i++ {
				sketch.UpdateInt64(int64(i))
			}

			assert.True(t, sketch.IsEmpty() == (n == 0))
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)

			compactSketch := sketch.CompactOrdered()
			var buffer bytes.Buffer
			encoder := NewEncoder(&buffer, false)
			err = encoder.Encode(compactSketch)
			assert.NoError(t, err)

			err = os.MkdirAll(internal.GoPath, os.ModePerm)
			assert.NoError(t, err)
			err = os.WriteFile(fmt.Sprintf("%s/theta_n%d_go.sk", internal.GoPath, n), buffer.Bytes(), 0644)
			assert.NoError(t, err)
		}
	})

	t.Run("theta sketch generate compressed", func(t *testing.T) {
		ns := []int{10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			sketch, err := NewQuickSelectUpdateSketch()
			assert.NoError(t, err)
			for i := 0; i < n; i++ {
				sketch.UpdateInt64(int64(i))
			}

			assert.True(t, sketch.IsEmpty() == (n == 0))
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)

			compactSketch := sketch.CompactOrdered()
			var buffer bytes.Buffer
			encoder := NewEncoder(&buffer, true)
			err = encoder.Encode(compactSketch)
			assert.NoError(t, err)

			err = os.MkdirAll(internal.GoPath, os.ModePerm)
			assert.NoError(t, err)
			err = os.WriteFile(fmt.Sprintf("%s/theta_compressed_n%d_go.sk", internal.GoPath, n), buffer.Bytes(), 0644)
			assert.NoError(t, err)
		}
	})

	t.Run("theta sketch generate non-empty no entries", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchP(0.01))
		assert.NoError(t, err)

		sketch.UpdateInt64(int64(1))
		assert.False(t, sketch.IsEmpty())
		assert.Zero(t, sketch.NumRetained())

		compactSketch := sketch.CompactOrdered()
		var buffer bytes.Buffer
		encoder := NewEncoder(&buffer, true)
		err = encoder.Encode(compactSketch)
		assert.NoError(t, err)

		err = os.MkdirAll(internal.GoPath, os.ModePerm)
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/theta_non_empty_no_entries_go.sk", internal.GoPath), buffer.Bytes(), 0644)
		assert.NoError(t, err)
	})
}

func TestJavaCompat(t *testing.T) {
	t.Run("theta sketch", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			b, err := os.ReadFile(fmt.Sprintf("%s/theta_n%d_java.sk", internal.JavaPath, n))
			assert.NoError(t, err)

			sketch, err := Decode(b, DefaultSeed)
			assert.NoError(t, err)

			assert.Equal(t, n == 0, sketch.IsEmpty())
			assert.Equal(t, n > 1000, sketch.IsEstimationMode())
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
			for entry := range sketch.All() {
				assert.Less(t, entry, sketch.Theta64())
			}
			assert.True(t, sketch.IsOrdered())
			entries := make([]uint64, 0)
			for entry := range sketch.All() {
				entries = append(entries, entry)
			}
			assert.True(t, slices.IsSorted(entries))
		}
	})

	t.Run("theta sketch compressed", func(t *testing.T) {
		ns := []int{10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			b, err := os.ReadFile(fmt.Sprintf("%s/theta_compressed_n%d_java.sk", internal.JavaPath, n))
			assert.NoError(t, err)

			sketch, err := Decode(b, DefaultSeed)
			assert.NoError(t, err)

			assert.Equal(t, n > 1000, sketch.IsEstimationMode())
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
			for entry := range sketch.All() {
				assert.Less(t, entry, sketch.Theta64())
			}
			assert.True(t, sketch.IsOrdered())
			entries := make([]uint64, 0)
			for entry := range sketch.All() {
				entries = append(entries, entry)
			}
			assert.True(t, slices.IsSorted(entries))
		}
	})

	t.Run("theta sketch non-empty no entries", func(t *testing.T) {
		b, err := os.ReadFile(fmt.Sprintf("%s/theta_non_empty_no_entries_java.sk", internal.JavaPath))
		assert.NoError(t, err)

		sketch, err := Decode(b, DefaultSeed)
		assert.NoError(t, err)

		assert.False(t, sketch.IsEmpty())
		assert.Zero(t, sketch.NumRetained())
	})

	t.Run("Compact V1 Empty From Java", func(t *testing.T) {
		// Always read sketch from same directory as the test file
		_, currentFileName, _, _ := runtime.Caller(0)
		currentDir := filepath.Dir(currentFileName)
		path := filepath.Join(currentDir, "theta_compact_empty_from_java_v1.sk")
		b, err := os.ReadFile(path)
		assert.NoError(t, err)

		sketch, err := Decode(b, DefaultSeed)
		assert.NoError(t, err)

		assert.True(t, sketch.IsEmpty())
		assert.False(t, sketch.IsEstimationMode())
		assert.Equal(t, uint32(0), sketch.NumRetained())
		assert.Equal(t, 1.0, sketch.Theta())
		assert.Equal(t, 0.0, sketch.Estimate())
		lb, err := sketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err := sketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, ub)
	})

	t.Run("Compact V2 Empty From Java", func(t *testing.T) {
		// Always read sketch from same directory as the test file
		_, currentFileName, _, _ := runtime.Caller(0)
		currentDir := filepath.Dir(currentFileName)
		path := filepath.Join(currentDir, "theta_compact_empty_from_java_v2.sk")
		b, err := os.ReadFile(path)
		assert.NoError(t, err)

		sketch, err := Decode(b, DefaultSeed)
		assert.NoError(t, err)

		assert.True(t, sketch.IsEmpty())
		assert.False(t, sketch.IsEstimationMode())
		assert.Equal(t, uint32(0), sketch.NumRetained())
		assert.Equal(t, 1.0, sketch.Theta())
		assert.Equal(t, 0.0, sketch.Estimate())
		lb, err := sketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err := sketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, ub)
	})

	t.Run("Compact V1 Estimation From Java", func(t *testing.T) {
		// Always read sketch from same directory as the test file
		_, currentFileName, _, _ := runtime.Caller(0)
		currentDir := filepath.Dir(currentFileName)
		path := filepath.Join(currentDir, "theta_compact_estimation_from_java_v1.sk")
		b, err := os.ReadFile(path)
		assert.NoError(t, err)

		decoded, err := Decode(b, DefaultSeed)
		assert.NoError(t, err)

		assert.False(t, decoded.IsEmpty())
		assert.True(t, decoded.IsEstimationMode())
		assert.True(t, decoded.IsOrdered())
		assert.Equal(t, uint32(4342), decoded.NumRetained())
		assert.InDelta(t, 0.531700444213199, decoded.Theta(), 1e-10)
		assert.InDelta(t, 8166.25234614053, decoded.Estimate(), 1e-10)
		lb, err := decoded.LowerBound(2)
		assert.NoError(t, err)
		assert.InDelta(t, 7996.956955317471, lb, 1e-10)
		ub, err := decoded.UpperBound(2)
		assert.NoError(t, err)
		assert.InDelta(t, 8339.090301078124, ub, 1e-10)

		// the same construction process in Java must have produced exactly the same sketch
		updateSketch, err := NewQuickSelectUpdateSketch()
		n := 8192
		for i := 0; i < n; i++ {
			updateSketch.UpdateInt64(int64(i))
		}
		assert.Equal(t, decoded.NumRetained(), updateSketch.NumRetained())
		assert.InDelta(t, decoded.Theta(), updateSketch.Theta(), 1e-10)
		assert.InDelta(t, decoded.Estimate(), updateSketch.Estimate(), 1e-10)

		expectedLB, err := decoded.LowerBound(1)
		assert.NoError(t, err)
		resultLB, err := updateSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.InDelta(t, expectedLB, resultLB, 1e-10)
		expectedUB, err := decoded.UpperBound(1)
		assert.NoError(t, err)
		resultUB, err := updateSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.InDelta(t, expectedUB, resultUB, 1e-10)

		expectedLB, err = decoded.LowerBound(2)
		assert.NoError(t, err)
		resultLB, err = updateSketch.LowerBound(2)
		assert.NoError(t, err)
		assert.InDelta(t, expectedLB, resultLB, 1e-10)
		expectedUB, err = decoded.UpperBound(2)
		assert.NoError(t, err)
		resultUB, err = updateSketch.UpperBound(2)
		assert.NoError(t, err)
		assert.InDelta(t, expectedUB, resultUB, 1e-10)

		expectedLB, err = decoded.LowerBound(3)
		assert.NoError(t, err)
		resultLB, err = updateSketch.LowerBound(3)
		assert.NoError(t, err)
		assert.InDelta(t, expectedLB, resultLB, 1e-10)
		expectedUB, err = decoded.UpperBound(3)
		assert.NoError(t, err)
		resultUB, err = updateSketch.UpperBound(3)
		assert.NoError(t, err)
		assert.InDelta(t, expectedUB, resultUB, 1e-10)

		var expectedEntries []uint64
		for entry := range decoded.All() {
			expectedEntries = append(expectedEntries, entry)
		}
		compacted := updateSketch.CompactOrdered()
		var resultEntries []uint64
		for entry := range compacted.All() {
			resultEntries = append(resultEntries, entry)
		}
		assert.Equal(t, expectedEntries, resultEntries)
	})

	t.Run("Compact V2 Estimation From Java", func(t *testing.T) {
		// Always read sketch from same directory as the test file
		_, currentFileName, _, _ := runtime.Caller(0)
		currentDir := filepath.Dir(currentFileName)
		path := filepath.Join(currentDir, "theta_compact_estimation_from_java_v2.sk")
		b, err := os.ReadFile(path)
		assert.NoError(t, err)

		decoded, err := Decode(b, DefaultSeed)
		assert.NoError(t, err)

		assert.False(t, decoded.IsEmpty())
		assert.True(t, decoded.IsEstimationMode())
		assert.True(t, decoded.IsOrdered())
		assert.Equal(t, uint32(4342), decoded.NumRetained())
		assert.InDelta(t, 0.531700444213199, decoded.Theta(), 1e-10)
		assert.InDelta(t, 8166.25234614053, decoded.Estimate(), 1e-10)
		lb, err := decoded.LowerBound(2)
		assert.NoError(t, err)
		assert.InDelta(t, 7996.956955317471, lb, 1e-10)
		ub, err := decoded.UpperBound(2)
		assert.NoError(t, err)
		assert.InDelta(t, 8339.090301078124, ub, 1e-10)

		// the same construction process in Java must have produced exactly the same sketch
		updateSketch, err := NewQuickSelectUpdateSketch()
		n := 8192
		for i := 0; i < n; i++ {
			updateSketch.UpdateInt64(int64(i))
		}
		assert.Equal(t, decoded.NumRetained(), updateSketch.NumRetained())
		assert.InDelta(t, decoded.Theta(), updateSketch.Theta(), 1e-10)
		assert.InDelta(t, decoded.Estimate(), updateSketch.Estimate(), 1e-10)

		expectedLB, err := decoded.LowerBound(1)
		assert.NoError(t, err)
		resultLB, err := updateSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.InDelta(t, expectedLB, resultLB, 1e-10)
		expectedUB, err := decoded.UpperBound(1)
		assert.NoError(t, err)
		resultUB, err := updateSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.InDelta(t, expectedUB, resultUB, 1e-10)

		expectedLB, err = decoded.LowerBound(2)
		assert.NoError(t, err)
		resultLB, err = updateSketch.LowerBound(2)
		assert.NoError(t, err)
		assert.InDelta(t, expectedLB, resultLB, 1e-10)
		expectedUB, err = decoded.UpperBound(2)
		assert.NoError(t, err)
		resultUB, err = updateSketch.UpperBound(2)
		assert.NoError(t, err)
		assert.InDelta(t, expectedUB, resultUB, 1e-10)

		expectedLB, err = decoded.LowerBound(3)
		assert.NoError(t, err)
		resultLB, err = updateSketch.LowerBound(3)
		assert.NoError(t, err)
		assert.InDelta(t, expectedLB, resultLB, 1e-10)
		expectedUB, err = decoded.UpperBound(3)
		assert.NoError(t, err)
		resultUB, err = updateSketch.UpperBound(3)
		assert.NoError(t, err)
		assert.InDelta(t, expectedUB, resultUB, 1e-10)

		var expectedEntries []uint64
		for entry := range decoded.All() {
			expectedEntries = append(expectedEntries, entry)
		}
		compacted := updateSketch.CompactOrdered()
		var resultEntries []uint64
		for entry := range compacted.All() {
			resultEntries = append(resultEntries, entry)
		}
		assert.Equal(t, expectedEntries, resultEntries)
	})
}

func TestCPPCompat(t *testing.T) {
	t.Run("theta sketch", func(t *testing.T) {
		ns := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			b, err := os.ReadFile(fmt.Sprintf("%s/theta_n%d_cpp.sk", internal.CppPath, n))
			assert.NoError(t, err)

			sketch, err := Decode(b, DefaultSeed)
			assert.NoError(t, err)

			assert.Equal(t, n == 0, sketch.IsEmpty())
			assert.Equal(t, n > 1000, sketch.IsEstimationMode())
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
			for entry := range sketch.All() {
				assert.Less(t, entry, sketch.Theta64())
			}
			assert.True(t, sketch.IsOrdered())
			entries := make([]uint64, 0)
			for entry := range sketch.All() {
				entries = append(entries, entry)
			}
			assert.True(t, slices.IsSorted(entries))
		}
	})

	t.Run("theta sketch compressed", func(t *testing.T) {
		ns := []int{10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range ns {
			b, err := os.ReadFile(fmt.Sprintf("%s/theta_compressed_n%d_cpp.sk", internal.CppPath, n))
			assert.NoError(t, err)

			sketch, err := Decode(b, DefaultSeed)
			assert.NoError(t, err)

			assert.Equal(t, n > 1000, sketch.IsEstimationMode())
			assert.InDelta(t, n, sketch.Estimate(), float64(n)*0.03)
			for entry := range sketch.All() {
				assert.Less(t, entry, sketch.Theta64())
			}
			assert.True(t, sketch.IsOrdered())
			entries := make([]uint64, 0)
			for entry := range sketch.All() {
				entries = append(entries, entry)
			}
			assert.True(t, slices.IsSorted(entries))
		}
	})

	t.Run("theta sketch non-empty no entries", func(t *testing.T) {
		b, err := os.ReadFile(fmt.Sprintf("%s/theta_non_empty_no_entries_cpp.sk", internal.CppPath))
		assert.NoError(t, err)

		sketch, err := Decode(b, DefaultSeed)
		assert.NoError(t, err)

		assert.False(t, sketch.IsEmpty())
		assert.Zero(t, sketch.NumRetained())
	})
}

func TestEncodingAndDecoding(t *testing.T) {
	t.Run("Equivalent to encoding without compression", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < 8192; i++ {
			updateSketch.UpdateInt64(int64(i))
		}

		compactSketch := updateSketch.CompactOrdered()

		var buffer bytes.Buffer
		encoder := NewEncoder(&buffer, false)
		err = encoder.Encode(compactSketch)
		assert.NoError(t, err)
		b := buffer.Bytes()

		decoded, err := Decode(b, DefaultSeed)
		assert.NoError(t, err)

		assert.Equal(t, compactSketch.IsEmpty(), decoded.IsEmpty())
		assert.Equal(t, compactSketch.IsOrdered(), decoded.IsOrdered())
		assert.Equal(t, compactSketch.NumRetained(), decoded.NumRetained())
		assert.Equal(t, compactSketch.Theta64(), decoded.Theta64())
		assert.InDelta(t, compactSketch.Estimate(), decoded.Estimate(), 0.01)

		expectedLB, err := compactSketch.LowerBound(1)
		assert.NoError(t, err)
		resultLB, err := decoded.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, expectedLB, resultLB)

		expectedUB, err := compactSketch.UpperBound(1)
		assert.NoError(t, err)
		resultUB, err := decoded.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, expectedUB, resultUB)

		var expectedEntries []uint64
		for entry := range compactSketch.All() {
			expectedEntries = append(expectedEntries, entry)
		}
		var resultEntries []uint64
		for entry := range decoded.All() {
			resultEntries = append(resultEntries, entry)
		}
		assert.Equal(t, expectedEntries, resultEntries)
	})

	t.Run("Equivalent to encoding with compression", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < 8192; i++ {
			updateSketch.UpdateInt64(int64(i))
		}

		compactSketch := updateSketch.CompactOrdered()

		var buffer bytes.Buffer
		encoder := NewEncoder(&buffer, true)
		err = encoder.Encode(compactSketch)
		assert.NoError(t, err)
		b := buffer.Bytes()
		assert.Equal(t, compactSketch.SerializedSizeBytes(true), len(b))

		decoded, err := Decode(b, DefaultSeed)
		assert.NoError(t, err)

		assert.Equal(t, compactSketch.IsEmpty(), decoded.IsEmpty())
		assert.Equal(t, compactSketch.IsOrdered(), decoded.IsOrdered())
		assert.Equal(t, compactSketch.NumRetained(), decoded.NumRetained())
		assert.Equal(t, compactSketch.Theta64(), decoded.Theta64())
		assert.InDelta(t, compactSketch.Estimate(), decoded.Estimate(), 0.01)

		expectedLB, err := compactSketch.LowerBound(1)
		assert.NoError(t, err)
		resultLB, err := decoded.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, expectedLB, resultLB)

		expectedUB, err := compactSketch.UpperBound(1)
		assert.NoError(t, err)
		resultUB, err := decoded.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, expectedUB, resultUB)

		var expectedEntries []uint64
		for entry := range compactSketch.All() {
			expectedEntries = append(expectedEntries, entry)
		}
		var resultEntries []uint64
		for entry := range decoded.All() {
			resultEntries = append(resultEntries, entry)
		}
		assert.Equal(t, expectedEntries, resultEntries)
	})

	t.Run("Decoder reusability - same decoder multiple sketches", func(t *testing.T) {
		decoder := NewDecoder(DefaultSeed)

		sketch1, _ := NewQuickSelectUpdateSketch()
		for i := 0; i < 10; i++ {
			sketch1.UpdateInt64(int64(i))
		}
		compact1 := sketch1.CompactOrdered()
		data1, _ := compact1.MarshalBinary()

		sketch2, _ := NewQuickSelectUpdateSketch()
		for i := 100; i < 200; i++ {
			sketch2.UpdateInt64(int64(i))
		}
		compact2 := sketch2.CompactOrdered()
		data2, _ := compact2.MarshalBinary()

		decoded1, err1 := decoder.Decode(bytes.NewReader(data1))
		decoded2, err2 := decoder.Decode(bytes.NewReader(data2))

		assert.NoError(t, err1)
		assert.NoError(t, err2)

		assert.NotEqual(t, decoded1.NumRetained(), decoded2.NumRetained())
		assert.Equal(t, compact1.NumRetained(), decoded1.NumRetained())
		assert.Equal(t, compact2.NumRetained(), decoded2.NumRetained())
	})

	t.Run("Encode with custom seed, decode with wrong seed", func(t *testing.T) {
		customSeed := uint64(12345)
		wrongSeed := uint64(67890)

		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(customSeed))
		assert.NoError(t, err)
		for i := 0; i < 100; i++ {
			sketch.UpdateInt64(int64(i))
		}

		compact := sketch.CompactOrdered()
		data, err := compact.MarshalBinary()
		assert.NoError(t, err)

		_, err = Decode(data, wrongSeed)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "seed hash mismatch")
	})

	t.Run("Serialize unordered compact sketch", func(t *testing.T) {
		sketch, _ := NewQuickSelectUpdateSketch()
		for i := 0; i < 100; i++ {
			sketch.UpdateInt64(int64(i))
		}

		unordered := sketch.Compact(false)
		assert.False(t, unordered.IsOrdered())

		data, err := unordered.MarshalBinary()
		assert.NoError(t, err)

		decoded, err := Decode(data, DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, decoded.IsOrdered())
		assert.Equal(t, unordered.NumRetained(), decoded.NumRetained())
	})

	t.Run("Unordered sketch with estimation mode", func(t *testing.T) {
		sketch, _ := NewQuickSelectUpdateSketch()
		for i := 0; i < 10000; i++ {
			sketch.UpdateInt64(int64(i))
		}

		assert.True(t, sketch.IsEstimationMode())

		unordered := sketch.Compact(false)
		data, _ := unordered.MarshalBinary()

		decoded, _ := Decode(data, DefaultSeed)
		assert.False(t, decoded.IsOrdered())
		assert.True(t, decoded.IsEstimationMode())
	})

	t.Run("Decode compressed sketch produces correct results", func(t *testing.T) {
		sketch, _ := NewQuickSelectUpdateSketch()
		for i := 0; i < 8192; i++ {
			sketch.UpdateInt64(int64(i))
		}

		compact := sketch.CompactOrdered()

		var compressedBuf bytes.Buffer
		compressedEncoder := NewEncoder(&compressedBuf, true)
		err := compressedEncoder.Encode(compact)
		assert.NoError(t, err)

		decoded, err := Decode(compressedBuf.Bytes(), DefaultSeed)
		assert.NoError(t, err)

		assert.Equal(t, compact.IsEmpty(), decoded.IsEmpty())
		assert.Equal(t, compact.IsOrdered(), decoded.IsOrdered())
		assert.Equal(t, compact.NumRetained(), decoded.NumRetained())
		assert.Equal(t, compact.Theta64(), decoded.Theta64())
		assert.InDelta(t, compact.Estimate(), decoded.Estimate(), 0.01)

		var compactEntries []uint64
		for entry := range compact.All() {
			compactEntries = append(compactEntries, entry)
		}
		var decodedEntries []uint64
		for entry := range decoded.All() {
			decodedEntries = append(decodedEntries, entry)
		}
		assert.Equal(t, compactEntries, decodedEntries)
	})

	t.Run("Empty sketch compression", func(t *testing.T) {
		sketch, _ := NewQuickSelectUpdateSketch()
		compact := sketch.CompactOrdered()

		var compressedBuf bytes.Buffer
		compressedEncoder := NewEncoder(&compressedBuf, true)
		err := compressedEncoder.Encode(compact)
		assert.NoError(t, err)

		var uncompressedBuf bytes.Buffer
		uncompressedEncoder := NewEncoder(&uncompressedBuf, false)
		err = uncompressedEncoder.Encode(compact)
		assert.NoError(t, err)

		assert.Equal(t, uncompressedBuf.Len(), compressedBuf.Len())
	})

	t.Run("Compressed sketch with custom seed", func(t *testing.T) {
		customSeed := uint64(9999)
		sketch, _ := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(customSeed))
		for i := 0; i < 5000; i++ {
			sketch.UpdateInt64(int64(i))
		}

		compact := sketch.CompactOrdered()

		var compressedBuf bytes.Buffer
		compressedEncoder := NewEncoder(&compressedBuf, true)
		err := compressedEncoder.Encode(compact)
		assert.NoError(t, err)

		decoded, err := Decode(compressedBuf.Bytes(), customSeed)
		assert.NoError(t, err)
		assert.Equal(t, compact.NumRetained(), decoded.NumRetained())
	})
}

type errorWriter struct {
	err error
}

func (w *errorWriter) Write(p []byte) (n int, err error) {
	return 0, w.err
}

type shortWriter struct {
	writeN int
}

func (w *shortWriter) Write(p []byte) (n int, err error) {
	if len(p) > w.writeN {
		return w.writeN, nil // Partial write without error
	}
	return len(p), nil
}

type errorReader struct {
	err error
}

func (r *errorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}

type partialReader struct {
	data      []byte
	readCount int
}

func (r *partialReader) Read(p []byte) (n int, err error) {
	r.readCount++
	if r.readCount == 1 {
		n = copy(p, r.data)
		return n, nil
	}
	return 0, io.ErrUnexpectedEOF
}

func TestEncoderErrors(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)
	for i := 0; i < 100; i++ {
		sketch.UpdateInt64(int64(i))
	}
	compact := sketch.CompactOrdered()

	t.Run("Writer returns error", func(t *testing.T) {
		expectedErr := errors.New("disk full")
		errWriter := &errorWriter{err: expectedErr}

		encoder := NewEncoder(errWriter, false)
		err := encoder.Encode(compact)

		assert.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("Writer short write uncompressed", func(t *testing.T) {
		shortWriter := &shortWriter{writeN: 5}

		encoder := NewEncoder(shortWriter, false)
		err := encoder.Encode(compact)

		assert.Error(t, err)
		assert.ErrorIs(t, err, io.ErrShortWrite)
	})

	t.Run("Writer short write compressed", func(t *testing.T) {
		shortWriter := &shortWriter{writeN: 5}

		encoder := NewEncoder(shortWriter, true)
		err := encoder.Encode(compact)

		assert.Error(t, err)
		assert.ErrorIs(t, err, io.ErrShortWrite)
	})

	t.Run("Writer error with compressed encoding", func(t *testing.T) {
		expectedErr := errors.New("network timeout")
		errWriter := &errorWriter{err: expectedErr}

		encoder := NewEncoder(errWriter, true)
		err := encoder.Encode(compact)

		assert.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("Empty Buffer Overrun", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		compactSketch := updateSketch.CompactOrdered()

		var buffer bytes.Buffer
		encoder := NewEncoder(&buffer, false)
		err = encoder.Encode(compactSketch)
		assert.NoError(t, err)
		b := buffer.Bytes()

		assert.Len(t, b, 8)

		// Attempt to Decode with insufficient bytes should fail
		_, err = Decode(b[:len(b)-1], DefaultSeed)
		assert.ErrorContains(t, err, "at least 8 bytes expected, actual 7")
	})
}

func TestDecoderErrors(t *testing.T) {
	t.Run("Reader returns error", func(t *testing.T) {
		expectedErr := errors.New("connection reset")
		errReader := &errorReader{err: expectedErr}

		decoder := NewDecoder(DefaultSeed)
		_, err := decoder.Decode(errReader)

		assert.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("Reader returns unexpected EOF", func(t *testing.T) {
		errReader := &errorReader{err: io.ErrUnexpectedEOF}

		decoder := NewDecoder(DefaultSeed)
		_, err := decoder.Decode(errReader)

		assert.Error(t, err)
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("Partial read with error", func(t *testing.T) {
		// Create some incomplete data
		partialData := []byte{0x01, 0x03, 0x03, 0x00}
		partialReader := &partialReader{data: partialData}

		decoder := NewDecoder(DefaultSeed)
		_, err := decoder.Decode(partialReader)

		assert.Error(t, err)
		assert.True(t, err != nil)
	})

	t.Run("Empty data", func(t *testing.T) {
		decoder := NewDecoder(DefaultSeed)
		_, err := decoder.Decode(bytes.NewReader([]byte{}))

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "at least 8 bytes expected")
	})

	t.Run("Insufficient bytes", func(t *testing.T) {
		invalidData := []byte{0x01, 0x02, 0x03}

		decoder := NewDecoder(DefaultSeed)
		_, err := decoder.Decode(bytes.NewReader(invalidData))

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "at least 8 bytes expected")
	})

	t.Run("Invalid sketch type", func(t *testing.T) {
		invalidData := make([]byte, 8)
		invalidData[0] = 1 // preamble longs
		invalidData[1] = UncompressedSerialVersion
		invalidData[2] = 99 // Invalid sketch type (should be 3)

		decoder := NewDecoder(DefaultSeed)
		_, err := decoder.Decode(bytes.NewReader(invalidData))

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid sketch type")
		assert.Contains(t, err.Error(), "expected 3, got 99")
	})

	t.Run("Unsupported serial version", func(t *testing.T) {
		// Create data with unsupported version
		invalidData := make([]byte, 8)
		invalidData[0] = 1  // preamble longs
		invalidData[1] = 99 // Invalid version
		invalidData[2] = CompactSketchType

		decoder := NewDecoder(DefaultSeed)
		_, err := decoder.Decode(bytes.NewReader(invalidData))

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported serial version: 99")
	})
}
