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
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
)

func TestWrappedCompactSketch_IsEmpty(t *testing.T) {
	t.Run("Empty Sketch", func(t *testing.T) {
		seed := DefaultSeed
		seedHash, _ := internal.ComputeSeedHash(int64(seed))

		emptySketch := newCompactSketchFromEntries(true, true, uint16(seedHash), MaxTheta, nil)
		var buf bytes.Buffer
		encoder := NewEncoder(&buf, false)
		err := encoder.Encode(emptySketch)
		assert.NoError(t, err)

		wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
		assert.NoError(t, err)
		assert.True(t, wrapped.IsEmpty())
	})

	t.Run("Non-empty Sketch", func(t *testing.T) {
		seed := DefaultSeed
		seedHash, _ := internal.ComputeSeedHash(int64(seed))

		var buf bytes.Buffer
		nonEmptySketch := newCompactSketchFromEntries(false, true, uint16(seedHash), MaxTheta, []uint64{100})
		encoder := NewEncoder(&buf, false)
		err := encoder.Encode(nonEmptySketch)
		assert.NoError(t, err)

		wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
		assert.NoError(t, err)
		assert.False(t, wrapped.IsEmpty())
	})
}

func TestWrappedCompactSketch_IsOrdered(t *testing.T) {
	t.Run("Ordered Sketch", func(t *testing.T) {
		seed := DefaultSeed
		seedHash, _ := internal.ComputeSeedHash(int64(seed))

		orderedSketch := newCompactSketchFromEntries(false, true, uint16(seedHash), MaxTheta, []uint64{100, 200})
		var buf bytes.Buffer
		encoder := NewEncoder(&buf, false)
		err := encoder.Encode(orderedSketch)

		wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
		assert.NoError(t, err)
		assert.True(t, wrapped.IsOrdered())
	})

	t.Run("Non-Ordered Sketch", func(t *testing.T) {
		seed := DefaultSeed
		seedHash, _ := internal.ComputeSeedHash(int64(seed))

		unorderedSketch := newCompactSketchFromEntries(false, false, uint16(seedHash), MaxTheta, []uint64{200, 100})
		var buf bytes.Buffer
		encoder := NewEncoder(&buf, false)
		err := encoder.Encode(unorderedSketch)

		wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
		assert.NoError(t, err)
		assert.False(t, wrapped.IsOrdered())
	})
}

func TestWrappedCompactSketch_Theta64(t *testing.T) {
	seed := DefaultSeed
	seedHash, _ := internal.ComputeSeedHash(int64(seed))

	theta := MaxTheta / 2
	sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), theta, []uint64{100})

	var buf bytes.Buffer
	encoder := NewEncoder(&buf, false)
	err := encoder.Encode(sketch)
	assert.NoError(t, err)

	wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
	assert.NoError(t, err)
	assert.Equal(t, theta, wrapped.Theta64())
}

func TestWrappedCompactSketch_NumRetained(t *testing.T) {
	seed := DefaultSeed
	seedHash, _ := internal.ComputeSeedHash(int64(seed))

	entries := []uint64{100, 200, 300}
	sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), MaxTheta, entries)

	var buf bytes.Buffer
	encoder := NewEncoder(&buf, false)
	err := encoder.Encode(sketch)

	wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), wrapped.NumRetained())
}

func TestWrappedCompactSketch_SeedHash(t *testing.T) {
	seed := DefaultSeed
	seedHash, _ := internal.ComputeSeedHash(int64(seed))

	sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), MaxTheta, []uint64{100})

	var buf bytes.Buffer
	encoder := NewEncoder(&buf, false)
	err := encoder.Encode(sketch)
	assert.NoError(t, err)

	wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
	assert.NoError(t, err)

	wrappedSeedHash, err := wrapped.SeedHash()
	assert.NoError(t, err)
	assert.Equal(t, uint16(seedHash), wrappedSeedHash)
}

func TestWrappedCompactSketch_Theta(t *testing.T) {
	seed := DefaultSeed
	seedHash, _ := internal.ComputeSeedHash(int64(seed))

	theta := MaxTheta / 2
	sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), theta, []uint64{100})

	var buf bytes.Buffer
	encoder := NewEncoder(&buf, false)
	err := encoder.Encode(sketch)
	assert.NoError(t, err)

	wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
	assert.NoError(t, err)
	assert.InDelta(t, 0.5, wrapped.Theta(), 0.01)
}

func TestWrappedCompactSketch_IsEstimationMode(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		seed := DefaultSeed
		seedHash, _ := internal.ComputeSeedHash(int64(seed))

		sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), MaxTheta, []uint64{100})
		var buf bytes.Buffer
		encoder := NewEncoder(&buf, false)
		err := encoder.Encode(sketch)
		assert.NoError(t, err)

		wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
		assert.NoError(t, err)
		assert.False(t, wrapped.IsEstimationMode())
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		seed := DefaultSeed
		seedHash, _ := internal.ComputeSeedHash(int64(seed))

		// In estimation mode
		theta := MaxTheta / 2
		sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), theta, []uint64{100})
		var buf bytes.Buffer
		encoder := NewEncoder(&buf, false)
		err := encoder.Encode(sketch)
		assert.NoError(t, err)

		wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
		assert.NoError(t, err)
		assert.True(t, wrapped.IsEstimationMode())
	})
}

func TestWrappedCompactSketch_Estimate(t *testing.T) {
	seed := DefaultSeed
	seedHash, _ := internal.ComputeSeedHash(int64(seed))

	entries := []uint64{100, 200, 300}
	sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), MaxTheta, entries)

	var buf bytes.Buffer
	encoder := NewEncoder(&buf, false)
	err := encoder.Encode(sketch)
	assert.NoError(t, err)

	wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
	assert.NoError(t, err)
	assert.Equal(t, 3.0, wrapped.Estimate())
}

func TestWrappedCompactSketch_LowerBound(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		seed := DefaultSeed
		seedHash, _ := internal.ComputeSeedHash(int64(seed))

		entries := []uint64{100, 200, 300}
		sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), MaxTheta, entries)

		var buf bytes.Buffer
		encoder := NewEncoder(&buf, false)
		err := encoder.Encode(sketch)
		assert.NoError(t, err)

		wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
		assert.NoError(t, err)

		lb, err := wrapped.LowerBound(2)
		assert.NoError(t, err)
		assert.Equal(t, 3.0, lb)
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		seed := DefaultSeed
		seedHash, _ := internal.ComputeSeedHash(int64(seed))

		theta := MaxTheta / 2
		entries := []uint64{100, 200}
		sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), theta, entries)

		var buf bytes.Buffer
		encoder := NewEncoder(&buf, false)
		err := encoder.Encode(sketch)
		assert.NoError(t, err)

		wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
		assert.NoError(t, err)

		estimate := wrapped.Estimate()
		lb, err := wrapped.LowerBound(2)
		assert.NoError(t, err)
		assert.LessOrEqual(t, lb, estimate)
	})
}

func TestWrappedCompactSketch_UpperBound(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		seed := DefaultSeed
		seedHash, _ := internal.ComputeSeedHash(int64(seed))

		entries := []uint64{100, 200, 300}
		sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), MaxTheta, entries)

		var buf bytes.Buffer
		encoder := NewEncoder(&buf, false)
		err := encoder.Encode(sketch)
		assert.NoError(t, err)

		wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
		assert.NoError(t, err)

		ub, err := wrapped.UpperBound(2)
		assert.NoError(t, err)
		assert.Equal(t, 3.0, ub)
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		seed := DefaultSeed
		seedHash, _ := internal.ComputeSeedHash(int64(seed))

		theta := MaxTheta / 2
		entries := []uint64{100, 200}
		sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), theta, entries)

		var buf bytes.Buffer
		encoder := NewEncoder(&buf, false)
		err := encoder.Encode(sketch)
		assert.NoError(t, err)

		wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
		assert.NoError(t, err)

		estimate := wrapped.Estimate()
		ub, err := wrapped.UpperBound(2)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, ub, estimate)
	})
}

func TestWrappedCompactSketch_All(t *testing.T) {
	t.Run("Uncompressed", func(t *testing.T) {
		seed := DefaultSeed
		seedHash, _ := internal.ComputeSeedHash(int64(seed))

		entries := []uint64{100, 200, 300}
		sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), MaxTheta, entries)

		var buf bytes.Buffer
		encoder := NewEncoder(&buf, false)
		err := encoder.Encode(sketch)
		assert.NoError(t, err)

		wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
		assert.NoError(t, err)

		count := 0
		seen := make(map[uint64]bool)
		for entry := range wrapped.All() {
			count++
			seen[entry] = true
		}

		assert.Equal(t, 3, count)
		assert.Equal(t, 3, len(seen))
		for _, entry := range entries {
			assert.True(t, seen[entry])
		}
	})

	t.Run("Compressed", func(t *testing.T) {
		seed := DefaultSeed
		seedHash, _ := internal.ComputeSeedHash(int64(seed))

		// Test with compressed format - 10 entries to test block-of-8 handling in compressed path
		entries := []uint64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
		sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), MaxTheta, entries)

		var buf bytes.Buffer
		encoder := NewEncoder(&buf, true)
		err := encoder.Encode(sketch)
		assert.NoError(t, err)

		wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
		assert.NoError(t, err)

		count := 0
		seen := make(map[uint64]bool)
		for entry := range wrapped.All() {
			count++
			seen[entry] = true
		}

		assert.Equal(t, 10, count)
		assert.Equal(t, 10, len(seen))
		for _, entry := range entries {
			assert.True(t, seen[entry])
		}
	})
}

func TestWrappedCompactSketch_String(t *testing.T) {
	t.Run("Without Items", func(t *testing.T) {
		seed := DefaultSeed
		seedHash, _ := internal.ComputeSeedHash(int64(seed))

		entries := []uint64{100, 200}
		sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), MaxTheta, entries)

		var buf bytes.Buffer
		encoder := NewEncoder(&buf, false)
		err := encoder.Encode(sketch)
		assert.NoError(t, err)

		wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
		assert.NoError(t, err)

		result := wrapped.String(false)
		assert.Contains(t, result, "### Theta sketch summary:")
		assert.Contains(t, result, "num retained entries : 2")
		assert.Contains(t, result, "empty?               : false")
		assert.Contains(t, result, "ordered?             : true")
		assert.NotContains(t, result, "### Retained entries")
	})

	t.Run("With Items", func(t *testing.T) {
		seed := DefaultSeed
		seedHash, _ := internal.ComputeSeedHash(int64(seed))

		entries := []uint64{100, 200}
		sketch := newCompactSketchFromEntries(false, true, uint16(seedHash), MaxTheta, entries)

		var buf bytes.Buffer
		encoder := NewEncoder(&buf, false)
		err := encoder.Encode(sketch)
		assert.NoError(t, err)

		wrapped, err := WrapCompactSketch(buf.Bytes(), seed)
		assert.NoError(t, err)

		result := wrapped.String(true)
		assert.Contains(t, result, "### Theta sketch summary:")
		assert.Contains(t, result, "### Retained entries")
		assert.Contains(t, result, "100")
		assert.Contains(t, result, "200")
		assert.Contains(t, result, "### End retained entries")
	})
}

func TestWrappedCompactSketch_EncodingAndDecoding(t *testing.T) {
	t.Run("Compact Sketch Equivalence", func(t *testing.T) {
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
		wrappedSketch, err := WrapCompactSketch(b, DefaultSeed)
		assert.NoError(t, err)

		assert.Equal(t, compactSketch.Estimate(), wrappedSketch.Estimate())
		expectedLB, err := compactSketch.LowerBound(1)
		assert.NoError(t, err)
		resultLB, err := wrappedSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, expectedLB, resultLB)
		expectedUB, err := compactSketch.UpperBound(1)
		assert.NoError(t, err)
		resultUB, err := wrappedSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, expectedUB, resultUB)
		assert.Equal(t, compactSketch.IsEstimationMode(), wrappedSketch.IsEstimationMode())
		assert.Equal(t, compactSketch.Theta(), wrappedSketch.Theta())

		var expectedEntries []uint64
		for entry := range compactSketch.All() {
			expectedEntries = append(expectedEntries, entry)
		}
		var resultEntries []uint64
		for entry := range compactSketch.All() {
			resultEntries = append(resultEntries, entry)
		}
		assert.Equal(t, expectedEntries, resultEntries)
	})
}

func TestWrapCompactSketch_Compatibility(t *testing.T) {
	t.Run("Compact V1 Empty From Java", func(t *testing.T) {
		// Always read sketch from same directory as the test file
		_, currentFileName, _, _ := runtime.Caller(0)
		currentDir := filepath.Dir(currentFileName)
		path := filepath.Join(currentDir, "theta_compact_empty_from_java_v1.sk")
		b, err := os.ReadFile(path)
		assert.NoError(t, err)

		sketch, err := WrapCompactSketch(b, DefaultSeed)
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

		sketch, err := WrapCompactSketch(b, DefaultSeed)
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

		sketch, err := WrapCompactSketch(b, DefaultSeed)
		assert.NoError(t, err)

		assert.False(t, sketch.IsEmpty())
		assert.True(t, sketch.IsEstimationMode())
		assert.True(t, sketch.IsOrdered())
		assert.Equal(t, uint32(4342), sketch.NumRetained())
		assert.InDelta(t, 0.531700444213199, sketch.Theta(), 1e-10)
		assert.InDelta(t, 8166.25234614053, sketch.Estimate(), 1e-10)
		lb, err := sketch.LowerBound(2)
		assert.NoError(t, err)
		assert.InDelta(t, 7996.956955317471, lb, 1e-10)
		ub, err := sketch.UpperBound(2)
		assert.NoError(t, err)
		assert.InDelta(t, 8339.090301078124, ub, 1e-10)

		// the same construction process in Java must have produced exactly the same sketch
		updateSketch, err := NewQuickSelectUpdateSketch()
		n := 8192
		for i := 0; i < n; i++ {
			updateSketch.UpdateInt64(int64(i))
		}
		assert.Equal(t, sketch.NumRetained(), updateSketch.NumRetained())
		assert.InDelta(t, sketch.Theta(), updateSketch.Theta(), 1e-10)
		assert.InDelta(t, sketch.Estimate(), updateSketch.Estimate(), 1e-10)

		expectedLB, err := sketch.LowerBound(1)
		assert.NoError(t, err)
		resultLB, err := updateSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.InDelta(t, expectedLB, resultLB, 1e-10)
		expectedUB, err := sketch.UpperBound(1)
		assert.NoError(t, err)
		resultUB, err := updateSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.InDelta(t, expectedUB, resultUB, 1e-10)

		expectedLB, err = sketch.LowerBound(2)
		assert.NoError(t, err)
		resultLB, err = updateSketch.LowerBound(2)
		assert.NoError(t, err)
		assert.InDelta(t, expectedLB, resultLB, 1e-10)
		expectedUB, err = sketch.UpperBound(2)
		assert.NoError(t, err)
		resultUB, err = updateSketch.UpperBound(2)
		assert.NoError(t, err)
		assert.InDelta(t, expectedUB, resultUB, 1e-10)

		expectedLB, err = sketch.LowerBound(3)
		assert.NoError(t, err)
		resultLB, err = updateSketch.LowerBound(3)
		assert.NoError(t, err)
		assert.InDelta(t, expectedLB, resultLB, 1e-10)
		expectedUB, err = sketch.UpperBound(3)
		assert.NoError(t, err)
		resultUB, err = updateSketch.UpperBound(3)
		assert.NoError(t, err)
		assert.InDelta(t, expectedUB, resultUB, 1e-10)

		var expectedEntries []uint64
		for entry := range sketch.All() {
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
