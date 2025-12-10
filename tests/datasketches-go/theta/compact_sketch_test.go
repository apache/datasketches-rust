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

func TestNewCompactSketch(t *testing.T) {
	t.Run("Empty Source", func(t *testing.T) {
		source, _ := NewQuickSelectUpdateSketch()
		sketch := NewCompactSketch(source, false)

		assert.NotNil(t, sketch)
		assert.True(t, sketch.IsEmpty())
		assert.Equal(t, uint32(0), sketch.NumRetained())
		assert.True(t, sketch.IsOrdered())
	})

	t.Run("Ordered Source", func(t *testing.T) {
		source, _ := NewQuickSelectUpdateSketch()
		_ = source.UpdateInt64(1)

		sketch := NewCompactSketch(source, false)

		assert.NotNil(t, sketch)
		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, uint32(1), sketch.NumRetained())
		assert.True(t, sketch.IsOrdered())
	})

	t.Run("Unordered Source With Ordering", func(t *testing.T) {
		entries := []uint64{100, 200}
		unordered := newCompactSketchFromEntries(false, false, 0x1234, MaxTheta, entries)

		sketch := NewCompactSketch(unordered, true)

		assert.True(t, sketch.IsOrdered())
	})

	t.Run("Unordered Source Without Ordering", func(t *testing.T) {
		entries := []uint64{100, 200}
		unordered := newCompactSketchFromEntries(false, false, 0x1234, MaxTheta, entries)

		sketch := NewCompactSketch(unordered, false)

		assert.False(t, sketch.IsOrdered())
	})
}

func TestCompactSketch_Estimate(t *testing.T) {
	sketch := newCompactSketchFromEntries(false, true, 0x1234, MaxTheta, []uint64{100, 200, 300})
	assert.Equal(t, 3.0, sketch.Estimate())
}

func TestCompactSketch_LowerBound(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		sketch := newCompactSketchFromEntries(false, true, 0x1234, MaxTheta, []uint64{100, 200, 300})
		assert.False(t, sketch.IsEstimationMode())

		lb, err := sketch.LowerBound(2)
		assert.NoError(t, err)
		assert.Equal(t, 3.0, lb)
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		theta := MaxTheta / 2
		sketch := newCompactSketchFromEntries(false, false, 0x1234, theta, []uint64{100, 200})
		assert.True(t, sketch.IsEstimationMode())

		estimate := sketch.Estimate()
		lb, err := sketch.LowerBound(2)
		assert.NoError(t, err)
		assert.LessOrEqual(t, lb, estimate)
	})
}

func TestCompactSketch_UpperBound(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		sketch := newCompactSketchFromEntries(false, true, 0x1234, MaxTheta, []uint64{100, 200, 300})
		assert.False(t, sketch.IsEstimationMode())

		ub, err := sketch.UpperBound(2)
		assert.NoError(t, err)
		assert.Equal(t, 3.0, ub)
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		theta := MaxTheta / 2
		sketch := newCompactSketchFromEntries(false, false, 0x1234, theta, []uint64{100, 200})
		assert.True(t, sketch.IsEstimationMode())

		estimate := sketch.Estimate()
		ub, err := sketch.UpperBound(2)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, ub, estimate)
	})
}

func TestCompactSketch_Theta(t *testing.T) {
	theta := MaxTheta / 2
	sketch := newCompactSketchFromEntries(false, true, 0x1234, theta, []uint64{100})
	assert.InDelta(t, 0.5, sketch.Theta(), 0.01)
}

func TestCompactSketch_String(t *testing.T) {
	t.Run("Without Items", func(t *testing.T) {
		sketch := newCompactSketchFromEntries(false, true, 0x1234, MaxTheta, []uint64{100, 200})

		result := sketch.String(false)
		assert.Contains(t, result, "### Theta sketch summary:")
		assert.Contains(t, result, "num retained entries : 2")
		assert.Contains(t, result, "seed hash            : 4660")
		assert.Contains(t, result, "empty?               : false")
		assert.Contains(t, result, "ordered?             : true")
		assert.NotContains(t, result, "### Retained entries")
	})

	t.Run("With Items", func(t *testing.T) {
		sketch := newCompactSketchFromEntries(false, true, 0x1234, MaxTheta, []uint64{100, 200})

		result := sketch.String(true)
		assert.Contains(t, result, "### Theta sketch summary:")
		assert.Contains(t, result, "### Retained entries")
		assert.Contains(t, result, "100")
		assert.Contains(t, result, "200")
		assert.Contains(t, result, "### End retained entries")
	})
}

func TestCompactSketch_All(t *testing.T) {
	entries := []uint64{100, 200, 300}
	sketch := newCompactSketchFromEntries(false, true, 0x1234, MaxTheta, entries)

	count := 0
	seen := make(map[uint64]bool)
	for entry := range sketch.All() {
		count++
		seen[entry] = true
	}

	assert.Equal(t, 3, count)
	assert.Equal(t, 3, len(seen))
	for _, entry := range entries {
		assert.True(t, seen[entry])
	}
}

func TestCompactSketch_MarshalBinary(t *testing.T) {
	t.Run("Empty sketch", func(t *testing.T) {
		sketch, _ := NewQuickSelectUpdateSketch()
		compact := sketch.CompactOrdered()

		data, err := compact.MarshalBinary()

		assert.NoError(t, err)
		assert.NotNil(t, data)
		assert.Greater(t, len(data), 0)

		decoded, err := Decode(data, DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, decoded.IsEmpty())
		assert.Equal(t, uint32(0), decoded.NumRetained())
	})

	t.Run("Single entry sketch", func(t *testing.T) {
		sketch, _ := NewQuickSelectUpdateSketch()
		sketch.UpdateInt64(42)
		compact := sketch.CompactOrdered()

		data, err := compact.MarshalBinary()

		assert.NoError(t, err)
		assert.NotNil(t, data)

		decoded, err := Decode(data, DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, decoded.IsEmpty())
		assert.Equal(t, uint32(1), decoded.NumRetained())
		assert.Equal(t, compact.Theta64(), decoded.Theta64())
	})

	t.Run("Multiple entries exact mode", func(t *testing.T) {
		sketch, _ := NewQuickSelectUpdateSketch()
		for i := 0; i < 10; i++ {
			sketch.UpdateInt64(int64(i))
		}
		compact := sketch.CompactOrdered()

		data, err := compact.MarshalBinary()

		assert.NoError(t, err)
		assert.NotNil(t, data)

		decoded, err := Decode(data, DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, decoded.IsEmpty())
		assert.Equal(t, compact.NumRetained(), decoded.NumRetained())
		assert.Equal(t, compact.Theta64(), decoded.Theta64())
		assert.Equal(t, compact.IsOrdered(), decoded.IsOrdered())
	})

	t.Run("Large sketch estimation mode", func(t *testing.T) {
		sketch, _ := NewQuickSelectUpdateSketch()
		for i := 0; i < 10000; i++ {
			sketch.UpdateInt64(int64(i))
		}
		compact := sketch.CompactOrdered()

		assert.True(t, compact.IsEstimationMode())

		data, err := compact.MarshalBinary()

		assert.NoError(t, err)
		assert.NotNil(t, data)

		decoded, err := Decode(data, DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, decoded.IsEstimationMode())
		assert.Equal(t, compact.NumRetained(), decoded.NumRetained())
		assert.Equal(t, compact.Theta64(), decoded.Theta64())
	})
}
