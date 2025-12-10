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

func TestNewQuickSelectUpdateSketch(t *testing.T) {
	t.Run("No Options And Empty", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		assert.True(t, updateSketch.IsEmpty())
		assert.False(t, updateSketch.IsEstimationMode())
		assert.Equal(t, 1.0, updateSketch.Theta())
		assert.Equal(t, 0.0, updateSketch.Estimate())
		lb, err := updateSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err := updateSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, ub)
		assert.True(t, updateSketch.IsOrdered())
	})

	t.Run("With Options", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(10),
			WithUpdateSketchResizeFactor(ResizeX2),
			WithUpdateSketchP(0.5),
			WithUpdateSketchSeed(12345),
		)
		assert.NoError(t, err)
		assert.NotNil(t, sketch)
		assert.Equal(t, uint8(10), sketch.LgK())
		assert.Equal(t, ResizeX2, sketch.ResizeFactor())
		assert.Equal(t, float32(0.5), sketch.table.p)
		assert.Equal(t, uint64(12345), sketch.table.seed)
	})

	t.Run("Non Empty No Retained Keys", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchP(0.001))
		assert.NoError(t, err)
		updateSketch.UpdateInt64(1)

		assert.Zero(t, updateSketch.NumRetained())
		assert.False(t, updateSketch.IsEmpty())
		assert.True(t, updateSketch.IsEstimationMode())
		assert.Equal(t, 0.0, updateSketch.Estimate())
		lb, err := updateSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err := updateSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Greater(t, ub, 0.0)

		updateSketch.Reset()
		assert.True(t, updateSketch.IsEmpty())
		assert.False(t, updateSketch.IsEstimationMode())
		assert.Equal(t, 1.0, updateSketch.Theta())
		assert.Equal(t, 0.0, updateSketch.Estimate())
		lb, err = updateSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err = updateSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, ub)
	})

	t.Run("Invalid Lgk", func(t *testing.T) {
		_, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(3))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "lg_k must not be less than")

		_, err = NewQuickSelectUpdateSketch(WithUpdateSketchLgK(30))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "lg_k must not be greater than")
	})

	t.Run("Invalid P", func(t *testing.T) {
		_, err := NewQuickSelectUpdateSketch(WithUpdateSketchP(0.0))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sampling probability must be between 0 and 1")

		_, err = NewQuickSelectUpdateSketch(WithUpdateSketchP(1.5))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sampling probability must be between 0 and 1")
	})
}

func TestQuickSelectUpdateSketch_Theta64(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
	assert.NoError(t, err)

	// Empty sketch has MaxTheta
	assert.Equal(t, MaxTheta, sketch.Theta64())

	initialTheta := sketch.table.theta
	assert.Equal(t, MaxTheta, initialTheta)

	// Insert many values to trigger rebuild
	for i := 0; i < 100; i++ {
		_ = sketch.UpdateInt64(int64(i))
	}

	assert.Less(t, sketch.table.theta, initialTheta)
	assert.Equal(t, sketch.table.theta, sketch.Theta64())
}

func TestQuickSelectUpdateSketch_SeedHash(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(12345))
	assert.NoError(t, err)

	seedHash, err := sketch.SeedHash()
	assert.NoError(t, err)
	assert.NotZero(t, seedHash)
}

func TestQuickSelectUpdateSketch_UpdateUint64(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	err = sketch.UpdateUint64(100)
	assert.NoError(t, err)
	assert.False(t, sketch.IsEmpty())

	err = sketch.UpdateUint64(100)
	assert.ErrorIs(t, err, ErrDuplicateKey)
	assert.Equal(t, uint32(1), sketch.NumRetained())
}

func TestQuickSelectUpdateSketch_UpdateInt64(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	err = sketch.UpdateInt64(-100)
	assert.NoError(t, err)
	assert.False(t, sketch.IsEmpty())

	err = sketch.UpdateInt64(-100)
	assert.ErrorIs(t, err, ErrDuplicateKey)
	assert.Equal(t, uint32(1), sketch.NumRetained())
}

func TestQuickSelectUpdateSketch_UpdateInt32(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	err = sketch.UpdateInt32(42)
	assert.NoError(t, err)
	assert.False(t, sketch.IsEmpty())

	err = sketch.UpdateInt32(42)
	assert.ErrorIs(t, err, ErrDuplicateKey)
	assert.Equal(t, uint32(1), sketch.NumRetained())
}

func TestQuickSelectUpdateSketch_UpdateString(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	err = sketch.UpdateString("")
	assert.ErrorIs(t, err, ErrUpdateEmptyString)

	err = sketch.UpdateString("hello")
	assert.NoError(t, err)
	assert.False(t, sketch.IsEmpty())
	assert.Equal(t, uint32(1), sketch.NumRetained())

	err = sketch.UpdateString("hello")
	assert.ErrorIs(t, err, ErrDuplicateKey)
	assert.Equal(t, uint32(1), sketch.NumRetained())
}

func TestQuickSelectUpdateSketch_UpdateBytes(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	err = sketch.UpdateBytes([]byte{1, 2, 3})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())

	err = sketch.UpdateBytes([]byte{1, 2, 3})
	assert.ErrorIs(t, err, ErrDuplicateKey)
	assert.Equal(t, uint32(1), sketch.NumRetained())
}

func TestQuickSelectUpdateSketch_UpdateFloat64(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	err = sketch.UpdateFloat64(3.14)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())

	err = sketch.UpdateFloat64(3.14)
	assert.ErrorIs(t, err, ErrDuplicateKey)
	assert.Equal(t, uint32(1), sketch.NumRetained())
}

func TestQuickSelectUpdateSketch_UpdateFloat32(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	err = sketch.UpdateFloat32(3.14)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), sketch.NumRetained())

	err = sketch.UpdateFloat32(3.14)
	assert.ErrorIs(t, err, ErrDuplicateKey)
	assert.Equal(t, uint32(1), sketch.NumRetained())
}

func TestQuickSelectUpdateSketch_Estimate(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	assert.Equal(t, 0.0, sketch.Estimate())

	err = sketch.UpdateInt64(1)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, sketch.Estimate())
}

func TestQuickSelectUpdateSketch_LowerBound(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		lb, err := sketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, float64(sketch.NumRetained()), lb)

		for _, stdDevs := range []uint8{1, 2, 3} {
			lb, err = sketch.LowerBound(stdDevs)
			assert.NoError(t, err)
			assert.Equal(t, float64(sketch.NumRetained()), lb)
		}
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		assert.True(t, sketch.IsEstimationMode())

		estimate := sketch.Estimate()
		lb, err := sketch.LowerBound(1)
		assert.NoError(t, err)
		assert.LessOrEqual(t, lb, estimate)

		lb1, err := sketch.LowerBound(1)
		assert.NoError(t, err)
		lb2, err := sketch.LowerBound(2)
		assert.NoError(t, err)
		lb3, err := sketch.LowerBound(3)
		assert.NoError(t, err)

		assert.GreaterOrEqual(t, lb1, lb2)
		assert.GreaterOrEqual(t, lb2, lb3)
	})

	t.Run("Invalid NumStdDevs", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		_, err = sketch.LowerBound(0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "numStdDevs must be 1, 2 or 3")

		_, err = sketch.LowerBound(4)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "numStdDevs must be 1, 2 or 3")
	})
}

func TestQuickSelectUpdateSketch_UpperBound(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		ub, err := sketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, ub)

		for i := 0; i < 10; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		ub, err = sketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, float64(sketch.NumRetained()), ub)

		for _, stdDevs := range []uint8{1, 2, 3} {
			ub, err = sketch.UpperBound(stdDevs)
			assert.NoError(t, err)
			assert.Equal(t, float64(sketch.NumRetained()), ub)
		}
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		assert.True(t, sketch.IsEstimationMode())

		estimate := sketch.Estimate()
		ub, err := sketch.UpperBound(1)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, ub, estimate)

		// Higher standard deviations should give higher upper bounds
		ub1, err := sketch.UpperBound(1)
		assert.NoError(t, err)
		ub2, err := sketch.UpperBound(2)
		assert.NoError(t, err)
		ub3, err := sketch.UpperBound(3)
		assert.NoError(t, err)

		assert.LessOrEqual(t, ub1, ub2)
		assert.LessOrEqual(t, ub2, ub3)
	})

	t.Run("Invalid NumStdDevs", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		_, err = sketch.UpperBound(0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "numStdDevs must be 1, 2 or 3")

		_, err = sketch.UpperBound(4)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "numStdDevs must be 1, 2 or 3")
	})
}

func TestQuickSelectUpdateSketch_IsEstimationMode(t *testing.T) {
	t.Run("Exact Mode", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		assert.False(t, sketch.IsEstimationMode())
		assert.Equal(t, MaxTheta, sketch.Theta64())

		for i := 0; i < 10; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}
		assert.False(t, sketch.IsEstimationMode())
		assert.Equal(t, MaxTheta, sketch.Theta64())
	})

	t.Run("Estimation Mode", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		assert.False(t, sketch.IsEstimationMode())

		// Add enough items to trigger estimation mode
		for i := 0; i < 100; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		assert.True(t, sketch.IsEstimationMode())
	})
}

func TestQuickSelectUpdateSketch_Theta(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
	assert.NoError(t, err)
	assert.Equal(t, 1.0, sketch.Theta())

	// Add enough items to trigger rebuild
	for i := 0; i < 100; i++ {
		_ = sketch.UpdateInt64(int64(i))
	}

	theta := sketch.Theta()
	assert.Less(t, theta, 1.0)
}

func TestQuickSelectUpdateSketch_All(t *testing.T) {
	sketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	count := 0
	for range sketch.All() {
		count++
	}
	assert.Equal(t, 0, count)

	// Add items and verify iteration
	values := []int64{1, 2, 3, 4, 5}
	for _, v := range values {
		_ = sketch.UpdateInt64(v)
	}
	count = 0
	for range sketch.All() {
		count++
	}
	assert.Equal(t, len(values), count)

	seen := make(map[uint64]bool)
	for hash := range sketch.All() {
		seen[hash] = true
		assert.NotZero(t, hash)
	}
	assert.Equal(t, len(values), len(seen))

	// Test early break
	count = 0
	for range sketch.All() {
		count++
		if count == 2 {
			break
		}
	}
	assert.Equal(t, 2, count)
}

func TestQuickSelectUpdateSketch_String(t *testing.T) {
	t.Run("Without Items", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(8))
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			_ = sketch.UpdateInt64(int64(i))
		}

		result := sketch.String(false)

		assert.Contains(t, result, "### Theta sketch summary:")
		assert.Contains(t, result, "num retained entries : 10")
		assert.Contains(t, result, "empty?               : false")
		assert.Contains(t, result, "ordered?             : false")
		assert.Contains(t, result, "estimation mode?     : false")
		assert.Contains(t, result, "lg nominal size      : 8")
		assert.Contains(t, result, "### End sketch summary")

		assert.NotContains(t, result, "### Retained entries")
	})

	t.Run("With Items", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		_ = sketch.UpdateInt64(100)
		_ = sketch.UpdateInt64(200)

		result := sketch.String(true)

		assert.Contains(t, result, "### Theta sketch summary:")
		assert.Contains(t, result, "num retained entries : 2")
		assert.Contains(t, result, "### End sketch summary")

		assert.Contains(t, result, "### Retained entries")
		assert.Contains(t, result, "### End retained entries")
	})
}

func TestQuickSelectUpdateSketch_SingleItem(t *testing.T) {
	updateSketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)
	updateSketch.UpdateInt64(1)

	assert.False(t, updateSketch.IsEmpty())
	assert.False(t, updateSketch.IsEstimationMode())
	assert.Equal(t, 1.0, updateSketch.Theta())
	assert.Equal(t, 1.0, updateSketch.Estimate())
	lb, err := updateSketch.LowerBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, lb)
	ub, err := updateSketch.UpperBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, ub)
	assert.True(t, updateSketch.IsOrdered())
}

func TestQuickSelectUpdateSketch_ResizeExact(t *testing.T) {
	updateSketch, err := NewQuickSelectUpdateSketch()
	assert.NoError(t, err)

	for i := 0; i < 2000; i++ {
		updateSketch.UpdateInt64(int64(i))
	}

	assert.False(t, updateSketch.IsEmpty())
	assert.False(t, updateSketch.IsEstimationMode())
	assert.Equal(t, 1.0, updateSketch.Theta())
	assert.Equal(t, 2000.0, updateSketch.Estimate())
	lb, err := updateSketch.LowerBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 2000.0, lb)
	ub, err := updateSketch.UpperBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 2000.0, ub)
	assert.False(t, updateSketch.IsOrdered())

	updateSketch.Reset()
	assert.True(t, updateSketch.IsEmpty())
	assert.False(t, updateSketch.IsEstimationMode())
	assert.Equal(t, 1.0, updateSketch.Theta())
	assert.Equal(t, 0.0, updateSketch.Estimate())
	lb, err = updateSketch.LowerBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, lb)
	ub, err = updateSketch.UpperBound(1)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, ub)
	assert.True(t, updateSketch.IsOrdered())
}

func TestQuickSelectUpdateSketch_Estimation(t *testing.T) {
	updateSketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchResizeFactor(ResizeX1))
	assert.NoError(t, err)

	n := 8000
	for i := 0; i < n; i++ {
		updateSketch.UpdateInt64(int64(i))
	}

	assert.False(t, updateSketch.IsEmpty())
	assert.True(t, updateSketch.IsEstimationMode())
	assert.Less(t, updateSketch.Theta(), 1.0)
	assert.InEpsilon(t, n, updateSketch.Estimate(), 0.01)
	lb, err := updateSketch.LowerBound(1)
	assert.NoError(t, err)
	assert.Less(t, lb, float64(n))
	ub, err := updateSketch.UpperBound(1)
	assert.NoError(t, err)
	assert.Greater(t, ub, float64(n))

	k := uint32(1) << DefaultLgK
	assert.GreaterOrEqual(t, updateSketch.NumRetained(), k)

	updateSketch.Trim()
	assert.Equal(t, k, updateSketch.NumRetained())
}

func TestUpdateSketch_Compact(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		compactSketch := updateSketch.Compact(true)
		assert.True(t, compactSketch.IsEmpty())
		assert.False(t, compactSketch.IsEstimationMode())
		assert.Equal(t, 1.0, compactSketch.Theta())
		assert.Equal(t, 0.0, compactSketch.Estimate())
		lb, err := compactSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err := compactSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, ub)
		assert.True(t, compactSketch.IsOrdered())

		assert.True(t, updateSketch.Compact(false).IsOrdered())
	})

	t.Run("Non Empty No Retained Keys", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchP(0.001))
		assert.NoError(t, err)
		updateSketch.UpdateInt64(1)

		compactSketch := updateSketch.Compact(true)
		assert.Zero(t, compactSketch.NumRetained())
		assert.False(t, compactSketch.IsEmpty())
		assert.True(t, compactSketch.IsEstimationMode())
		assert.Equal(t, 0.0, compactSketch.Estimate())
		lb, err := compactSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, lb)
		ub, err := compactSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Greater(t, ub, 0.0)
	})

	t.Run("Single Item", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		updateSketch.UpdateInt64(1)

		compactSketch := updateSketch.Compact(true)
		assert.False(t, compactSketch.IsEmpty())
		assert.False(t, compactSketch.IsEstimationMode())
		assert.Equal(t, 1.0, compactSketch.Theta())
		assert.Equal(t, 1.0, compactSketch.Estimate())
		lb, err := compactSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, lb)
		ub, err := compactSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, ub)
		assert.True(t, compactSketch.IsOrdered())

		assert.True(t, updateSketch.Compact(false).IsOrdered())
	})

	t.Run("Estimation", func(t *testing.T) {
		updateSketch, err := NewQuickSelectUpdateSketch(WithUpdateSketchResizeFactor(ResizeX1))
		assert.NoError(t, err)

		n := 8000
		for i := 0; i < n; i++ {
			updateSketch.UpdateInt64(int64(i))
		}
		updateSketch.Trim()

		compactSketch := updateSketch.Compact(true)
		assert.False(t, compactSketch.IsEmpty())
		assert.True(t, compactSketch.IsOrdered())
		assert.True(t, compactSketch.IsEstimationMode())
		assert.Less(t, compactSketch.Theta(), 1.0)
		assert.InEpsilon(t, n, compactSketch.Estimate(), 0.01)

		lb, err := compactSketch.LowerBound(1)
		assert.NoError(t, err)
		assert.Less(t, lb, float64(n))
		ub, err := compactSketch.UpperBound(1)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, ub, float64(n))
	})
}
