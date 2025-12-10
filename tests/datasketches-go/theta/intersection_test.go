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

func TestIntersection(t *testing.T) {
	t.Run("Invalid", func(t *testing.T) {
		intersection := NewIntersection()
		assert.False(t, intersection.HasResult())

		_, err := intersection.Result(true)
		assert.NotNil(t, err)
	})

	t.Run("Empty, Empty", func(t *testing.T) {
		intersection := NewIntersection()
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		err = intersection.Update(sketch1)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		err = intersection.Update(sketch2)
		assert.NoError(t, err)

		result, err = intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Empty Compact, Empty Compact", func(t *testing.T) {
		intersection := NewIntersection()
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		err = intersection.Update(sketch1.Compact(false))
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		err = intersection.Update(sketch2.Compact(false))
		assert.NoError(t, err)

		result, err = intersection.Result(false)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Empty, Exact", func(t *testing.T) {
		empty, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		exact, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact.UpdateInt64(3)

		intersection := NewIntersection()
		err = intersection.Update(empty)
		assert.NoError(t, err)
		err = intersection.Update(exact)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("Empty Compact, Exact Compact", func(t *testing.T) {
		empty, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		exact, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact.UpdateInt64(3)

		intersection := NewIntersection()
		err = intersection.Update(empty.Compact(false))
		assert.NoError(t, err)
		err = intersection.Update(exact.Compact(false))
		assert.NoError(t, err)

		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("Empty, Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6)

		intersection := NewIntersection()
		err = intersection.Update(a)
		assert.NoError(t, err)
		err = intersection.Update(b)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("Empty Compact, Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6)

		intersection := NewIntersection()
		err = intersection.Update(a.Compact(false))
		assert.NoError(t, err)
		err = intersection.Update(b.Compact(false))
		assert.NoError(t, err)

		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("Empty, Estimation Mode", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(4)

		intersection := NewIntersection()
		err = intersection.Update(a)
		assert.NoError(t, err)
		err = intersection.Update(b)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("Empty Compact, Estimation Mode Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(4)

		intersection := NewIntersection()
		err = intersection.Update(a.Compact(false))
		assert.NoError(t, err)
		err = intersection.Update(b.Compact(false))
		assert.NoError(t, err)

		result, err := intersection.Result(false)
		assert.NoError(t, err)
		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("Exact, Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6)

		intersection := NewIntersection()
		err = intersection.Update(a)
		assert.NoError(t, err)
		err = intersection.Update(b)
		assert.NoError(t, err)
		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Exact Compact, Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6)

		intersection := NewIntersection()
		err = intersection.Update(a.Compact(false))
		assert.NoError(t, err)
		err = intersection.Update(b.Compact(false))
		assert.NoError(t, err)
		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Estimation Mode, Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(4)

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6)

		intersection := NewIntersection()
		err = intersection.Update(a)
		assert.NoError(t, err)
		err = intersection.Update(b)
		assert.NoError(t, err)
		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Estimation Mode Compact, Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(4)

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6)

		intersection := NewIntersection()
		err = intersection.Update(a.Compact(false))
		assert.NoError(t, err)
		err = intersection.Update(b.Compact(false))
		assert.NoError(t, err)
		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Exact Mode Half Overlap", func(t *testing.T) {
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value))
			value++
		}

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value))
			value++
		}

		intersection := NewIntersection()
		err = intersection.Update(sketch1)
		assert.NoError(t, err)

		err = intersection.Update(sketch2)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 500.0, result.Estimate())
	})

	t.Run("Exact Mode Compact Half Overlap", func(t *testing.T) {
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value))
			value++
		}

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value))
			value++
		}

		intersection := NewIntersection()
		err = intersection.Update(sketch1.Compact(false))
		assert.NoError(t, err)

		err = intersection.Update(sketch2.Compact(false))
		assert.NoError(t, err)

		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 500.0, result.Estimate())
	})

	t.Run("Exact Mode Half Overlap Ordered", func(t *testing.T) {
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value))
			value++
		}

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value))
			value++
		}

		intersection := NewIntersection()
		err = intersection.Update(sketch1.Compact(true))
		assert.NoError(t, err)

		err = intersection.Update(sketch2.Compact(true))
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 500.0, result.Estimate())
	})

	t.Run("Exact Mode Disjoint", func(t *testing.T) {
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value))
			value++
		}

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value))
			value++
		}

		intersection := NewIntersection()
		err = intersection.Update(sketch1)
		assert.NoError(t, err)

		err = intersection.Update(sketch2)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Exact Mode Compact Disjoint", func(t *testing.T) {
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value))
			value++
		}

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value))
			value++
		}

		intersection := NewIntersection()
		err = intersection.Update(sketch1.Compact(false))
		assert.NoError(t, err)

		err = intersection.Update(sketch2.Compact(false))
		assert.NoError(t, err)

		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Estimation Mode Half Overlap", func(t *testing.T) {
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value))
			value++
		}

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value = 5000
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value))
			value++
		}

		intersection := NewIntersection()
		err = intersection.Update(sketch1)
		assert.NoError(t, err)

		err = intersection.Update(sketch2)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())

		assert.InEpsilon(t, 5000.0, result.Estimate(), 0.02)
	})

	t.Run("Estimation Mode Compact Half Overlap", func(t *testing.T) {
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value))
			value++
		}

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value = 5000
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value))
			value++
		}

		intersection := NewIntersection()
		err = intersection.Update(sketch1.Compact(false))
		assert.NoError(t, err)

		err = intersection.Update(sketch2.Compact(false))
		assert.NoError(t, err)

		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())

		assert.InEpsilon(t, 5000.0, result.Estimate(), 0.02)
	})

	t.Run("Estimation Mode Half Overlap Ordered Wrapped", func(t *testing.T) {
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value))
			value++
		}

		bytes1, err := sketch1.Compact(true).MarshalBinary()
		assert.NoError(t, err)

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value = 5000
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value))
			value++
		}
		bytes2, err := sketch2.Compact(true).MarshalBinary()
		assert.NoError(t, err)

		wrappedSketch1, err := Decode(bytes1, DefaultSeed)
		assert.NoError(t, err)

		wrappedSketch2, err := Decode(bytes2, DefaultSeed)
		assert.NoError(t, err)

		intersection := NewIntersection()
		err = intersection.Update(wrappedSketch1)
		assert.NoError(t, err)

		err = intersection.Update(wrappedSketch2)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())

		assert.InEpsilon(t, 5000.0, result.Estimate(), 0.02)
	})

	t.Run("Estimation Mode Disjoint", func(t *testing.T) {
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value))
			value++
		}

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value))
			value++
		}

		intersection := NewIntersection()
		err = intersection.Update(sketch1)
		assert.NoError(t, err)

		err = intersection.Update(sketch2)
		assert.NoError(t, err)

		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Estimation Mode Compact Disjoint", func(t *testing.T) {
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 10000; i++ {
			sketch1.UpdateInt64(int64(value))
			value++
		}

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			sketch2.UpdateInt64(int64(value))
			value++
		}

		intersection := NewIntersection()
		err = intersection.Update(sketch1.Compact(false))
		assert.NoError(t, err)

		err = intersection.Update(sketch2.Compact(false))
		assert.NoError(t, err)

		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Exact Mode, Estimation Mode Full Overlap", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(4))

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(4))

		intersection := NewIntersection()
		err = intersection.Update(a)
		assert.NoError(t, err)
		err = intersection.Update(b)
		assert.NoError(t, err)
		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Exact Mode Compact, Estimation Mode Compact Full Overlap", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(4))

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(4))

		intersection := NewIntersection()
		err = intersection.Update(a.Compact(false))
		assert.NoError(t, err)
		err = intersection.Update(b.Compact(false))
		assert.NoError(t, err)
		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Non Empty No Retained Keys, Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(3))

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(6))

		intersection := NewIntersection()
		err = intersection.Update(a)
		assert.NoError(t, err)
		err = intersection.Update(b)
		assert.NoError(t, err)
		result, err := intersection.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Non Empty No Retained Keys Compact, Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(3))

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(6))

		intersection := NewIntersection()
		err = intersection.Update(a.Compact(false))
		assert.NoError(t, err)
		err = intersection.Update(b.Compact(false))
		assert.NoError(t, err)
		result, err := intersection.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Seed Mismatch", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		sketch.UpdateInt64(1) // non-empty should not be ignored

		intersection := NewIntersection(WithIntersectionSeed(123))
		err = intersection.Update(sketch)
		assert.ErrorContains(t, err, "seed hash mismatch")
	})

	t.Run("Policy", func(t *testing.T) {
		intersection := NewIntersection()

		policy := intersection.Policy()
		assert.NotNil(t, policy)

		assert.IsType(t, &noopPolicy{}, policy)
	})

	t.Run("OrderedResult Method", func(t *testing.T) {
		sketch1, _ := NewQuickSelectUpdateSketch()
		for i := 0; i < 100; i++ {
			sketch1.UpdateInt64(int64(i))
		}

		sketch2, _ := NewQuickSelectUpdateSketch()
		for i := 50; i < 150; i++ {
			sketch2.UpdateInt64(int64(i))
		}

		intersection := NewIntersection()
		intersection.Update(sketch1)
		intersection.Update(sketch2)

		result, err := intersection.OrderedResult()
		assert.NoError(t, err)
		assert.True(t, result.IsOrdered())
		assert.Equal(t, 50.0, result.Estimate())
	})
}
