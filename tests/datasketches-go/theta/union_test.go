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

func TestUnion(t *testing.T) {
	t.Run("Empty, Empty", func(t *testing.T) {
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		union, err := NewUnion()
		assert.NoError(t, err)

		err = union.Update(sketch1)
		assert.NoError(t, err)
		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		err = union.Update(sketch2)

		result, err = union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
	})

	t.Run("Empty Compact, Empty Compact", func(t *testing.T) {
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		union, err := NewUnion()
		assert.NoError(t, err)

		err = union.Update(sketch1.Compact(false))
		assert.NoError(t, err)
		result, err := union.Result(false)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		err = union.Update(sketch2.Compact(false))
		assert.NoError(t, err)

		result, err = union.Result(false)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
	})

	t.Run("Empty, Exact", func(t *testing.T) {
		empty, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact.UpdateInt64(3)

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(empty)
		assert.NoError(t, err)
		err = union.Update(exact)
		assert.NoError(t, err)

		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Empty Compact, Exact Compact", func(t *testing.T) {
		empty, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		exact.UpdateInt64(3)

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(empty.Compact(false))
		assert.NoError(t, err)
		err = union.Update(exact.Compact(false))
		assert.NoError(t, err)

		result, err := union.Result(false)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Empty, Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6)

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(a)
		assert.NoError(t, err)
		err = union.Update(b)
		assert.NoError(t, err)

		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Empty Compact, Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6)

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(a.Compact(false))
		assert.NoError(t, err)
		err = union.Update(b.Compact(false))
		assert.NoError(t, err)

		result, err := union.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Empty, Estimation Mode", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(4)

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(a)
		assert.NoError(t, err)
		err = union.Update(b)
		assert.NoError(t, err)

		result, err := union.Result(true)
		assert.NoError(t, err)
		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Empty Compact, Estimation Mode Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(4)

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(a.Compact(false))
		assert.NoError(t, err)
		err = union.Update(b.Compact(false))
		assert.NoError(t, err)

		result, err := union.Result(false)
		assert.NoError(t, err)
		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
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

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(a)
		assert.NoError(t, err)
		err = union.Update(b)
		assert.NoError(t, err)
		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
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

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(a.Compact(false))
		assert.NoError(t, err)
		err = union.Update(b.Compact(false))
		assert.NoError(t, err)
		result, err := union.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Estimation Mode, B Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(4)

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6)

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(a)
		assert.NoError(t, err)
		err = union.Update(b)
		assert.NoError(t, err)
		result, err := union.Result(true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Estimation Mode Compact, B Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(4)

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6)

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(a.Compact(false))
		assert.NoError(t, err)
		err = union.Update(b.Compact(false))
		assert.NoError(t, err)
		result, err := union.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
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

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(sketch1)
		assert.NoError(t, err)

		err = union.Update(sketch2)
		assert.NoError(t, err)

		sketch3, err := union.Result(true)
		assert.NoError(t, err)

		assert.False(t, sketch3.IsEmpty())
		assert.False(t, sketch3.IsEstimationMode())
		assert.Equal(t, 1500.0, sketch3.Estimate())

		// Test reset
		union.Reset()
		sketch3, err = union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), sketch3.NumRetained())
		assert.True(t, sketch3.IsEmpty())
		assert.False(t, sketch3.IsEstimationMode())
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

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(sketch1.Compact(false))
		assert.NoError(t, err)

		err = union.Update(sketch2.Compact(false))
		assert.NoError(t, err)

		sketch3, err := union.Result(false)
		assert.NoError(t, err)

		assert.False(t, sketch3.IsEmpty())
		assert.False(t, sketch3.IsEstimationMode())
		assert.Equal(t, 1500.0, sketch3.Estimate())
	})

	t.Run("Exact Mode Half Overlap Wrapped", func(t *testing.T) {
		sketch1, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value := 0
		for i := 0; i < 1000; i++ {
			sketch1.UpdateInt64(int64(value))
			value++
		}
		bytes1, err := sketch1.Compact(true).MarshalBinary()
		assert.NoError(t, err)

		sketch2, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		value = 500
		for i := 0; i < 1000; i++ {
			sketch2.UpdateInt64(int64(value))
			value++
		}
		bytes2, err := sketch2.Compact(true).MarshalBinary()
		assert.NoError(t, err)

		wrappedSketch1, err := Decode(bytes1, DefaultSeed)
		assert.NoError(t, err)
		wrappedSketch2, err := Decode(bytes2, DefaultSeed)
		assert.NoError(t, err)

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(wrappedSketch1)
		assert.NoError(t, err)

		err = union.Update(wrappedSketch2)
		assert.NoError(t, err)

		sketch3, err := union.Result(true)
		assert.NoError(t, err)

		assert.False(t, sketch3.IsEmpty())
		assert.False(t, sketch3.IsEstimationMode())
		assert.Equal(t, 1500.0, sketch3.Estimate())
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

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(sketch1)
		assert.NoError(t, err)

		err = union.Update(sketch2)
		assert.NoError(t, err)

		sketch3, err := union.Result(true)
		assert.NoError(t, err)

		assert.False(t, sketch3.IsEmpty())
		assert.True(t, sketch3.IsEstimationMode())

		assert.InEpsilon(t, 15000.0, sketch3.Estimate(), 0.01)

		// Test reset
		union.Reset()
		sketch3, err = union.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, uint32(0), sketch3.NumRetained())
		assert.True(t, sketch3.IsEmpty())
		assert.False(t, sketch3.IsEstimationMode())
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

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(sketch1.Compact(false))
		assert.NoError(t, err)

		err = union.Update(sketch2.Compact(false))
		assert.NoError(t, err)

		sketch3, err := union.Result(false)
		assert.NoError(t, err)

		assert.False(t, sketch3.IsEmpty())
		assert.True(t, sketch3.IsEstimationMode())

		assert.InEpsilon(t, 15000.0, sketch3.Estimate(), 0.01)
	})

	t.Run("Exact Mode, Estimation Mode Full Overlap", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(4))

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(4))

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(a)
		assert.NoError(t, err)
		err = union.Update(b)
		assert.NoError(t, err)
		result, err := union.Result(true)
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

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(a.Compact(false))
		assert.NoError(t, err)
		err = union.Update(b.Compact(false))
		assert.NoError(t, err)
		result, err := union.Result(false)
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

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(a)
		assert.NoError(t, err)
		err = union.Update(b)
		assert.NoError(t, err)
		result, err := union.Result(true)
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

		union, err := NewUnion()
		assert.NoError(t, err)
		err = union.Update(a.Compact(false))
		assert.NoError(t, err)
		err = union.Update(b.Compact(false))
		assert.NoError(t, err)
		result, err := union.Result(false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Seed Mismatch", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		sketch.UpdateInt64(1) // non-empty should not be ignored

		union, err := NewUnion(WithUnionSeed(123))
		assert.NoError(t, err)
		err = union.Update(sketch)
		assert.ErrorContains(t, err, "seed hash mismatch")
	})

	t.Run("Larger K", func(t *testing.T) {
		updateSketch1, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(14))
		assert.NoError(t, err)
		for i := 0; i < 16384; i++ {
			updateSketch1.UpdateInt64(int64(i))
		}

		updateSketch2, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(14))
		assert.NoError(t, err)
		for i := 0; i < 26384; i++ {
			updateSketch2.UpdateInt64(int64(i))
		}

		updateSketch3, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(14))
		assert.NoError(t, err)
		for i := 0; i < 86384; i++ {
			updateSketch3.UpdateInt64(int64(i))
		}

		// First union
		union1, err := NewUnion(WithUnionLgK(16))
		assert.NoError(t, err)
		err = union1.Update(updateSketch2)
		assert.NoError(t, err)
		err = union1.Update(updateSketch1)
		assert.NoError(t, err)
		err = union1.Update(updateSketch3)
		assert.NoError(t, err)

		result1, err := union1.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, updateSketch3.Estimate(), result1.Estimate())

		// Second union with different order
		union2, err := NewUnion(WithUnionLgK(16))
		assert.NoError(t, err)
		err = union2.Update(updateSketch1)
		assert.NoError(t, err)
		err = union2.Update(updateSketch3)
		assert.NoError(t, err)
		err = union2.Update(updateSketch2)
		assert.NoError(t, err)

		result2, err := union2.Result(true)
		assert.NoError(t, err)

		assert.Equal(t, updateSketch3.Estimate(), result2.Estimate())
	})

	t.Run("lgK Too Small", func(t *testing.T) {
		_, err := NewUnion(WithUnionLgK(MinLgK - 1))
		assert.ErrorContains(t, err, "lg_k must not be less than")
	})

	t.Run("lgK Too Large", func(t *testing.T) {
		_, err := NewUnion(WithUnionLgK(MaxLgK + 1))
		assert.ErrorContains(t, err, "lg_k must not be greater than")
	})

	t.Run("P Is Zero", func(t *testing.T) {
		_, err := NewUnion(WithUnionSketchP(0))
		assert.ErrorContains(t, err, "sampling probability must be between 0 and 1")
	})

	t.Run("P Is Negative", func(t *testing.T) {
		_, err := NewUnion(WithUnionSketchP(-0.5))
		assert.ErrorContains(t, err, "sampling probability must be between 0 and 1")
	})

	t.Run("P Greater Than 1", func(t *testing.T) {
		_, err := NewUnion(WithUnionSketchP(1.5))
		assert.ErrorContains(t, err, "sampling probability must be between 0 and 1")
	})

	t.Run("Valid Minimum lgK", func(t *testing.T) {
		union, err := NewUnion(WithUnionLgK(MinLgK))
		assert.NoError(t, err)
		assert.NotNil(t, union)
	})

	t.Run("Valid Maximum lgK", func(t *testing.T) {
		union, err := NewUnion(WithUnionLgK(MaxLgK))
		assert.NoError(t, err)
		assert.NotNil(t, union)
	})

	t.Run("With Resize Factor", func(t *testing.T) {
		union, err := NewUnion(WithUnionResizeFactor(ResizeX4))
		assert.NoError(t, err)
		assert.NotNil(t, union)
	})

	t.Run("Result With QuickSelect Triggered", func(t *testing.T) {
		union, _ := NewUnion(WithUnionLgK(8))

		sketch, _ := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(10))
		for i := 0; i < 2000; i++ {
			sketch.UpdateInt64(int64(i))
		}

		union.Update(sketch)
		result, err := union.Result(true)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Result should have at most nominalNum entries
		assert.LessOrEqual(t, result.NumRetained(), uint32(256))
	})

	t.Run("Policy Returns Consistent Instance", func(t *testing.T) {
		union, _ := NewUnion()
		policy1 := union.Policy()
		policy2 := union.Policy()
		assert.Equal(t, policy1, policy2)
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

		union, _ := NewUnion()
		union.Update(sketch1)
		union.Update(sketch2)

		result, err := union.OrderedResult()
		assert.NoError(t, err)
		assert.True(t, result.IsOrdered())
		assert.Equal(t, 150.0, result.Estimate())
	})
}
