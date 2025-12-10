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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestANotB(t *testing.T) {
	t.Run("A Empty B Empty", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		b, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.Zero(t, result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("A Empty Compact B Empty Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		b, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		result, err := ANotB(a.Compact(false), b.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)

		assert.Zero(t, result.NumRetained())
		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("A Empty B Exact", func(t *testing.T) {
		sketchA, _ := NewQuickSelectUpdateSketch()
		sketchB, _ := NewQuickSelectUpdateSketch()

		_ = sketchB.UpdateInt64(1)
		_ = sketchB.UpdateInt64(2)
		assert.False(t, sketchB.IsEstimationMode())

		result, err := ANotB(sketchA, sketchB, DefaultSeed, false)
		assert.NoError(t, err)
		assert.True(t, result.IsEmpty())
		assert.Equal(t, uint32(0), result.NumRetained())
	})

	t.Run("A Empty Compact B Exact Compact", func(t *testing.T) {
		sketchA, _ := NewQuickSelectUpdateSketch()
		sketchB, _ := NewQuickSelectUpdateSketch()

		_ = sketchB.UpdateInt64(1)
		_ = sketchB.UpdateInt64(2)
		assert.False(t, sketchB.IsEstimationMode())

		result, err := ANotB(sketchA.Compact(false), sketchB.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)
		assert.True(t, result.IsEmpty())
		assert.Equal(t, uint32(0), result.NumRetained())
	})

	t.Run("A Exact B Empty", func(t *testing.T) {
		sketchA, _ := NewQuickSelectUpdateSketch()
		sketchB, _ := NewQuickSelectUpdateSketch()

		_ = sketchA.UpdateInt64(1)
		_ = sketchA.UpdateInt64(2)
		assert.False(t, sketchA.IsEstimationMode())

		result, err := ANotB(sketchA, sketchB, DefaultSeed, false)
		assert.NoError(t, err)
		assert.False(t, result.IsEmpty())
		assert.Equal(t, uint32(2), result.NumRetained())
	})

	t.Run("A Exact Compact B Empty Compact", func(t *testing.T) {
		sketchA, _ := NewQuickSelectUpdateSketch()
		sketchB, _ := NewQuickSelectUpdateSketch()

		_ = sketchA.UpdateInt64(1)
		_ = sketchA.UpdateInt64(2)
		assert.False(t, sketchA.IsEstimationMode())

		result, err := ANotB(sketchA.Compact(false), sketchB.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)
		assert.False(t, result.IsEmpty())
		assert.Equal(t, uint32(2), result.NumRetained())
	})

	t.Run("A Empty, B Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6)

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("A Empty Compact, B Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6)

		result, err := ANotB(a.Compact(false), b.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys, B Empty", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		a.UpdateInt64(6)

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys Compact, B Empty Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		a.UpdateInt64(6)

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Empty, B Estimation Mode", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(4)

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("A Empty Compact, B Estimation Mode Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(4)

		result, err := ANotB(a.Compact(false), b.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)

		assert.Equal(t, 1.0, result.Theta())
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.True(t, result.IsEmpty())
	})

	t.Run("A Estimation Mode, B Empty", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		a.UpdateInt64(4)

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Estimation Mode Compact, B Empty Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		a.UpdateInt64(4)

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)

		result, err := ANotB(a.Compact(false), b.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Exact, B Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6)

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Exact Compact, B Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(4)

		b, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		b.UpdateInt64(6)

		result, err := ANotB(a.Compact(false), b.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys, B Exact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		a.UpdateInt64(6)

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		b.UpdateInt64(4)

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys Compact, B Exact Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(
			WithUpdateSketchLgK(5), WithUpdateSketchP(0.1),
		)
		assert.NoError(t, err)
		a.UpdateInt64(6)

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		b.UpdateInt64(4)

		result, err := ANotB(a.Compact(false), b.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Estimation Mode, B Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(4)

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(6)

		result, err := ANotB(a, b, DefaultSeed, true)
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

		result, err := ANotB(a.Compact(false), b.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(1), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys, B Estimation Mode", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(6)

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(4)

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys Compact, B Estimation Mode Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(6)

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(4)

		result, err := ANotB(a.Compact(false), b.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Exact Mode Disjoint", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 1000; i++ {
			a.UpdateInt64(int64(value))
			value++
		}

		b, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		for i := 0; i < 1000; i++ {
			b.UpdateInt64(int64(value))
			value++
		}

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 1000.0, result.Estimate())
	})

	t.Run("Exact Mode Compact Disjoint", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 1000; i++ {
			a.UpdateInt64(int64(value))
			value++
		}

		b, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		for i := 0; i < 1000; i++ {
			b.UpdateInt64(int64(value))
			value++
		}

		result, err := ANotB(a.Compact(false), b.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 1000.0, result.Estimate())
	})

	t.Run("Exact Mode Half Overlap", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 1000; i++ {
			a.UpdateInt64(int64(value))
			value++
		}

		b, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value = 500
		for i := 0; i < 1000; i++ {
			b.UpdateInt64(int64(value))
			value++
		}

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.True(t, result.IsOrdered())
		assert.Equal(t, 500.0, result.Estimate())
	})

	t.Run("Exact Mode Compact Half Overlap", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 1000; i++ {
			a.UpdateInt64(int64(value))
			value++
		}

		b, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value = 500
		for i := 0; i < 1000; i++ {
			b.UpdateInt64(int64(value))
			value++
		}

		result, err := ANotB(a.Compact(false), b.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.False(t, result.IsOrdered())
		assert.Equal(t, 500.0, result.Estimate())
	})

	t.Run("Exact Mode Full Overlap", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 1000; i++ {
			sketch.UpdateInt64(int64(value))
			value++
		}

		result, err := ANotB(sketch, sketch, DefaultSeed, true)
		assert.NoError(t, err)

		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Exact Mode Compact Full Overlap", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 1000; i++ {
			sketch.UpdateInt64(int64(value))
			value++
		}

		compact := sketch.Compact(false)
		result, err := ANotB(compact, compact, DefaultSeed, false)
		assert.NoError(t, err)

		assert.True(t, result.IsEmpty())
		assert.False(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Estimation Mode Disjoint", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 10000; i++ {
			a.UpdateInt64(int64(value))
			value++
		}

		b, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		for i := 0; i < 10000; i++ {
			b.UpdateInt64(int64(value))
			value++
		}

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())

		margin := 10000.0 * 0.02
		assert.Less(t, math.Abs(result.Estimate()-10000.0), margin)
	})

	t.Run("Estimation Mode Compact Disjoint", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 10000; i++ {
			a.UpdateInt64(int64(value))
			value++
		}

		b, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		for i := 0; i < 10000; i++ {
			b.UpdateInt64(int64(value))
			value++
		}

		result, err := ANotB(a.Compact(false), b.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)

		margin := 10000.0 * 0.02
		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())
		assert.Less(t, math.Abs(result.Estimate()-10000.0), margin)
	})

	t.Run("Estimation Mode Half Overlap", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 10000; i++ {
			a.UpdateInt64(int64(value))
			value++
		}

		b, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value = 5000
		for i := 0; i < 10000; i++ {
			b.UpdateInt64(int64(value))
			value++
		}

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())

		margin := 5000.0 * 0.02
		assert.Less(t, math.Abs(result.Estimate()-5000.0), margin)
	})

	t.Run("Estimation Mode Compact Half Overlap", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 10000; i++ {
			a.UpdateInt64(int64(value))
			value++
		}

		b, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value = 5000
		for i := 0; i < 10000; i++ {
			b.UpdateInt64(int64(value))
			value++
		}

		result, err := ANotB(a.Compact(false), b.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())
		margin := 5000.0 * 0.02
		assert.Less(t, math.Abs(result.Estimate()-5000.0), margin)
	})

	t.Run("Estimation Mode Half Overlap Wrapped", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 10000; i++ {
			a.UpdateInt64(int64(value))
			value++
		}

		var bytesA bytes.Buffer
		encoder := NewEncoder(&bytesA, false)
		err = encoder.Encode(a.CompactOrdered())
		assert.NoError(t, err)

		b, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value = 5000
		for i := 0; i < 10000; i++ {
			b.UpdateInt64(int64(value))
			value++
		}

		var bytesB bytes.Buffer
		encoder = NewEncoder(&bytesB, false)
		err = encoder.Encode(b.CompactOrdered())
		assert.NoError(t, err)

		wrappedA, err := WrapCompactSketch(bytesA.Bytes(), DefaultSeed)
		assert.NoError(t, err)

		wrappedB, err := WrapCompactSketch(bytesB.Bytes(), DefaultSeed)
		assert.NoError(t, err)

		result, err := ANotB(wrappedA, wrappedB, DefaultSeed, true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())

		margin := 5000.0 * 0.02
		assert.Less(t, math.Abs(result.Estimate()-5000.0), margin)
	})

	t.Run("Estimation Mode Full Overlap", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 10000; i++ {
			sketch.UpdateInt64(int64(value))
			value++
		}

		result, err := ANotB(sketch, sketch, DefaultSeed, true)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("Estimation Mode Compact Full Overlap", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		value := 0
		for i := 0; i < 10000; i++ {
			sketch.UpdateInt64(int64(value))
			value++
		}

		compact := sketch.Compact(false)
		result, err := ANotB(compact, compact, DefaultSeed, false)
		assert.NoError(t, err)

		assert.False(t, result.IsEmpty())
		assert.True(t, result.IsEstimationMode())
		assert.Equal(t, 0.0, result.Estimate())
	})

	t.Run("A Exact Mode, B Estimation Mode Full Overlap", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(4))

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(4))

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Exact Mode Compact, B Estimation Mode Compact Full Overlap", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(4))

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(4))

		result, err := ANotB(a.Compact(false), b.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys, B Non Empty No Retained Keys", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(3))

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(6))

		result, err := ANotB(a, b, DefaultSeed, true)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("A Non Empty No Retained Keys Compact, B Non Empty No Retained Keys Compact", func(t *testing.T) {
		a, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.5))
		assert.NoError(t, err)
		a.UpdateInt64(int64(3))

		b, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(5), WithUpdateSketchP(0.1))
		assert.NoError(t, err)
		b.UpdateInt64(int64(6))

		result, err := ANotB(a.Compact(false), b.Compact(false), DefaultSeed, false)
		assert.NoError(t, err)

		assert.InDelta(t, 0.1, result.Theta(), 1e-8)
		assert.Equal(t, uint32(0), result.NumRetained())
		assert.False(t, result.IsEmpty())
	})

	t.Run("Seed Mismatch A", func(t *testing.T) {
		seed1 := uint64(9001)
		seed2 := uint64(12345)

		sketchA, _ := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(seed2))
		sketchB, _ := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(seed1))

		_ = sketchA.UpdateInt64(1)
		_ = sketchB.UpdateInt64(2)

		_, err := ANotB(sketchA, sketchB, seed1, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sketch A seed hash mismatch")
	})

	t.Run("Seed Mismatch B", func(t *testing.T) {
		seed1 := uint64(9001)
		seed2 := uint64(12345)

		sketchA, _ := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(seed1))
		sketchB, _ := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(seed2))

		_ = sketchA.UpdateInt64(1)
		_ = sketchB.UpdateInt64(2)

		_, err := ANotB(sketchA, sketchB, seed1, false)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sketch B seed hash mismatch")
	})

	t.Run("Seed Mismatch Both", func(t *testing.T) {
		sketch, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		sketch.UpdateInt64(1) // non-empty should not be ignored

		_, err = ANotB(sketch, sketch, 123, true)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "seed hash mismatch")
	})
}
