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

func assertBoundsInvariant(t *testing.T, jc JaccardSimilarityResult) {
	t.Helper()
	assert.LessOrEqual(t, jc.LowerBound, jc.Estimate)
	assert.LessOrEqual(t, jc.Estimate, jc.UpperBound)
}

func TestJaccard(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		skA, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		skB, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 1,
			Estimate:   1,
			UpperBound: 1,
		}
		assert.Equal(t, expected, jc)
		assertBoundsInvariant(t, jc)
	})

	t.Run("Only SketchA Empty", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()

		skB, _ := NewQuickSelectUpdateSketch()
		for i := 0; i < 100; i++ {
			skB.UpdateInt64(int64(i))
		}

		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 0,
			Estimate:   0,
			UpperBound: 0,
		}
		assert.Equal(t, expected, jc)
		assertBoundsInvariant(t, jc)
	})

	t.Run("Only SketchB Empty", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		for i := 0; i < 100; i++ {
			skA.UpdateInt64(int64(i))
		}

		skB, _ := NewQuickSelectUpdateSketch()

		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 0,
			Estimate:   0,
			UpperBound: 0,
		}
		assert.Equal(t, expected, jc)
		assertBoundsInvariant(t, jc)
	})

	t.Run("Same Sketch Exact Mode", func(t *testing.T) {
		sk, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			sk.UpdateInt64(int64(i))
		}

		expected := JaccardSimilarityResult{
			LowerBound: 1,
			Estimate:   1,
			UpperBound: 1,
		}

		// update sketch
		jc, err := Jaccard(sk, sk, DefaultSeed)
		assert.NoError(t, err)
		assert.Equal(t, expected, jc)
		assertBoundsInvariant(t, jc)

		// compact sketch
		compactSk := sk.Compact(true)
		jc, err = Jaccard(compactSk, compactSk, DefaultSeed)
		assert.NoError(t, err)
		assert.Equal(t, expected, jc)
		assertBoundsInvariant(t, jc)
	})

	t.Run("Full Overlap Exact Mode", func(t *testing.T) {
		skA, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		skB, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i))
			skB.UpdateInt64(int64(i))
		}

		expected := JaccardSimilarityResult{
			LowerBound: 1,
			Estimate:   1,
			UpperBound: 1,
		}

		// update sketches
		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.Equal(t, expected, jc)
		assertBoundsInvariant(t, jc)

		// compact sketches
		jc, err = Jaccard(skA.Compact(true), skB.Compact(true), DefaultSeed)
		assert.NoError(t, err)
		assert.Equal(t, expected, jc)
		assertBoundsInvariant(t, jc)
	})

	t.Run("Disjoint Exact Mode", func(t *testing.T) {
		skA, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		skB, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i))
			skB.UpdateInt64(int64(i + 1000))
		}

		expected := JaccardSimilarityResult{
			LowerBound: 0,
			Estimate:   0,
			UpperBound: 0,
		}

		// update sketches
		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.Equal(t, expected, jc)
		assertBoundsInvariant(t, jc)

		// compact sketches
		jc, err = Jaccard(skA.Compact(true), skB.Compact(true), DefaultSeed)
		assert.NoError(t, err)
		assert.Equal(t, expected, jc)
		assertBoundsInvariant(t, jc)
	})

	t.Run("Half Overlap Estimation Mode", func(t *testing.T) {
		skA, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		skB, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			skA.UpdateInt64(int64(i))
			skB.UpdateInt64(int64(i + 5000))
		}

		expectedValue := 0.33
		margin := 0.01

		// update sketches
		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.InDelta(t, expectedValue, jc.Estimate, margin)
		assert.InDelta(t, expectedValue, jc.LowerBound, margin)
		assert.InDelta(t, expectedValue, jc.UpperBound, margin)
		assertBoundsInvariant(t, jc)

		// compact sketches
		jc, err = Jaccard(skA.Compact(true), skB.Compact(true), DefaultSeed)
		assert.NoError(t, err)
		assert.InDelta(t, expectedValue, jc.Estimate, margin)
		assert.InDelta(t, expectedValue, jc.LowerBound, margin)
		assert.InDelta(t, expectedValue, jc.UpperBound, margin)
		assertBoundsInvariant(t, jc)
	})

	t.Run("Half Overlap Estimation Mode Custom Seed", func(t *testing.T) {
		seed := uint64(123)
		skA, err := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		skB, err := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			skA.UpdateInt64(int64(i))
			skB.UpdateInt64(int64(i + 5000))
		}

		expectedValue := 0.33
		margin := 0.01

		// update sketches
		jc, err := Jaccard(skA, skB, seed)
		assert.NoError(t, err)
		assert.InDelta(t, expectedValue, jc.Estimate, margin)
		assert.InDelta(t, expectedValue, jc.LowerBound, margin)
		assert.InDelta(t, expectedValue, jc.UpperBound, margin)
		assertBoundsInvariant(t, jc)

		// compact sketches
		jc, err = Jaccard(skA.Compact(true), skB.Compact(true), seed)
		assert.NoError(t, err)
		assert.InDelta(t, expectedValue, jc.Estimate, margin)
		assert.InDelta(t, expectedValue, jc.LowerBound, margin)
		assert.InDelta(t, expectedValue, jc.UpperBound, margin)
		assertBoundsInvariant(t, jc)
	})

	t.Run("Identical Sets Different Objects", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skB, _ := NewQuickSelectUpdateSketch()
		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i))
			skB.UpdateInt64(int64(i))
		}

		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 1,
			Estimate:   1,
			UpperBound: 1,
		}
		assert.Equal(t, expected, jc)
		assertBoundsInvariant(t, jc)
	})

	t.Run("Single Element Same", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skA.UpdateInt64(42)

		skB, _ := NewQuickSelectUpdateSketch()
		skB.UpdateInt64(42)

		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 1,
			Estimate:   1,
			UpperBound: 1,
		}
		assert.Equal(t, expected, jc)
		assertBoundsInvariant(t, jc)
	})

	t.Run("Single Element Different", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skA.UpdateInt64(42)

		skB, _ := NewQuickSelectUpdateSketch()
		skB.UpdateInt64(99)

		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 0,
			Estimate:   0,
			UpperBound: 0,
		}
		assert.Equal(t, expected, jc)
		assertBoundsInvariant(t, jc)
	})

	t.Run("Two Elements One Overlap", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skA.UpdateInt64(1)
		skA.UpdateInt64(2)

		skB, _ := NewQuickSelectUpdateSketch()
		skB.UpdateInt64(2)
		skB.UpdateInt64(3)

		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)

		// J = |A ∩ B| / |A ∪ B| = 1 / 3 ≈ 0.333
		assert.InDelta(t, 0.333, jc.Estimate, 0.01)
		assert.InDelta(t, 0.333, jc.LowerBound, 0.01)
		assert.InDelta(t, 0.333, jc.UpperBound, 0.01)
		assertBoundsInvariant(t, jc)
	})

	t.Run("Two Elements No Overlap", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skA.UpdateInt64(1)
		skA.UpdateInt64(2)

		skB, _ := NewQuickSelectUpdateSketch()
		skB.UpdateInt64(3)
		skB.UpdateInt64(4)

		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 0,
			Estimate:   0,
			UpperBound: 0,
		}
		assert.Equal(t, expected, jc)
		assertBoundsInvariant(t, jc)
	})

	t.Run("Two Elements Full Overlap", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skA.UpdateInt64(1)
		skA.UpdateInt64(2)

		skB, _ := NewQuickSelectUpdateSketch()
		skB.UpdateInt64(1)
		skB.UpdateInt64(2)

		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)

		expected := JaccardSimilarityResult{
			LowerBound: 1,
			Estimate:   1,
			UpperBound: 1,
		}
		assert.Equal(t, expected, jc)
		assertBoundsInvariant(t, jc)
	})

	t.Run("Subset Relationship - Small Subset of Large", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skB, _ := NewQuickSelectUpdateSketch()

		for i := 0; i < 100; i++ {
			skA.UpdateInt64(int64(i))
		}
		for i := 0; i < 1000; i++ {
			skB.UpdateInt64(int64(i))
		}

		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assertBoundsInvariant(t, jc)

		// J = |A ∩ B| / |A ∪ B| = 100 / 1000 = 0.1
		assert.InDelta(t, 0.1, jc.Estimate, 0.01)
	})

	t.Run("Subset Relationship - Large Subset of Larger", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skB, _ := NewQuickSelectUpdateSketch()

		for i := 0; i < 5000; i++ {
			skA.UpdateInt64(int64(i))
		}
		for i := 0; i < 10000; i++ {
			skB.UpdateInt64(int64(i))
		}

		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assertBoundsInvariant(t, jc)

		// J = |A ∩ B| / |A ∪ B| = 5000 / 10000 = 0.5
		assert.InDelta(t, 0.5, jc.Estimate, 0.02)
	})

	t.Run("Subset Relationship - Reversed Order", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skB, _ := NewQuickSelectUpdateSketch()

		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i))
		}
		for i := 0; i < 100; i++ {
			skB.UpdateInt64(int64(i))
		}

		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assertBoundsInvariant(t, jc)

		// J = |A ∩ B| / |A ∪ B| = 100 / 1000 = 0.1
		assert.InDelta(t, 0.1, jc.Estimate, 0.01)
	})

	t.Run("Subset Relationship - Almost Complete Subset", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skB, _ := NewQuickSelectUpdateSketch()

		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i))
		}
		for i := 0; i < 900; i++ {
			skB.UpdateInt64(int64(i))
		}

		jc, err := Jaccard(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assertBoundsInvariant(t, jc)

		// J = |A ∩ B| / |A ∪ B| = 900 / 1000 = 0.9
		assert.InDelta(t, 0.9, jc.Estimate, 0.01)
	})

	t.Run("Mismatched Seeds - Error Case", func(t *testing.T) {
		seedA := uint64(123)
		seedB := uint64(456)

		skA, err := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(seedA))
		assert.NoError(t, err)
		skB, err := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(seedB))
		assert.NoError(t, err)

		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i))
			skB.UpdateInt64(int64(i))
		}

		jc, err := Jaccard(skA, skB, seedA)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "seed hash mismatch")

		assert.Equal(t, JaccardSimilarityResult{}, jc)
	})

	t.Run("Mismatched Seeds - Same Seed for Jaccard as Sketches", func(t *testing.T) {
		seed := uint64(789)

		skA, err := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		skB, err := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(seed))
		assert.NoError(t, err)

		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i))
			skB.UpdateInt64(int64(i))
		}

		jc, err := Jaccard(skA, skB, seed)
		assert.NoError(t, err)
		assertBoundsInvariant(t, jc)

		expected := JaccardSimilarityResult{
			LowerBound: 1,
			Estimate:   1,
			UpperBound: 1,
		}
		assert.Equal(t, expected, jc)
	})

	t.Run("Mismatched Seeds - Both Sketches Wrong Seed Different from Jaccard Seed", func(t *testing.T) {
		sketchSeed := uint64(111)
		jaccardSeed := uint64(222)

		skA, err := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(sketchSeed))
		assert.NoError(t, err)
		skB, err := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(sketchSeed))
		assert.NoError(t, err)

		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i))
			skB.UpdateInt64(int64(i))
		}

		jc, err := Jaccard(skA, skB, jaccardSeed)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "seed hash mismatch")

		assert.Equal(t, JaccardSimilarityResult{}, jc)
	})
}

func TestIsExactlyEqual(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		skA, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		skB, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)

		result, err := IsExactlyEqual(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Only SketchA Empty", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()

		skB, _ := NewQuickSelectUpdateSketch()
		for i := 0; i < 100; i++ {
			skB.UpdateInt64(int64(i))
		}

		result, err := IsExactlyEqual(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Only SketchB Empty", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		for i := 0; i < 100; i++ {
			skA.UpdateInt64(int64(i))
		}

		skB, _ := NewQuickSelectUpdateSketch()

		result, err := IsExactlyEqual(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Same Sketch Exact Mode", func(t *testing.T) {
		sk, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			sk.UpdateInt64(int64(i))
		}

		// update sketch
		result, err := IsExactlyEqual(sk, sk, DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)

		// compact sketch
		compactSk := sk.Compact(true)
		result, err = IsExactlyEqual(compactSk, compactSk, DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Full Overlap Exact Mode", func(t *testing.T) {
		skA, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		skB, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i))
			skB.UpdateInt64(int64(i))
		}

		// update sketches
		result, err := IsExactlyEqual(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)

		// compact sketches
		result, err = IsExactlyEqual(skA.Compact(true), skB.Compact(true), DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Disjoint Exact Mode", func(t *testing.T) {
		skA, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		skB, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i))
			skB.UpdateInt64(int64(i + 1000))
		}

		// update sketches
		result, err := IsExactlyEqual(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)

		// compact sketches
		result, err = IsExactlyEqual(skA.Compact(true), skB.Compact(true), DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Half Overlap Estimation Mode", func(t *testing.T) {
		skA, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		skB, err := NewQuickSelectUpdateSketch()
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			skA.UpdateInt64(int64(i))
			skB.UpdateInt64(int64(i + 5000))
		}

		// update sketches
		result, err := IsExactlyEqual(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)

		// compact sketches
		result, err = IsExactlyEqual(skA.Compact(true), skB.Compact(true), DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Identical Sets Different Objects", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skB, _ := NewQuickSelectUpdateSketch()
		for i := 0; i < 1000; i++ {
			skA.UpdateInt64(int64(i))
			skB.UpdateInt64(int64(i))
		}

		result, err := IsExactlyEqual(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Single Element Same", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skA.UpdateInt64(42)

		skB, _ := NewQuickSelectUpdateSketch()
		skB.UpdateInt64(42)

		result, err := IsExactlyEqual(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Single Element Different", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skA.UpdateInt64(42)

		skB, _ := NewQuickSelectUpdateSketch()
		skB.UpdateInt64(99)

		result, err := IsExactlyEqual(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Two Elements One Overlap", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skA.UpdateInt64(1)
		skA.UpdateInt64(2)

		skB, _ := NewQuickSelectUpdateSketch()
		skB.UpdateInt64(2)
		skB.UpdateInt64(3)

		result, err := IsExactlyEqual(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Two Elements No Overlap", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skA.UpdateInt64(1)
		skA.UpdateInt64(2)

		skB, _ := NewQuickSelectUpdateSketch()
		skB.UpdateInt64(3)
		skB.UpdateInt64(4)

		result, err := IsExactlyEqual(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Two Elements Full Overlap", func(t *testing.T) {
		skA, _ := NewQuickSelectUpdateSketch()
		skA.UpdateInt64(1)
		skA.UpdateInt64(2)

		skB, _ := NewQuickSelectUpdateSketch()
		skB.UpdateInt64(1)
		skB.UpdateInt64(2)

		result, err := IsExactlyEqual(skA, skB, DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Custom Seed", func(t *testing.T) {
		seed := uint64(123)
		skA, err := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		skB, err := NewQuickSelectUpdateSketch(WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		for i := 0; i < 10000; i++ {
			skA.UpdateInt64(int64(i))
			skB.UpdateInt64(int64(i))
		}

		result, err := IsExactlyEqual(skA, skB, seed)
		assert.NoError(t, err)
		assert.True(t, result)
	})
}

func TestIsSimilarity(t *testing.T) {
	// The distribution is quite tight, about +/- 0.7%, which is pretty good since the accuracy of the
	// underlying sketch is about +/- 1.56%.
	t.Run("Default Seed", func(t *testing.T) {
		minLgK := uint8(12)
		u1 := 1 << 20
		u2 := int(float64(u1) * 0.95)
		threshold := 0.943

		expected, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(minLgK))
		assert.NoError(t, err)
		for i := 0; i < u1; i++ {
			expected.UpdateInt64(int64(i))
		}

		actual, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(minLgK))
		assert.NoError(t, err)
		for i := 0; i < u2; i++ {
			actual.UpdateInt64(int64(i))
		}

		result, err := IsSimilar(actual, expected, threshold, DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)

		result, err = IsSimilar(actual, actual, threshold, DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)
	})

	t.Run("Custom Seed", func(t *testing.T) {
		minLgK := uint8(12)
		u1 := 1 << 20
		u2 := int(float64(u1) * 0.95)
		threshold := 0.943
		seed := uint64(1234)

		expected, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(minLgK), WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		for i := 0; i < u1; i++ {
			expected.UpdateInt64(int64(i))
		}

		actual, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(minLgK), WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		for i := 0; i < u2; i++ {
			actual.UpdateInt64(int64(i))
		}

		result, err := IsSimilar(actual, expected, threshold, seed)
		assert.NoError(t, err)
		assert.True(t, result)

		result, err = IsSimilar(actual, actual, threshold, seed)
		assert.NoError(t, err)
		assert.True(t, result)
	})
}

func TestIsDissimilar(t *testing.T) {
	// The distribution is much looser here, about +/- 14%. This is due to the fact that intersections lose accuracy
	// as the ratio of intersection to the union becomes a small number.
	t.Run("Default Seed", func(t *testing.T) {
		minLgK := uint8(12)
		u1 := 1 << 20
		u2 := int(float64(u1) * 0.05)
		threshold := 0.061

		expected, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(minLgK))
		assert.NoError(t, err)
		for i := 0; i < u1; i++ {
			expected.UpdateInt64(int64(i))
		}

		actual, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(minLgK))
		assert.NoError(t, err)
		for i := 0; i < u2; i++ {
			actual.UpdateInt64(int64(i))
		}

		result, err := IsDissimilar(actual, expected, threshold, DefaultSeed)
		assert.NoError(t, err)
		assert.True(t, result)

		result, err = IsDissimilar(actual, actual, threshold, DefaultSeed)
		assert.NoError(t, err)
		assert.False(t, result)
	})

	t.Run("Custom Seed", func(t *testing.T) {
		minLgK := uint8(12)
		u1 := 1 << 20
		u2 := int(float64(u1) * 0.05)
		threshold := 0.061
		seed := uint64(1234)

		expected, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(minLgK), WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		for i := 0; i < u1; i++ {
			expected.UpdateInt64(int64(i))
		}

		actual, err := NewQuickSelectUpdateSketch(WithUpdateSketchLgK(minLgK), WithUpdateSketchSeed(seed))
		assert.NoError(t, err)
		for i := 0; i < u2; i++ {
			actual.UpdateInt64(int64(i))
		}

		result, err := IsDissimilar(actual, expected, threshold, seed)
		assert.NoError(t, err)
		assert.True(t, result)

		result, err = IsDissimilar(actual, actual, threshold, seed)
		assert.NoError(t, err)
		assert.False(t, result)
	})
}
