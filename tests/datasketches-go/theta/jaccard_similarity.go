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
	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
)

// JaccardSimilarityResult represents the result of Jaccard similarity computation
// with lower bound, estimate, and upper bound
type JaccardSimilarityResult struct {
	LowerBound float64
	Estimate   float64
	UpperBound float64
}

// Jaccard computes the Jaccard similarity index with upper and lower bounds.
// The Jaccard similarity index J(A,B) = (A ∩ B)/(A ∪ B) is used to measure
// how similar the two sketches are to each other. If J = 1.0, the sketches
// are considered equal. If J = 0, the two sketches are disjoint. A Jaccard
// of 0.95 means the overlap between the two sets is 95% of the union of the
// two sets.
//
// The seed parameter should match the seed used to create sketchA and sketchB.
// The returned JaccardSimilarityResult contains LowerBound, Estimate, and
// UpperBound of the Jaccard index. The Upper and Lower bounds are for a
// confidence interval of 95.4% or +/- 2 standard deviations.
//
// Note: For very large pairs of sketches, where the configured nominal entries
// of the sketches are 2^25 or 2^26, this method may produce unpredictable results.
func Jaccard(sketchA, sketchB Sketch, seed uint64) (JaccardSimilarityResult, error) {
	if sketchA == sketchB {
		return JaccardSimilarityResult{1, 1, 1}, nil
	}
	if sketchA.IsEmpty() && sketchB.IsEmpty() {
		return JaccardSimilarityResult{1, 1, 1}, nil
	}
	if sketchA.IsEmpty() || sketchB.IsEmpty() {
		return JaccardSimilarityResult{0, 0, 0}, nil
	}

	unionAB, err := computeUnion(sketchA, sketchB, seed)
	if err != nil {
		return JaccardSimilarityResult{}, err
	}

	if identicalSets(sketchA, sketchB, unionAB) {
		return JaccardSimilarityResult{1, 1, 1}, nil
	}

	intersection := NewIntersection(WithIntersectionSeed(seed))
	if err := intersection.Update(sketchA); err != nil {
		return JaccardSimilarityResult{}, err
	}
	if err := intersection.Update(sketchB); err != nil {
		return JaccardSimilarityResult{}, err
	}
	// ensures that intersection is a subset of the union
	if err := intersection.Update(unionAB); err != nil {
		return JaccardSimilarityResult{}, err
	}

	interABU, err := intersection.Result(false)
	if err != nil {
		return JaccardSimilarityResult{}, err
	}

	lb, err := lowerBoundForBOverAInSketchedSets(unionAB, interABU)
	if err != nil {
		return JaccardSimilarityResult{}, err
	}

	est, err := estimateOfBOverAInSketchedSets(unionAB, interABU)
	if err != nil {
		return JaccardSimilarityResult{}, err
	}

	ub, err := upperBoundForBOverAInSketchedSets(unionAB, interABU)
	if err != nil {
		return JaccardSimilarityResult{}, err
	}

	return JaccardSimilarityResult{
		LowerBound: lb,
		Estimate:   est,
		UpperBound: ub,
	}, nil
}

// IsExactlyEqual returns true if the two given sketches are equivalent.
// The seed parameter should match the seed used to create sketchA and sketchB.
func IsExactlyEqual(sketchA, sketchB Sketch, seed uint64) (bool, error) {
	if sketchA == sketchB {
		return true, nil
	}
	if sketchA.IsEmpty() && sketchB.IsEmpty() {
		return true, nil
	}
	if sketchA.IsEmpty() || sketchB.IsEmpty() {
		return false, nil
	}

	unionAB, err := computeUnion(sketchA, sketchB, seed)
	if err != nil {
		return false, err
	}

	return identicalSets(sketchA, sketchB, unionAB), nil
}

// IsSimilar tests similarity of an actual Sketch against an expected Sketch.
// It computes the lower bound of the Jaccard index J_LB of the actual and expected sketches.
// If J_LB >= threshold, then the sketches are considered to be similar with a confidence of 97.7%.
// The actual parameter is the sketch to be tested, and expected is the reference sketch
// that is considered to be correct. The threshold should be a real value between zero and one.
// The seed parameter should match the seed used to create the sketches. It returns true if
// the similarity of the two sketches is greater than the given threshold with at least 97.7%
// confidence.
func IsSimilar(actual, expected Sketch, threshold float64, seed uint64) (bool, error) {
	jc, err := Jaccard(actual, expected, seed)
	if err != nil {
		return false, err
	}
	return jc.LowerBound >= threshold, nil
}

// IsDissimilar tests dissimilarity of an actual Sketch against an expected Sketch.
// It computes the upper bound of the Jaccard index J_UB of the actual and expected sketches.
// If J_UB <= threshold, then the sketches are considered to be dissimilar with a confidence of 97.7%.
// The actual parameter is the sketch to be tested, and expected is the reference sketch
// that is considered to be correct. The threshold should be a real value between zero and one.
// The seed parameter should match the seed used to create the sketches. It returns true if
// the dissimilarity of the two sketches is greater than the given threshold with at least 97.7%
// confidence.
func IsDissimilar(actual, expected Sketch, threshold float64, seed uint64) (bool, error) {
	jc, err := Jaccard(actual, expected, seed)
	if err != nil {
		return false, err
	}
	return jc.UpperBound <= threshold, nil
}

func computeUnion(sketchA, sketchB Sketch, seed uint64) (Sketch, error) {
	countA := sketchA.NumRetained()
	countB := sketchB.NumRetained()

	lgKValue := internal.Log2Floor(uint32(common.CeilingPowerOf2(int(countA + countB))))
	if lgKValue < MinLgK {
		lgKValue = MinLgK
	}
	if lgKValue > MaxLgK {
		lgKValue = MaxLgK
	}

	union, err := NewUnion(
		WithUnionLgK(lgKValue),
		WithUnionSeed(seed),
	)
	if err != nil {
		return nil, err
	}

	if err := union.Update(sketchA); err != nil {
		return nil, err
	}
	if err := union.Update(sketchB); err != nil {
		return nil, err
	}

	return union.Result(false)
}

func identicalSets(sketchA, sketchB, unionAB Sketch) bool {
	return unionAB.NumRetained() == sketchA.NumRetained() &&
		unionAB.NumRetained() == sketchB.NumRetained() &&
		unionAB.Theta64() == sketchA.Theta64() &&
		unionAB.Theta64() == sketchB.Theta64()
}
