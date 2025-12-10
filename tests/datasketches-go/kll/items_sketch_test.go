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

package kll

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/datasketches-go/common"
)

const (
	PMF_EPS_FOR_K_8         = 0.35  // PMF rank error (epsilon) for k=8
	PMF_EPS_FOR_K_256       = 0.013 // PMF rank error (epsilon) for k=256
	NUMERIC_NOISE_TOLERANCE = 1e-6
)

func TestItemsSketch_KLimits(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	_, err := NewKllItemsSketch[string](_MIN_K, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	_, err = NewKllItemsSketch[string](uint16(_MAX_K), _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	_, err = NewKllItemsSketch[string](_MIN_K-1, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.Error(t, err)
}

func TestItemsSketch_Empty(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch, err := NewKllItemsSketch[string](200, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	assert.True(t, sketch.IsEmpty())
	assert.False(t, sketch.IsEstimationMode())
	assert.Equal(t, uint64(0), sketch.GetN())
	assert.Equal(t, uint32(0), sketch.GetNumRetained())
	_, err = sketch.GetMinItem()
	assert.Error(t, err)
	_, err = sketch.GetMaxItem()
	assert.Error(t, err)
	_, err = sketch.GetRank("", true)
	assert.Error(t, err)
	_, err = sketch.GetQuantile(0.5, true)
	assert.Error(t, err)
	splitPoints := []string{""}
	_, err = sketch.GetPMF(splitPoints, true)
	assert.Error(t, err)
	_, err = sketch.GetCDF(splitPoints, true)
	assert.Error(t, err)
}

func TestItemsSketch_BadQuantile(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch, err := NewKllItemsSketch[string](200, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sketch.Update("") // has to be non-empty to reach the check
	_, err = sketch.GetQuantile(-1, true)
	assert.Error(t, err)
}

func TestItemsSketch_OneValue(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch, err := NewKllItemsSketch[string](200, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sketch.Update("A")
	assert.False(t, sketch.IsEmpty())
	assert.Equal(t, uint64(1), sketch.GetN())
	assert.Equal(t, uint32(1), sketch.GetNumRetained())
	v, err := sketch.GetRank("A", false)
	assert.Equal(t, float64(0), v)
	v, err = sketch.GetRank("B", false)
	assert.Equal(t, float64(1), v)
	v, err = sketch.GetRank("A", false)
	assert.Equal(t, float64(0), v)
	v, err = sketch.GetRank("B", false)
	assert.Equal(t, float64(1), v)
	v, err = sketch.GetRank("@", true)
	assert.Equal(t, float64(0), v)
	v, err = sketch.GetRank("A", true)
	assert.Equal(t, float64(1), v)
	s, err := sketch.GetMinItem()
	assert.Equal(t, "A", s)
	s, err = sketch.GetMaxItem()
	assert.Equal(t, "A", s)
	s, err = sketch.GetQuantile(0.5, false)
	assert.Equal(t, "A", s)
	s, err = sketch.GetQuantile(0.5, true)
	assert.Equal(t, "A", s)
}

func TestItemsSketch_TenValues(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	tenStr := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	sketch, err := NewKllItemsSketch[string](20, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	strLen := len(tenStr)
	dblStrLen := float64(strLen)
	for i := 1; i <= strLen; i++ {
		sketch.Update(tenStr[i-1])
	}
	assert.False(t, sketch.IsEmpty())
	assert.Equal(t, uint64(strLen), sketch.GetN())
	assert.Equal(t, uint32(strLen), sketch.GetNumRetained())
	for i := 1; i <= strLen; i++ {
		v, err := sketch.GetRank(tenStr[i-1], false)
		assert.Equal(t, float64(i-1)/dblStrLen, v, "i: %d", i)
		assert.NoError(t, err, "i: %d", i)
		v, err = sketch.GetRank(tenStr[i-1], true)
		assert.Equal(t, float64(i)/dblStrLen, v)
		assert.NoError(t, err)
	}
	qArr := tenStr
	rOut, err := sketch.GetRanks(qArr, true) //inclusive
	assert.NoError(t, err)
	for i := 0; i < len(qArr); i++ {
		assert.Equal(t, float64(i+1)/dblStrLen, rOut[i])
	}
	rOut, err = sketch.GetRanks(qArr, false) //exclusive
	assert.NoError(t, err)
	for i := 0; i < len(qArr); i++ {
		assert.Equal(t, float64(i)/dblStrLen, rOut[i])
	}

	for i := 0; i <= strLen; i++ {
		rank := float64(i) / dblStrLen
		var q string
		if rank == 1.0 {
			q = tenStr[i-1]
		} else {
			q = tenStr[i]
		}
		s, err := sketch.GetQuantile(rank, false)
		assert.Equal(t, q, s, "i: %d", i)
		assert.NoError(t, err)
		if rank == 0 {
			q = tenStr[i]
		} else {
			q = tenStr[i-1]
		}
		s, err = sketch.GetQuantile(rank, true)
		assert.Equal(t, q, s)
		assert.NoError(t, err)
	}

	{
		// getQuantile() and getQuantiles() equivalence EXCLUSIVE
		quantiles, err := sketch.GetQuantiles([]float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}, false)
		assert.NoError(t, err)
		for i := 0; i <= 10; i++ {
			q, err := sketch.GetQuantile(float64(i)/10.0, false)
			assert.NoError(t, err)
			assert.Equal(t, q, quantiles[i])
		}
	}

	{
		// getQuantile() and getQuantiles() equivalence INCLUSIVE
		quantiles, err := sketch.GetQuantiles([]float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}, true)
		assert.NoError(t, err)
		for i := 0; i <= 10; i++ {
			q, err := sketch.GetQuantile(float64(i)/10.0, true)
			assert.NoError(t, err)
			assert.Equal(t, q, quantiles[i])
		}
	}
}

func TestItemsSketch_ManyValuesEstimationMode(T *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(T, err)
	n := 1_000_000
	digits := numDigits(n)

	for i := 1; i <= n; i++ {
		// i == 201
		sketch.Update(intToFixedLengthString(i, digits))
		assert.Equal(T, uint64(i), sketch.GetN())
	}

	lastItem := sketch.items[len(sketch.items)-1]
	assert.NotNil(T, lastItem)
	// test getRank
	for i := 1; i <= 1; i++ {
		trueRank := float64(i) / float64(n)
		s := intToFixedLengthString(i, digits)
		r, err := sketch.GetRank(s, true)
		assert.InDelta(T, trueRank, r, PMF_EPS_FOR_K_256)
		assert.NoError(T, err)
	}

	s := intToFixedLengthString(n/2, digits)
	pmf, err := sketch.GetPMF([]string{s}, true) // split at median
	assert.NoError(T, err)
	assert.Equal(T, 2, len(pmf))
	assert.InDelta(T, 0.5, pmf[0], PMF_EPS_FOR_K_256)
	assert.InDelta(T, 0.5, pmf[1], PMF_EPS_FOR_K_256)

	minV, err := sketch.GetMinItem()
	assert.NoError(T, err)
	assert.Equal(T, intToFixedLengthString(1, digits), minV)

	maxV, err := sketch.GetMaxItem()
	assert.NoError(T, err)
	assert.Equal(T, intToFixedLengthString(n, digits), maxV)

	// check at every 0.1 percentage point
	fractions := make([]float64, 1001)
	reverseFractions := make([]float64, 1001) // check that ordering doesn't matter
	for i := 0; i <= 1000; i++ {
		fractions[i] = float64(i) / 1000.0
		reverseFractions[1000-i] = fractions[i]
	}
	quantiles, err := sketch.GetQuantiles(fractions, true)
	assert.NoError(T, err)
	reverseQuantiles, err := sketch.GetQuantiles(reverseFractions, true)
	assert.NoError(T, err)
	previousQuantile := ""
	for i := 0; i <= 1000; i++ {
		quantile, err := sketch.GetQuantile(fractions[i], true)
		assert.NoError(T, err)
		assert.Equal(T, quantile, quantiles[i])
		assert.Equal(T, quantile, reverseQuantiles[1000-i])
		assert.True(T, previousQuantile <= quantile)
		previousQuantile = quantile
	}
}

func TestItemsSketch_GetRankGetCdfGetPmfConsistency(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	n := 1000
	digits := numDigits(n)
	quantiles := make([]string, n)
	for i := 0; i < n; i++ {
		str := intToFixedLengthString(i, digits)
		sketch.Update(str)
		quantiles[i] = str
	}
	{ //EXCLUSIVE
		ranks, err := sketch.GetCDF(quantiles, false)
		assert.NoError(t, err)
		pmf, err := sketch.GetPMF(quantiles, false)
		assert.NoError(t, err)
		sumPmf := 0.0
		for i := 0; i < n; i++ {
			r, err := sketch.GetRank(quantiles[i], false)
			assert.NoError(t, err)
			assert.InDelta(t, ranks[i], r, NUMERIC_NOISE_TOLERANCE)
			sumPmf += pmf[i]
			assert.InDelta(t, ranks[i], sumPmf, NUMERIC_NOISE_TOLERANCE)
		}
		sumPmf += pmf[n]
		assert.InDelta(t, sumPmf, 1.0, NUMERIC_NOISE_TOLERANCE)
		assert.InDelta(t, ranks[n], 1.0, NUMERIC_NOISE_TOLERANCE)
	}
	{ // INCLUSIVE (default)
		ranks, err := sketch.GetCDF(quantiles, true)
		assert.NoError(t, err)
		pmf, err := sketch.GetPMF(quantiles, true)
		assert.NoError(t, err)
		sumPmf := 0.0
		for i := 0; i < n; i++ {
			r, err := sketch.GetRank(quantiles[i], true)
			assert.NoError(t, err)
			assert.InDelta(t, ranks[i], r, NUMERIC_NOISE_TOLERANCE)
			sumPmf += pmf[i]
			assert.InDelta(t, ranks[i], sumPmf, NUMERIC_NOISE_TOLERANCE)
		}
		sumPmf += pmf[n]
		assert.InDelta(t, sumPmf, 1.0, NUMERIC_NOISE_TOLERANCE)
		assert.InDelta(t, ranks[n], 1.0, NUMERIC_NOISE_TOLERANCE)
	}
}

func TestItemsSketch_Merge(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch1, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sketch2, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	n := 10000
	digits := numDigits(2 * n)
	for i := 0; i < n; i++ {
		sketch1.Update(intToFixedLengthString(i, digits))
		sketch2.Update(intToFixedLengthString(2*n-i-1, digits))
	}

	minV, err := sketch1.GetMinItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(0, digits), minV)
	maxV, err := sketch1.GetMaxItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(n-1, digits), maxV)

	minV, err = sketch2.GetMinItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(n, digits), minV)
	maxV, err = sketch2.GetMaxItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(2*n-1, digits), maxV)

	sketch1.Merge(sketch2)
	assert.False(t, sketch1.IsEmpty())
	assert.Equal(t, uint64(2*n), sketch1.GetN())
	minV, err = sketch1.GetMinItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(0, digits), minV)
	maxV, err = sketch1.GetMaxItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(2*n-1, digits), maxV)
	upperBound := intToFixedLengthString(n+(int)(math.Ceil(float64(n)*PMF_EPS_FOR_K_256)), digits)
	lowerBound := intToFixedLengthString(n-(int)(math.Ceil(float64(n)*PMF_EPS_FOR_K_256)), digits)
	median, err := sketch1.GetQuantile(0.5, false)
	assert.NoError(t, err)
	assert.True(t, median < upperBound)
	assert.True(t, lowerBound < median)
}

func TestItemsSketch_MergeLowerK(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch1, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sketch2, err := NewKllItemsSketch[string](_DEFAULT_K/2, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	n := 10000
	digits := numDigits(2 * n)
	for i := 0; i < n; i++ {
		sketch1.Update(intToFixedLengthString(i, digits))
		sketch2.Update(intToFixedLengthString(2*n-i-1, digits))
	}

	minV, err := sketch1.GetMinItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(0, digits), minV)
	maxV, err := sketch1.GetMaxItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(n-1, digits), maxV)

	minV, err = sketch2.GetMinItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(n, digits), minV)
	maxV, err = sketch2.GetMaxItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(2*n-1, digits), maxV)

	sketch1.Merge(sketch2)

	//sketch1 must get "contaminated" by the lower K in sketch2
	assert.Equal(t, sketch1.GetNormalizedRankError(false), sketch2.GetNormalizedRankError(false))
	assert.Equal(t, sketch1.GetNormalizedRankError(true), sketch2.GetNormalizedRankError(true))

	assert.False(t, sketch1.IsEmpty())
	assert.Equal(t, uint64(2*n), sketch1.GetN())
	minV, err = sketch1.GetMinItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(0, digits), minV)
	maxV, err = sketch1.GetMaxItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(2*n-1, digits), maxV)
	upperBound := intToFixedLengthString(n+(int)(math.Ceil(float64(n)*PMF_EPS_FOR_K_256)), digits)
	lowerBound := intToFixedLengthString(n-(int)(math.Ceil(float64(n)*PMF_EPS_FOR_K_256)), digits)
	median, err := sketch1.GetQuantile(0.5, false)
	assert.NoError(t, err)
	assert.True(t, median < upperBound)
	assert.True(t, lowerBound < median)
}

func TestItemsSketch_MergeEmptyLowerK(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch1, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sketch2, err := NewKllItemsSketch[string](_DEFAULT_K/2, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	n := 10000
	digits := numDigits(n)
	for i := 0; i < n; i++ {
		sketch1.Update(intToFixedLengthString(i, digits)) //sketch2 is empty
	}

	// rank error should not be affected by a merge with an empty sketch with lower K
	rankErrorBeforeMerge := sketch1.GetNormalizedRankError(true)
	sketch1.Merge(sketch2)
	assert.Equal(t, sketch1.GetNormalizedRankError(true), rankErrorBeforeMerge)

	{
		assert.False(t, sketch1.IsEmpty())
		assert.True(t, sketch2.IsEmpty())
		assert.Equal(t, uint64(n), sketch1.GetN())
		minV, err := sketch1.GetMinItem()
		assert.NoError(t, err)
		assert.Equal(t, intToFixedLengthString(0, digits), minV)
		maxV, err := sketch1.GetMaxItem()
		assert.NoError(t, err)
		assert.Equal(t, intToFixedLengthString(n-1, digits), maxV)
		upperBound := intToFixedLengthString(n/2+(int)(math.Ceil(float64(n)*PMF_EPS_FOR_K_256)), digits)
		lowerBound := intToFixedLengthString(n/2-(int)(math.Ceil(float64(n)*PMF_EPS_FOR_K_256)), digits)
		median, err := sketch1.GetQuantile(0.5, false)
		assert.NoError(t, err)
		assert.True(t, median < upperBound)
		assert.True(t, lowerBound < median)
	}
	{
		//merge the other way
		sketch2.Merge(sketch1)
		assert.False(t, sketch1.IsEmpty())
		assert.False(t, sketch2.IsEmpty())
		assert.Equal(t, uint64(n), sketch1.GetN())
		assert.Equal(t, uint64(n), sketch2.GetN())
		minV, err := sketch1.GetMinItem()
		assert.NoError(t, err)
		assert.Equal(t, intToFixedLengthString(0, digits), minV)
		maxV, err := sketch1.GetMaxItem()
		assert.NoError(t, err)
		assert.Equal(t, intToFixedLengthString(n-1, digits), maxV)
		minV, err = sketch2.GetMinItem()
		assert.NoError(t, err)
		assert.Equal(t, intToFixedLengthString(0, digits), minV)
		maxV, err = sketch2.GetMaxItem()
		assert.NoError(t, err)
		assert.Equal(t, intToFixedLengthString(n-1, digits), maxV)
		upperBound := intToFixedLengthString(n/2+(int)(math.Ceil(float64(n)*PMF_EPS_FOR_K_256)), digits)
		lowerBound := intToFixedLengthString(n/2-(int)(math.Ceil(float64(n)*PMF_EPS_FOR_K_256)), digits)
		median, err := sketch2.GetQuantile(0.5, false)
		assert.NoError(t, err)
		assert.True(t, median < upperBound)
		assert.True(t, lowerBound < median)
	}
}

func TestItemsSketch_MergeExactModeLowerK(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch1, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sketch2, err := NewKllItemsSketch[string](_DEFAULT_K/2, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	n := 10000
	digits := numDigits(n)
	for i := 0; i < n; i++ {
		sketch1.Update(intToFixedLengthString(i, digits))
	}
	sketch2.Update(intToFixedLengthString(1, digits))

	// rank error should not be affected by a merge with a sketch in exact mode with lower K
	rankErrorBeforeMerge := sketch1.GetNormalizedRankError(true)
	sketch1.Merge(sketch2)
	assert.Equal(t, sketch1.GetNormalizedRankError(true), rankErrorBeforeMerge)
}

func TestItemsSketch_MergeMinMinValueFromOther(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch1, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sketch2, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sketch1.Update(intToFixedLengthString(1, 1))
	sketch2.Update(intToFixedLengthString(2, 1))
	sketch2.Merge(sketch1)
	minV, err := sketch2.GetMinItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(1, 1), minV)
}

func TestItemsSketch_MergeMinAndMaxFromOther(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch1, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sketch2, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	n := 1_000_000
	digits := numDigits(n)
	for i := 1; i <= 1_000_000; i++ {
		sketch1.Update(intToFixedLengthString(i, digits)) //sketch2 is empty
	}
	sketch2.Merge(sketch1)
	minV, err := sketch2.GetMinItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(1, digits), minV)
	maxV, err := sketch2.GetMaxItem()
	assert.NoError(t, err)
	assert.Equal(t, intToFixedLengthString(n, digits), maxV)
}

func TestItemsSketch_KTooSmall(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	_, err := NewKllItemsSketch[string](_MIN_K-1, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.Error(t, err)
}

func TestItemsSketch_MinK(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch, err := NewKllItemsSketch[string](uint16(8), _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	n := 1000
	digits := numDigits(n)
	for i := 0; i < n; i++ {
		sketch.Update(intToFixedLengthString(i, digits))
	}
	assert.Equal(t, sketch.GetK(), uint16(_DEFAULT_M))
	upperBound := intToFixedLengthString(n/2+(int)(math.Ceil(float64(n)*PMF_EPS_FOR_K_8)), digits)
	lowerBound := intToFixedLengthString(n/2-(int)(math.Ceil(float64(n)*PMF_EPS_FOR_K_8)), digits)
	median, err := sketch.GetQuantile(0.5, true)
	assert.NoError(t, err)
	assert.LessOrEqual(t, median, upperBound)
	assert.LessOrEqual(t, lowerBound, median)
}

func TestItemsSketch_MaxK(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch, err := NewKllItemsSketch[string](uint16(_MAX_K), _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	n := 1000
	digits := numDigits(n)
	for i := 0; i < n; i++ {
		sketch.Update(intToFixedLengthString(i, digits))
	}
	assert.Equal(t, sketch.GetK(), uint16(_MAX_K))
	upperBound := intToFixedLengthString(n/2+(int)(math.Ceil(float64(n)*PMF_EPS_FOR_K_256)), digits)
	lowerBound := intToFixedLengthString(n/2-(int)(math.Ceil(float64(n)*PMF_EPS_FOR_K_256)), digits)
	median, err := sketch.GetQuantile(0.5, true)
	assert.NoError(t, err)
	assert.LessOrEqual(t, median, upperBound)
	assert.LessOrEqual(t, lowerBound, median)
}

func TestItemsSketch_OutOfOrderSplitPoints(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	s0 := intToFixedLengthString(0, 1)
	s1 := intToFixedLengthString(1, 1)
	sketch.Update(s0)
	_, err = sketch.GetCDF([]string{s1, s0}, true)
	assert.Error(t, err)
}

func TestItemsSketch_DuplicateSplitPoints(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sketch.Update("A")
	sketch.Update("B")
	sketch.Update("C")
	sketch.Update("D")
	quantiles1, err := sketch.GetQuantiles([]float64{0.0, 0.5, 1.0}, false)
	assert.NoError(t, err)
	boundaries, err := sketch.GetPartitionBoundaries(2, false)
	quantiles2 := boundaries.GetBoundaries()
	assert.NoError(t, err)
	assert.Equal(t, quantiles1, quantiles2)
}

func TestItemsSketch_CheckReset(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch, err := NewKllItemsSketch[string](20, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	n := 100
	digits := numDigits(n)
	for i := 1; i <= n; i++ {
		sketch.Update(intToFixedLengthString(i, digits))
	}
	n1 := sketch.GetN()
	min1, err := sketch.GetMinItem()
	assert.NoError(t, err)
	max1, err := sketch.GetMaxItem()
	assert.NoError(t, err)
	sketch.Reset()
	for i := 1; i <= 100; i++ {
		sketch.Update(intToFixedLengthString(i, digits))
	}
	n2 := sketch.GetN()
	min2, err := sketch.GetMinItem()
	assert.NoError(t, err)
	max2, err := sketch.GetMaxItem()
	assert.NoError(t, err)
	assert.Equal(t, n2, n1)
	assert.Equal(t, min2, min1)
	assert.Equal(t, max2, max1)
}

func TestItemsSketch_SortedView(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch, err := NewKllItemsSketch[string](20, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sketch.Update("A")
	sketch.Update("AB")
	sketch.Update("ABC")

	view, err := sketch.GetSortedView()
	assert.NoError(t, err)
	itr := view.Iterator()
	assert.True(t, itr.Next())
	assert.Equal(t, "A", itr.GetQuantile())
	assert.Equal(t, int64(1), itr.GetWeight())
	assert.Equal(t, int64(0), itr.GetNaturalRank(false))
	assert.Equal(t, int64(1), itr.GetNaturalRank(true))
	assert.True(t, itr.Next())
	assert.Equal(t, "AB", itr.GetQuantile())
	assert.Equal(t, int64(1), itr.GetWeight())
	assert.Equal(t, int64(1), itr.GetNaturalRank(false))
	assert.Equal(t, int64(2), itr.GetNaturalRank(true))
	assert.True(t, itr.Next())
	assert.Equal(t, "ABC", itr.GetQuantile())
	assert.Equal(t, int64(1), itr.GetWeight())
	assert.Equal(t, int64(2), itr.GetNaturalRank(false))
	assert.Equal(t, int64(3), itr.GetNaturalRank(true))
	assert.False(t, itr.Next())
}

func TestItemsSketch_CDF_PDF(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	cdfI := []float64{.25, .50, .75, 1.0, 1.0}
	cdfE := []float64{0.0, .25, .50, .75, 1.0}
	pmfI := []float64{.25, .25, .25, .25, 0.0}
	pmfE := []float64{0.0, .25, .25, .25, .25}
	toll := 1e-10
	sketch, err := NewKllItemsSketch[string](20, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	strIn := []string{"A", "AB", "ABC", "ABCD"}
	for i := 0; i < len(strIn); i++ {
		sketch.Update(strIn[i])
	}
	sp := []string{"A", "AB", "ABC", "ABCD"}
	t.Logf("SplitPoints: %v", sp)
	for i := 0; i < len(sp); i++ {
		t.Logf("%10s", sp[i])
	}
	t.Logf("")
	t.Logf("INCLUSIVE:")
	cdf, err := sketch.GetCDF(sp, true)
	assert.NoError(t, err)
	pmf, err := sketch.GetPMF(sp, true)
	assert.NoError(t, err)
	t.Logf("%10s%10s\n", "CDF", "PMF")
	for i := 0; i < len(cdf); i++ {
		t.Logf("%10.2f%10.2f\n", cdf[i], pmf[i])
		assert.InDelta(t, cdf[i], cdfI[i], toll)
		assert.InDelta(t, pmf[i], pmfI[i], toll)
	}
	t.Logf("EXCLUSIVE")
	cdf, err = sketch.GetCDF(sp, false)
	assert.NoError(t, err)
	pmf, err = sketch.GetPMF(sp, false)
	assert.NoError(t, err)
	t.Logf("%10s%10s\n", "CDF", "PMF")
	for i := 0; i < len(cdf); i++ {
		t.Logf("%10.2f%10.2f\n", cdf[i], pmf[i])
		assert.InDelta(t, cdf[i], cdfE[i], toll)
		assert.InDelta(t, pmf[i], pmfE[i], toll)
	}
}

func TestItemsSketch_DeserializeEmpty(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sk1, err := NewKllItemsSketch[string](20, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	mem, err := sk1.ToSlice()
	assert.NoError(t, err)
	assert.NotNil(t, mem)
	memVal, err := newItemsSketchMemoryValidate[string](mem, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	assert.Equal(t, memVal.sketchStructure, _COMPACT_EMPTY)
	assert.Equal(t, len(mem), 8)

	sk2, err := NewKllItemsSketchFromSlice[string](mem, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	assert.Equal(t, sk2.GetN(), uint64(0))
	_, err = sk2.GetMinItem()
	assert.Error(t, err)
	_, err = sk2.GetMaxItem()
	assert.Error(t, err)
}

func TestItemsSketch_DeserializeSingleItem(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sk1, err := NewKllItemsSketch[string](20, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sk1.Update("A")
	mem, err := sk1.ToSlice()
	assert.NoError(t, err)
	assert.NotNil(t, mem)
	memVal, err := newItemsSketchMemoryValidate[string](mem, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	assert.Equal(t, memVal.sketchStructure, _COMPACT_SINGLE)
	sk2, err := NewKllItemsSketchFromSlice[string](mem, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	assert.Equal(t, sk2.GetN(), uint64(1))
	minV, err := sk2.GetMinItem()
	assert.NoError(t, err)
	assert.Equal(t, minV, "A")
	maxV, err := sk2.GetMaxItem()
	assert.NoError(t, err)
	assert.Equal(t, maxV, "A")
}

func TestItemsSketch_FewItems(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sk1, err := NewKllItemsSketch[string](20, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sk1.Update("A")
	sk1.Update("AB")
	sk1.Update("ABC")
	mem, err := sk1.ToSlice()
	assert.NoError(t, err)
	assert.NotNil(t, mem)
	memVal, err := newItemsSketchMemoryValidate[string](mem, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	assert.Equal(t, memVal.sketchStructure, _COMPACT_FULL)
	assert.Equal(t, len(mem), memVal.sketchBytes)
}

func TestItemsSketch_ManyItems(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sk1, err := NewKllItemsSketch[string](20, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	n := 109
	digits := numDigits(n)
	for i := 1; i <= n; i++ {
		sk1.Update(intToFixedLengthString(i, digits))
	}
	mem, err := sk1.ToSlice()
	assert.NoError(t, err)
	assert.NotNil(t, mem)
	memVal, err := newItemsSketchMemoryValidate[string](mem, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	assert.Equal(t, memVal.sketchStructure, _COMPACT_FULL)
	assert.Equal(t, len(mem), memVal.sketchBytes)
}

func TestItemsSketch_SortedViewAfterReset(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sk, err := NewKllItemsSketch[string](20, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sk.Update("1")
	sv, err := sk.GetSortedView()
	assert.NoError(t, err)
	ssv, err := sv.GetQuantile(1.0, true)
	assert.NoError(t, err)
	assert.Equal(t, ssv, "1")
	sk.Reset()
	_, err = sk.GetSortedView()
	assert.Error(t, err)
}

func TestItemsSketch_SerializeDeserializeEmpty(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sk1, err := NewKllItemsSketch[string](20, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	mem, err := sk1.ToSlice()
	assert.NoError(t, err)
	assert.NotNil(t, mem)
	sk2, err := NewKllItemsSketchFromSlice[string](mem, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	s, err := sk1.GetSerializedSizeBytes()
	assert.NoError(t, err)
	assert.Equal(t, len(mem), s)
	assert.True(t, sk2.IsEmpty())
	assert.Equal(t, sk2.GetNumRetained(), sk1.GetNumRetained())
	assert.Equal(t, sk2.GetN(), sk1.GetN())
	assert.Equal(t, sk2.GetNormalizedRankError(false), sk1.GetNormalizedRankError(false))
	_, err = sk2.GetMinItem()
	assert.Error(t, err)
	_, err = sk2.GetMaxItem()
	assert.Error(t, err)
	s1, err := sk2.GetSerializedSizeBytes()
	assert.NoError(t, err)
	s2, err := sk1.GetSerializedSizeBytes()
	assert.NoError(t, err)
	assert.Equal(t, s2, s1)
	mem2, err := sk2.ToSlice()
	assert.NoError(t, err)
	assert.Equal(t, mem, mem2)
}

func TestItemsSketch_SerializeDeserializeOneValue(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sk1, err := NewKllItemsSketch[string](20, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sk1.Update(" 1")
	mem, err := sk1.ToSlice()
	assert.NoError(t, err)
	assert.NotNil(t, mem)
	sk2, err := NewKllItemsSketchFromSlice[string](mem, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	s1SizeBytes, err := sk1.GetSerializedSizeBytes()
	assert.Equal(t, len(mem), s1SizeBytes)
	assert.False(t, sk2.IsEmpty())
	assert.Equal(t, sk2.GetNumRetained(), uint32(1))
	assert.Equal(t, sk2.GetN(), uint64(1))
	assert.Equal(t, sk2.GetNormalizedRankError(false), sk1.GetNormalizedRankError(false))
	minV, err := sk2.GetMinItem()
	assert.NoError(t, err)
	assert.Equal(t, minV, " 1")
	maxV, err := sk2.GetMaxItem()
	assert.NoError(t, err)
	assert.Equal(t, maxV, " 1")
	s1, err := sk2.GetSerializedSizeBytes()
	assert.NoError(t, err)
	s2, err := sk1.GetSerializedSizeBytes()
	assert.NoError(t, err)
	assert.Equal(t, s2, s1)
	mem2, err := sk2.ToSlice()
	assert.NoError(t, err)
	assert.Equal(t, mem, mem2)
}

func TestItemsSketch_SerializeDeserializeMultipleValue(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sk1, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	n := 1000
	for i := 0; i < n; i++ {
		sk1.Update(intToFixedLengthString(i, 4))
	}
	minV, err := sk1.GetMinItem()
	assert.NoError(t, err)
	assert.Equal(t, minV, "   0")
	maxV, err := sk1.GetMaxItem()
	assert.NoError(t, err)
	assert.Equal(t, maxV, " 999")
	mem, err := sk1.ToSlice()
	assert.NoError(t, err)
	assert.NotNil(t, mem)
	sk2, err := NewKllItemsSketchFromSlice[string](mem, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	s1, err := sk2.GetSerializedSizeBytes()
	assert.NoError(t, err)
	s2, err := sk1.GetSerializedSizeBytes()
	assert.NoError(t, err)
	assert.Equal(t, s2, s1)
	assert.False(t, sk2.IsEmpty())
	assert.Equal(t, sk2.GetNumRetained(), sk1.GetNumRetained())
	assert.Equal(t, sk2.GetN(), sk1.GetN())
	assert.Equal(t, sk2.GetNormalizedRankError(false), sk1.GetNormalizedRankError(false))
	minV2, err := sk2.GetMinItem()
	assert.NoError(t, err)
	assert.Equal(t, minV, minV2)
	maxV2, err := sk2.GetMaxItem()
	assert.NoError(t, err)
	assert.Equal(t, maxV, maxV2)
	mem2, err := sk2.ToSlice()
	assert.NoError(t, err)
	assert.Equal(t, mem, mem2)
}

func TestSerializeDeserializeString(t *testing.T) {
	nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	comparator := common.ItemSketchStringComparator(false)
	for _, n := range nArr {
		digits := numDigits(n)
		sk, err := NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
		assert.NoError(t, err)
		for i := 1; i <= n; i++ {
			sk.Update(intToFixedLengthString(i, digits))
		}
		slc, err := sk.ToSlice()
		assert.NoError(t, err)

		sketch, err := NewKllItemsSketchFromSlice[string](slc, comparator, common.ItemSketchStringSerDe{})
		if err != nil {
			return
		}

		assert.Equal(t, sketch.GetK(), uint16(200))
		if n == 0 {
			assert.True(t, sketch.IsEmpty())
		} else {
			assert.False(t, sketch.IsEmpty())
		}

		if n > 100 {
			assert.True(t, sketch.IsEstimationMode())
		} else {
			assert.False(t, sketch.IsEstimationMode())
		}

		if n > 0 {
			minV, err := sketch.GetMinItem()
			assert.NoError(t, err)
			assert.Equal(t, minV, intToFixedLengthString(1, digits))

			maxV, err := sketch.GetMaxItem()
			assert.NoError(t, err)
			assert.Equal(t, maxV, intToFixedLengthString(n, digits))

			weight := int64(0)
			it := sketch.GetIterator()
			compareFn := comparator
			for it.Next() {
				qut := it.GetQuantile()
				assert.True(t, compareFn(minV, qut) || minV == qut, fmt.Sprintf("min: \"%v\" \"%v\"", minV, qut))
				assert.True(t, !compareFn(maxV, qut) || maxV == qut, fmt.Sprintf("max: \"%v\" \"%v\"", maxV, qut))
				weight += it.GetWeight()
			}
			assert.Equal(t, weight, int64(n))
		}
	}
}

func TestSerializeDeserializeFloat(t *testing.T) {
	nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	comparator := common.ItemSketchDoubleComparator(false)
	for _, n := range nArr {
		sk, err := NewKllItemsSketchWithDefault[float64](comparator, common.ItemSketchDoubleSerDe{})
		assert.NoError(t, err)
		for i := 1; i <= n; i++ {
			sk.Update(float64(i))
		}
		slc, err := sk.ToSlice()
		assert.NoError(t, err)

		sketch, err := NewKllItemsSketchFromSlice[float64](slc, comparator, common.ItemSketchDoubleSerDe{})
		if err != nil {
			return
		}

		assert.Equal(t, sketch.GetK(), uint16(200))
		if n == 0 {
			assert.True(t, sketch.IsEmpty())
		} else {
			assert.False(t, sketch.IsEmpty())
		}

		if n > 100 {
			assert.True(t, sketch.IsEstimationMode())
		} else {
			assert.False(t, sketch.IsEstimationMode())
		}

		if n > 0 {
			minV, err := sketch.GetMinItem()
			assert.NoError(t, err)
			assert.Equal(t, minV, float64(1))

			maxV, err := sketch.GetMaxItem()
			assert.NoError(t, err)
			assert.Equal(t, maxV, float64(n))

			weight := int64(0)
			it := sketch.GetIterator()
			compareFn := comparator
			for it.Next() {
				qut := it.GetQuantile()
				assert.True(t, compareFn(minV, qut) || minV == qut, fmt.Sprintf("min: \"%v\" \"%v\"", minV, qut))
				assert.True(t, !compareFn(maxV, qut) || maxV == qut, fmt.Sprintf("max: \"%v\" \"%v\"", maxV, qut))
				weight += it.GetWeight()
			}
			assert.Equal(t, weight, int64(n))
		}
	}
}

func TestSerializeStringDeserializeNoSerde(t *testing.T) {
	comparator := common.ItemSketchStringComparator(false)
	sketch1, err := NewKllItemsSketchWithDefault[string](comparator, nil)
	assert.NoError(t, err)
	_, err = sketch1.ToSlice()
	assert.Error(t, err)

	_, err = NewKllItemsSketchFromSlice[string](nil, comparator, nil)
	assert.Error(t, err)
}

func TestSerializeDeserializeLargeK(t *testing.T) {
	sketch, err := NewKllItemsSketch[float64](710, 8, common.ItemSketchDoubleComparator(false), common.ItemSketchDoubleSerDe{})
	assert.NoError(t, err)
	for _, val := range []float64{0.1, 1.0, 5.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 99.9} {
		sketch.Update(val)
	}
	numericalQuantileSketchBytes, err := sketch.ToSlice()
	assert.NoError(t, err)

	deserialized, err := NewKllItemsSketchFromSlice[float64](
		numericalQuantileSketchBytes, common.ItemSketchDoubleComparator(false), common.ItemSketchDoubleSerDe{},
	)
	assert.NoError(t, err)

	assert.Equal(t, sketch.GetK(), deserialized.GetK())
	assert.Equal(t, sketch.GetN(), deserialized.GetN())
	assert.Equal(t, sketch.GetNumRetained(), deserialized.GetNumRetained())
	assert.Equal(t, sketch.IsEmpty(), deserialized.IsEmpty())
	assert.Equal(t, sketch.IsEstimationMode(), deserialized.IsEstimationMode())

	minV1, err := sketch.GetMinItem()
	assert.NoError(t, err)
	minV2, err := deserialized.GetMinItem()
	assert.NoError(t, err)
	assert.Equal(t, minV1, minV2)

	maxV1, err := sketch.GetMaxItem()
	assert.NoError(t, err)
	maxV2, err := deserialized.GetMaxItem()
	assert.NoError(t, err)
	assert.Equal(t, maxV1, maxV2)

	assert.Equal(t, sketch.GetNormalizedRankError(false), deserialized.GetNormalizedRankError(false))
	assert.Equal(t, sketch.GetNormalizedRankError(true), deserialized.GetNormalizedRankError(true))

	size1, err := sketch.GetSerializedSizeBytes()
	assert.NoError(t, err)
	size2, err := deserialized.GetSerializedSizeBytes()
	assert.NoError(t, err)
	assert.Equal(t, size1, size2)

	bytes2, err := deserialized.ToSlice()
	assert.NoError(t, err)
	assert.Equal(t, numericalQuantileSketchBytes, bytes2)

	ranks := []float64{0.0, 0.25, 0.5, 0.75, 1.0}
	for _, rank := range ranks {
		q1, err := sketch.GetQuantile(rank, true)
		assert.NoError(t, err)
		q2, err := deserialized.GetQuantile(rank, true)
		assert.NoError(t, err)
		assert.Equal(t, q1, q2)
	}
}

func TestNoCompare(t *testing.T) {
	_, err := NewKllItemsSketchWithDefault[string](nil, nil)
	assert.Error(t, err)
}

// There is no guarantee that L0 is sorted after a merge.
// The issue is, during a merge, L0 must be sorted prior to a compaction to a higher level.
// Otherwise the higher levels would not be sorted properly.
func TestL0SortDuringMerge(t *testing.T) {
	comparator := common.ItemSketchStringComparator(true)
	sk1, err := NewKllItemsSketch[string](8, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	sk2, err := NewKllItemsSketch[string](8, _DEFAULT_M, comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)
	n := 26 //don't change this
	for i := 1; i <= n; i++ {
		j := rand.Intn(n) + 1
		sk1.Update(intToFixedLengthString(j, 3))
		sk2.Update(intToFixedLengthString(j+100, 3))
	}
	sk1.Merge(sk2)
	//println(sk1.String(true, true)) //L1 and above should be sorted in reverse. Ignore L0.
	lvl1size := sk1.levels[2] - sk1.levels[1]
	itr := sk1.GetIterator()
	itr.Next()
	prev, _ := strconv.Atoi(strings.TrimSpace(itr.GetQuantile()))
	for i := uint32(1); i < lvl1size; i++ {
		if itr.Next() {
			v, _ := strconv.Atoi(strings.TrimSpace(itr.GetQuantile()))
			assert.True(t, v <= prev)
			prev = v
		}
	}
}
