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

package examples

import (
	"fmt"
	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/kll"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKllItemsSketch(t *testing.T) {
	// Create a comparison function for strings (or use common.ItemSketchStringComparator{})
	comparator := common.ItemSketchStringComparator(false)

	// Create a new KLL sketch
	sketch, err := kll.NewKllItemsSketchWithDefault[string](comparator, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)

	// Update the sketch with 1000 items
	for i := 0; i < 1000; i++ {
		sketch.Update(fmt.Sprintf("item_%012d", i))
	}

	// Get the quantiles
	quantiles := []float64{0.0, 0.5, 1.0}
	values, err := sketch.GetQuantiles(quantiles, true)
	assert.NoError(t, err)

	// Kll sketch is a stochastic data structure, so we can't validate the exact values
	// Additionally when the sketch recompress, it randomly picks a pivot point meaning
	// we can only validate the range of the values.
	// This is not more or less wrong, just not comparable deterministically

	// Validate the quantiles
	assert.LessOrEqual(t, values[0], "item_000000000003")
	assert.GreaterOrEqual(t, values[1], "item_000000000498")
	assert.LessOrEqual(t, values[1], "item_000000000501")
	assert.GreaterOrEqual(t, values[2], "item_000000000999")

	// Get the PMF
	pmf, err := sketch.GetPMF([]string{"item_000000000000", "item_000000000498", "item_000000000999"}, true)
	assert.NoError(t, err)

	// Validate the PMF
	assert.LessOrEqual(t, pmf[0], 0.004)
	assert.InDelta(t, pmf[1], 0.500, 0.01)
	assert.GreaterOrEqual(t, pmf[2], 0.498)

	// Get the CDF
	cdf, err := sketch.GetCDF([]string{"item_000000000000", "item_000000000498", "item_000000000999"}, true)
	assert.NoError(t, err)

	// Validate the CDF
	assert.LessOrEqual(t, cdf[0], 0.004)
	assert.InDelta(t, cdf[1], 0.500, 0.01)
	assert.Equal(t, cdf[2], 1.0)

	// Get the rank of an item
	rank, err := sketch.GetRank("item_000000000498", true)
	assert.NoError(t, err)
	assert.InDelta(t, rank, 0.5, 0.01)
}
