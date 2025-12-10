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
	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/frequencies"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFrequencyItemsSketch(t *testing.T) {
	// Create a new sketch with a maximum of 16 items
	sketch, err := frequencies.NewFrequencyItemsSketchWithMaxMapSize[string](16, common.ItemSketchStringHasher{}, common.ItemSketchStringSerDe{})
	assert.NoError(t, err)

	// Update the sketch with some items
	sketch.Update("a")
	sketch.Update("b")
	sketch.Update("c")
	sketch.Update("a")
	sketch.Update("b")
	sketch.Update("a")
	sketch.Update("d")
	sketch.Update("a")
	sketch.Update("b")
	sketch.Update("c")
	sketch.Update("a")
	sketch.Update("b")
	sketch.Update("c")
	sketch.Update("a")
	sketch.Update("b")
	sketch.Update("c")
	sketch.Update("a")
	sketch.Update("b")
	sketch.Update("c")
	sketch.Update("a")
	sketch.Update("b")
	sketch.Update("c")
	sketch.Update("a")
	sketch.Update("b")
	sketch.Update("c")

	// Get the frequent items
	frequentItems, err := sketch.GetFrequentItems(frequencies.ErrorTypeEnum.NoFalsePositives)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(frequentItems))
	assert.Equal(t, "a", frequentItems[0].GetItem())
	assert.Equal(t, "b", frequentItems[1].GetItem())
	assert.Equal(t, "c", frequentItems[2].GetItem())
	assert.Equal(t, "d", frequentItems[3].GetItem())

	// Get the estimates
	assert.Equal(t, int64(9), frequentItems[0].GetEstimate())
	assert.Equal(t, int64(8), frequentItems[1].GetEstimate())
	assert.Equal(t, int64(7), frequentItems[2].GetEstimate())
	assert.Equal(t, int64(1), frequentItems[3].GetEstimate())

	// Get the lower bounds
	assert.Equal(t, int64(9), frequentItems[0].GetLowerBound())
	assert.Equal(t, int64(8), frequentItems[1].GetLowerBound())
	assert.Equal(t, int64(7), frequentItems[2].GetLowerBound())
	assert.Equal(t, int64(1), frequentItems[3].GetLowerBound())

	// Get the upper bounds
	assert.Equal(t, int64(9), frequentItems[0].GetUpperBound())
	assert.Equal(t, int64(8), frequentItems[1].GetUpperBound())
	assert.Equal(t, int64(7), frequentItems[2].GetUpperBound())
	assert.Equal(t, int64(1), frequentItems[3].GetUpperBound())
}
