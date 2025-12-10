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
	"github.com/apache/datasketches-go/hll"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHllItemsSketch(t *testing.T) {
	// Create a new KLL sketch
	sketch, err := hll.NewHllSketchWithDefault()
	assert.NoError(t, err)

	// Update the sketch with 1000 items
	for i := 0; i < 1000; i++ {
		err := sketch.UpdateString(fmt.Sprintf("item_%d", i))
		assert.NoError(t, err)
	}

	// Get the estimate of the number of unique items
	estimate, err := sketch.GetEstimate()
	assert.NoError(t, err)
	assert.InDelta(t, 1000, estimate, 1000*0.03)

	// Create a new KLL sketch with a different log2m
	sketch2, err := hll.NewHllSketch(12, hll.TgtHllTypeDefault)
	assert.NoError(t, err)

	// Update the sketch with another 1000 items
	for i := 500; i < 1500; i++ {
		err := sketch2.UpdateString(fmt.Sprintf("item_%d", i))
		assert.NoError(t, err)
	}

	// Merge the two sketches
	union, err := hll.NewUnionWithDefault()
	assert.NoError(t, err)

	err = union.UpdateSketch(sketch)
	assert.NoError(t, err)
	err = union.UpdateSketch(sketch2)
	assert.NoError(t, err)

	mergedSketch, err := union.GetResult(hll.TgtHllTypeDefault)
	assert.NoError(t, err)

	// Get the estimate of the number of unique items
	estimate, err = mergedSketch.GetEstimate()
	assert.NoError(t, err)
	assert.InDelta(t, 1500, estimate, 1500*0.03)

	// Serialize the sketches
	bytes, err := sketch.ToCompactSlice()
	assert.NoError(t, err)
	bytes2, err := sketch2.ToCompactSlice()
	assert.NoError(t, err)

	// Deserialize the sketches into a union
	union2, err := hll.NewUnionWithDefault()
	assert.NoError(t, err)

	sketch_1, err := hll.NewHllSketchFromSlice(bytes, true)
	assert.NoError(t, err)

	sketch_2, err := hll.NewHllSketchFromSlice(bytes2, true)
	assert.NoError(t, err)

	err = union2.UpdateSketch(sketch_1)
	assert.NoError(t, err)
	err = union2.UpdateSketch(sketch_2)
	assert.NoError(t, err)

	mergedSketch2, err := union2.GetResult(hll.TgtHllTypeDefault)
	assert.NoError(t, err)

	// Get the estimate of the number of unique items
	estimate, err = mergedSketch2.GetEstimate()
	assert.NoError(t, err)
	assert.InDelta(t, 1500, estimate, 1500*0.03)

}
