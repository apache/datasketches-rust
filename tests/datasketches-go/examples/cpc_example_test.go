/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
	"testing"

	"github.com/apache/datasketches-go/cpc"
	"github.com/stretchr/testify/assert"
)

func TestCpcItemsSketch(t *testing.T) {
	// 1) Create a new CPC sketch with lgK=10
	sketch, err := cpc.NewCpcSketchWithDefault(10)
	assert.NoError(t, err, "Failed to create CPC sketch")

	// 2) Update the sketch with 1000 distinct items
	for i := 0; i < 1000; i++ {
		item := fmt.Sprintf("item_%d", i)
		err := sketch.UpdateString(item)
		assert.NoError(t, err, "UpdateString error")
	}

	// 3) Get an estimate of the unique count
	estimate := sketch.GetEstimate()
	// CPC is approximate, so we allow a small relative error margin, say 3%
	assert.InDelta(t, 1000, estimate, 1000*0.03, "Estimate should be close to 1000")

	// 4) Create a second CPC sketch with a different lgK
	sketch2, err := cpc.NewCpcSketchWithDefault(12)
	assert.NoError(t, err, "Failed to create second CPC sketch")

	// Update with overlapping range [500..1500)
	for i := 500; i < 1500; i++ {
		item := fmt.Sprintf("item_%d", i)
		err := sketch2.UpdateString(item)
		assert.NoError(t, err, "UpdateString error (sketch2)")
	}

	// 5) Merge the two sketches using a CPC union
	union, err := cpc.NewCpcUnionSketchWithDefault(10)
	assert.NoError(t, err, "Failed to create CPC union")

	// Add both sketches to the union
	err = union.Update(&(*sketch)) // pass pointer to first sketch
	assert.NoError(t, err, "Union update with first sketch failed")
	err = union.Update(&(*sketch2)) // pass pointer to second
	assert.NoError(t, err, "Union update with second sketch failed")

	// Get the merged result
	mergedSketch, err := union.GetResult()
	assert.NoError(t, err, "Failed to get result from union")

	estimateMerged := mergedSketch.GetEstimate()
	// We expect about 1500 unique items total
	assert.InDelta(t, 1500, estimateMerged, 1500*0.03, "Merged estimate should be close to 1500")

	// 6) Serialize the two sketches
	bytes1, err := sketch.ToCompactSlice()
	assert.NoError(t, err, "Serialize first sketch error")
	bytes2, err := sketch2.ToCompactSlice()
	assert.NoError(t, err, "Serialize second sketch error")

	// 7) Deserialize them into new CPC sketches
	deser1, err := cpc.NewCpcSketchFromSliceWithDefault(bytes1)
	assert.NoError(t, err, "Deserialize first sketch error")
	deser2, err := cpc.NewCpcSketchFromSliceWithDefault(bytes2)
	assert.NoError(t, err, "Deserialize second sketch error")

	// 8) Merge the deserialized sketches
	union2, err := cpc.NewCpcUnionSketchWithDefault(10)
	assert.NoError(t, err, "Failed to create second union")

	err = union2.Update(deser1)
	assert.NoError(t, err, "Union update with deserialization 1 failed")
	err = union2.Update(deser2)
	assert.NoError(t, err, "Union update with deserialization 2 failed")

	mergedSketch2, err := union2.GetResult()
	assert.NoError(t, err, "Failed to get result from union2")

	estimateMerged2 := mergedSketch2.GetEstimate()
	assert.InDelta(t, 1500, estimateMerged2, 1500*0.03, "Merged estimate (deserialized) should be close to 1500")
}
