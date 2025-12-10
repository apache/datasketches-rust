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

package cpc

import (
	"fmt"
	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestGenerateGoFiles(t *testing.T) {
	nArr := []int{0, 100, 200, 2000, 20000}
	flavorArr := []CpcFlavor{CpcFlavorEmpty, CpcFlavorSparse, CpcFlavorHybrid, CpcFlavorPinned, CpcFlavorSliding}
	for flavorIdx, n := range nArr {
		sketch, err := NewCpcSketchWithDefault(11)
		assert.NoError(t, err)
		for i := 0; i < n; i++ {
			assert.NoError(t, sketch.UpdateUint64(uint64(i)))
		}
		assert.Equal(t, sketch.getFlavor(), flavorArr[flavorIdx])

		err = os.MkdirAll(internal.GoPath, os.ModePerm)
		assert.NoError(t, err)

		sl, err := sketch.ToCompactSlice()
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/cpc_n%d_go.sk", internal.GoPath, n), sl, 0644)
		assert.NoError(t, err)
	}
}

func TestJavaCompat(t *testing.T) {
	t.Run("Java CPC", func(t *testing.T) {
		nArr := []int{0, 100, 200, 2000, 20000}
		flavorArr := []CpcFlavor{CpcFlavorEmpty, CpcFlavorSparse, CpcFlavorHybrid, CpcFlavorPinned, CpcFlavorSliding}
		for flavorIdx, n := range nArr {
			bytes, err := os.ReadFile(fmt.Sprintf("%s/cpc_n%d_java.sk", internal.JavaPath, n))
			assert.NoError(t, err)
			sketch, err := NewCpcSketchFromSliceWithDefault(bytes)
			assert.NoError(t, err)
			assert.Equal(t, sketch.getFlavor(), flavorArr[flavorIdx])
			assert.InDelta(t, float64(n), sketch.GetEstimate(), float64(n)*0.02)
		}
	})
}

func TestNegativeIntEquivalence(t *testing.T) {
	// Create a new CPC sketch with default parameters
	sk, err := NewCpcSketchWithDefault(11)
	assert.NoError(t, err)

	// Update with -1 as a byte, short, int, and long
	var b int8 = -1
	err = sk.UpdateInt64(int64(b))
	assert.NoError(t, err)

	var s int16 = -1
	err = sk.UpdateInt64(int64(s))
	assert.NoError(t, err)

	var i int32 = -1
	err = sk.UpdateInt64(int64(i))
	assert.NoError(t, err)

	var l int64 = -1
	err = sk.UpdateInt64(l)
	assert.NoError(t, err)

	// Check that the estimate is 1 (since -1 in all forms is the same hash)
	assert.InDelta(t, 1.0, sk.GetEstimate(), 0.01)

	// Write out the binary so that Java/C++ can read it
	err = os.MkdirAll(internal.GoPath, os.ModePerm)
	assert.NoError(t, err)
	bytes, err := sk.ToCompactSlice()
	assert.NoError(t, err)
	err = os.WriteFile(fmt.Sprintf("%s/cpc_negative_one_go.sk", internal.GoPath), bytes, 0644)
	assert.NoError(t, err)
}
