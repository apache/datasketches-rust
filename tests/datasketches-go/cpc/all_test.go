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
	"github.com/apache/datasketches-go/internal"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamingCheck(t *testing.T) {
	// Input parameters
	lgMinK := 10
	lgMaxK := 10
	trials := 10
	ppoN := 1

	sVal := NewStreamingValidation(lgMinK, lgMaxK, trials, ppoN, nil, nil)

	sVal.Start()
}

func TestMatrixCouponCountCheck(t *testing.T) {
	pat := uint64(0xA5A5A5A55A5A5A5A)
	length := 16
	arr := make([]uint64, length)
	for i := range arr {
		arr[i] = pat
	}

	trueCount := uint64(length) * uint64(bits.OnesCount64(pat))
	testCount := CountCoupons(arr)

	assert.Equal(t, trueCount, testCount, "bit counts should match")
}

func TestCompressionCharacterizationCheck(t *testing.T) {
	// Input parameters
	lgMinK := 10
	lgMaxK := 10
	lgMaxT := 5 // Trials at start
	lgMinT := 2 // Trials at end
	lgMulK := 7
	uPPO := 1
	incLgK := 1

	cc := NewCompressionCharacterization(
		lgMinK, lgMaxK, lgMinT, lgMaxT, lgMulK, uPPO, incLgK,
		nil,
		nil,
	)
	assert.NoError(t, cc.doRangeOfLgK())
}

func TestSingleRowColCheck(t *testing.T) {
	lgK := 20
	srcSketch, _ := NewCpcSketchWithDefault(lgK)

	rowCol := 54746379
	err := srcSketch.rowColUpdate(rowCol)
	assert.NoError(t, err)
	t.Log(srcSketch.String()) // or some debug

	state, err := NewCpcCompressedStateFromSketch(srcSketch)
	assert.NoError(t, err)
	t.Log(state)

	uncSketch, err := NewCpcSketch(state.LgK, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	err = state.uncompress(uncSketch)
	assert.NoError(t, err)
	t.Log(uncSketch.String())
}

func TestMergingValidationCheck(t *testing.T) {
	lgMinK := 10
	lgMaxK := 10
	lgMulK := 5
	uPPO := 1
	incLgK := 1

	mv := NewMergingValidation(lgMinK, lgMaxK, lgMulK, uPPO, incLgK, nil, nil)
	assert.NoError(t, mv.Start())
}

func TestQuickMergingValidationCheck(t *testing.T) {
	lgMinK := 10
	lgMaxK := 10
	incLgK := 1

	qmv := NewQuickMergingValidation(lgMinK, lgMaxK, incLgK, nil, nil)
	assert.NoError(t, qmv.Start())
}

func TestCheckPwrLaw10NextDouble(t *testing.T) {
	got := pwrLaw10NextDouble(1, 10.0)
	assert.Equal(t, 100.0, got, "pwrLaw10NextDouble(1,10.0) should return 100.0")
}
