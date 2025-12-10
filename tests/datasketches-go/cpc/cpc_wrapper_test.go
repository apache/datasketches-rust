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
	"testing"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
)

func TestCpcWrapper_Check(t *testing.T) {
	lgK := 10

	// Create three CPC sketches with the same lgK
	sk1, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	sk2, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	skD, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)

	// Query skD while empty
	dEst := skD.GetEstimate()
	dLb := skD.GetLowerBound(2)
	dUb := skD.GetUpperBound(2)
	_ = dEst
	_ = dLb
	_ = dUb

	// Populate sketches
	n := 100000
	for i := 0; i < n; i++ {
		// sk1 gets i
		assert.NoError(t, sk1.UpdateInt64(int64(i)))
		// sk2 gets i + n
		assert.NoError(t, sk2.UpdateInt64(int64(i+n)))
		// skD gets both i and i + n
		assert.NoError(t, skD.UpdateInt64(int64(i)))
		assert.NoError(t, skD.UpdateInt64(int64(i+n)))
	}
	concatArr, err := skD.ToCompactSlice()
	assert.NoError(t, err)
	// Create a union
	u, err := NewCpcUnionSketchWithDefault(lgK)
	assert.NoError(t, err)
	// Query the union while empty
	result, err := u.GetResult()
	assert.NoError(t, err)
	uEst := result.GetEstimate()
	uLb := result.GetLowerBound(2)
	uUb := result.GetUpperBound(2)
	_ = uEst
	_ = uLb
	_ = uUb
	// Merge sk1 and sk2 into the union
	assert.NoError(t, u.Update(sk1))
	assert.NoError(t, u.Update(sk2))
	merged, err := u.GetResult()
	assert.NoError(t, err)
	// Convert merged to a byte array
	mergedArr, err := merged.ToCompactSlice()
	assert.NoError(t, err)
	// Create a CpcWrapper from concatArr
	concatSk, err := NewCpcWrapperFromBytes(concatArr)
	assert.NoError(t, err)
	assert.Equal(t, lgK, concatSk.GetLgK())

	ccEst := concatSk.GetEstimate()
	ccLb := concatSk.GetLowerBound(2)
	ccUb := concatSk.GetUpperBound(2)
	t.Logf("Concatenated: Lb=%.0f, Est=%.0f, Ub=%.0f", ccLb, ccEst, ccUb)

	// Create a CpcWrapper from mergedArr
	mergedSk, err := NewCpcWrapperFromBytes(mergedArr)
	assert.NoError(t, err)

	mEst := mergedSk.GetEstimate()
	mLb := mergedSk.GetLowerBound(2)
	mUb := mergedSk.GetUpperBound(2)
	t.Logf("Merged: Lb=%.0f, Est=%.0f, Ub=%.0f", mLb, mEst, mUb)

	// Check the Family
	assert.Equal(t, internal.FamilyEnum.CPC.Id, mergedSk.GetFamily())
}

// It corrupts a CPC sketch’s byte array to fail the "isCompressed" check.
func TestCpcWrapper_CheckIsCompressed(t *testing.T) {
	sk, err := NewCpcSketch(10, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)

	// Convert to bytes
	byteArr, err := sk.ToCompactSlice()
	assert.NoError(t, err)

	// (byte)-3 is 0xFD in hex, so we do an AND with 0xFD.
	byteArr[5] = byteArr[5] & 0xFD

	// Attempt to wrap in CpcWrapper. We expect an error or panic (depending on your logic).
	_, err = NewCpcWrapperFromBytes(byteArr)
	if err == nil {
		t.Fatalf("Expected error or panic from isCompressed check, but got nil")
	}
}
