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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCpcUnionError(t *testing.T) {
	sk, err := NewCpcSketch(10, 1)
	assert.NoError(t, err)
	union, err := NewCpcUnionSketchWithDefault(defaultLgK)
	assert.NoError(t, err)
	assert.Error(t, union.Update(sk))
}

func TestCpcGetters(t *testing.T) {
	lgK := 10
	union, err := NewCpcUnionSketchWithDefault(lgK)
	if err != nil {
		t.Errorf("Error: %s", err)
	}
	assert.Equal(t, union.lgK, lgK)
	assert.Equal(t, union.getNumCoupons(), uint64(0))
	sk, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	for i := 0; i <= (4 << lgK); i++ {
		err := sk.UpdateInt64(int64(i))
		assert.NoError(t, err)
	}
	err = union.Update(sk)
	assert.NoError(t, err)
	assert.True(t, union.getNumCoupons() > 0)
	assert.NotNil(t, getBitMatrix(t, union))
	assert.Equal(t, union.GetFamilyId(), internal.FamilyEnum.CPC.Id)
}

func TestCpcReduceK(t *testing.T) {
	union, err := NewCpcUnionSketch(12, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	sk, err := NewCpcSketch(11, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	u := uint64(1)
	err = sk.UpdateUint64(u)
	assert.NoError(t, err)
	err = union.Update(sk)
	assert.NoError(t, err)
	getBitMatrix(t, union)
	sk2, err := NewCpcSketch(10, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	shTrans := uint64((3 * 512) / 32) //sparse-hybrid transition for lgK=9
	for sk2.numCoupons < shTrans {
		u++
		err = sk2.UpdateUint64(u)
		assert.NoError(t, err)
	}
	err = union.Update(sk2)
	assert.NoError(t, err)
	sk3, err := NewCpcSketch(9, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	u++
	err = sk3.UpdateUint64(u)
	assert.NoError(t, err)
	err = union.Update(sk3)
	assert.NoError(t, err)
	sk4, err := NewCpcSketch(8, internal.DEFAULT_UPDATE_SEED)
	assert.NoError(t, err)
	u++
	err = sk4.UpdateUint64(u)
	assert.NoError(t, err)
}

func getBitMatrix(t *testing.T, union CpcUnion) []uint64 {
	err := union.checkUnionState()
	assert.NoError(t, err)
	if union.bitMatrix != nil {
		return union.bitMatrix
	}
	bitMatrix, err := union.accumulator.bitMatrixOfSketch()
	if err != nil {
		panic(err)
	}
	return bitMatrix
}
