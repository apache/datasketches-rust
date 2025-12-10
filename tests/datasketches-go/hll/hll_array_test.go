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

package hll

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompositeEst(t *testing.T) {
	testComposite(t, 4, TgtHllTypeHll4, 1000)
	testComposite(t, 5, TgtHllTypeHll4, 1000)
	testComposite(t, 6, TgtHllTypeHll4, 1000)
	testComposite(t, 13, TgtHllTypeHll4, 10000)

	testComposite(t, 4, TgtHllTypeHll6, 1000)
	testComposite(t, 5, TgtHllTypeHll6, 1000)
	testComposite(t, 6, TgtHllTypeHll6, 1000)
	testComposite(t, 13, TgtHllTypeHll6, 10000)

	testComposite(t, 4, TgtHllTypeHll8, 1000)
	testComposite(t, 5, TgtHllTypeHll8, 1000)
	testComposite(t, 6, TgtHllTypeHll8, 1000)
	testComposite(t, 13, TgtHllTypeHll8, 10000)
}

func testComposite(t *testing.T, lgK int, tgtHllType TgtHllType, n int) {
	u, err := NewUnion(lgK)
	assert.NoError(t, err)
	sk, err := NewHllSketch(lgK, tgtHllType)
	assert.NoError(t, err)

	for i := 0; i < n; i++ {
		assert.NoError(t, u.UpdateInt64(int64(i)))
		assert.NoError(t, sk.UpdateInt64(int64(i)))
	}

	err = u.UpdateSketch(sk)
	assert.NoError(t, err)
	res, err := u.GetResult(tgtHllType)
	assert.NoError(t, err)
	_, err = res.GetCompositeEstimate()
	assert.NoError(t, err)

}

func TestBigHipGetRse(t *testing.T) {
	sk, err := NewHllSketch(13, TgtHllTypeHll8)
	assert.NoError(t, err)

	for i := 0; i < 10000; i++ {
		assert.NoError(t, sk.UpdateInt64(int64(i)))
	}
}

func TestToArraySliceDeserialize(t *testing.T) {
	lgK := 4
	u := 8
	toArraySliceDeserialize(t, lgK, TgtHllTypeHll4, u)
	toArraySliceDeserialize(t, lgK, TgtHllTypeHll6, u)
	toArraySliceDeserialize(t, lgK, TgtHllTypeHll8, u)

	lgK = 16
	u = (((1 << (lgK - 3)) * 3) / 4) + 100
	toArraySliceDeserialize(t, lgK, TgtHllTypeHll4, u)
	toArraySliceDeserialize(t, lgK, TgtHllTypeHll6, u)
	toArraySliceDeserialize(t, lgK, TgtHllTypeHll8, u)

	lgK = 21
	u = (((1 << (lgK - 3)) * 3) / 4) + 1000
	toArraySliceDeserialize(t, lgK, TgtHllTypeHll4, u)
	toArraySliceDeserialize(t, lgK, TgtHllTypeHll6, u)
	toArraySliceDeserialize(t, lgK, TgtHllTypeHll8, u)
}

func toArraySliceDeserialize(t *testing.T, lgK int, tgtHllType TgtHllType, u int) {
	sk1, err := NewHllSketch(lgK, tgtHllType)
	assert.NoError(t, err)

	for i := 0; i < u; i++ {
		assert.NoError(t, sk1.UpdateInt64(int64(i)))
	}
	_, isArray := sk1.(*hllSketchState).sketch.(hllArray)
	assert.True(t, isArray)

	// Update
	est1, err := sk1.GetEstimate()
	assert.NoError(t, err)
	assert.InDelta(t, est1, u, float64(u)*.03)
	est, err := sk1.(*hllSketchState).GetHipEstimate()
	assert.NoError(t, err)
	assert.Equal(t, est, est1, 0.0)

	// misc
	sk1.(*hllSketchState).putRebuildCurMinNumKxQFlag(true)
	sk1.(*hllSketchState).putRebuildCurMinNumKxQFlag(false)

	sl1, err := sk1.ToCompactSlice()
	assert.NoError(t, err)
	sk2, e := NewHllSketchFromSlice(sl1, true)
	assert.NoError(t, e)
	est2, err := sk2.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, est2, est1, 0.0)

	err = sk1.Reset()
	assert.NoError(t, err)
	est, err = sk1.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, est, 0.0, 0.0)
}
