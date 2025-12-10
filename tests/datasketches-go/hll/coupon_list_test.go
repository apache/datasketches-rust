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

func TestCouponIterator(t *testing.T) {
	lgK := 4
	n := 7
	sk, err := NewHllSketch(lgK, TgtHllTypeDefault)
	assert.NoError(t, err)
	for i := 0; i < n; i++ {
		assert.NoError(t, sk.UpdateInt64(int64(i)))
	}

	iter := sk.iterator()
	for iter.nextAll() {
		if iter.getIndex() >= (1 << lgK) {
			t.Errorf("Slot %d is out of range", iter.getSlot())
		}
	}
}

func TestCouponDuplicatesAndMisc(t *testing.T) {
	sk, err := NewHllSketch(8, TgtHllTypeDefault)
	assert.NoError(t, err)
	for i := 1; i <= 7; i++ {
		assert.NoError(t, sk.UpdateInt64(int64(i)))
		assert.NoError(t, sk.UpdateInt64(int64(i)))
	}
	assert.Equal(t, sk.GetCurMode(), curModeList)
	est, err := sk.GetCompositeEstimate()
	assert.NoError(t, err)
	assert.InDelta(t, est, 7.0, 7*.01)
	est, err = sk.(*hllSketchState).sketch.GetHipEstimate()
	assert.NoError(t, err)
	assert.InDelta(t, est, 7.0, 7*.01)
	sk.(*hllSketchState).putRebuildCurMinNumKxQFlag(false) //dummy

	assert.NoError(t, sk.UpdateInt64(8))
	assert.NoError(t, sk.UpdateInt64(8))
	assert.Equal(t, sk.GetCurMode(), curModeSet)
	est, err = sk.GetCompositeEstimate()
	assert.NoError(t, err)
	assert.InDelta(t, est, 8.0, 8*.01)
	est, err = sk.(*hllSketchState).GetHipEstimate()
	assert.NoError(t, err)
	assert.InDelta(t, est, 8.0, 8*.01)

	for i := 9; i <= 25; i++ {
		assert.NoError(t, sk.UpdateInt64(int64(i)))
		assert.NoError(t, sk.UpdateInt64(int64(i)))
	}

	assert.Equal(t, sk.GetCurMode(), curModeHll)
	est, err = sk.GetCompositeEstimate()
	assert.NoError(t, err)
	assert.InDelta(t, est, 25.0, 25*.1)
}

func TestToCouponSliceDeserialize(t *testing.T) {
	toCouponSliceDeserialize(t, 7)
	toCouponSliceDeserialize(t, 21)

}

func toCouponSliceDeserialize(t *testing.T, lgK int) {
	sk1, err := NewHllSketch(lgK, TgtHllTypeDefault)
	assert.NoError(t, err)

	u := 7
	if lgK >= 8 {
		u = (1 << (lgK - 3)) * 3 / 4
	}

	for i := 0; i < u; i++ {
		assert.NoError(t, sk1.UpdateInt64(int64(i)))
	}

	_, isCoupon := sk1.(*hllSketchState).sketch.(hllCoupon)
	assert.True(t, isCoupon)

	est1, err := sk1.GetEstimate()
	assert.NoError(t, err)
	assert.InDelta(t, est1, float64(u), float64(u)*100.0e-6)

	sl1, err := sk1.ToCompactSlice()
	assert.NoError(t, err)
	sk2, err := NewHllSketchFromSlice(sl1, true)
	assert.NoError(t, err)
	est2, err := sk2.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, est2, est1)

	sl1, err = sk1.ToUpdatableSlice()
	assert.NoError(t, err)
	sk2, err = NewHllSketchFromSlice(sl1, true)
	assert.NoError(t, err)
	est2, err = sk2.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, est2, est1)
}
