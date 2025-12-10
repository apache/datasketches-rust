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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var nArr = []int{1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000}

func TestUnions(t *testing.T) {
	//HLL_4: t=0,  HLL_6: t=1, HLL_8: t=2
	const (
		t1 = 2 //type = HLL_8
		t2 = 2
		rt = 2 //result type
	)

	const (
		lgK1   = 7
		lgK2   = 7
		lgMaxK = 7
		n1     = 7
		n2     = 7
	)

	checkBasicUnion(t, n1, n2, lgK1, lgK2, lgMaxK, t1, t2, rt)
}

func checkBasicUnion(t *testing.T, n1 int, n2 int, lgK1 int, lgK2 int, lgMaxK int, t1 int, t2 int, rt int) {
	v := 0
	tot := n1 + n2

	type1 := TgtHllType(t1)
	type2 := TgtHllType(t2)
	resultType := TgtHllType(rt)

	h1, err := NewHllSketch(lgK1, type1)
	assert.NoError(t, err)
	h2, err := NewHllSketch(lgK1, type2)
	assert.NoError(t, err)

	lgControlK := min(min(lgK1, lgK2), lgMaxK)
	control, err := NewHllSketch(lgControlK, resultType)
	assert.NoError(t, err)

	for i := 0; i < n1; i++ {
		assert.NoError(t, h1.UpdateInt64(int64(v+i)))
		assert.NoError(t, control.UpdateInt64(int64(v+i)))
	}
	v += n1
	for i := 0; i < n2; i++ {
		assert.NoError(t, h2.UpdateInt64(int64(v+i)))
		assert.NoError(t, control.UpdateInt64(int64(v+i)))
	}
	//v += n2

	union, err := NewUnion(lgMaxK)
	assert.NoError(t, err)
	assert.NoError(t, union.UpdateSketch(h1))
	assert.NoError(t, union.UpdateSketch(h2))
	result, err := union.GetResult(resultType)
	assert.NoError(t, err)

	uEst, err := result.GetEstimate()
	assert.NoError(t, err)
	uUb, err := result.GetUpperBound(2)
	assert.NoError(t, err)
	uLb, err := result.GetLowerBound(2)
	assert.NoError(t, err)
	//rErr := ((uEst / float64(tot)) - 1.0) * 100
	//
	//mode1 := h1.GetCurMode()
	//mode2 := h2.GetCurMode()
	//modeR := result.GetCurMode()

	// Control
	controlEst, err := control.GetEstimate()
	assert.NoError(t, err)
	controlUb, err := control.GetUpperBound(2)
	assert.NoError(t, err)
	controlLb, err := control.GetLowerBound(2)
	assert.NoError(t, err)
	//h1ooo := h1.isOutOfOrderFlag()
	//h1ooo := h2.isOutOfOrderFlag()
	//resultooo := result.isOutOfOrderFlag()

	assert.True(t, controlUb-controlEst >= 0)
	assert.True(t, uUb-uEst >= 0)
	assert.True(t, controlEst-controlLb >= 0)
	assert.True(t, uEst-uLb >= 0)

	assert.Equal(t, 7, result.GetLgConfigK())
	est, err := result.GetEstimate()
	assert.NoError(t, err)
	assert.InDelta(t, tot, est, float64(tot)*0.03)
}

func TestToFromUnion1(t *testing.T) {
	for i := 0; i < 10; i++ {
		n := nArr[i]
		for lgK := 4; lgK <= 13; lgK++ {
			toFrom1(t, lgK, TgtHllTypeHll4, n)
			toFrom1(t, lgK, TgtHllTypeHll6, n)
			toFrom1(t, lgK, TgtHllTypeHll8, n)
		}
	}
}

func toFrom1(t *testing.T, lgK int, tgtHllType TgtHllType, n int) {
	srcU, err := NewUnion(lgK)
	assert.NoError(t, err)
	srcSk, err := NewHllSketch(lgK, tgtHllType)
	assert.NoError(t, err)
	for i := 0; i < n; i++ {
		assert.NoError(t, srcSk.UpdateInt64(int64(i)))
	}
	assert.NoError(t, srcU.UpdateSketch(srcSk))
	fmt.Printf("n: %d, lgK: %d, type: %d\n", n, lgK, tgtHllType)

	byteArr, err := srcU.ToCompactSlice()
	assert.NoError(t, err)
	dstU, _ := NewUnionFromSlice(byteArr)

	dstUest, err := dstU.GetEstimate()
	assert.NoError(t, err)
	srcUest, err := srcU.GetEstimate()
	assert.NoError(t, err)

	assert.Equal(t, dstUest, srcUest)
}

func TestUnionCompositeEst(t *testing.T) {
	u, err := NewUnionWithDefault()
	assert.NoError(t, err)
	est, err := u.GetCompositeEstimate()
	assert.NoError(t, err)
	assert.Equal(t, est, 0.0)
	for i := 1; i <= 15; i++ {
		assert.NoError(t, u.UpdateInt64(int64(i)))
	}
	est, err = u.GetCompositeEstimate()
	assert.NoError(t, err)

	assert.InDelta(t, est, 15.0, 15.0*0.03)
	for i := 15; i <= 1000; i++ {
		assert.NoError(t, u.UpdateInt64(int64(i)))
	}
	est, err = u.GetCompositeEstimate()
	assert.NoError(t, err)
	assert.InDelta(t, est, 1000.0, 1000.0*0.03)
}

func TestDeserialize1k(t *testing.T) {
	u, err := NewUnion(16)
	assert.NoError(t, err)
	for i := 0; i < (1 << 10); i++ {
		assert.NoError(t, u.UpdateInt64(int64(i)))
	}
	expected, err := u.GetEstimate()
	assert.NoError(t, err)
	byteArr, err := u.ToUpdatableSlice()
	assert.NoError(t, err)
	u2, e := NewUnionFromSlice(byteArr)
	assert.NoError(t, e)
	est, err := u2.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, expected, est)
}

func TestDeserialize1M(t *testing.T) {
	u, err := NewUnion(16)
	assert.NoError(t, err)
	for i := 0; i < (1 << 20); i++ {
		assert.NoError(t, u.UpdateInt64(int64(i)))
	}
	expected, err := u.GetEstimate()
	assert.NoError(t, err)
	byteArr, err := u.ToUpdatableSlice()
	assert.NoError(t, err)
	u2, e := NewUnionFromSlice(byteArr)
	assert.NoError(t, e)
	est, err := u2.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, expected, est)
}

func TestEmptyCouponMisc(t *testing.T) {
	lgK := 8
	u, err := NewUnion(lgK)
	assert.NoError(t, err)
	for i := 0; i < 20; i++ {
		assert.NoError(t, u.UpdateInt64(int64(i)))
	}
	_, err = u.couponUpdate(0)
	assert.NoError(t, err)
	est, err := u.GetEstimate()
	assert.NoError(t, err)
	assert.InDelta(t, est, 20.0, 0.001)
	assert.Equal(t, u.GetTgtHllType(), TgtHllTypeHll8)
	bytes := u.GetUpdatableSerializationBytes()
	assert.True(t, bytes <= getMaxUpdatableSerializationBytes(lgK, TgtHllTypeHll8))
}

func TestUnionWithWrap(t *testing.T) {
	lgK := 4
	type1 := TgtHllTypeHll4
	n := 2
	sk, err := NewHllSketch(lgK, type1)
	assert.NoError(t, err)
	for i := 0; i < n; i++ {
		assert.NoError(t, sk.UpdateInt64(int64(i)))
	}
	est, err := sk.GetEstimate()
	assert.NoError(t, err)
	skByteArr, err := sk.ToCompactSlice()
	assert.NoError(t, err)

	sk2, _ := NewHllSketchFromSlice(skByteArr, false)
	est2, err := sk2.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, est2, est)

	u, err := NewUnion(lgK)
	assert.NoError(t, err)
	err = u.UpdateSketch(sk2)
	assert.NoError(t, err)
	estU, err := u.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, estU, est)
}

func TestUnionWithWrap2(t *testing.T) {
	lgK := 10
	n := 128
	sk, err := NewHllSketch(lgK, TgtHllTypeDefault)
	assert.NoError(t, err)
	for i := 0; i < n; i++ {
		assert.NoError(t, sk.UpdateInt64(int64(i)))
	}
	est, err := sk.GetEstimate()
	assert.NoError(t, err)
	skByteArr, err := sk.ToCompactSlice()
	assert.NoError(t, err)

	sk2, _ := NewHllSketchFromSlice(skByteArr, false)
	sk2Est, err := sk2.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, sk2Est, est)

	u, err := NewUnion(lgK)
	assert.NoError(t, err)
	err = u.UpdateSketch(sk2)
	assert.NoError(t, err)
	estU, err := u.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, estU, est)
}

func TestConversions(t *testing.T) {
	lgK := 4
	sk1, err := NewHllSketch(lgK, TgtHllTypeHll8)
	assert.NoError(t, err)
	sk2, err := NewHllSketch(lgK, TgtHllTypeHll8)
	assert.NoError(t, err)
	u := 1 << 20
	for i := 0; i < u; i++ {
		assert.NoError(t, sk1.UpdateInt64(int64(i)))
		assert.NoError(t, sk2.UpdateInt64(int64(i+u)))
	}
	union, err := NewUnion(lgK)
	assert.NoError(t, err)
	assert.NoError(t, union.UpdateSketch(sk1))
	assert.NoError(t, union.UpdateSketch(sk2))
	rsk1, err := union.GetResult(TgtHllTypeHll8)
	assert.NoError(t, err)
	rsk2, err := union.GetResult(TgtHllTypeHll6)
	assert.NoError(t, err)
	rsk3, err := union.GetResult(TgtHllTypeHll4)
	assert.NoError(t, err)
	est1, err := rsk1.GetEstimate()
	assert.NoError(t, err)
	est2, err := rsk2.GetEstimate()
	assert.NoError(t, err)
	est3, err := rsk3.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, est2, est1)
	assert.Equal(t, est3, est1)
}

func TestCheckUnionDeserializeRebuildAfterMerge(t *testing.T) {
	lgK := 12
	//Build 2 sketches in HLL (dense) mode.
	u := 1 << (lgK - 3) //(lgK < 8) ? 16 : 1 << (lgK - 3) //allows changing lgK above
	sk1, err := NewHllSketch(lgK, TgtHllTypeDefault)
	assert.NoError(t, err)
	sk2, err := NewHllSketch(lgK, TgtHllTypeDefault)
	assert.NoError(t, err)
	for i := 0; i < u; i++ {
		assert.NoError(t, sk1.UpdateInt64(int64(i)))
		assert.NoError(t, sk2.UpdateInt64(int64(i+u)))
	}
	union1, err := NewUnion(lgK)
	assert.NoError(t, err)
	assert.NoError(t, union1.UpdateSketch(sk1))
	assert.NoError(t, union1.UpdateSketch(sk2)) //oooFlag = Rebuild_KxQ = TRUE
	rebuild := union1.(*unionImpl).gadget.(*hllSketchState).sketch.(*hll8ArrayImpl).isRebuildCurMinNumKxQFlag()
	hipAccum := union1.(*unionImpl).gadget.(*hllSketchState).sketch.(*hll8ArrayImpl).hipAccum
	assert.True(t, rebuild)
	assert.Equal(t, hipAccum, 0.0)
	//Deserialize byteArr as if it were a sketch, but it is actually a union!
	sl, err := union1.ToUpdatableSlice() //forces rebuild
	assert.NoError(t, err)
	sk3, e := NewHllSketchFromSlice(sl, false) //rebuilds sk3
	assert.NoError(t, e)
	rebuild = sk3.(*hllSketchState).sketch.(*hll8ArrayImpl).isRebuildCurMinNumKxQFlag()
	assert.False(t, rebuild)

}
