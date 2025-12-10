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

var v = 0

// Merges a type1 to an empty union (heap, HLL_8), and gets result as type1, checks binary equivalence
func TestIsomorphicUnionUpdatableHeap(t *testing.T) {
	for lgK := 4; lgK <= 21; lgK++ { //All LgK
		for cm := 0; cm <= 2; cm++ { //List, Set, Hll
			if (lgK < 8) && (cm == 1) { //lgk < 8 list transistions directly to HLL
				continue
			}
			curMode := curMode(cm)
			for tt := 0; tt <= 2; tt++ { //HLL_4, HLL_6, HLL_8
				tgtHllType1 := TgtHllType(tt)
				sk1, err := buildHeapSketch(lgK, tgtHllType1, curMode)
				assert.NoError(t, err)
				sk1bytes, err := sk1.ToUpdatableSlice()
				assert.NoError(t, err) //UPDATABLE
				union, err := NewUnion(lgK)
				assert.NoError(t, err)
				//UNION
				err = union.UpdateSketch(sk1)
				assert.NoError(t, err)
				sk2, err := union.GetResult(tgtHllType1)
				assert.NoError(t, err)
				sk2bytes, err := sk2.ToUpdatableSlice() //UPDATABLE
				assert.NoError(t, err)
				comp := fmt.Sprintf("LgK=%d, curMode=%d, Type:%d", lgK, curMode, tgtHllType1)
				checkArrays(t, sk1bytes, sk2bytes, comp, false)
			}
		}
	}
}

func TestIsomorphicUnionCompactHeap(t *testing.T) {
	for lgK := 4; lgK <= 21; lgK++ { //All LgK
		for cm := 0; cm <= 2; cm++ { //List, Set, Hll
			if (lgK < 8) && (cm == 1) { //lgk < 8 list transistions directly to HLL
				continue
			}
			curMode := curMode(cm)
			for tt := 0; tt <= 2; tt++ { //HLL_4, HLL_6, HLL_8
				tgtHllType1 := TgtHllType(tt)
				sk1, err := buildHeapSketch(lgK, tgtHllType1, curMode)
				assert.NoError(t, err)
				sk1bytes, err := sk1.ToCompactSlice() //COMPACT
				assert.NoError(t, err)
				union, err := NewUnion(lgK) //UNION
				assert.NoError(t, err)
				err = union.UpdateSketch(sk1)
				assert.NoError(t, err)
				sk2, err := union.GetResult(tgtHllType1)
				assert.NoError(t, err)
				sk2bytes, err := sk2.ToCompactSlice() //COMPACT
				assert.NoError(t, err)
				comp := fmt.Sprintf("LgK=%d, curMode=%d, Type:%d", lgK, curMode, tgtHllType1)
				checkArrays(t, sk1bytes, sk2bytes, comp, false)
			}
		}
	}
}

func TestIsomorphicCopyAsUpdatableHeap(t *testing.T) {
	for lgK := 4; lgK <= 21; lgK++ { //All LgK
		for cm := 0; cm <= 2; cm++ { //List, Set, Hll
			if (lgK < 8) && (cm == 1) { //lgk < 8 list transistions directly to HLL
				continue
			}
			curMode := curMode(cm)
			for t1 := 0; t1 <= 2; t1++ { //HLL_4, HLL_6, HLL_8
				tgtHllType1 := TgtHllType(t1)
				sk1, err := buildHeapSketch(lgK, tgtHllType1, curMode)
				assert.NoError(t, err)
				sk1bytes, err := sk1.ToUpdatableSlice() //UPDATABLE
				assert.NoError(t, err)
				for t2 := 0; t2 <= 2; t2++ { //HLL_4, HLL_6, HLL_8
					if t2 == t1 {
						continue
					}
					tgtHllType2 := TgtHllType(t2)
					sk2, err := sk1.CopyAs(tgtHllType2) //COPY AS
					assert.NoError(t, err)
					sk1B, err := sk2.CopyAs(tgtHllType1) //COPY AS
					assert.NoError(t, err)
					sk1Bbytes, err := sk1B.ToUpdatableSlice() //UPDATABLE
					assert.NoError(t, err)
					comp := fmt.Sprintf("LgK=%d, curMode=%d, Type1:%d, Type2:%d", lgK, curMode, tgtHllType1, tgtHllType2)
					checkArrays(t, sk1bytes, sk1Bbytes, comp, false)
				}
			}
		}
	}
}

func TestIsomorphicHllMerges2(t *testing.T) {
	for lgK := 4; lgK <= 4; lgK++ { //All LgK
		u, err := buildHeapUnionHllMode(lgK, 0)
		assert.NoError(t, err)
		sk, err := buildHeapSketchHllMode(lgK, TgtHllTypeHll8, 1<<lgK)
		assert.NoError(t, err)
		err = u.UpdateSketch(sk)
		assert.NoError(t, err)
		resultOut8, err := u.GetResult(TgtHllTypeHll8) //The reference
		assert.NoError(t, err)
		bytesOut8, err := resultOut8.ToUpdatableSlice()
		assert.NoError(t, err)

		u, err = buildHeapUnionHllMode(lgK, 0)
		assert.NoError(t, err)
		sk, err = buildHeapSketchHllMode(lgK, TgtHllTypeHll6, 1<<lgK)
		assert.NoError(t, err)
		err = u.UpdateSketch(sk)
		assert.NoError(t, err)
		resultOut6, err := u.GetResult(TgtHllTypeHll8) //should be identical except for HllAccum
		assert.NoError(t, err)
		bytesOut6, err := resultOut6.ToUpdatableSlice()
		assert.NoError(t, err)

		comb := fmt.Sprintf("LgK: %d, SkType: HLL_6, Compared with SkType HLL_8", lgK)
		checkArrays(t, bytesOut8, bytesOut6, comb, false)

		u, err = buildHeapUnionHllMode(lgK, 0)
		assert.NoError(t, err)
		sk, err = buildHeapSketchHllMode(lgK, TgtHllTypeHll4, 1<<lgK)
		assert.NoError(t, err)
		err = u.UpdateSketch(sk)
		assert.NoError(t, err)
		resultOut4, err := u.GetResult(TgtHllTypeHll8) //should be identical except for HllAccum
		assert.NoError(t, err)
		bytesOut4, err := resultOut4.ToUpdatableSlice()
		assert.NoError(t, err)
		comb = fmt.Sprintf("LgK: %d, SkType: HLL_4, Compared with SkType HLL_8", lgK)
		checkArrays(t, bytesOut8, bytesOut4, comb, false)
	}
}

func TestIsomorphicCopyAsCompactHeap(t *testing.T) {
	for lgK := 4; lgK <= 21; lgK++ { //All LgK
		for cm := 0; cm <= 2; cm++ { //List, Set, Hll
			if (lgK < 8) && (cm == 1) { //lgk < 8 list transistions directly to HLL
				continue
			}
			curMode := curMode(cm)
			for t1 := 0; t1 <= 2; t1++ { //HLL_4, HLL_6, HLL_8
				tgtHllType1 := TgtHllType(t1)
				sk1, err := buildHeapSketch(lgK, tgtHllType1, curMode)
				assert.NoError(t, err)
				sk1bytes, err := sk1.ToCompactSlice() //COMPACT
				assert.NoError(t, err)
				for t2 := 0; t2 <= 2; t2++ { //HLL_4, HLL_6, HLL_8
					if t2 == t1 {
						continue
					}
					tgtHllType2 := TgtHllType(t2)
					sk2, err := sk1.CopyAs(tgtHllType2) //COPY AS
					assert.NoError(t, err)
					sk1B, err := sk2.CopyAs(tgtHllType1) //COPY AS
					assert.NoError(t, err)
					sk1Bbytes, err := sk1B.ToCompactSlice() //COMPACT
					assert.NoError(t, err)
					comp := fmt.Sprintf("LgK=%d, curMode=%d, Type1:%d, Type2:%d", lgK, curMode, tgtHllType1, tgtHllType2)
					checkArrays(t, sk1bytes, sk1Bbytes, comp, false)
				}
			}
		}
	}
}

func checkArrays(t *testing.T, sk1bytes, sk2bytes []byte, comb string, omitHipAccum bool) {
	leng := len(sk1bytes)
	if leng != len(sk2bytes) {
		t.Errorf("Sketch images not the same length: %s", comb)
		return
	}
	for i := 0; i < leng; i++ {
		if omitHipAccum && (i >= 8) && (i <= 15) {
			continue
		}
		if sk1bytes[i] == sk2bytes[i] {
			continue
		}
		t.Errorf("%s: %d", comb, i)
	}
}

func buildHeapUnionHllMode(lgK int, startN int) (Union, error) {
	u, err := NewUnion(lgK)
	if err != nil {
		return nil, err
	}
	n := getN(lgK, curModeHll)
	for i := 0; i < n; i++ {
		err = u.UpdateUInt64(uint64(i + startN))
		if err != nil {
			return nil, err
		}
	}
	return u, nil
}

func buildHeapSketch(lgK int, tgtHllType TgtHllType, curMode curMode) (HllSketch, error) {
	sk, err := NewHllSketch(lgK, tgtHllType)
	if err != nil {
		return nil, err
	}
	n := getN(lgK, curMode)
	for i := 0; i < n; i++ {
		err = sk.UpdateUInt64(uint64(i + v))
		if err != nil {
			return nil, err
		}
	}
	v += n
	return sk, nil
}

func buildHeapSketchHllMode(lgK int, tgtHllType TgtHllType, startN int) (HllSketch, error) {
	sk, err := NewHllSketch(lgK, tgtHllType)
	if err != nil {
		return nil, err
	}
	n := getN(lgK, curModeHll)
	for i := 0; i < n; i++ {
		err = sk.UpdateUInt64(uint64(i + startN))
		if err != nil {
			return nil, err
		}
	}
	return sk, nil
}

// if lgK >= 8, curMode != SET!
func getN(lgK int, curMode curMode) int {
	if curMode == curModeList {
		return 4
	}
	if curMode == curModeSet {
		return 1 << (lgK - 4)
	}
	if (lgK < 8) && (curMode == curModeHll) {
		return 1 << lgK
	}
	return 1 << (lgK - 3)
}
