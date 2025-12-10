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
)

// hll4ArrayImpl Uses 4 bits per slot in a packed byte array
type hll4ArrayImpl struct {
	hllArrayImpl
}

func (h *hll4ArrayImpl) getSlotValue(slotNo int) (int, error) {
	nib := h.getNibble(slotNo)
	if nib == auxToken {
		auxHashMap := h.getAuxHashMap()
		return auxHashMap.mustFindValueFor(slotNo)
	} else {
		return nib + h.curMin, nil
	}
}

type hll4Iterator struct {
	hllPairIterator
	hll *hll4ArrayImpl
}

func (h *hll4ArrayImpl) iterator() pairIterator {
	a := newHll4Iterator(1<<h.lgConfigK, h)
	return &a
}

func (h *hll4ArrayImpl) ToCompactSlice() ([]byte, error) {
	return toHllByteArr(h, true)
}

func (h *hll4ArrayImpl) ToUpdatableSlice() ([]byte, error) {
	return toHllByteArr(h, false)
}

func (h *hll4ArrayImpl) GetUpdatableSerializationBytes() int {
	auxHashMap := h.getAuxHashMap()
	auxBytes := 0
	if auxHashMap == nil {
		auxBytes = 4 << lgAuxArrInts[h.lgConfigK]
	} else {
		auxBytes = 4 << auxHashMap.getLgAuxArrInts()
	}
	return hllByteArrStart + h.getHllByteArrBytes() + auxBytes
}

func (h *hll4ArrayImpl) copyAs(tgtHllType TgtHllType) (hllSketchStateI, error) {
	if tgtHllType == h.tgtHllType {
		return h.copy()
	}
	if tgtHllType == TgtHllTypeHll6 {
		return convertToHll6(h)
	}
	if tgtHllType == TgtHllTypeHll8 {
		return convertToHll8(h)
	}
	return nil, fmt.Errorf("cannot convert to TgtHllType id: %d ", int(tgtHllType))
}

func (h *hll4ArrayImpl) copy() (hllSketchStateI, error) {
	return &hll4ArrayImpl{
		hllArrayImpl: h.copyCommon(),
	}, nil
}

// newHll4Array returns a new Hll4Array.
func newHll4Array(lgConfigK int) hllArray {
	return &hll4ArrayImpl{
		hllArrayImpl: hllArrayImpl{
			hllSketchConfig: newHllSketchConfig(lgConfigK, TgtHllTypeHll4, curModeHll),
			curMin:          0,
			numAtCurMin:     1 << lgConfigK,
			hipAccum:        0,
			kxq0:            float64(uint64(1 << lgConfigK)),
			kxq1:            0,
			hllByteArr:      make([]byte, 1<<(lgConfigK-1)),
			auxStart:        hllByteArrStart + 1<<(lgConfigK-1),
		},
	}
}

// deserializeHll4 returns a new Hll4Array from the given byte array.
func deserializeHll4(byteArray []byte) (hllArray, error) {
	lgConfigK := extractLgK(byteArray)
	hll4 := newHll4Array(lgConfigK)
	hll4.extractCommonHll(byteArray)

	auxStart := hll4.getAuxStart()
	auxCount := extractAuxCount(byteArray)
	compact := extractCompactFlag(byteArray)

	if auxCount > 0 {
		auxHashMap, err := deserializeAuxHashMap(byteArray, auxStart, lgConfigK, auxCount, compact)
		if err != nil {
			return nil, err
		}
		hll4.putAuxHashMap(auxHashMap, false)
	}

	return hll4, nil
}

func convertToHll4(srcAbsHllArr hllArray) (hllSketchStateI, error) {
	lgConfigK := srcAbsHllArr.GetLgConfigK()
	hll4Array := newHll4Array(lgConfigK)
	hll4Array.putOutOfOrder(srcAbsHllArr.isOutOfOrder())

	// 1st pass: compute starting curMin and numAtCurMin:
	pair, err := curMinAndNum(srcAbsHllArr)
	if err != nil {
		return nil, err
	}
	curMin := getPairValue(pair)
	numAtCurMin := getPairLow26(pair)

	// 2nd pass: Must know curMin to create auxHashMap.
	// Populate KxQ registers, build auxHashMap if needed
	srcItr := srcAbsHllArr.iterator()
	auxHashMap := hll4Array.getAuxHashMap() //may be null
	for srcItr.nextValid() {
		slotNo := srcItr.getIndex()
		actualValue, err := srcItr.getValue()
		if err != nil {
			return nil, err
		}
		err = hll4Array.hipAndKxQIncrementalUpdate(0, actualValue)
		if err != nil {
			return nil, err
		}
		if actualValue >= (curMin + 15) {
			hll4Array.putNibble(slotNo, auxToken)
			if auxHashMap == nil {
				auxHashMap = newAuxHashMap(lgAuxArrInts[lgConfigK], lgConfigK)
				hll4Array.putAuxHashMap(auxHashMap, false)
			}
			err := auxHashMap.mustAdd(slotNo, actualValue)
			if err != nil {
				return nil, err
			}
		} else {
			hll4Array.putNibble(slotNo, byte(actualValue-curMin))
		}
	}
	hll4Array.putCurMin(curMin)
	hll4Array.putNumAtCurMin(numAtCurMin)
	hll4Array.putHipAccum(srcAbsHllArr.getHipAccum()) //intentional overwrite
	hll4Array.putRebuildCurMinNumKxQFlag(false)
	return hll4Array, nil
}

// couponUpdate updates the Hll4Array with the given coupon and returns the updated Hll4Array.
func (h *hll4ArrayImpl) couponUpdate(coupon int) (hllSketchStateI, error) {
	newValue := coupon >> keyBits26
	slotNo := coupon & h.slotNoMask
	err := internalHll4Update(h, slotNo, newValue)
	return h, err
}

func curMinAndNum(absHllArr hllArray) (int, error) {
	curMin := 64
	numAtCurMin := 0
	itr := absHllArr.iterator()
	for itr.nextAll() {
		v, err := itr.getValue()
		if err != nil {
			return 0, err
		}
		if v > curMin {
			continue
		}
		if v < curMin {
			curMin = v
			numAtCurMin = 1
		} else {
			numAtCurMin++
		}
	}
	return pair(numAtCurMin, curMin), nil
}

func newHll4Iterator(lengthPairs int, hll *hll4ArrayImpl) hll4Iterator {
	return hll4Iterator{
		hllPairIterator: newHllPairIterator(lengthPairs),
		hll:             hll,
	}
}

func (itr *hll4Iterator) getValue() (int, error) {
	return itr.hll.getSlotValue(itr.getIndex())
}

func (itr *hll4Iterator) getPair() (int, error) {
	v, err := itr.getValue()
	return pair(itr.index, v), err
}
