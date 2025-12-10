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

	"github.com/apache/datasketches-go/internal"
)

// hll6ArrayImpl Uses 6 bits per slot in a packed byte array
type hll6ArrayImpl struct {
	hllArrayImpl
}

type hll6Iterator struct {
	hllPairIterator
	hll       *hll6ArrayImpl
	bitOffset int
}

func (h *hll6ArrayImpl) iterator() pairIterator {
	a := newHll6Iterator(1<<h.lgConfigK, h)
	return &a
}

func (h *hll6ArrayImpl) copyAs(tgtHllType TgtHllType) (hllSketchStateI, error) {
	if tgtHllType == h.tgtHllType {
		return h.copy()
	}
	if tgtHllType == TgtHllTypeHll4 {
		return convertToHll4(h)
	}
	if tgtHllType == TgtHllTypeHll8 {
		return convertToHll8(h)
	}
	return nil, fmt.Errorf("cannot convert to TgtHllType id: %d", int(tgtHllType))
}

func (h *hll6ArrayImpl) copy() (hllSketchStateI, error) {
	return &hll6ArrayImpl{
		hllArrayImpl: h.copyCommon(),
	}, nil
}

func (h *hll6ArrayImpl) ToCompactSlice() ([]byte, error) {
	return h.ToUpdatableSlice()
}

func (h *hll6ArrayImpl) ToUpdatableSlice() ([]byte, error) {
	return toHllByteArr(h, false)
}

// newHll6Array returns a new Hll4Array.
func newHll6Array(lgConfigK int) hllArray {
	return &hll6ArrayImpl{
		hllArrayImpl: hllArrayImpl{
			hllSketchConfig: newHllSketchConfig(lgConfigK, TgtHllTypeHll6, curModeHll),
			curMin:          0,
			numAtCurMin:     1 << lgConfigK,
			hipAccum:        0,
			kxq0:            float64(uint64(1 << lgConfigK)),
			kxq1:            0,
			hllByteArr:      make([]byte, (((1<<lgConfigK)*3)>>2)+1),
			auxStart:        hllByteArrStart + 1<<(lgConfigK-1),
		},
	}
}

// deserializeHll6 returns a new Hll6Array from the given byte array.
func deserializeHll6(byteArray []byte) hllArray {
	lgConfigK := extractLgK(byteArray)
	hll6 := newHll6Array(lgConfigK)
	hll6.extractCommonHll(byteArray)
	return hll6
}

func (h *hll6ArrayImpl) couponUpdate(coupon int) (hllSketchStateI, error) {
	newValue := coupon >> keyBits26
	slotNo := coupon & h.slotNoMask
	err := h.updateSlotWithKxQ(slotNo, newValue)
	return h, err
}

func (h *hll6ArrayImpl) updateSlotWithKxQ(slotNo int, newValue int) error {
	oldValue := h.getSlotValue(slotNo)
	if newValue > oldValue {
		put6Bit(h.hllByteArr, 0, slotNo, newValue)
		err := h.hipAndKxQIncrementalUpdate(oldValue, newValue)
		if err != nil {
			return err
		}
		if oldValue == 0 {
			h.numAtCurMin-- //interpret numAtCurMin as num Zeros
			if h.numAtCurMin < 0 {
				return fmt.Errorf("numAtCurMin < 0")
			}
		}
	}
	return nil
}

func (h *hll6ArrayImpl) getSlotValue(slotNo int) int {
	return get6Bit(h.hllByteArr, 0, slotNo)
}

func get6Bit(arr []byte, offsetBytes int, slotNo int) int {
	startBit := slotNo * 6
	shift := startBit & 0x7
	byteIdx := (startBit >> 3) + offsetBytes
	return (internal.GetShortLE(arr, byteIdx) >> shift) & 0x3F
}

func put6Bit(arr []byte, offsetBytes int, slotNo int, newValue int) {
	startBit := slotNo * 6
	shift := startBit & 0x7
	byteIdx := (startBit >> 3) + offsetBytes
	valShifted := (newValue & 0x3F) << shift
	curMasked := internal.GetShortLE(arr, byteIdx) & (^(valMask6 << shift))
	insert := curMasked | valShifted
	internal.PutShortLE(arr, byteIdx, insert)
}

func convertToHll6(srcAbsHllArr hllArray) (hllSketchStateI, error) {
	lgConfigK := srcAbsHllArr.GetLgConfigK()
	hll6Array := newHll6Array(lgConfigK)
	hll6Array.putOutOfOrder(srcAbsHllArr.isOutOfOrder())
	numZeros := 1 << lgConfigK
	srcItr := srcAbsHllArr.iterator()
	for srcItr.nextAll() {
		v, err := srcItr.getValue()
		if err != nil {
			return nil, err
		}
		if v != empty {
			numZeros--
			p, err := srcItr.getPair()
			if err != nil {
				return nil, err
			}
			_, err = hll6Array.couponUpdate(p) //couponUpdate creates KxQ registers
			if err != nil {
				return nil, err
			}
		}
	}
	hll6Array.putNumAtCurMin(numZeros)
	hll6Array.putHipAccum(srcAbsHllArr.getHipAccum()) //intentional overwrite
	hll6Array.putRebuildCurMinNumKxQFlag(false)
	return hll6Array, nil
}

func newHll6Iterator(lengthPairs int, hll *hll6ArrayImpl) hll6Iterator {
	return hll6Iterator{
		hllPairIterator: newHllPairIterator(lengthPairs),
		hll:             hll,
		bitOffset:       -6,
	}
}

func (h *hll6Iterator) nextAll() bool {
	h.index++
	if h.index >= h.lengthPairs {
		return false
	}
	h.bitOffset += 6
	return true
}

func (h *hll6Iterator) nextValid() bool {
	for h.index+1 < h.lengthPairs {
		h.index++
		h.bitOffset += 6
		tmp := internal.GetShortLE(h.hll.hllByteArr, h.bitOffset/8)
		h.value = (tmp >> ((h.bitOffset % 8) & 0x7)) & valMask6
		if h.value != empty {
			return true
		}
	}
	return false
}

func (h *hll6Iterator) getValue() (int, error) {
	tmp := internal.GetShortLE(h.hll.hllByteArr, h.bitOffset/8)
	shift := (h.bitOffset % 8) & 0x7
	return (tmp >> shift) & valMask6, nil
}

func (h *hll6Iterator) getPair() (int, error) {
	v, err := h.getValue()
	return pair(h.index, v), err
}
