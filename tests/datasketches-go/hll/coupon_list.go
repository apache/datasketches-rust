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
	"encoding/binary"
	"fmt"
)

type couponListImpl struct {
	hllSketchConfig
	hllCouponState
}

func (c *couponListImpl) GetCompositeEstimate() (float64, error) {
	return getEstimate(c)
}

func (c *couponListImpl) GetEstimate() (float64, error) {
	return getEstimate(c)
}

func (c *couponListImpl) GetHipEstimate() (float64, error) {
	return getEstimate(c)
}

func (c *couponListImpl) GetLowerBound(numStdDev int) (float64, error) {
	return getLowerBound(c, numStdDev)
}

func (c *couponListImpl) GetUpperBound(numStdDev int) (float64, error) {
	return getUpperBound(c, numStdDev)
}

func (c *couponListImpl) GetUpdatableSerializationBytes() int {
	return c.getMemDataStart() + (4 << c.getLgCouponArrInts())
}

func (c *couponListImpl) ToCompactSlice() ([]byte, error) {
	return toCouponSlice(c, true)
}

func (c *couponListImpl) ToUpdatableSlice() ([]byte, error) {
	return toCouponSlice(c, false)
}

// couponUpdate updates the couponListImpl with the given coupon.
// it returns the updated couponListImpl or a newly promoted couponHashSetImpl.
func (c *couponListImpl) couponUpdate(coupon int) (hllSketchStateI, error) {
	length := 1 << c.lgCouponArrInts
	for i := 0; i < length; i++ {
		couponAtIdx := c.couponIntArr[i]
		if couponAtIdx == empty {
			c.couponIntArr[i] = coupon //update
			c.couponCount++
			if c.couponCount >= length {
				if c.lgConfigK < 8 {
					return promoteListToHll(c) //oooFlag = false
				}
				return promoteListToSet(c) //oooFlag = true
			}
			return c, nil
		}
		//cell not empty
		if couponAtIdx == coupon {
			return c, nil //duplicate
		}
		//cell not empty & not a duplicate, continue
	}
	return nil, fmt.Errorf("array invalid: no empties & no duplicates")
}

// iterator returns an iterator over the couponListImpl.
func (c *couponListImpl) iterator() pairIterator {
	return newIntArrayPairIterator(c.couponIntArr, c.lgConfigK)
}

func (c *couponListImpl) getMemDataStart() int {
	return listIntArrStart
}

func (c *couponListImpl) getPreInts() int {
	return listPreInts
}

func (c *couponListImpl) copyAs(tgtHllType TgtHllType) (hllSketchStateI, error) {
	newC := &couponListImpl{
		hllSketchConfig: newHllSketchConfig(c.lgConfigK, tgtHllType, curModeList),
		hllCouponState:  newHllCouponState(c.lgCouponArrInts, c.couponCount, make([]int, len(c.couponIntArr))),
	}

	copy(newC.couponIntArr, c.couponIntArr)
	return newC, nil
}

func (c *couponListImpl) copy() (hllSketchStateI, error) {
	return c.copyAs(c.tgtHllType)
}

func (c *couponListImpl) mergeTo(dest HllSketch) error {
	return mergeCouponTo(c, dest)
}

// promoteListToHll move coupons to an hllArray from couponListImpl
func promoteListToHll(src *couponListImpl) (hllArray, error) {
	tgtHllArr, _ := newHllArray(src.lgConfigK, src.tgtHllType)
	srcIter := src.iterator()
	tgtHllArr.putKxQ0(float64(uint64(1) << src.lgConfigK))

	for srcIter.nextValid() {
		p, err := srcIter.getPair()
		if err != nil {
			return nil, err
		}
		_, err = tgtHllArr.couponUpdate(p)
		if err != nil {
			return nil, err
		}
	}
	est, err := src.GetEstimate()
	if err != nil {
		return nil, err
	}
	tgtHllArr.putHipAccum(est)
	tgtHllArr.putOutOfOrder(false)
	return tgtHllArr, nil
}

// promoteListToSet move coupons to a couponHashSetImpl from couponListImpl
func promoteListToSet(c *couponListImpl) (hllSketchStateI, error) {
	couponCount := c.getCouponCount()
	arr := c.couponIntArr
	chSet, err := newCouponHashSet(c.lgConfigK, c.tgtHllType)
	if err != nil {
		return nil, err
	}
	for i := 0; i < couponCount && err == nil; i++ {
		_, err = chSet.couponUpdate(arr[i])
	}

	if err != nil {
		return nil, err
	}
	return &chSet, nil
}

// newCouponList returns a new couponListImpl.
func newCouponList(lgConfigK int, tgtHllType TgtHllType, curMode curMode) (couponListImpl, error) {
	var (
		lgCouponArrInts = lgInitSetSize //SET
	)

	if curMode == curModeList {
		lgCouponArrInts = lgInitListSize
	} else if lgConfigK <= 7 {
		return couponListImpl{}, fmt.Errorf("lgConfigK must be > 7 for non-HLL mode")
	}

	couponIntArr := make([]int, 1<<lgCouponArrInts)
	couponCount := 0

	return couponListImpl{
		hllSketchConfig: newHllSketchConfig(lgConfigK, tgtHllType, curMode),
		hllCouponState:  newHllCouponState(lgCouponArrInts, couponCount, couponIntArr),
	}, nil
}

// deserializeCouponList returns a new couponListImpl from the given byte slice.
func deserializeCouponList(byteArray []byte) (hllCoupon, error) {
	lgConfigK := extractLgK(byteArray)
	tgtHllType := extractTgtHllType(byteArray)

	list, err := newCouponList(lgConfigK, tgtHllType, curModeList)
	if err != nil {
		return nil, err
	}
	couponCount := extractListCount(byteArray)
	// TODO there must be a more efficient to reinterpret the byte array as an int array
	for it := 0; it < couponCount; it++ {
		list.couponIntArr[it] = int(binary.LittleEndian.Uint32(byteArray[listIntArrStart+it*4 : listIntArrStart+it*4+4]))
	}
	list.couponCount = couponCount
	return &list, nil
}
