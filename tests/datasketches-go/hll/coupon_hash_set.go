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

type couponHashSetImpl struct {
	hllSketchConfig
	hllCouponState
}

func (c *couponHashSetImpl) GetCompositeEstimate() (float64, error) {
	return getEstimate(c)
}

func (c *couponHashSetImpl) GetEstimate() (float64, error) {
	return getEstimate(c)
}

func (c *couponHashSetImpl) GetHipEstimate() (float64, error) {
	return getEstimate(c)
}

func (c *couponHashSetImpl) GetLowerBound(numStdDev int) (float64, error) {
	return getLowerBound(c, numStdDev)
}

func (c *couponHashSetImpl) GetUpperBound(numStdDev int) (float64, error) {
	return getUpperBound(c, numStdDev)
}

func (c *couponHashSetImpl) GetUpdatableSerializationBytes() int {
	return c.getMemDataStart() + (4 << c.getLgCouponArrInts())
}

func (c *couponHashSetImpl) ToCompactSlice() ([]byte, error) {
	return toCouponSlice(c, true)
}

func (c *couponHashSetImpl) ToUpdatableSlice() ([]byte, error) {
	return toCouponSlice(c, false)
}

// couponUpdate updates the couponHashSetImpl with the given coupon.
func (c *couponHashSetImpl) couponUpdate(coupon int) (hllSketchStateI, error) {
	index, err := findCoupon(c.couponIntArr, c.lgCouponArrInts, coupon)
	if err != nil {
		return nil, err
	}
	if index >= 0 {
		return c, nil //found duplicate, ignore
	}
	c.couponIntArr[^index] = coupon
	c.couponCount++ //found empty
	t, err := c.checkGrowOrPromote()
	if err != nil {
		return nil, err
	}
	if t {
		return promoteSetToHll(c)
	}
	return c, nil
}

func (c *couponHashSetImpl) iterator() pairIterator {
	return newIntArrayPairIterator(c.couponIntArr, c.lgConfigK)
}

func (c *couponHashSetImpl) getMemDataStart() int {
	return hashSetIntArrStart
}

func (c *couponHashSetImpl) getPreInts() int {
	return hashSetPreInts
}

func (c *couponHashSetImpl) copyAs(tgtHllType TgtHllType) (hllSketchStateI, error) {
	newC := &couponHashSetImpl{
		hllSketchConfig: newHllSketchConfig(c.lgConfigK, tgtHllType, curModeSet),
		hllCouponState:  newHllCouponState(c.lgCouponArrInts, c.couponCount, make([]int, len(c.couponIntArr))),
	}

	copy(newC.couponIntArr, c.couponIntArr)
	return newC, nil
}

func (c *couponHashSetImpl) copy() (hllSketchStateI, error) {
	return c.copyAs(c.tgtHllType)
}

func (c *couponHashSetImpl) mergeTo(dest HllSketch) error {
	return mergeCouponTo(c, dest)
}

// checkGrowOrPromote checks if the couponHashSetImpl should grow or promote to HLL.
func (c *couponHashSetImpl) checkGrowOrPromote() (bool, error) {
	if (resizeDenom * c.couponCount) <= (resizeNumber * (1 << c.lgCouponArrInts)) {
		return false, nil
	}
	if c.lgCouponArrInts == (c.lgConfigK - 3) {
		return true, nil // promote to HLL
	}
	c.lgCouponArrInts++
	arr, err := growHashSet(c.couponIntArr, c.lgCouponArrInts)
	c.couponIntArr = arr
	return false, err
}

// growHashSet doubles the size of the given couponHashSetImpl and reinsert the existing entries.
func growHashSet(couponIntArr []int, tgtLgCoupArrSize int) ([]int, error) {
	tgtCouponIntArr := make([]int, 1<<tgtLgCoupArrSize)
	for _, fetched := range couponIntArr {
		if fetched != empty {
			idx, err := findCoupon(tgtCouponIntArr, tgtLgCoupArrSize, fetched)
			if err != nil {
				return nil, err
			}
			if idx < 0 {
				tgtCouponIntArr[^idx] = fetched
				continue
			}
			return nil, fmt.Errorf("growHashSet, found duplicate")
		}
	}
	return tgtCouponIntArr, nil
}

// promoteSetToHll move coupons to an hllArray from couponHashSetImpl
func promoteSetToHll(src *couponHashSetImpl) (hllArray, error) {
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

// findCoupon searches the Coupon hash table for an empty slot or a duplicate depending on the context.
// If entire entry is empty, returns one's complement of index = found empty.
// If entry equals given coupon, returns its index = found duplicate coupon.
// Continues searching.
// If the probe comes back to original index, return an error.
func findCoupon(array []int, lgArrInts int, coupon int) (int, error) {
	arrMask := len(array) - 1
	probe := coupon & arrMask
	loopIndex := probe

	for ok := true; ok; ok = probe != loopIndex {
		couponAtIdx := array[probe]
		if couponAtIdx == empty {
			return ^probe, nil //empty
		} else if coupon == couponAtIdx {
			return probe, nil //duplicate
		}
		stride := ((coupon & keyMask26) >> lgArrInts) | 1
		probe = (probe + stride) & arrMask
	}
	return 0, fmt.Errorf("key not found and no empty slots")
}

// newCouponHashSet returns a new couponHashSetImpl.
// lgConfigK the configured Lg K
// tgtHllType the target HLL type
func newCouponHashSet(lgConfigK int, tgtHllType TgtHllType) (couponHashSetImpl, error) {
	if lgConfigK <= 7 {
		return couponHashSetImpl{}, fmt.Errorf("lgConfigK must be > 7 for SET mode")
	}
	cl, err := newCouponList(lgConfigK, tgtHllType, curModeSet)
	if err != nil {
		return couponHashSetImpl{}, err
	}
	return couponHashSetImpl(cl), nil
}

// deserializeCouponHashSet returns a new couponHashSetImpl from the given byte array.
func deserializeCouponHashSet(byteArray []byte) (hllCoupon, error) {
	lgConfigK := extractLgK(byteArray)
	tgtHllType := extractTgtHllType(byteArray)

	curMode := extractCurMode(byteArray)
	memArrStart := listIntArrStart
	if curMode == curModeSet {
		memArrStart = hashSetIntArrStart
	}
	set, err := newCouponHashSet(lgConfigK, tgtHllType)
	if err != nil {
		return nil, err
	}
	memIsCompact := extractCompactFlag(byteArray)
	couponCount := extractHashSetCount(byteArray)
	lgCouponArrInts := extractLgArr(byteArray)
	if lgCouponArrInts < lgInitSetSize {
		lgCouponArrInts, err = computeLgArr(byteArray, couponCount, lgConfigK)
		if err != nil {
			return nil, err
		}
	}
	if memIsCompact {
		for it := 0; it < couponCount && err == nil; it++ {
			_, err = set.couponUpdate(int(binary.LittleEndian.Uint32(byteArray[memArrStart+(it<<2) : memArrStart+(it<<2)+4])))
		}
		if err != nil {
			return nil, err
		}
	} else {
		set.couponCount = couponCount
		set.lgCouponArrInts = lgCouponArrInts
		couponArrInts := 1 << lgCouponArrInts
		set.couponIntArr = make([]int, couponArrInts)
		for it := 0; it < couponArrInts; it++ {
			set.couponIntArr[it] = int(binary.LittleEndian.Uint32(byteArray[hashSetIntArrStart+(it<<2) : hashSetIntArrStart+(it<<2)+4]))
		}
	}
	return &set, nil
}
