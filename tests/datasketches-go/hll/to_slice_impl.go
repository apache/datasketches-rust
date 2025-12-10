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

func toHllByteArr(impl hllArray, compact bool) ([]byte, error) {
	auxBytes := 0
	if impl.GetTgtHllType() == TgtHllTypeHll4 {
		auxHashMap := impl.getAuxHashMap()
		if auxHashMap != nil {
			if compact {
				auxBytes = auxHashMap.getCompactSizeBytes()
			} else {
				auxBytes = auxHashMap.getUpdatableSizeBytes()
			}
		} else {
			if compact {
				auxBytes = 0
			} else {
				auxBytes = 4 << lgAuxArrInts[impl.GetLgConfigK()]
			}
		}
	}
	totalBytes := hllByteArrStart + impl.getHllByteArrBytes() + auxBytes
	byteArr := make([]byte, totalBytes)
	err := insertHll(impl, byteArr, compact)
	return byteArr, err
}

func toCouponSlice(impl hllCoupon, dstCompact bool) ([]byte, error) {
	srcCouponCount := impl.getCouponCount()
	srcLgCouponArrInts := impl.getLgCouponArrInts()
	srcCouponArrInts := 1 << srcLgCouponArrInts
	list := impl.GetCurMode() == curModeList
	if dstCompact {
		//Src Heap,   Src Updatable, Dst Compact
		dataStart := impl.getMemDataStart()
		bytesOut := dataStart + (srcCouponCount << 2)
		byteArrOut := make([]byte, bytesOut)
		copyCommonListAndSet(impl, byteArrOut)
		insertCompactFlag(byteArrOut, dstCompact)
		itr := impl.iterator()
		cnt := 0
		for itr.nextValid() {
			p, err := itr.getPair()
			if err != nil {
				return nil, err
			}
			binary.LittleEndian.PutUint32(byteArrOut[dataStart+(cnt<<2):dataStart+(cnt<<2)+4], uint32(p))
			cnt++
		}
		if list {
			insertListCount(byteArrOut, srcCouponCount)
		} else {
			insertHashSetCount(byteArrOut, srcCouponCount)
		}
		return byteArrOut, nil
	} else {
		//Src Heap, Src Updatable, Dst Updatable
		dataStart := impl.getMemDataStart()
		bytesOut := dataStart + (srcCouponArrInts << 2)
		byteArrOut := make([]byte, bytesOut)
		copyCommonListAndSet(impl, byteArrOut)
		for _, v := range impl.getCouponIntArr() {
			binary.LittleEndian.PutUint32(byteArrOut[dataStart:dataStart+4], uint32(v))
			dataStart += 4
		}
		if list {
			insertListCount(byteArrOut, srcCouponCount)
		} else {
			insertHashSetCount(byteArrOut, srcCouponCount)
		}
		return byteArrOut, nil
	}
}

func copyCommonListAndSet(impl hllCoupon, dst []byte) {
	insertPreInts(dst, impl.getPreInts())
	insertSerVer(dst)
	insertFamilyID(dst)
	insertLgK(dst, impl.GetLgConfigK())
	insertLgArr(dst, impl.getLgCouponArrInts())
	insertEmptyFlag(dst, impl.IsEmpty())
	insertOooFlag(dst, impl.isOutOfOrder())
	insertCurMode(dst, impl.GetCurMode())
	insertTgtHllType(dst, impl.GetTgtHllType())
}

func insertHll(impl hllArray, dst []byte, compact bool) error {
	insertCommonHll(impl, dst, compact)
	hllByteArr := impl.getHllByteArr()
	copy(dst[hllByteArrStart:], hllByteArr)
	if impl.getAuxHashMap() != nil {
		return insertAux(impl, dst, compact)
	} else {
		return insertAuxCount(dst, 0)
	}
}

func insertCommonHll(impl hllArray, dst []byte, compact bool) {
	insertPreInts(dst, impl.getPreInts())
	insertSerVer(dst)
	insertFamilyID(dst)
	insertLgK(dst, impl.GetLgConfigK())
	insertEmptyFlag(dst, impl.IsEmpty())
	insertCompactFlag(dst, compact)
	insertOooFlag(dst, impl.isOutOfOrder())
	insertCurMin(dst, impl.getCurMin())
	insertCurMode(dst, impl.GetCurMode())
	insertTgtHllType(dst, impl.GetTgtHllType())
	insertHipAccum(dst, impl.getHipAccum())
	insertKxQ0(dst, impl.getKxQ0())
	insertKxQ1(dst, impl.getKxQ1())
	insertNumAtCurMin(dst, impl.getNumAtCurMin())
	insertRebuildCurMinNumKxQFlag(dst, impl.isRebuildCurMinNumKxQFlag())
}

func insertAux(impl hllArray, dst []byte, compact bool) error {
	auxHashMap := impl.getAuxHashMap()
	auxCount := auxHashMap.getAuxCount()
	err := insertAuxCount(dst, auxCount)
	if err != nil {
		return err
	}
	insertLgArr(dst, auxHashMap.getLgAuxArrInts())
	auxStart := impl.getAuxStart()
	if compact {
		itr := auxHashMap.iterator()
		cnt := 0
		for itr.nextValid() {
			p, err := itr.getPair()
			if err != nil {
				return err
			}
			binary.LittleEndian.PutUint32(dst[auxStart+(cnt<<2):auxStart+(cnt<<2)+4], uint32(p))
			cnt++
		}
		if cnt != auxCount {
			return fmt.Errorf("corruption, should not happen: %d != %d", cnt, auxCount)
		}
	} else {
		auxInts := 1 << auxHashMap.getLgAuxArrInts()
		auxArr := auxHashMap.getAuxIntArr()
		for i, v := range auxArr[:auxInts] {
			binary.LittleEndian.PutUint32(dst[auxStart+(i<<2):auxStart+(i<<2)+4], uint32(v))
		}
	}
	return nil
}
