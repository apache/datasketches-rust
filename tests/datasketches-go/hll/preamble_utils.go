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
	"math"

	"github.com/apache/datasketches-go/internal"
)

const (
	preambleIntsBytes = 0
	serVerByte        = 1
	familyByte        = 2
	lgKByte           = 3
	lgArrByte         = 4
	flagsByte         = 5
	listCountByte     = 6
	hllCurMinByte     = 6
	// modeByte
	// mode encoding of combined curMode and TgtHllType:
	// Dec  Lo4Bits TgtHllType, curMode
	//   0     0000      HLL_4,    LIST
	//   1     0001      HLL_4,     SET
	//   2     0010      HLL_4,     HLL
	//   4     0100      HLL_6,    LIST
	//   5     0101      HLL_6,     SET
	//   6     0110      HLL_6,     HLL
	//   8     1000      HLL_8,    LIST
	//   9     1001      HLL_8,     SET
	//  10     1010      HLL_8,     HLL
	modeByte = 7 //lo2bits = curMode, next 2 bits = tgtHllType

	listIntArrStart = 8
)

const (
	//Coupon Hash Set
	hashSetCountInt    = 8
	hashSetIntArrStart = 12
)

const (
	// HLL
	hipAccumDouble  = 8
	kxq0Double      = 16
	kxq1Double      = 24
	curMinCountInt  = 32
	auxCountInt     = 36
	hllByteArrStart = 40
)

const (
	//Flag bit masks
	emptyFlagMask           = 4
	compactFlagMask         = 8
	outOfOrderFlagMask      = 16
	rebuildCurminNumKxqMask = 32
)

const (
	//Mode byte masks
	curModeMask    = 3
	tgtHllTypeMask = 12
)

const (
	// Other constants
	serVer         = 1
	familyId       = 7
	listPreInts    = 2
	hashSetPreInts = 3
	hllPreInts     = 10
)

func extractPreInts(byteArr []byte) int {
	return int(byteArr[preambleIntsBytes] & 0x3F)
}

func extractSerVer(byteArr []byte) int {
	return int((byteArr[serVerByte]) & 0xFF)
}

func extractFamilyID(byteArr []byte) int {
	return int((byteArr[familyByte]) & 0xFF)
}

func extractCurMode(byteArr []byte) curMode {
	return curMode(byteArr[modeByte] & curModeMask)
}

func extractTgtHllType(byteArr []byte) TgtHllType {
	typeId := byteArr[modeByte] & tgtHllTypeMask
	return TgtHllType(typeId >> 2)
}

func extractLgK(byteArr []byte) int {
	return int(byteArr[lgKByte] & 0xFF)
}

func extractListCount(byteArr []byte) int {
	return int(byteArr[listCountByte] & 0xFF)
}

func extractCompactFlag(byteArr []byte) bool {
	return (int(byteArr[flagsByte]) & compactFlagMask) > 0
}

func extractHashSetCount(byteArr []byte) int {
	return int(binary.LittleEndian.Uint32(byteArr[hashSetCountInt : hashSetCountInt+4]))
}

func extractLgArr(byteArr []byte) int {
	return int(byteArr[lgArrByte] & 0xFF)
}

func extractOooFlag(byteArr []byte) bool {
	flags := byteArr[flagsByte]
	return (flags & outOfOrderFlagMask) > 0
}

func extractCurMin(byteArr []byte) int {
	return int(byteArr[hllCurMinByte] & 0xFF)
}

func extractHipAccum(byteArr []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(byteArr[hipAccumDouble : hipAccumDouble+8]))
}

func extractKxQ0(byteArr []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(byteArr[kxq0Double : kxq0Double+8]))
}

func extractKxQ1(byteArr []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(byteArr[kxq1Double : kxq1Double+8]))
}

func extractNumAtCurMin(byteArr []byte) int {
	return int(binary.LittleEndian.Uint32(byteArr[curMinCountInt : curMinCountInt+4]))
}

func extractRebuildCurMinNumKxQFlag(byteArr []byte) bool {
	return (byteArr[flagsByte] & rebuildCurminNumKxqMask) > 0
}

func extractAuxCount(byteArr []byte) int {
	return int(binary.LittleEndian.Uint32(byteArr[auxCountInt : auxCountInt+4]))
}

func computeLgArr(byteArr []byte, couponCount int, lgConfigK int) (int, error) {
	//value is missing, recompute
	curMode := extractCurMode(byteArr)
	if curMode == curModeList {
		return lgInitListSize, nil
	}
	ceilPwr2 := internal.CeilPowerOf2(couponCount)
	if (resizeDenom * couponCount) > (resizeNumber * ceilPwr2) {
		ceilPwr2 <<= 1
	}
	if curMode == curModeSet {
		v, err := internal.ExactLog2(ceilPwr2)
		return max(lgInitSetSize, v), err
	}
	//only used for HLL4
	v, err := internal.ExactLog2(ceilPwr2)
	return max(lgAuxArrInts[lgConfigK], v), err

}

func insertAuxCount(byteArr []byte, auxCount int) error {
	binary.LittleEndian.PutUint32(byteArr[auxCountInt:auxCountInt+4], uint32(auxCount))
	return nil
}

func insertListCount(byteArr []byte, listCnt int) {
	byteArr[listCountByte] = byte(listCnt)
}

func insertHashSetCount(byteArr []byte, hashSetCnt int) {
	binary.LittleEndian.PutUint32(byteArr[hashSetCountInt:hashSetCountInt+4], uint32(hashSetCnt))
}

func insertPreInts(byteArr []byte, preInts int) {
	byteArr[preambleIntsBytes] = byte(preInts & 0x3F)
}

func insertSerVer(byteArr []byte) {
	byteArr[serVerByte] = byte(serVer)
}

func insertFamilyID(byteArr []byte) {
	byteArr[familyByte] = byte(familyId)
}

func insertLgK(byteArr []byte, lgK int) {
	byteArr[lgKByte] = byte(lgK)
}

func insertLgArr(byteArr []byte, lgArr int) {
	byteArr[lgArrByte] = byte(lgArr)

}

func insertEmptyFlag(byteArr []byte, emptyFlag bool) {
	flags := byteArr[flagsByte]
	if emptyFlag {
		flags |= emptyFlagMask
	} else {
		flags &= ^uint8(emptyFlagMask)
	}
	byteArr[flagsByte] = flags
}

func insertOooFlag(byteArr []byte, oooFlag bool) {
	flags := byteArr[flagsByte]
	if oooFlag {
		flags |= outOfOrderFlagMask
	} else {
		flags &= ^uint8(outOfOrderFlagMask)
	}
	byteArr[flagsByte] = flags
}

func insertCurMode(byteArr []byte, curMode curMode) {
	mode := byteArr[modeByte] & ^uint8(curModeMask)
	mode |= uint8(curMode) & curModeMask
	byteArr[modeByte] = mode
}

func insertTgtHllType(byteArr []byte, tgtHllType TgtHllType) {
	mode := byteArr[modeByte] & ^uint8(tgtHllTypeMask)
	mode |= (uint8(tgtHllType) << 2) & tgtHllTypeMask
	byteArr[modeByte] = mode
}

func insertCompactFlag(byteArr []byte, compactFlag bool) {
	flags := byteArr[flagsByte]
	if compactFlag {
		flags |= compactFlagMask
	} else {
		flags &= ^uint8(compactFlagMask)
	}
	byteArr[flagsByte] = flags
}

func insertCurMin(byteArr []byte, curMin int) {
	byteArr[hllCurMinByte] = byte(curMin)
}

func insertHipAccum(byteArr []byte, hipAccum float64) {
	binary.LittleEndian.PutUint64(byteArr[hipAccumDouble:hipAccumDouble+8], math.Float64bits(hipAccum))
}

func insertKxQ0(byteArr []byte, kxq0 float64) {
	binary.LittleEndian.PutUint64(byteArr[kxq0Double:kxq0Double+8], math.Float64bits(kxq0))
}

func insertKxQ1(byteArr []byte, kxq1 float64) {
	binary.LittleEndian.PutUint64(byteArr[kxq1Double:kxq1Double+8], math.Float64bits(kxq1))
}

func insertNumAtCurMin(byteArr []byte, numAtCurMin int) {
	binary.LittleEndian.PutUint32(byteArr[curMinCountInt:curMinCountInt+4], uint32(numAtCurMin))
}

func insertRebuildCurMinNumKxQFlag(byteArr []byte, rebuild bool) {
	flags := byteArr[flagsByte]
	if rebuild {
		flags |= rebuildCurminNumKxqMask
	} else {
		flags &= ^uint8(rebuildCurminNumKxqMask)
	}
	byteArr[flagsByte] = flags
}
