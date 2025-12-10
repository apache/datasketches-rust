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

type hllArray interface {
	hllSketchStateI

	getAuxHashMap() *auxHashMap
	getAuxStart() int
	getCurMin() int
	getHipAccum() float64
	getHllByteArr() []byte
	getHllByteArrBytes() int
	getKxQ0() float64
	getKxQ1() float64
	getNumAtCurMin() int

	putAuxHashMap(auxHashMap *auxHashMap, compact bool)
	putCurMin(curMin int)
	putHipAccum(hipAccum float64)
	putKxQ0(kxq0 float64)
	putKxQ1(kxq1 float64)
	putNibble(slotNo int, value byte)
	putNumAtCurMin(numAtCurMin int)
	putOutOfOrder(oooFlag bool)

	extractCommonHll(byteArr []byte)
	hipAndKxQIncrementalUpdate(oldValue int, newValue int) error
}

type hllArrayImpl struct {
	hllSketchConfig
	oooFrag             bool //Out-Of-Order Flag
	rebuildCurMinNumKxQ bool
	curMin              int //always zero for Hll6 and Hll8, only used by Hll4Array
	numAtCurMin         int //# of values at curMin. If curMin = 0, it is # of zeros
	hipAccum            float64
	kxq0                float64
	kxq1                float64

	hllByteArr []byte

	auxHashMap *auxHashMap
	auxStart   int //used for direct HLL4
}

// newHllArray returns a new hllArray of the given lgConfigK and tgtHllType.
func newHllArray(lgConfigK int, tgtHllType TgtHllType) (hllArray, error) {
	switch tgtHllType {
	case TgtHllTypeHll4:
		return newHll4Array(lgConfigK), nil
	case TgtHllTypeHll6:
		return newHll6Array(lgConfigK), nil
	case TgtHllTypeHll8:
		return newHll8Array(lgConfigK), nil
	}
	return nil, fmt.Errorf("unknown TgtHllType")
}

func (a *hllArrayImpl) getPreInts() int {
	return hllPreInts
}

func (a *hllArrayImpl) IsEmpty() bool {
	return false
}

func (a *hllArrayImpl) GetEstimate() (float64, error) {
	if a.oooFrag {
		return a.GetCompositeEstimate()
	}
	return a.hipAccum, nil
}

// GetCompositeEstimate getCompositeEstimate returns the composite estimate.
func (a *hllArrayImpl) GetCompositeEstimate() (float64, error) {
	return hllCompositeEstimate(a)
}

func (a *hllArrayImpl) GetHipEstimate() (float64, error) {
	return a.hipAccum, nil
}

func (a *hllArrayImpl) getMemDataStart() int {
	return hllByteArrStart
}

func (a *hllArrayImpl) GetUpperBound(numStdDev int) (float64, error) {
	err := checkNumStdDev(numStdDev)
	if err != nil {
		return 0, err
	}
	return hllUpperBound(a, numStdDev)
}

func (a *hllArrayImpl) GetLowerBound(numStdDev int) (float64, error) {
	err := checkNumStdDev(numStdDev)
	if err != nil {
		return 0, err
	}
	return hllLowerBound(a, numStdDev)
}

func (a *hllArrayImpl) GetUpdatableSerializationBytes() int {
	return hllByteArrStart + a.getHllByteArrBytes()
}

func (a *hllArrayImpl) getCurMin() int {
	return a.curMin
}

func (a *hllArrayImpl) getNumAtCurMin() int {
	return a.numAtCurMin
}

func (a *hllArrayImpl) getKxQ1() float64 {
	return a.kxq1
}

func (a *hllArrayImpl) getKxQ0() float64 {
	return a.kxq0
}

func (a *hllArrayImpl) getHllByteArrBytes() int {
	return len(a.hllByteArr)
}

func (a *hllArrayImpl) getHllByteArr() []byte {
	return a.hllByteArr
}

// putHipAccum sets the HipAccum.
func (a *hllArrayImpl) putHipAccum(hipAccum float64) {
	a.hipAccum = hipAccum
}

// getHipAccum sets the HipAccum.
func (a *hllArrayImpl) getHipAccum() float64 {
	return a.hipAccum
}

// addToHipAccum adds the given value to the HipAccum.
func (a *hllArrayImpl) addToHipAccum(value float64) {
	a.hipAccum += value
}

// putOutOfOrder sets the Out-Of-Order Flag
func (a *hllArrayImpl) putOutOfOrder(oooFlag bool) {
	if oooFlag {
		a.putHipAccum(0)
	}
	a.oooFrag = oooFlag
}

func (a *hllArrayImpl) isOutOfOrder() bool {
	return a.oooFrag
}

func (a *hllArrayImpl) putAuxHashMap(auxHashMap *auxHashMap, _ bool) {
	a.auxHashMap = auxHashMap
}

func (a *hllArrayImpl) putCurMin(curMin int) {
	a.curMin = curMin
}

// putKxQ0 sets the kxq0 value.
func (a *hllArrayImpl) putKxQ0(kxq0 float64) {
	a.kxq0 = kxq0
}

// putKxQ1 sets the kxq1 value.
func (a *hllArrayImpl) putKxQ1(kxq1 float64) {
	a.kxq1 = kxq1
}

func (a *hllArrayImpl) putNumAtCurMin(numAtCurMin int) {
	a.numAtCurMin = numAtCurMin
}

func (a *hllArrayImpl) putRebuildCurMinNumKxQFlag(rebuildCurMinNumKxQ bool) {
	a.rebuildCurMinNumKxQ = rebuildCurMinNumKxQ
}

// getNewAuxHashMap returns a new auxHashMap.
func (a *hllArrayImpl) getNewAuxHashMap() *auxHashMap {
	return newAuxHashMap(lgAuxArrInts[a.lgConfigK], a.lgConfigK)
}

// getAuxHashMap returns the auxHashMap.
func (a *hllArrayImpl) getAuxHashMap() *auxHashMap {
	return a.auxHashMap
}

func (a *hllArrayImpl) getAuxStart() int {
	return a.auxStart
}

// getNibble returns the value of the nibble at the given slotNo.
func (a *hllArrayImpl) getNibble(slotNo int) int {
	theByte := int(a.hllByteArr[slotNo>>1])

	if (slotNo & 1) > 0 { //odd?
		theByte >>= 4
	}
	return theByte & loNibbleMask
}

// putNibble sets the value of the nibble at the given slotNo.
func (a *hllArrayImpl) putNibble(slotNo int, value byte) {
	byteNo := slotNo >> 1
	oldValue := a.hllByteArr[byteNo]
	if (slotNo & 1) == 0 {
		a.hllByteArr[byteNo] = (oldValue & hiNibbleMask) | (value & loNibbleMask)
	} else {
		a.hllByteArr[byteNo] = (oldValue & loNibbleMask) | ((value << 4) & hiNibbleMask)
	}
}

func (a *hllArrayImpl) mergeTo(HllSketch) error {
	return fmt.Errorf("possible Corruption, improper access")
}

func (a *hllArrayImpl) copyCommon() hllArrayImpl {
	newH := *a
	if newH.getAuxHashMap() != nil {
		newH.putAuxHashMap(a.getAuxHashMap().copy(), false)
	} else {
		newH.putAuxHashMap(nil, false)
	}
	newH.hllByteArr = make([]byte, len(a.hllByteArr))
	copy(newH.hllByteArr, a.hllByteArr)
	return newH
}

func (a *hllArrayImpl) isRebuildCurMinNumKxQFlag() bool {
	return a.rebuildCurMinNumKxQ
}

// hipAndKxQIncrementalUpdate is the HIP and KxQ incremental update for hll.
// This is used when incrementally updating an existing array with non-zero values.
func (a *hllArrayImpl) hipAndKxQIncrementalUpdate(oldValue int, newValue int) error {
	if oldValue >= newValue {
		return fmt.Errorf("oldValue >= newValue")
	}
	kxq0 := a.kxq0
	kxq1 := a.kxq1
	//update hipAccum BEFORE updating kxq0 and kxq1
	a.addToHipAccum(float64(uint64(1<<a.lgConfigK)) / (kxq0 + kxq1))
	return a.incrementalUpdateKxQ(oldValue, newValue, kxq0, kxq1)
}

// incrementalUpdateKxQ updates kxq0 and kxq1.
func (a *hllArrayImpl) incrementalUpdateKxQ(oldValue int, newValue int, kxq0 float64, kxq1 float64) error {
	//update kxq0 and kxq1; subtract first, then add.
	if oldValue < 32 {
		v, err := internal.InvPow2(oldValue)
		if err != nil {
			return err
		}
		kxq0 -= v
		a.kxq0 = kxq0
	} else {
		v, err := internal.InvPow2(oldValue)
		if err != nil {
			return err
		}
		kxq1 -= v
		a.kxq1 = kxq1
	}
	if newValue < 32 {
		v, err := internal.InvPow2(newValue)
		if err != nil {
			return err
		}
		kxq0 += v
		a.kxq0 = kxq0
	} else {
		v, err := internal.InvPow2(newValue)
		if err != nil {
			return err
		}
		kxq1 += v
		a.kxq1 = kxq1
	}
	return nil
}

// extractCommonHll extracts the common fields from the given byte array.
func (a *hllArrayImpl) extractCommonHll(byteArr []byte) {
	a.putOutOfOrder(extractOooFlag(byteArr))
	a.putCurMin(extractCurMin(byteArr))
	a.putHipAccum(extractHipAccum(byteArr))
	a.putKxQ0(extractKxQ0(byteArr))
	a.putKxQ1(extractKxQ1(byteArr))
	a.putNumAtCurMin(extractNumAtCurMin(byteArr))
	a.putRebuildCurMinNumKxQFlag(extractRebuildCurMinNumKxQFlag(byteArr))

	a.hllByteArr = byteArr[hllByteArrStart : hllByteArrStart+len(a.hllByteArr)]
}
