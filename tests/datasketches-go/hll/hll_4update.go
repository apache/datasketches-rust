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

// internalHll4Update is the internal update method for Hll4Array.
func internalHll4Update(h *hll4ArrayImpl, slotNo int, newValue int) error {
	var (
		actualOldValue     int
		shiftedNewValue    int //value - curMin
		curMin             = h.curMin
		rawStoredOldNibble = h.getNibble(slotNo)           // could be 0
		lb0nOldValue       = rawStoredOldNibble + h.curMin // provable lower bound, could be 0
		err                error
	)

	if newValue <= lb0nOldValue {
		return nil
	}

	// Based on whether we have an AUX_TOKEN and whether the shiftedNewValue is greater than
	// AUX_TOKEN, we have four cases for how to actually modify the data structure:
	// 1. (shiftedNewValue >= AUX_TOKEN) && (rawStoredOldNibble = AUX_TOKEN) //881:
	//    The byte array already contains aux token
	//    This is the case where old and new values are both exceptions.
	//    Therefore, the 4-bit array already is AUX_TOKEN. Only need to update auxMap
	// 2. (shiftedNewValue < AUX_TOKEN) && (rawStoredOldNibble = AUX_TOKEN) //885
	//    This is the (hypothetical) case where old value is an exception and the new one is not,
	//    which is impossible given that curMin has not changed here and the newValue > oldValue.
	// 3. (shiftedNewValue >= AUX_TOKEN) && (rawStoredOldNibble < AUX_TOKEN) //892
	//    This is the case where the old value is not an exception and the new value is.
	//    Therefore, the AUX_TOKEN must be stored in the 4-bit array and the new value
	//    added to the exception table.
	// 4. (shiftedNewValue < AUX_TOKEN) && (rawStoredOldNibble < AUX_TOKEN) //897
	//    This is the case where neither the old value nor the new value is an exception.
	//    Therefore, we just overwrite the 4-bit array with the shifted new value.

	if rawStoredOldNibble == auxToken { //846 Note: This is rare and really hard to test!
		if h.auxHashMap == nil {
			return fmt.Errorf("auxHashMap must already exist")
		}
		actualOldValue, err = h.auxHashMap.mustFindValueFor(slotNo)
		if newValue <= actualOldValue || err != nil {
			return err
		}
		// We know that the array will be changed, but we haven't actually updated yet.
		err := h.hipAndKxQIncrementalUpdate(actualOldValue, newValue)
		if err != nil {
			return err
		}
		shiftedNewValue = newValue - curMin
		if shiftedNewValue < 0 {
			return fmt.Errorf("shifedNewValue < 0")
		}
		if shiftedNewValue >= auxToken { //CASE 1:
			err := h.auxHashMap.mustReplace(slotNo, newValue)
			if err != nil {
				return err
			}
		} //else                         //CASE 2: impossible
	} else { //rawStoredOldNibble < AUX_TOKEN
		actualOldValue = lb0nOldValue
		// We know that the array will be changed, but we haven't actually updated yet.
		err := h.hipAndKxQIncrementalUpdate(actualOldValue, newValue)
		if err != nil {
			return err
		}
		shiftedNewValue = newValue - curMin
		if shiftedNewValue < 0 {
			return fmt.Errorf("shifedNewValue < 0")
		}
		if shiftedNewValue >= auxToken { //CASE 3: //892
			h.putNibble(slotNo, auxToken)
			if h.auxHashMap == nil {
				h.auxHashMap = h.getNewAuxHashMap()
			}
			err := h.auxHashMap.mustAdd(slotNo, newValue)
			if err != nil {
				return err
			}
		} else { // CASE 4: //897
			h.putNibble(slotNo, byte(shiftedNewValue))
		}
	}

	// We just changed the HLL array, so it might be time to change curMin.
	if actualOldValue == curMin {
		if h.numAtCurMin < 1 {
			return fmt.Errorf("h.numAtCurMin < 1")
		}
		h.numAtCurMin--
		for h.numAtCurMin == 0 {
			// Increases curMin by 1, and builds a new aux table,
			// shifts values in 4-bit table, and recounts curMin.
			err := shiftToBiggerCurMin(h)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// This scheme only works with two double registers (2 kxq values).
// HipAccum, kxq0 and kxq1 remain untouched.
// This changes curMin, numAtCurMin, hllByteArr and auxMap.
//
// Entering this routine assumes that all slots have valid nibbles > 0 and <= 15.
// An auxHashMap must exist if any values in the current hllByteArray are already 15.
func shiftToBiggerCurMin(h *hll4ArrayImpl) error {
	var (
		oldCurMin   = h.curMin
		newCurMin   = oldCurMin + 1
		lgConfigK   = h.lgConfigK
		configK     = 1 << lgConfigK
		configKmask = configK - 1

		numAtNewCurMin = 0
		numAuxTokens   = 0
	)

	// Walk through the slots of 4-bit array decrementing stored values by one unless it
	// equals AUX_TOKEN, where it is left alone but counted to be checked later.
	// If oldStoredValue is 0 it is an error.
	// If the decremented value is 0, we increment numAtNewCurMin.
	// Because getNibble is masked to 4 bits oldStoredValue can never be > 15 or negative
	for i := 0; i < configK; i++ { //724
		oldStoredNibble := uint64(h.getNibble(i))
		if oldStoredNibble == 0 {
			return fmt.Errorf("array slots cannot be 0 at this point")
		}
		if oldStoredNibble < auxToken {
			oldStoredNibble--
			h.putNibble(i, byte(oldStoredNibble))
			if oldStoredNibble == 0 {
				numAtNewCurMin++
			}
		} else { //oldStoredNibble == AUX_TOKEN
			numAuxTokens++
			if h.auxHashMap == nil {
				return fmt.Errorf("auxHashMap cannot be nil at this point")
			}
		}
	}
	// If old auxHashMap exists, walk through it updating some slots and build a new auxHashMap
	// if needed.
	var (
		newAuxMap *auxHashMap
		oldAuxMap = h.auxHashMap
	)

	if oldAuxMap != nil {
		var (
			slotNum       int
			oldActualVal  int
			newShiftedVal int
			err           error
		)

		itr := oldAuxMap.iterator()
		for itr.nextValid() {
			slotNum = itr.getKey() & configKmask
			oldActualVal, err = itr.getValue()
			if err != nil {
				return err
			}
			newShiftedVal = oldActualVal - newCurMin
			if newShiftedVal < 0 {
				return fmt.Errorf("newShiftedVal < 0")
			}
			if h.getNibble(slotNum) != auxToken {
				return fmt.Errorf("Array slot != AUX_TOKEN %d", h.getNibble(slotNum))
			}
			if newShiftedVal < auxToken {
				if newShiftedVal != 14 {
					return fmt.Errorf("newShiftedVal != 14")
				}
				// The former exception value isn't one anymore, so it stays out of new auxHashMap.
				// Correct the AUX_TOKEN value in the HLL array to the newShiftedVal (14).
				h.putNibble(slotNum, byte(newShiftedVal))
				numAuxTokens--
			} else { // newShiftedVal >= AUX_TOKEN
				// the former exception remains an exception, so must be added to the newAuxMap
				if newAuxMap == nil {
					newAuxMap = h.getNewAuxHashMap()
				}
				err := newAuxMap.mustAdd(slotNum, oldActualVal)
				if err != nil {
					return err
				}
			}
		}
	} else {
		if numAuxTokens != 0 {
			return fmt.Errorf("numAuxTokens != 0")
		}
	}
	if newAuxMap != nil {
		if newAuxMap.getAuxCount() != numAuxTokens {
			return fmt.Errorf("newAuxMap.getAuxCount() != numAuxTokens")
		}
	}
	h.auxHashMap = newAuxMap
	h.curMin = newCurMin
	h.numAtCurMin = numAtNewCurMin
	return nil
}
