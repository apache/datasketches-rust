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

// auxHashMap is a hash table for the Aux array.
type auxHashMap struct {
	lgConfigK    int //required for #slot bits
	lgAuxArrInts int
	auxCount     int
	auxIntArr    []int
}

func (a *auxHashMap) copy() *auxHashMap {
	newA := *a
	newA.auxIntArr = make([]int, len(a.auxIntArr))
	copy(newA.auxIntArr, a.auxIntArr)
	return &newA
}

// newAuxHashMap returns a new auxHashMap.
func newAuxHashMap(lgAuxArrInts int, lgConfigK int) *auxHashMap {
	return &auxHashMap{
		lgConfigK:    lgConfigK,
		lgAuxArrInts: lgAuxArrInts,
		auxCount:     0,
		auxIntArr:    make([]int, 1<<lgAuxArrInts),
	}
}

// deserializeAuxHashMap returns a new auxHashMap from the given byte array.
func deserializeAuxHashMap(byteArray []byte, offset int, lgConfigL int, auxCount int, srcCompact bool) (*auxHashMap, error) {
	var (
		lgAuxArrInts int
	)

	if srcCompact {
		v, err := computeLgArr(byteArray, auxCount, lgConfigL)
		if err != nil {
			return nil, err
		}
		lgAuxArrInts = v
	} else {
		lgAuxArrInts = extractLgArr(byteArray)
	}

	auxMap := newAuxHashMap(lgAuxArrInts, lgConfigL)
	configKMask := (1 << lgConfigL) - 1

	if srcCompact {
		for i := 0; i < auxCount; i++ {
			pair := int(binary.LittleEndian.Uint32(byteArray[offset+(i<<2) : offset+(i<<2)+4]))
			slotNo := getPairLow26(pair) & configKMask
			value := getPairValue(pair)
			err := auxMap.mustAdd(slotNo, value) //increments count
			if err != nil {
				return nil, err
			}
		}
	} else { //updatable
		auxArrInts := 1 << lgAuxArrInts
		for i := 0; i < auxArrInts; i++ {
			pair := int(binary.LittleEndian.Uint32(byteArray[offset+(i<<2) : offset+(i<<2)+4]))
			if pair == empty {
				continue
			}
			slotNo := getPairLow26(pair) & configKMask
			value := getPairValue(pair)
			err := auxMap.mustAdd(slotNo, value) //increments count
			if err != nil {
				return nil, err
			}
		}
	}
	return auxMap, nil
}

func (a *auxHashMap) getAuxIntArr() []int {
	return a.auxIntArr
}

func (a *auxHashMap) getCompactSizeBytes() int {
	return a.auxCount << 2
}

func (a *auxHashMap) getUpdatableSizeBytes() int {
	return 4 << a.lgAuxArrInts
}

func (a *auxHashMap) mustFindValueFor(slotNo int) (int, error) {
	index, err := findAuxHashMap(a.auxIntArr, a.lgAuxArrInts, a.lgConfigK, slotNo)
	if err != nil {
		return 0, err
	}
	if index < 0 {
		return 0, fmt.Errorf("SlotNo not found: %d", slotNo)
	}
	return getPairValue(a.auxIntArr[index]), nil
}

func (a *auxHashMap) mustReplace(slotNo int, value int) error {
	index, err := findAuxHashMap(a.auxIntArr, a.lgAuxArrInts, a.lgConfigK, slotNo)
	if err != nil {
		return err
	}
	if index < 0 {
		pairStr := pairString(pair(slotNo, value))
		return fmt.Errorf("pair not found: %v", pairStr)
	}
	a.auxIntArr[index] = pair(slotNo, value)
	return nil
}

// mustAdd adds the slotNo and value to the aux array.
// slotNo the index from the HLL array
// value the HLL value at the slotNo.
func (a *auxHashMap) mustAdd(slotNo int, value int) error {
	index, err := findAuxHashMap(a.auxIntArr, a.lgAuxArrInts, a.lgConfigK, slotNo)
	if err != nil {
		return err
	}
	pair := pair(slotNo, value)
	if index >= 0 {
		pairStr := pairString(pair)
		return fmt.Errorf("found a slotNo that should not be there: %s", pairStr)
	}
	a.auxIntArr[^index] = pair
	a.auxCount++
	return a.checkGrow()
}

func (a *auxHashMap) getLgAuxArrInts() int {
	return a.lgAuxArrInts
}

// iterator returns an iterator over the Aux array.
func (a *auxHashMap) iterator() pairIterator {
	return newIntArrayPairIterator(a.auxIntArr, a.lgConfigK)
}

// getAuxCount returns the number of entries in the Aux array.
func (a *auxHashMap) getAuxCount() int {
	return a.auxCount
}

// checkGrow checks to see if the aux array should be grown and does so if needed.
func (a *auxHashMap) checkGrow() error {
	if (resizeDenom * a.auxCount) <= (resizeNumber * len(a.auxIntArr)) {
		return nil
	}
	return a.growAuxSpace()
}

// growAuxSpace doubles the size of the aux array and reinsert the existing entries.
func (a *auxHashMap) growAuxSpace() error {
	oldArray := a.auxIntArr
	configKMask := int((1 << a.lgConfigK) - 1)
	a.lgAuxArrInts++
	a.auxIntArr = make([]int, 1<<a.lgAuxArrInts)
	for _, fetched := range oldArray {
		if fetched != empty {
			//find empty in new array
			idx, err := findAuxHashMap(a.auxIntArr, a.lgAuxArrInts, a.lgConfigK, fetched&configKMask)
			if err != nil {
				return err
			}
			a.auxIntArr[^idx] = fetched
		}
	}
	return nil
}

// findAuxHashMap searches the Aux arr hash table for an empty or a matching slotNo depending on the context.
// If entire entry is empty, returns one's complement of index = found empty.
// If entry contains given slotNo, returns its index = found slotNo.
// Continues searching.
// If the probe comes back to original index, return an error.
func findAuxHashMap(auxArr []int, lgAuxArrInts int, lgConfigK int, slotNo int) (int, error) {
	if lgAuxArrInts >= lgConfigK {
		return 0, fmt.Errorf("lgAuxArrInts >= lgConfigK")
	}
	auxArrMask := (1 << lgAuxArrInts) - 1
	configKMask := (1 << lgConfigK) - 1
	probe := slotNo & auxArrMask
	loopIndex := probe
	for {
		arrVal := auxArr[probe]
		if arrVal == empty {
			return ^probe, nil
		} else if slotNo == (arrVal & configKMask) {
			return probe, nil
		}
		stride := (slotNo >> lgAuxArrInts) | 1
		probe = (probe + stride) & auxArrMask
		if probe == loopIndex {
			return 0, fmt.Errorf("key not found and no empty slots")
		}
	}
}
