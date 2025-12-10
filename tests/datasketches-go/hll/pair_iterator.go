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

type pairIterator interface {
	nextValid() bool
	nextAll() bool
	getIndex() int
	getPair() (int, error)
	getKey() int
	getValue() (int, error)
	getSlot() int
}

type intArrayPairIterator struct {
	array    []int
	arrLen   int
	slotMask int
	index    int
	pair     int
}

func (i *intArrayPairIterator) getIndex() int {
	return i.index
}

// newIntArrayPairIterator returns a new intArrayPairIterator.
func newIntArrayPairIterator(array []int, lgConfigK int) pairIterator {
	return &intArrayPairIterator{
		array:    array,
		slotMask: (1 << lgConfigK) - 1,
		arrLen:   len(array),
		index:    -1,
	}
}

// getPair returns the current key, value pair as a single int where the key is the lower 26 bits
// and the value is in the upper 6 bits.
func (i *intArrayPairIterator) getPair() (int, error) {
	return i.pair, nil
}

// nextValid returns true at the next pair where getKey() and getValue() are valid.
// If false, the iteration is done.
func (i *intArrayPairIterator) nextValid() bool {
	for (i.index + 1) < i.arrLen {
		i.index++
		pair := i.array[i.index]
		if pair != empty {
			i.pair = pair
			return true
		}
	}
	return false
}

// nextAll returns true if there is another pair in the array.
func (i *intArrayPairIterator) nextAll() bool {
	i.index++
	if i.index < i.arrLen {
		i.pair = i.array[i.index]
		return true
	}
	return false
}

// getKey returns the key of the pair.
// the low 26 bits of a pair, and can be up to 26 bits in length.
func (i *intArrayPairIterator) getKey() int {
	return getPairLow26(i.pair)
}

// getValue returns the value of the pair.
func (i *intArrayPairIterator) getValue() (int, error) {
	return getPairValue(i.pair), nil
}

func (i *intArrayPairIterator) getSlot() int {
	return i.getKey() & i.slotMask
}
