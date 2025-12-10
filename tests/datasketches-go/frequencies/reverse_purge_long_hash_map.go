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

package frequencies

import (
	"errors"
	"fmt"
	"math/bits"
	"strconv"
	"strings"

	"github.com/apache/datasketches-go/internal"
)

type reversePurgeLongHashMap struct {
	lgLength      int
	loadThreshold int
	keys          []int64
	values        []int64
	states        []int16
	numActive     int
}

type iteratorLongHashMap struct {
	keys_      []int64
	values_    []int64
	states_    []int16
	numActive_ int
	stride_    int
	mask_      int
	i_         int
	count_     int
}

const (
	reversePurgeLongHashMapLoadFactor = float64(0.75)
	reversePurgeLongHashMapDriftLimit = 1024 //used only in stress testing
)

// newReversePurgeLongHashMap constructs a new reversePurgeLongHashMap.
// It will create arrays of length mapSize, which must be a power of two.
// This restriction was made to ensure fast hashing.
// The member loadThreshold is then set to the largest value that
// will not overload the hashFn table.
func newReversePurgeLongHashMap(mapSize int) (*reversePurgeLongHashMap, error) {
	lgLength, err := internal.ExactLog2(mapSize)
	if err != nil {
		return nil, fmt.Errorf("mapSize: %e", err)
	}
	loadThreshold := int(float64(mapSize) * reversePurgeLongHashMapLoadFactor)
	keys := make([]int64, mapSize)
	values := make([]int64, mapSize)
	states := make([]int16, mapSize)
	return &reversePurgeLongHashMap{
		lgLength:      lgLength,
		loadThreshold: loadThreshold,
		keys:          keys,
		values:        values,
		states:        states,
	}, nil
}

func (r *reversePurgeLongHashMap) get(key int64) (int64, error) {
	probe := r.hashProbe(key)
	if r.states[probe] > 0 {
		if r.keys[probe] == key {
			return r.values[probe], nil
		}
		return 0, errors.New("key not found")
	}
	return 0, nil
}

// getCapacity returns the current capacity of the hashFn map (i.e., max number of keys that can be stored).
func (r *reversePurgeLongHashMap) getCapacity() int {
	return r.loadThreshold
}

// adjustOrPutValue adjusts the value associated with the given key.
// Increments the value mapped to the key if the key is present in the map. Otherwise,
// the key is inserted with the putAmount.
//
// key the key of the value to increment
// adjustAmount the amount by which to increment the value
func (r *reversePurgeLongHashMap) adjustOrPutValue(key int64, adjustAmount int64) error {
	var (
		arrayMask = len(r.keys) - 1
		probe     = hashFn(key) & int64(arrayMask)
		drift     = 1
	)
	for r.states[probe] != 0 && r.keys[probe] != key {
		probe = (probe + 1) & int64(arrayMask)
		drift++
		if drift >= reversePurgeLongHashMapDriftLimit {
			return errors.New("drift >= driftLimit")
		}
	}
	//found either an empty slot or the key
	if r.states[probe] == 0 { //found empty slot
		// adding the key and value to the table
		if r.numActive > r.loadThreshold {
			return errors.New("numActive >= loadThreshold")
		}
		r.keys[probe] = key
		r.values[probe] = adjustAmount
		r.states[probe] = int16(drift) //how far off we are
		r.numActive++
	} else { //found the key, adjust the value
		if r.keys[probe] != key {
			return errors.New("keys[probe] != key")
		}
		r.values[probe] += adjustAmount
	}
	return nil
}

func (r *reversePurgeLongHashMap) resize(newSize int) error {
	oldKeys := r.keys
	oldValues := r.values
	oldStates := r.states
	r.keys = make([]int64, newSize)
	r.values = make([]int64, newSize)
	r.states = make([]int16, newSize)
	r.loadThreshold = int(float64(newSize) * reversePurgeLongHashMapLoadFactor)
	r.lgLength = bits.TrailingZeros(uint(newSize))
	r.numActive = 0
	err := error(nil)
	for i := 0; i < len(oldKeys) && err == nil; i++ {
		if oldStates[i] > 0 {
			err = r.adjustOrPutValue(oldKeys[i], oldValues[i])
		}
	}
	return err
}

func (r *reversePurgeLongHashMap) purge(sampleSize int) int64 {
	limit := min(sampleSize, r.numActive)
	numSamples := 0
	i := 0
	samples := make([]int64, limit)
	for numSamples < limit {
		if r.states[i] > 0 {
			samples[numSamples] = r.values[i]
			numSamples++
		}
		i++
	}

	val := internal.QuickSelect(samples, 0, numSamples-1, limit/2)
	r.adjustAllValuesBy(-1 * val)
	r.keepOnlyPositiveCounts()
	return val
}

func (r *reversePurgeLongHashMap) serializeToString() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d,%d,", r.numActive, len(r.keys)))
	for i := 0; i < len(r.keys); i++ {
		if r.states[i] != 0 {
			sb.WriteString(fmt.Sprintf("%d,%d,", r.keys[i], r.values[i]))
		}
	}
	return sb.String()
}

// adjustAllValuesBy adjust amount value by which to shift all values. Only keys corresponding to positive
// values are retained.
func (r *reversePurgeLongHashMap) adjustAllValuesBy(adjustAmount int64) {
	for i := len(r.keys); i > 0; {
		i--
		r.values[i] += adjustAmount
	}
}

func (r *reversePurgeLongHashMap) keepOnlyPositiveCounts() {
	// Starting from the back, find the first empty cell, which marks a boundary between clusters.
	firstProbe := len(r.keys) - 1
	for r.states[firstProbe] > 0 {
		firstProbe--
	}
	//Work towards the front; delete any non-positive entries.
	for probe := firstProbe; probe > 0; {
		probe--
		// When we find the next non-empty cell, we know we are at the high end of a cluster,
		//  which is tracked by firstProbe.
		if r.states[probe] > 0 && r.values[probe] <= 0 {
			r.hashDelete(probe) //does the work of deletion and moving higher items towards the front.
			r.numActive--
		}
	}
	//now work on the first cluster that was skipped.
	for probe := len(r.keys); probe-1 > firstProbe; {
		probe--
		if r.states[probe] > 0 && r.values[probe] <= 0 {
			r.hashDelete(probe)
			r.numActive--
		}
	}
}

func (r *reversePurgeLongHashMap) hashDelete(deleteProbe int) error {
	// Looks ahead in the table to search for another item to move to this location.
	// If none are found, the status is changed
	r.states[deleteProbe] = 0 //mark as empty
	drift := 1
	arrayMask := len(r.keys) - 1
	probe := (deleteProbe + drift) & arrayMask //map length must be a power of 2
	// advance until you find a free location replacing locations as needed
	for r.states[probe] != 0 {
		if r.states[probe] > int16(drift) {
			// move current element
			r.keys[deleteProbe] = r.keys[probe]
			r.values[deleteProbe] = r.values[probe]
			r.states[deleteProbe] = r.states[probe] - int16(drift)
			// marking the current probe location as deleted
			r.states[probe] = 0
			drift = 0
			deleteProbe = probe
		}
		probe = (probe + 1) & arrayMask
		drift++
		//only used for theoretical analysis
		if drift >= reversePurgeLongHashMapDriftLimit {
			return errors.New("drift >= driftLimit")
		}
	}
	return nil
}

func deserializeReversePurgeLongHashMapFromString(string string) (*reversePurgeLongHashMap, error) {
	tokens := strings.Split(string, ",")
	if len(tokens) < 2 {
		return nil, errors.New("len(tokens) < 2")
	}
	numActive, err := strconv.Atoi(tokens[0])
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(tokens[1])
	if err != nil {
		return nil, err
	}
	table, err := newReversePurgeLongHashMap(length)
	if err != nil {
		return nil, err
	}
	j := 2
	for i := 0; i < numActive && err == nil; i++ {
		key, err := strconv.Atoi(tokens[j])
		if err != nil {
			return nil, err
		}
		value, err := strconv.Atoi(tokens[j+1])
		if err != nil {
			return nil, err
		}
		err = table.adjustOrPutValue(int64(key), int64(value))
		if err != nil {
			return nil, err
		}
		j += 2
	}
	return table, nil
}

func deserializeFromStringArray(tokens []string) (*reversePurgeLongHashMap, error) {
	ignore := strPreambleTokens
	numActive, _ := strconv.ParseUint(tokens[ignore], 10, 32)
	length, _ := strconv.ParseUint(tokens[ignore+1], 10, 32)
	hashMap, err := newReversePurgeLongHashMap(int(length))
	if err != nil {
		return nil, err
	}
	j := 2 + ignore
	for i := 0; i < int(numActive); i++ {
		key, err := strconv.ParseUint(tokens[j], 10, 64)
		if err != nil {
			return nil, err
		}
		value, err := strconv.ParseUint(tokens[j+1], 10, 64)
		if err != nil {
			return nil, err
		}
		err = hashMap.adjustOrPutValue(int64(key), int64(value))
		if err != nil {
			return nil, err
		}
		j += 2
	}
	return hashMap, nil
}

func (r *reversePurgeLongHashMap) getActiveValues() []int64 {
	if r.numActive == 0 {
		return nil
	}
	returnValues := make([]int64, 0, r.numActive)
	for i := 0; i < len(r.values); i++ {
		if r.states[i] > 0 { //isActive
			returnValues = append(returnValues, r.values[i])
		}
	}
	return returnValues
}

func (r *reversePurgeLongHashMap) getActiveKeys() []int64 {
	if r.numActive == 0 {
		return nil
	}
	returnValues := make([]int64, 0, r.numActive)
	for i := 0; i < len(r.keys); i++ {
		if r.states[i] > 0 { //isActive
			returnValues = append(returnValues, r.keys[i])
		}
	}
	return returnValues
}

func (s *reversePurgeLongHashMap) iterator() *iteratorLongHashMap {
	return newIteratorLong(s.keys, s.values, s.states, s.numActive)
}

func (s *reversePurgeLongHashMap) hashProbe(key int64) int {
	arrayMask := len(s.keys) - 1
	probe := int(hashFn(key)) & arrayMask
	for s.states[probe] > 0 && s.keys[probe] != key {
		probe = (probe + 1) & arrayMask
	}
	return probe
}

func (s *reversePurgeLongHashMap) String() string {
	var sb strings.Builder
	sb.WriteString("ReversePurgeLongHashMap:\n")
	sb.WriteString(fmt.Sprintf("  %12s:%11s%20s %s\n", "Index", "States", "Values", "Keys"))
	for i := 0; i < len(s.keys); i++ {
		if s.states[i] <= 0 {
			continue
		}
		sb.WriteString(fmt.Sprintf("  %12d:%11d%20d %d\n", i, s.states[i], s.values[i], s.keys[i]))
	}
	return sb.String()
}

func newIteratorLong(keys []int64, values []int64, states []int16, numActive int) *iteratorLongHashMap {
	stride := int(uint64(float64(len(keys))*internal.InverseGolden) | 1)
	return &iteratorLongHashMap{
		keys_:      keys,
		values_:    values,
		states_:    states,
		numActive_: numActive,

		stride_: stride,
		mask_:   len(keys) - 1,
		i_:      -stride,
	}
}

func (i *iteratorLongHashMap) next() bool {
	i.i_ = (i.i_ + i.stride_) & i.mask_
	for i.count_ < i.numActive_ {
		if i.states_[i.i_] > 0 {
			i.count_++
			return true
		}
		i.i_ = (i.i_ + i.stride_) & i.mask_
	}
	return false
}

func (i *iteratorLongHashMap) getKey() int64 {
	return i.keys_[i.i_]
}

func (i *iteratorLongHashMap) getValue() int64 {
	return i.values_[i.i_]
}
