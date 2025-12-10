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

// Package frequencies is dedicated to streaming algorithms that enable estimation of the
// frequency of occurrence of items in a weighted multiset stream of items.
// If the frequency distribution of items is sufficiently skewed, these algorithms are very
// useful in identifying the "Heavy Hitters" that occurred most frequently in the stream.
// The accuracy of the estimation of the frequency of an item has well understood error
// bounds that can be returned by the sketch.
//
// These algorithms are sometimes referred to as "TopN" algorithms.
package frequencies

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
)

type ItemsSketch[C comparable] struct {
	// Log2 Maximum length of the arrays internal to the hash map supported by the data
	// structure.
	lgMaxMapSize int
	// The current number of counters supported by the hash map.
	curMapCap int //the threshold to purge
	// Tracks the total of decremented counts.
	offset int64
	// The sum of all frequencies of the stream so far.
	streamWeight int64
	// The maximum number of samples used to compute approximate median of counters when doing
	// decrement
	sampleSize int
	// Hash map mapping stored items to approximate counts
	hashMap *reversePurgeItemHashMap[C]
}

// NewFrequencyItemsSketch constructs a new ItemsSketch with the given parameters.
// this internal constructor is used when deserializing the sketch.
//
//   - lgMaxMapSize, log2 of the physical size of the internal hash map managed by this
//     sketch. The maximum capacity of this internal hash map is 0.75 times 2^lgMaxMapSize.
//     Both the ultimate accuracy and size of this sketch are functions of lgMaxMapSize.
//   - lgCurMapSize, log2 of the starting (current) physical size of the internal hashFn
//     map managed by this sketch.
func NewFrequencyItemsSketch[C comparable](lgMaxMapSize int, lgCurMapSize int, hasher common.ItemSketchHasher[C], serde common.ItemSketchSerde[C]) (*ItemsSketch[C], error) {
	lgMaxMapSz := max(lgMaxMapSize, _LG_MIN_MAP_SIZE)
	lgCurMapSz := max(lgCurMapSize, _LG_MIN_MAP_SIZE)
	hashMap, err := newReversePurgeItemHashMap[C](1<<lgCurMapSz, hasher, serde)
	if err != nil {
		return nil, err
	}
	curMapCap := hashMap.getCapacity()
	maxMapCap := int(float64(uint64(1)<<lgMaxMapSize) * reversePurgeItemHashMapLoadFactor)
	offset := int64(0)
	sampleSize := min(_SAMPLE_SIZE, maxMapCap)

	return &ItemsSketch[C]{
		lgMaxMapSize: lgMaxMapSz,
		curMapCap:    curMapCap,
		offset:       offset,
		sampleSize:   sampleSize,
		hashMap:      hashMap,
	}, nil
}

// NewFrequencyItemsSketchWithMaxMapSize constructs a new ItemsSketch with the given maxMapSize and the default
// initialMapSize (8).
//
//   - maxMapSize, Determines the physical size of the internal hash map managed by this
//     sketch and must be a power of 2. The maximum capacity of this internal hash map is
//     0.75 times * maxMapSize. Both the ultimate accuracy and size of this sketch are
//     functions of maxMapSize.
func NewFrequencyItemsSketchWithMaxMapSize[C comparable](maxMapSize int, hasher common.ItemSketchHasher[C], serde common.ItemSketchSerde[C]) (*ItemsSketch[C], error) {
	maxMapSz, err := internal.ExactLog2(maxMapSize)
	if err != nil {
		return nil, err
	}
	return NewFrequencyItemsSketch[C](maxMapSz, _LG_MIN_MAP_SIZE, hasher, serde)
}

// NewFrequencyItemsSketchFromSlice constructs a new ItemsSketch with the given maxMapSize and the
// default initialMapSize (8).
//
// maxMapSize determines the physical size of the internal hashmap managed by this
// sketch and must be a power of 2.  The maximum capacity of this internal hash map is
// 0.75 times * maxMapSize. Both the ultimate accuracy and size of this sketch are a
// function of maxMapSize.
func NewFrequencyItemsSketchFromSlice[C comparable](slc []byte, hasher common.ItemSketchHasher[C], serde common.ItemSketchSerde[C]) (*ItemsSketch[C], error) {
	if serde == nil {
		return nil, errors.New("no SerDe provided")
	}

	pre0, err := checkPreambleSize(slc) //make sure preamble will fit
	maxPreLongs := internal.FamilyEnum.Frequency.MaxPreLongs

	preLongs := extractPreLongs(pre0)                     //Byte 0
	serVer := extractSerVer(pre0)                         //Byte 1
	familyID := extractFamilyID(pre0)                     //Byte 2
	lgMaxMapSize := extractLgMaxMapSize(pre0)             //Byte 3
	lgCurMapSize := extractLgCurMapSize(pre0)             //Byte 4
	empty := (extractFlags(pre0) & _EMPTY_FLAG_MASK) != 0 //Byte 5

	// Checks
	preLongsEq1 := (preLongs == 1) //Byte 0
	preLongsEqMax := (preLongs == maxPreLongs)
	if !preLongsEq1 && !preLongsEqMax {
		return nil, fmt.Errorf("possible corruption: preLongs must be 1 or %d: %d", maxPreLongs, preLongs)
	}
	if serVer != _SER_VER { //Byte 1
		return nil, fmt.Errorf("possible corruption: ser ver must be %d: %d", _SER_VER, serVer)
	}
	actFamID := internal.FamilyEnum.Frequency.Id //Byte 2
	if familyID != actFamID {
		return nil, fmt.Errorf("possible corruption: familyID must be %d: %d", actFamID, familyID)
	}
	if empty && !preLongsEq1 { //Byte 5 and Byte 0
		return nil, fmt.Errorf("(preLongs == 1) ^ empty == true")
	}
	if empty {
		return NewFrequencyItemsSketchWithMaxMapSize[C](1<<_LG_MIN_MAP_SIZE, hasher, serde)
	}
	// Get full preamble
	preArr := make([]int64, preLongs)
	for j := 0; j < preLongs; j++ {
		preArr[j] = int64(binary.LittleEndian.Uint64(slc[j<<3:]))
	}

	fis, err := NewFrequencyItemsSketch[C](lgMaxMapSize, lgCurMapSize, hasher, serde)
	if err != nil {
		return nil, err
	}
	fis.streamWeight = 0 // update after
	fis.offset = preArr[3]

	preBytes := preLongs << 3
	activeItems := extractActiveItems(preArr[1])

	// Get countArray
	countArray := make([]int64, activeItems)
	reqBytes := preBytes + activeItems*8 // count Arr only
	if len(slc) < reqBytes {
		return nil, fmt.Errorf("possible Corruption: Insufficient bytes in array: %d, %d", len(slc), reqBytes)
	}
	for j := 0; j < activeItems; j++ {
		countArray[j] = int64(binary.LittleEndian.Uint64(slc[preBytes+j<<3:]))
	}
	// Get itemArray
	itemsOffset := preBytes + (8 * activeItems)
	itemArray, err := serde.DeserializeManyFromSlice(slc[itemsOffset:], 0, activeItems)
	if err != nil {
		return nil, err
	}
	// update the sketch
	for j := 0; j < activeItems; j++ {
		err := fis.UpdateMany(itemArray[j], countArray[j])
		if err != nil {
			return nil, err
		}
	}
	fis.streamWeight = preArr[2] // override streamWeight due to updating
	return fis, nil
}

// GetAprioriErrorFrequencyItemsSketch returns the estimated a priori error given the maxMapSize for the sketch and the
// estimatedTotalStreamWeight.
//
// maxMapSize is the planned map size to be used when constructing this sketch.
// estimatedTotalStreamWeight is the estimated total stream weight.
func GetAprioriErrorFrequencyItemsSketch(maxMapSize int, estimatedTotalStreamWeight int64) (float64, error) {
	epsilon, err := GetEpsilonLongsSketch(maxMapSize)
	if err != nil {
		return 0, err
	}
	return epsilon * float64(estimatedTotalStreamWeight), nil
}

// GetEpsilonFrequencyItemsSketch returns epsilon used to compute a priori error.
// This is just the value 3.5 / maxMapSize.
//
// maxMapSize is the planned map size to be used when constructing this sketch.
func GetEpsilonFrequencyItemsSketch(maxMapSize int) (float64, error) {
	if !internal.IsPowerOf2(maxMapSize) {
		return 0, errors.New("maxMapSize is not a power of 2")
	}
	return 3.5 / float64(maxMapSize), nil
}

// GetCurrentMapCapacity returns the current number of counters the sketch is configured to support.
func (i *ItemsSketch[C]) GetCurrentMapCapacity() int {
	return i.curMapCap
}

// GetEstimate gets the estimate of the frequency of the given item.
// Note: The true frequency of an item would be the sum of the counts as a result of the
// two update functions.
//
// item is the given item
//
// return the estimate of the frequency of the given item
func (i *ItemsSketch[C]) GetEstimate(item C) (int64, error) {
	// If item is tracked:
	// Estimate = itemCount + offset; Otherwise it is 0.
	v, err := i.hashMap.get(item)
	if v > 0 {
		return v + i.offset, err
	}
	return 0, err
}

// GetLowerBound gets the guaranteed lower bound frequency of the given item, which can never be
// negative.
//
//   - item, the given item.
func (i *ItemsSketch[C]) GetLowerBound(item C) (int64, error) {
	return i.hashMap.get(item)
}

// GetUpperBound gets the guaranteed upper bound frequency of the given item.
//
//   - item, the given item.
func (i *ItemsSketch[C]) GetUpperBound(item C) (int64, error) {
	// UB = itemCount + offset
	v, err := i.hashMap.get(item)
	return v + i.offset, err
}

// frequencies return estimated frequency, lower bound frequency,
// upper bound frequency at once.
func (i *ItemsSketch[C]) frequencies(item C) (est, lower, upper int64, err error) {
	var v int64
	v, err = i.hashMap.get(item)

	lower = v
	upper = v + i.offset
	if v > 0 {
		est = v + i.offset
	} else {
		est = 0
	}

	return
}

// GetFrequentItemsWithThreshold returns an array of RowItem that include frequent items, estimates, upper and
// lower bounds given a threshold and an ErrorCondition. If the threshold is lower than
// getMaximumError(), then getMaximumError() will be used instead.
//
// The method first examines all active items in the sketch (items that have a counter).
//
// If errorType = NO_FALSE_NEGATIVES, this will include an item in the result list if
// GetUpperBound(item) > threshold. There will be no false negatives, i.e., no Type II error.
// There may be items in the set with true frequencies less than the threshold (false positives).
//
// If errorType = NO_FALSE_POSITIVES, this will include an item in the result list if
// GetLowerBound(item) > threshold. There will be no false positives, i.e., no Type I error.
// There may be items omitted from the set with true frequencies greater than the threshold
// (false negatives). This is a subset of the NO_FALSE_NEGATIVES case.
//
// threshold to include items in the result list
// errorType determines whether no false positives or no false negatives are desired.
// an array of frequent items
func (i *ItemsSketch[C]) GetFrequentItemsWithThreshold(threshold int64, errorType errorType) ([]*RowItem[C], error) {
	finalThreshold := i.GetMaximumError()
	if threshold > finalThreshold {
		finalThreshold = threshold
	}
	return i.sortItems(finalThreshold, errorType)
}

// GetFrequentItems returns an array of Row that include frequent items, estimates, upper and
// lower bounds given an ErrorCondition and the default threshold.
// This is the same as GetFrequentItemsWithThreshold(getMaximumError(), errorType)
//
// errorType determines whether no false positives or no false negatives are desired.
func (i *ItemsSketch[C]) GetFrequentItems(errorType errorType) ([]*RowItem[C], error) {
	return i.sortItems(i.GetMaximumError(), errorType)
}

// GetNumActiveItems returns the number of active items in the sketch.
func (i *ItemsSketch[C]) GetNumActiveItems() int {
	return i.hashMap.numActive
}

// GetMaximumError return an upper bound on the maximum error of GetEstimate(item) for any item.
// This is equivalent to the maximum distance between the upper bound and the lower bound
// for any item.
func (i *ItemsSketch[C]) GetMaximumError() int64 {
	return i.offset
}

// GetMaximumMapCapacity returns the maximum number of counters the sketch is configured to
// support.
func (i *ItemsSketch[C]) GetMaximumMapCapacity() int {
	return int(float64(uint64(1<<i.lgMaxMapSize)) * reversePurgeItemHashMapLoadFactor)
}

// GetStreamLength returns the sum of the frequencies in the stream seen so far by the sketch.
func (i *ItemsSketch[C]) GetStreamLength() int64 {
	return i.streamWeight
}

// IsEmpty returns true if this sketch is empty.
func (i *ItemsSketch[C]) IsEmpty() bool {
	return i.GetNumActiveItems() == 0
}

// Update this sketch with an item and a frequency count of one.
//
// item for which the frequency should be increased.
func (i *ItemsSketch[C]) Update(item C) error {
	return i.UpdateMany(item, 1)
}

// UpdateMany update this sketch with an item and a positive frequency count (or weight).
//
// Item for which the frequency should be increased. The item can be any long value
// and is only used by the sketch to determine uniqueness.
// count the amount by which the frequency of the item should be increased.
// A count of zero is a no-op, and a negative count will throw an exception.
func (i *ItemsSketch[C]) UpdateMany(item C, count int64) error {
	if internal.IsNil(item) || count == 0 {
		return nil
	}
	if count < 0 {
		return fmt.Errorf("count may not be negative")
	}

	i.streamWeight += count
	err := i.hashMap.adjustOrPutValue(item, count)
	if err != nil {
		return err
	}

	if i.GetNumActiveItems() > i.curMapCap { //over the threshold, we need to do something
		if i.hashMap.lgLength < i.lgMaxMapSize { //below tgt size, we can grow
			err := i.hashMap.resize(2 * len(i.hashMap.keys))
			if err != nil {
				return err
			}
			i.curMapCap = i.hashMap.getCapacity()
		} else {
			i.offset += i.hashMap.purge(i.sampleSize)
			if i.GetNumActiveItems() > i.GetMaximumMapCapacity() {
				return fmt.Errorf("purge did not reduce active items")
			}
		}
	}
	return nil
}

// Merge merges the other sketch into this one. The other sketch may be of a different size.
//
// other sketch of this class
//
// return a sketch whose estimates are within the guarantees of the largest error tolerance
// of the two merged sketches.
func (i *ItemsSketch[C]) Merge(other *ItemsSketch[C]) (*ItemsSketch[C], error) {
	if other == nil || other.IsEmpty() {
		return i, nil
	}

	streamLen := i.streamWeight + other.streamWeight //capture before merge
	iter := other.hashMap.iterator()

	for iter.next() {
		err := i.UpdateMany(iter.getKey(), iter.getValue())
		if err != nil {
			return nil, err
		}
	}
	i.offset += other.offset
	i.streamWeight = streamLen //corrected streamWeight
	return i, nil
}

// ToString returns a String representation of this sketch
func (i *ItemsSketch[C]) ToString() (string, error) {
	var sb strings.Builder
	//start the string with parameters of the sketch
	serVer := _SER_VER //0
	famID := internal.FamilyEnum.Frequency.Id
	lgMaxMapSz := i.lgMaxMapSize
	flags := 0
	if i.hashMap.numActive == 0 {
		flags = _EMPTY_FLAG_MASK
	}
	_, err := fmt.Fprintf(&sb, "%d,%d,%d,%d,%d,%d,", serVer, famID, lgMaxMapSz, flags, i.streamWeight, i.offset)
	if err != nil {
		return "", err
	}
	sb.WriteString(i.hashMap.serializeToString()) //numActive, curMaplen, key[i], value[i], ...
	return sb.String(), nil
}

// ToSlice returns a slice representation of this sketch
func (i *ItemsSketch[C]) ToSlice() ([]byte, error) {
	if i.hashMap.serde == nil {
		return nil, errors.New("no SerDe provided")
	}
	preLongs := 0
	outBytes := 0
	empty := i.IsEmpty()
	activeItems := i.GetNumActiveItems()
	bytes := make([]byte, 0)
	if empty {
		preLongs = 1
		outBytes = 8
	} else {
		preLongs = internal.FamilyEnum.Frequency.MaxPreLongs
		bytes = i.hashMap.serde.SerializeManyToSlice(i.hashMap.getActiveKeys())
		outBytes = ((preLongs + activeItems) << 3) + len(bytes)
	}

	outArr := make([]byte, outBytes)
	pre0 := int64(0)
	pre0 = insertPreLongs(int64(preLongs), pre0)                         //Byte 0
	pre0 = insertSerVer(_SER_VER, pre0)                                  //Byte 1
	pre0 = insertFamilyID(int64(internal.FamilyEnum.Frequency.Id), pre0) //Byte 2
	pre0 = insertLgMaxMapSize(int64(i.lgMaxMapSize), pre0)               //Byte 3
	pre0 = insertLgCurMapSize(int64(i.hashMap.lgLength), pre0)           //Byte 4
	if empty {
		pre0 = insertFlags(_EMPTY_FLAG_MASK, pre0) //Byte 5
	} else {
		pre0 = insertFlags(0, pre0) //Byte 5
	}

	if empty {
		binary.LittleEndian.PutUint64(outArr, uint64(pre0))
	} else {
		pre := int64(0)
		preArr := make([]int64, preLongs)
		preArr[0] = pre0
		preArr[1] = insertActiveItems(int64(activeItems), pre)
		preArr[2] = int64(i.streamWeight)
		preArr[3] = int64(i.offset)
		for j := 0; j < preLongs; j++ {
			binary.LittleEndian.PutUint64(outArr[j<<3:], uint64(preArr[j]))
		}
		preBytes := preLongs << 3
		for j := 0; j < activeItems; j++ {
			binary.LittleEndian.PutUint64(outArr[preBytes+j<<3:], uint64(i.hashMap.getActiveValues()[j]))
		}
		copy(outArr[preBytes+(activeItems<<3):], bytes)
	}
	return outArr, nil
}

// Reset resets this sketch to a virgin state.
func (i *ItemsSketch[C]) Reset() error {
	hashMap, err := newReversePurgeItemHashMap[C](1<<_LG_MIN_MAP_SIZE, i.hashMap.hasher, i.hashMap.serde)
	if err != nil {
		return err
	}
	i.hashMap = hashMap
	i.curMapCap = hashMap.getCapacity()
	i.offset = 0
	i.streamWeight = 0
	return nil
}

func (i *ItemsSketch[C]) String() string {
	var sb strings.Builder
	sb.WriteString("FrequentItemsSketch:")
	sb.WriteString("\n")
	sb.WriteString("  Stream Length    : " + strconv.FormatInt(i.streamWeight, 10))
	sb.WriteString("\n")
	sb.WriteString("  Max Error Offset : " + strconv.FormatInt(i.offset, 10))
	sb.WriteString("\n")
	sb.WriteString(i.hashMap.String())
	return sb.String()
}

func (i *ItemsSketch[C]) sortItems(threshold int64, errorType errorType) ([]*RowItem[C], error) {
	rowList := make([]*RowItem[C], 0)
	iter := i.hashMap.iterator()
	if errorType == ErrorTypeEnum.NoFalseNegatives {
		for iter.next() {
			est, lb, ub, err := i.frequencies(iter.getKey())
			if err != nil {
				return nil, err
			}
			if ub >= threshold {
				row := newRowItem[C](iter.getKey(), est, ub, lb)
				rowList = append(rowList, row)
			}
		}
	} else { //NO_FALSE_POSITIVES
		for iter.next() {
			est, lb, ub, err := i.frequencies(iter.getKey())
			if err != nil {
				return nil, err
			}
			if lb >= threshold {
				row := newRowItem[C](iter.getKey(), est, ub, lb)
				rowList = append(rowList, row)
			}
		}
	}

	slices.SortFunc(rowList, func(a, b *RowItem[C]) int {
		if a.est > b.est {
			return -1
		}
		if a.est < b.est {
			return 1
		}
		return 0
	})

	return rowList, nil
}
