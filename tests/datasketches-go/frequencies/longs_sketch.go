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
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/datasketches-go/internal"
)

type LongsSketch struct {
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
	hashMap *reversePurgeLongHashMap
}

const (
	strPreambleTokens = 6
)

// NewLongsSketch returns a new LongsSketch with the given lgMaxMapSize and lgCurMapSize.
//
// lgMaxMapSize is the log2 of the physical size of the internal hash map managed by this
// sketch. The maximum capacity of this internal hash map is 0.75 times 2^lgMaxMapSize.
// Both the ultimate accuracy and size of this sketch are a function of lgMaxMapSize.
//
// lgCurMapSize is the log2 of the starting (current) physical size of the internal hashFn
// map managed by this sketch.
func NewLongsSketch(lgMaxMapSize int, lgCurMapSize int) (*LongsSketch, error) {
	//set initial size of hash map
	lgMaxMapSize = max(lgMaxMapSize, _LG_MIN_MAP_SIZE)
	lgCurMapSize = max(lgCurMapSize, _LG_MIN_MAP_SIZE)
	hashMap, err := newReversePurgeLongHashMap(1 << lgCurMapSize)
	if err != nil {
		return nil, err
	}
	curMapCap := hashMap.getCapacity()
	maxMapCap := int(float64(uint64(1<<lgMaxMapSize)) * reversePurgeLongHashMapLoadFactor)
	offset := int64(0)
	sampleSize := min(_SAMPLE_SIZE, maxMapCap)
	return &LongsSketch{
		lgMaxMapSize: lgMaxMapSize,
		curMapCap:    curMapCap,
		offset:       offset,
		sampleSize:   sampleSize,
		hashMap:      hashMap,
	}, nil
}

// NewLongsSketchWithMaxMapSize constructs a new LongsSketch with the given maxMapSize and the
// default initialMapSize (8).
//
// maxMapSize determines the physical size of the internal hash map managed by this
// sketch and must be a power of 2.  The maximum capacity of this internal hash map is
// 0.75 times * maxMapSize. Both the ultimate accuracy and size of this sketch are a
// function of maxMapSize.
func NewLongsSketchWithMaxMapSize(maxMapSize int) (*LongsSketch, error) {
	log2OfInt, err := internal.ExactLog2(maxMapSize)
	if err != nil {
		return nil, fmt.Errorf("maxMapSize, %e", err)
	}
	return NewLongsSketch(log2OfInt, _LG_MIN_MAP_SIZE)
}

// NewLongsSketchFromSlice returns a sketch instance of this class from the given slice,
// which must be a byte slice representation of this sketch class.
//
// slc is a byte slice representation of a sketch of this class.
func NewLongsSketchFromSlice(slc []byte) (*LongsSketch, error) {
	pre0, err := checkPreambleSize(slc)
	if err != nil {
		return nil, err
	}
	maxPreLongs := internal.FamilyEnum.Frequency.MaxPreLongs
	preLongs := extractPreLongs(pre0)
	serVer := extractSerVer(pre0)
	familyID := extractFamilyID(pre0)
	lgMaxMapSize := extractLgMaxMapSize(pre0)
	lgCurMapSize := extractLgCurMapSize(pre0)
	empty := (extractFlags(pre0) & _EMPTY_FLAG_MASK) != 0

	// Checks
	preLongsEq1 := preLongs == 1
	preLongsEqMax := preLongs == maxPreLongs
	if !preLongsEq1 && !preLongsEqMax {
		return nil, fmt.Errorf("possible Corruption: PreLongs must be 1 or %d: %d", maxPreLongs, preLongs)
	}
	if serVer != _SER_VER {
		return nil, fmt.Errorf("possible Corruption: Ser Ver must be %d: %d", _SER_VER, serVer)
	}
	actFamID := internal.FamilyEnum.Frequency.Id
	if familyID != actFamID {
		return nil, fmt.Errorf("possible Corruption: FamilyID must be %d: %d", actFamID, familyID)
	}
	if empty && !preLongsEq1 {
		return nil, fmt.Errorf("possible Corruption: Empty Flag set incorrectly: %t", preLongsEq1)
	}
	if empty {
		return NewLongsSketch(lgMaxMapSize, _LG_MIN_MAP_SIZE)
	}
	// get full preamble
	preArr := make([]int64, preLongs)
	for i := 0; i < preLongs; i++ {
		preArr[i] = int64(binary.LittleEndian.Uint64(slc[i<<3:]))
	}
	fls, err := NewLongsSketch(lgMaxMapSize, lgCurMapSize)
	if err != nil {
		return nil, err
	}
	fls.streamWeight = 0 //update after
	fls.offset = preArr[3]

	preBytes := preLongs << 3
	activeItems := extractActiveItems(preArr[1])

	// Get countArray
	countArray := make([]int64, activeItems)
	reqBytes := preBytes + 2*activeItems*8 //count Arr + Items Arr
	if len(slc) < reqBytes {
		return nil, fmt.Errorf("possible Corruption: Insufficient bytes in array: %d, %d", len(slc), reqBytes)
	}
	for i := 0; i < activeItems; i++ {
		countArray[i] = int64(binary.LittleEndian.Uint64(slc[preBytes+(i<<3):]))
	}

	// Get itemArray
	itemsOffset := preBytes + (8 * activeItems)
	itemArray := make([]int64, activeItems)
	for i := 0; i < activeItems; i++ {
		itemArray[i] = int64(binary.LittleEndian.Uint64(slc[itemsOffset+(i<<3):]))
	}

	// UpdateMany the sketch
	for i := 0; i < activeItems && err == nil; i++ {
		err = fls.UpdateMany(itemArray[i], countArray[i])
	}
	if err != nil {
		return nil, err
	}
	fls.streamWeight = preArr[2] //override streamWeight due to updating
	return fls, nil
}

// NewLongsSketchFromString returns a sketch instance of this class from the given string,
// which must be a string representation of this sketch class.
//
// str is a string representation of a sketch of this class.
func NewLongsSketchFromString(str string) (*LongsSketch, error) {
	if len(str) < 1 {
		return nil, errors.New("string is empty")
	}
	// Remove trailing comma if present
	// as this will cause a problem with the split
	if str[len(str)-1] == ',' {
		str = str[:len(str)-1]
	}
	tokens := strings.Split(str, ",")
	if len(tokens) < (strPreambleTokens + 2) {
		return nil, fmt.Errorf("string not long enough: %d", len(tokens))
	}
	serVe, err := strconv.ParseInt(tokens[0], 10, 32)
	if err != nil {
		return nil, err
	}
	famID, err := strconv.ParseInt(tokens[1], 10, 32)
	if err != nil {
		return nil, err
	}
	lgMax, err := strconv.ParseInt(tokens[2], 10, 32)
	if err != nil {
		return nil, err
	}
	flags, err := strconv.ParseInt(tokens[3], 10, 32)
	if err != nil {
		return nil, err
	}
	streamWt, err := strconv.ParseInt(tokens[4], 10, 64)
	if err != nil {
		return nil, err
	}
	offset, err := strconv.ParseInt(tokens[5], 10, 64)
	if err != nil {
		return nil, err
	}
	//should always get at least the next 2 from the map
	numActive, err := strconv.ParseInt(tokens[6], 10, 32)
	if err != nil {
		return nil, err
	}
	lgCurOrigin, err := strconv.ParseUint(tokens[7], 10, 32)
	if err != nil {
		return nil, err
	}
	lgCur := bits.TrailingZeros64(lgCurOrigin)

	//checks
	if serVe != _SER_VER {
		return nil, fmt.Errorf("possible Corruption: Bad SerVer: %d", serVe)
	}
	if famID != int64(internal.FamilyEnum.Frequency.Id) {
		return nil, fmt.Errorf("possible Corruption: Bad Family: %d", famID)
	}
	empty := flags > 0
	if !empty && (numActive == 0) {
		return nil, fmt.Errorf("Possible Corruption: !Empty && NumActive=0;  strLen: %d", numActive)
	}
	numTokens := int64(len(tokens))
	if (2 * numActive) != (numTokens - strPreambleTokens - 2) {
		return nil, fmt.Errorf("possible Corruption: Incorrect # of tokens: %d, numActive: %d", numTokens, numActive)
	}
	//    if ((2 * numActive) != (numTokens - STR_PREAMBLE_TOKENS - 2)) {
	sk, err := NewLongsSketch(int(lgMax), int(lgCur))
	if err != nil {
		return nil, err
	}
	sk.streamWeight = streamWt
	sk.offset = offset
	sk.hashMap, err = deserializeFromStringArray(tokens)
	if err != nil {
		return nil, err
	}
	return sk, nil
}

// GetAprioriErrorLongsSketch returns the estimated a priori error given the maxMapSize for the sketch and the
// estimatedTotalStreamWeight.
//
// maxMapSize is the planned map size to be used when constructing this sketch.
// estimatedTotalStreamWeight is the estimated total stream weight.
func GetAprioriErrorLongsSketch(maxMapSize int, estimatedTotalStreamWeight int64) (float64, error) {
	epsilon, err := GetEpsilonLongsSketch(maxMapSize)
	if err != nil {
		return 0, err
	}
	return epsilon * float64(estimatedTotalStreamWeight), nil
}

// GetCurrentMapCapacity returns the current number of counters the sketch is configured to support.
func (s *LongsSketch) GetCurrentMapCapacity() int {
	return s.curMapCap
}

// GetEpsilonLongsSketch returns epsilon used to compute a priori error.
// This is just the value 3.5 / maxMapSize.
//
// maxMapSize is the planned map size to be used when constructing this sketch.
func GetEpsilonLongsSketch(maxMapSize int) (float64, error) {
	if !internal.IsPowerOf2(maxMapSize) {
		return 0, errors.New("maxMapSize is not a power of 2")
	}
	return 3.5 / float64(maxMapSize), nil
}

// GetEstimate gets the estimate of the frequency of the given item.
// Note: The true frequency of an item would be the sum of the counts as a result of the
// two update functions.
//
// item is the given item
//
// return the estimate of the frequency of the given item
func (s *LongsSketch) GetEstimate(item int64) (int64, error) {
	itemCount, err := s.hashMap.get(item)
	if err != nil {
		return 0, err
	}
	return itemCount + s.offset, nil
}

// GetLowerBound gets the guaranteed lower bound frequency of the given item, which can never be
// negative.
//
// item is the given item.
//
// return the guaranteed lower bound frequency of the given item. That is, a number which
// is guaranteed to be no larger than the real frequency.
func (s *LongsSketch) GetLowerBound(item int64) (int64, error) {
	// LB = itemCount
	return s.hashMap.get(item)
}

// GetUpperBound gets the guaranteed upper bound frequency of the given item.
//
// item is the given item.
//
// return the guaranteed upper bound frequency of the given item. That is, a number which
// is guaranteed to be no smaller than the real frequency.
func (s *LongsSketch) GetUpperBound(item int64) (int64, error) {
	itemCount, err := s.hashMap.get(item)
	if err != nil {
		return 0, err
	}
	return itemCount + s.offset, nil
}

// GetFrequentItemsWithThreshold returns an array of Row that include frequent items, estimates, upper and
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
func (s *LongsSketch) GetFrequentItemsWithThreshold(threshold int64, errorType errorType) ([]*Row, error) {
	finalThreshold := s.GetMaximumError()
	if threshold > finalThreshold {
		finalThreshold = threshold
	}
	return s.sortItems(finalThreshold, errorType)
}

// GetFrequentItems returns an array of Row that include frequent items, estimates, upper and
// lower bounds given an ErrorCondition and the default threshold.
// This is the same as GetFrequentItemsWithThreshold(getMaximumError(), errorType)
//
// errorType determines whether no false positives or no false negatives are desired.
func (s *LongsSketch) GetFrequentItems(errorType errorType) ([]*Row, error) {
	return s.sortItems(s.GetMaximumError(), errorType)
}

// GetNumActiveItems returns the number of active items in the sketch.
func (s *LongsSketch) GetNumActiveItems() int {
	return s.hashMap.numActive
}

// GetMaximumError return an upper bound on the maximum error of GetEstimate(item) for any item.
// This is equivalent to the maximum distance between the upper bound and the lower bound
// for any item.
func (s *LongsSketch) GetMaximumError() int64 {
	return s.offset
}

// GetMaximumMapCapacity returns the maximum number of counters the sketch is configured to
// support.
func (s *LongsSketch) GetMaximumMapCapacity() int {
	return int(float64(uint64(1<<s.lgMaxMapSize)) * reversePurgeLongHashMapLoadFactor)
}

// GetStorageBytes returns the number of bytes required to store this sketch as slice
func (s *LongsSketch) GetStorageBytes() int {
	if s.IsEmpty() {
		return 8
	}
	return (4 * 8) + (16 * s.GetNumActiveItems())
}

// GetStreamLength returns the sum of the frequencies (weights or counts) in the stream seen
// so far by the sketch
func (s *LongsSketch) GetStreamLength() int64 {
	return s.streamWeight
}

// IsEmpty returns true if this sketch is empty
func (s *LongsSketch) IsEmpty() bool {
	return s.GetNumActiveItems() == 0
}

// Update this sketch with an item and a frequency count of one.
//
// item for which the frequency should be increased.
func (s *LongsSketch) Update(item int64) error {
	return s.UpdateMany(item, 1)
}

// UpdateMany this sketch with an item and a positive frequency count (or weight).
//
// Item for which the frequency should be increased. The item can be any long value
// and is only used by the sketch to determine uniqueness.
// count the amount by which the frequency of the item should be increased.
// A count of zero is a no-op, and a negative count will throw an exception.
func (s *LongsSketch) UpdateMany(item int64, count int64) error {
	if count == 0 {
		return nil
	}
	if count < 0 {
		return errors.New("count may not be negative")
	}
	s.streamWeight += count
	err := s.hashMap.adjustOrPutValue(item, count)
	if err != nil {
		return err
	}

	if s.hashMap.numActive > s.curMapCap {
		// Over the threshold, we need to do something
		if s.hashMap.lgLength < s.lgMaxMapSize {
			// Below tgt size, we can grow
			err = s.hashMap.resize(2 * len(s.hashMap.keys))
			if err != nil {
				return err
			}
			s.curMapCap = s.hashMap.getCapacity()
		} else {
			// At tgt size, must purge
			s.offset += s.hashMap.purge(s.sampleSize)
			if s.GetNumActiveItems() > s.GetMaximumMapCapacity() {
				return errors.New("purge did not reduce active items")
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
func (s *LongsSketch) Merge(other *LongsSketch) (*LongsSketch, error) {
	if other == nil || other.IsEmpty() {
		return s, nil
	}
	streamWt := s.streamWeight + other.streamWeight //capture before Merge
	iter := other.hashMap.iterator()
	for iter.next() {
		err := s.UpdateMany(iter.getKey(), iter.getValue())
		if err != nil {
			return nil, err
		}
	}
	s.offset += other.offset
	s.streamWeight = streamWt //corrected streamWeight
	return s, nil
}

// ToString returns a String representation of this sketch
func (s *LongsSketch) ToString() (string, error) {
	var sb strings.Builder
	//start the string with parameters of the sketch
	serVer := _SER_VER //0
	famID := internal.FamilyEnum.Frequency.Id
	lgMaxMapSz := s.lgMaxMapSize
	flags := 0
	if s.hashMap.numActive == 0 {
		flags = _EMPTY_FLAG_MASK
	}
	_, err := fmt.Fprintf(&sb, "%d,%d,%d,%d,%d,%d,", serVer, famID, lgMaxMapSz, flags, s.streamWeight, s.offset)
	if err != nil {
		return "", err
	}
	sb.WriteString(s.hashMap.serializeToString()) //numActive, curMaplen, key[i], value[i], ...
	return sb.String(), nil
}

// ToSlice returns a slice representation of this sketch
func (s *LongsSketch) ToSlice() []byte {
	empty := s.IsEmpty()
	activeItems := s.GetNumActiveItems()
	preLongs := 1
	outBytes := 8
	if !empty {
		preLongs = internal.FamilyEnum.Frequency.MaxPreLongs //4
		outBytes = (preLongs + (2 * activeItems)) << 3       //2 because both keys and values are longs
	}
	outArr := make([]byte, outBytes)

	//build first preLong empty or not
	pre0 := int64(0)
	pre0 = insertPreLongs(int64(preLongs), pre0)                         //Byte 0
	pre0 = insertSerVer(_SER_VER, pre0)                                  //Byte 1
	pre0 = insertFamilyID(int64(internal.FamilyEnum.Frequency.Id), pre0) //Byte 2
	pre0 = insertLgMaxMapSize(int64(s.lgMaxMapSize), pre0)               //Byte 3
	pre0 = insertLgCurMapSize(int64(s.hashMap.lgLength), pre0)           //Byte 4
	if empty {
		pre0 = insertFlags(_EMPTY_FLAG_MASK, pre0) //Byte 5
		binary.LittleEndian.PutUint64(outArr, uint64(pre0))
		return outArr
	}
	pre := int64(0)
	pre0 = insertFlags(0, pre0) //Byte 5
	preArr := make([]int64, preLongs)
	preArr[0] = pre0
	preArr[1] = insertActiveItems(int64(activeItems), pre)
	preArr[2] = s.streamWeight
	preArr[3] = s.offset

	for i := 0; i < preLongs; i++ {
		binary.LittleEndian.PutUint64(outArr[i<<3:], uint64(preArr[i]))
	}

	preBytes := preLongs << 3
	activeValues := s.hashMap.getActiveValues()
	for i := 0; i < activeItems; i++ {
		binary.LittleEndian.PutUint64(outArr[preBytes+(i<<3):], uint64(activeValues[i]))
	}

	activeKeys := s.hashMap.getActiveKeys()
	for i := 0; i < activeItems; i++ {
		binary.LittleEndian.PutUint64(outArr[preBytes+((activeItems+i)<<3):], uint64(activeKeys[i]))
	}

	return outArr
}

// Reset resets this sketch to a virgin state.
func (s *LongsSketch) Reset() {
	hasMap, _ := newReversePurgeLongHashMap(1 << _LG_MIN_MAP_SIZE)
	s.curMapCap = hasMap.getCapacity()
	s.offset = 0
	s.streamWeight = 0
	s.hashMap = hasMap
}

func (s *LongsSketch) String() string {
	var sb strings.Builder
	sb.WriteString("FrequentLongsSketch:")
	sb.WriteString("\n")
	sb.WriteString("  Stream Length    : " + strconv.FormatInt(s.streamWeight, 10))
	sb.WriteString("\n")
	sb.WriteString("  Max Error Offset : " + strconv.FormatInt(s.offset, 10))
	sb.WriteString("\n")
	sb.WriteString(s.hashMap.String())
	return sb.String()
}

func (s *LongsSketch) sortItems(threshold int64, errorType errorType) ([]*Row, error) {
	rowList := make([]*Row, 0)
	iter := s.hashMap.iterator()
	if errorType == ErrorTypeEnum.NoFalseNegatives {
		for iter.next() {
			est, lb, ub, err := s.frequencies(iter.getKey())
			if err != nil {
				return nil, err
			}
			if ub >= threshold {
				row := newRow(iter.getKey(), est, ub, lb)
				rowList = append(rowList, row)
			}
		}
	} else { //NO_FALSE_POSITIVES
		for iter.next() {
			est, lb, ub, err := s.frequencies(iter.getKey())
			if err != nil {
				return nil, err
			}
			if lb >= threshold {
				row := newRow(iter.getKey(), est, ub, lb)
				rowList = append(rowList, row)
			}
		}
	}

	slices.SortFunc(rowList, func(a, b *Row) int {
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

// frequencies return estimated frequency, lower bound frequency,
// upper bound frequency at once.
func (s *LongsSketch) frequencies(item int64) (est, lower, upper int64, err error) {
	var cnt int64
	cnt, err = s.hashMap.get(item)
	if err != nil {
		return 0, 0, 0, err
	}

	est = cnt + s.offset
	lower = cnt
	upper = cnt + s.offset
	return
}
