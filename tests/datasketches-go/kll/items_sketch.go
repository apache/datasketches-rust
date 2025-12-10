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

// Package kll is an implementation of a very compact quantiles sketch with lazy compaction scheme
// and nearly optimal accuracy per retained quantile.</p>
//
// Reference: https://arxiv.org/abs/1603.05346v2" Optimal Quantile Approximation in Streams
//
// The default k of 200 yields a "single-sided" epsilon of about 1.33% and a
// "double-sided" (PMF) epsilon of about 1.65%, with a confidence of 99%.
//
// See "https://datasketches.apache.org/docs/KLL/KLLSketch.html" KLL Sketch
package kll

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"slices"

	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
)

type ItemsSketch[C comparable] struct {
	// k is the config that controls the accuracy of the sketch and its memory space usage
	// The default k = 200 results in a normalized rank error of about 1.65%.
	k uint16
	// m is the number of items in the base level of the KLL array
	m                 uint8
	minK              uint16
	numLevels         uint8
	isLevelZeroSorted bool
	n                 uint64
	levels            []uint32
	items             []C
	minItem           *C
	maxItem           *C
	sortedView        *ItemsSketchSortedView[C]
	serde             common.ItemSketchSerde[C]
	compareFn         common.CompareFn[C]

	// Force deterministic offset for test, so that we can compare results across implementation.
	deterministicOffsetForTest bool
}

const (
	_DEFAULT_K = uint16(200)
	_DEFAULT_M = uint8(8)
	_MIN_K     = uint16(_DEFAULT_M)
	_MAX_K     = (1 << 16) - 1
	_MIN_M     = 2 //The minimum M
	_MAX_M     = 8 //The maximum M
)

var (
	powersOfThree = []uint64{1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049, 177147, 531441,
		1594323, 4782969, 14348907, 43046721, 129140163, 387420489, 1162261467,
		3486784401, 10460353203, 31381059609, 94143178827, 282429536481,
		847288609443, 2541865828329, 7625597484987, 22876792454961, 68630377364883,
		205891132094649}

	// Used for deterministic rand behavior in tests
	nextOffsetForTest = 0
)

// NewKllItemsSketch create a new ItemsSketch with the given k and m.
// The default k = 200 results in a normalized rank error of about 1.65%.
// Larger K will have smaller error but the sketch will be larger (and slower).
func NewKllItemsSketch[C comparable](k uint16, m uint8, compareFn common.CompareFn[C], serde common.ItemSketchSerde[C]) (*ItemsSketch[C], error) {
	if k < _MIN_K || k > _MAX_K {
		return nil, fmt.Errorf("k must be >= %d and <= %d: %d", _MIN_K, _MAX_K, k)
	}
	if compareFn == nil {
		return nil, fmt.Errorf("no compare function provided")
	}
	return &ItemsSketch[C]{
		k:         k,
		m:         m,
		minK:      k,
		numLevels: uint8(1),
		levels:    []uint32{uint32(k), uint32(k)},
		items:     make([]C, k),
		serde:     serde,
		compareFn: compareFn,
	}, nil
}

// NewKllItemsSketchWithDefault create a new ItemsSketch with default k and m.
// The default k = 200 results in a normalized rank error of about 1.65%.
func NewKllItemsSketchWithDefault[C comparable](compareFn common.CompareFn[C], serde common.ItemSketchSerde[C]) (*ItemsSketch[C], error) {
	return NewKllItemsSketch[C](_DEFAULT_K, _DEFAULT_M, compareFn, serde)
}

// NewKllItemsSketchFromSlice create a new ItemsSketch from the given byte slice (serialized sketch).
func NewKllItemsSketchFromSlice[C comparable](sl []byte, compareFn common.CompareFn[C], serde common.ItemSketchSerde[C]) (*ItemsSketch[C], error) {
	if serde == nil {
		return nil, fmt.Errorf("no SerDe provided")
	}

	if compareFn == nil {
		return nil, fmt.Errorf("no compare function provided")
	}

	memVal, err := newItemsSketchMemoryValidate(sl, serde)
	if err != nil {
		return nil, err
	}

	var (
		k                 = memVal.k
		m                 = memVal.m
		levelsArr         = memVal.levelsArr
		n                 = memVal.n
		minK              = memVal.minK
		isLevelZeroSorted = memVal.level0SortedFlag
		minItem           *C
		maxItem           *C
		items             = make([]C, levelsArr[memVal.numLevels])
	)

	switch memVal.sketchStructure {
	case _COMPACT_EMPTY:
		minItem = nil
		maxItem = nil
		items = make([]C, k)
	case _COMPACT_SINGLE:
		offset := _N_LONG_ADR
		deserItems, err := serde.DeserializeManyFromSlice(sl, offset, 1)
		if err != nil {
			return nil, err
		}
		minItem = &deserItems[0]
		maxItem = &deserItems[0]
		items = make([]C, k)
		items[k-1] = deserItems[0]
	case _COMPACT_FULL:
		offset := int(_DATA_START_ADR + memVal.numLevels*4)
		deserMinItems, err := serde.DeserializeManyFromSlice(sl, offset, 1)
		minItem = &deserMinItems[0]
		if err != nil {
			return nil, err
		}
		offset += serde.SizeOf(*minItem)
		deserMaxItems, err := serde.DeserializeManyFromSlice(sl, offset, 1)
		maxItem = &deserMaxItems[0]
		if err != nil {
			return nil, err
		}
		offset += serde.SizeOf(*maxItem)
		numRetained := levelsArr[memVal.numLevels] - levelsArr[0]
		deseRetItems, err := serde.DeserializeManyFromSlice(sl, offset, int(numRetained))
		if err != nil {
			return nil, err
		}
		for i := uint32(0); i < numRetained; i++ {
			items[i+levelsArr[0]] = deseRetItems[i]
		}
	}

	return &ItemsSketch[C]{
		k:                 k,
		m:                 m,
		minK:              minK,
		numLevels:         memVal.numLevels,
		isLevelZeroSorted: isLevelZeroSorted,
		n:                 n,
		levels:            levelsArr,
		items:             items,
		minItem:           minItem,
		maxItem:           maxItem,
		serde:             serde,
		compareFn:         compareFn,
	}, nil
}

// IsEmpty returns true if the sketch is empty, otherwise false.
func (s *ItemsSketch[C]) IsEmpty() bool {
	return s.n == 0
}

// GetN returns the value of n (the length of the input stream offered to the sketch)
func (s *ItemsSketch[C]) GetN() uint64 {
	return s.n
}

// GetK returns the value of k (which controls the accuracy of the sketch and its memory space usage)
func (s *ItemsSketch[C]) GetK() uint16 {
	return s.k
}

// GetNumRetained returns the number of quantiles retained by the sketch.
func (s *ItemsSketch[C]) GetNumRetained() uint32 {
	return s.levels[s.numLevels] - s.levels[0]
}

// GetMinItem returns the minimum item of the stream. This may be distinct from the smallest item retained by the sketch algorithm.
func (s *ItemsSketch[C]) GetMinItem() (C, error) {
	if s.IsEmpty() {
		return *new(C), fmt.Errorf("operation is undefined for an empty sketch")
	}
	return *s.minItem, nil
}

// GetMaxItem returns the maximum item of the stream. This may be distinct from the largest item retained by the sketch algorithm.
func (s *ItemsSketch[C]) GetMaxItem() (C, error) {
	if s.IsEmpty() {
		return *new(C), fmt.Errorf("operation is undefined for an empty sketch")
	}
	return *s.maxItem, nil
}

// IsEstimationMode returns true if the sketch is in estimation mode, otherwise false.
func (s *ItemsSketch[C]) IsEstimationMode() bool {
	return s.numLevels > 1
}

// GetTotalItemsArray return the serialized byte array of the entire internal items hypothetical structure.
// It does not include the preamble, the levels array, or minimum or maximum items.
// It may include empty or garbage items.
func (s *ItemsSketch[C]) GetTotalItemsArray() []C {
	if s.n == 0 {
		return make([]C, s.k)
	}
	outArr := make([]C, len(s.items))
	copy(outArr, s.items)
	return outArr
}

// GetRank return the normalized rank corresponding to the given a quantile.
// if INCLUSIVE the given quantile is included into the rank.
func (s *ItemsSketch[C]) GetRank(item C, inclusive bool) (float64, error) {
	if s.IsEmpty() {
		return 0, fmt.Errorf("operation is undefined for an empty sketch")
	}
	err := s.setupSortedView()
	if err != nil {
		return 0, err
	}
	return s.sortedView.GetRank(item, inclusive)
}

// GetRanks return an array of normalized ranks corresponding to the given array of quantiles and the given search criterion.
// if INCLUSIVE, the given quantiles include the rank directly corresponding to each quantile.
func (s *ItemsSketch[C]) GetRanks(item []C, inclusive bool) ([]float64, error) {
	if s.IsEmpty() {
		return nil, fmt.Errorf("operation is undefined for an empty sketch")
	}
	err := s.setupSortedView()
	if err != nil {
		return nil, err
	}
	ranks := make([]float64, len(item))
	for i := range item {
		ranks[i], err = s.sortedView.GetRank(item[i], inclusive)
		if err != nil {
			return nil, err
		}
	}
	return ranks, nil
}

// GetQuantile return the approximate quantile of the given normalized rank and the given search criterion.
// If INCLUSIVE, the given rank includes all quantiles <= the quantile directly corresponding to the given rank.
// If EXCLUSIVE, the given rank includes all quantiles < the quantile directly corresponding to the given rank.
func (s *ItemsSketch[C]) GetQuantile(rank float64, inclusive bool) (C, error) {
	if s.IsEmpty() {
		return *new(C), fmt.Errorf("operation is undefined for an empty sketch")
	}
	if rank < 0.0 || rank > 1.0 {
		return *new(C), fmt.Errorf("normalized rank cannot be less than zero or greater than 1.0: %f", rank)
	}
	err := s.setupSortedView()
	if err != nil {
		return *new(C), err
	}
	return s.sortedView.GetQuantile(rank, inclusive)
}

// GetQuantiles return an array of quantiles from the given array of normalized ranks.
// if INCLUSIVE, the given ranks include all quantiles <= the quantile directly corresponding to each rank.
func (s *ItemsSketch[C]) GetQuantiles(ranks []float64, inclusive bool) ([]C, error) {
	if s.IsEmpty() {
		return nil, fmt.Errorf("operation is undefined for an empty sketch")
	}
	err := s.setupSortedView()
	if err != nil {
		return nil, err
	}
	quantiles := make([]C, len(ranks))
	for i := range ranks {
		quantiles[i], err = s.sortedView.GetQuantile(ranks[i], inclusive)
		if err != nil {
			return nil, err
		}
	}
	return quantiles, nil
}

// GetPMF returns an approximation to the Probability Mass Function (PMF) of the input stream
// as an array of probability masses as doubles on the interval [0.0, 1.0], given a set of splitPoints.
//
// The resulting approximations have a probabilistic guarantee that can be obtained from the
// getNormalizedRankError(true) function.</p>
//
//   - splitPoints an array of m unique, monotonically increasing items
//     (of the same type as the input items)
//     that divide the item input domain into <i>m+1</i> consecutive, non-overlapping intervals.
//
// Each interval except for the end intervals starts with a split point and ends with the next split
// point in sequence.
//
// The first interval starts below the lowest item retained by the sketch
// corresponding to a zero rank or zero probability, and ends with the first split point</p>
//
// The last (m+1)th interval starts with the last split point and ends after the last
// item retained by the sketch corresponding to a rank or probability of 1.0.
//
// The sum of the probability masses of all (m+1) intervals is 1.0.
//
// If the search criterion is:
//
//   - INCLUSIVE, and the upper split point of an interval equals an item retained by the sketch, the interval
//     will include that item. If the lower split point equals an item retained by the sketch, the interval will exclude
//     that item.
//
//   - EXCLUSIVE, and the upper split point of an interval equals an item retained by the sketch, the interval
//     will exclude that item. If the lower split point equals an item retained by the sketch, the interval will include
//     that item.
//
// It is not recommended to include either the minimum or maximum items of the input stream.
func (s *ItemsSketch[C]) GetPMF(splitPoints []C, inclusive bool) ([]float64, error) {
	if s.IsEmpty() {
		return nil, fmt.Errorf("operation is undefined for an empty sketch")
	}
	err := s.setupSortedView()
	if err != nil {
		return nil, err
	}
	return s.sortedView.GetPMF(splitPoints, inclusive)
}

// GetCDF returns an approximation to the Cumulative Distribution Function (CDF) of the input stream
// as a monotonically increasing array of double ranks (or cumulative probabilities) on the interval [0.0, 1.0],
// given a set of splitPoints.
//
// The resulting approximations have a probabilistic guarantee that can be obtained from the
// getNormalizedRankError(false) function.
//
// - splitPoints an array of <i>m</i> unique, monotonically increasing items
// (of the same type as the input items)
// that divide the item input domain into <i>m+1</i> overlapping intervals.
//
// The start of each interval is below the lowest item retained by the sketch
// corresponding to a zero rank or zero probability, and the end of the interval
// is the rank or cumulative probability corresponding to the split point.
//
// The (m+1)th interval represents 100% of the distribution represented by the sketch
// and consistent with the definition of a cumulative probability distribution, thus the (m+1)th
// rank or probability in the returned array is always 1.0.
//
// If a split point exactly equals a retained item of the sketch and the search criterion is:
//
// - INCLUSIVE, the resulting cumulative probability will include that item.
// - EXCLUSIVE, the resulting cumulative probability will not include the weight of that split point.
//
// It is not recommended to include either the minimum or maximum items of the input stream.
func (s *ItemsSketch[C]) GetCDF(splitPoints []C, inclusive bool) ([]float64, error) {
	if s.IsEmpty() {
		return nil, fmt.Errorf("operation is undefined for an empty sketch")
	}
	err := s.setupSortedView()
	if err != nil {
		return nil, err
	}
	return s.sortedView.GetCDF(splitPoints, inclusive)
}

// GetNormalizedRankError return the approximate rank error of this sketch normalized as a fraction between zero and one.
// The epsilon returned is a best fit to 99 percent confidence empirically measured max error
// in thousands of trials.
// = pmf if true, returns the "double-sided" normalized rank error for the getPMF() function.
// Otherwise, it is the "single-sided" normalized rank error for all the other queries.
// @return if pmf is true, returns the "double-sided" normalized rank error for the getPMF() function.
// Otherwise, it is the "single-sided" normalized rank error for all the other queries.
func (s *ItemsSketch[C]) GetNormalizedRankError(pmf bool) float64 {
	return getNormalizedRankError(s.minK, pmf)
}

// GetPartitionBoundaries returns an instance of ItemsSketchPartitionBoundaries
// which provides sufficient information for the user to create the given number of equally sized partitions,
// where "equally sized" refers to an approximately equal number of items per partition.
//
// - numEquallySized an integer that specifies the number of equally sized partitions between getMinItem() and
// getMaxItem().
// This must be a positive integer greater than zero.
//
// A 1 will return: minItem, maxItem.
// A 2 will return: minItem, median quantile, maxItem.
// Etc.
//
// - searchCrit
// If INCLUSIVE, all the returned quantiles are the upper boundaries of the equally sized partitions
// except for the lowest returned quantile, which is the lowest boundary of the lowest ranked partition.
// If EXCLUSIVE, all the returned quantiles are the lower boundaries of the equally sized partitions
// except for the highest returned quantile, which is the upper boundary of the highest ranked partition.
func (s *ItemsSketch[C]) GetPartitionBoundaries(numEquallySized int, inclusive bool) (*ItemsSketchPartitionBoundaries[C], error) {
	if s.IsEmpty() {
		return nil, fmt.Errorf("operation is undefined for an empty sketch")
	}
	err := s.setupSortedView()
	if err != nil {
		return nil, err

	}
	return s.sortedView.GetPartitionBoundaries(numEquallySized, inclusive)
}

// GetSortedView return the sorted view of this sketch.
func (s *ItemsSketch[C]) GetSortedView() (*ItemsSketchSortedView[C], error) {
	if s.IsEmpty() {
		return nil, fmt.Errorf("operation is undefined for an empty sketch")
	}
	err := s.setupSortedView()
	if err != nil {
		return nil, err
	}
	return s.sortedView, nil
}

// Update this sketch with the given item.
func (s *ItemsSketch[C]) Update(item C) {
	s.updateItem(item, s.compareFn)
	s.sortedView = nil
}

// Merge the given sketch into this sketch.
func (s *ItemsSketch[C]) Merge(other *ItemsSketch[C]) {
	if other.IsEmpty() {
		return
	}
	s.mergeItemsSketch(other)
	s.sortedView = nil
}

// Reset this sketch to the empty state.
func (s *ItemsSketch[C]) Reset() {
	s.n = 0
	s.isLevelZeroSorted = false
	s.numLevels = 1
	s.levels = []uint32{uint32(s.k), uint32(s.k)}
	s.minItem = nil
	s.maxItem = nil
	s.items = make([]C, s.k)
	s.sortedView = nil
}

// ToSlice returns the serialized byte array of this sketch.
func (s *ItemsSketch[C]) ToSlice() ([]byte, error) {
	if s.serde == nil {
		return nil, fmt.Errorf("no SerDe provided")
	}

	srcN := s.n
	var tgtStructure = _COMPACT_FULL
	if srcN == 0 {
		tgtStructure = _COMPACT_EMPTY
	} else if srcN == 1 {
		tgtStructure = _COMPACT_SINGLE
	}
	totalBytes, err := s.currentSerializedSizeBytes()
	if err != nil {
		return nil, err
	}
	bytesOut := make([]byte, totalBytes)

	//ints 0,1
	preInts := byte(tgtStructure.getPreInts())
	serVer := byte(tgtStructure.getSerVer())
	famId := byte(internal.FamilyEnum.Kll.Id)
	flags := byte(0)
	if s.IsEmpty() {
		flags |= _EMPTY_BIT_MASK
	}
	if s.isLevelZeroSorted {
		flags |= _LEVEL_ZERO_SORTED_BIT_MASK
	}
	if s.n == 1 {
		flags |= _SINGLE_ITEM_BIT_MASK
	}
	k := uint16(s.k)
	m := uint8(s.m)

	bytesOut[0] = preInts
	bytesOut[1] = serVer
	bytesOut[2] = famId
	bytesOut[3] = flags
	binary.LittleEndian.PutUint16(bytesOut[4:6], k)
	bytesOut[6] = m

	if tgtStructure == _COMPACT_EMPTY {
		return bytesOut, nil
	}

	if tgtStructure == _COMPACT_SINGLE {
		siByteArr, err := s.getSingleItemByteArr()
		if err != nil {
			return nil, err
		}
		copy(bytesOut[_DATA_START_ADR_SINGLE_ITEM:], siByteArr)
		return bytesOut, nil
	}

	// Tgt is either COMPACT_FULL or UPDATABLE
	//ints 2,3
	n := s.n
	//ints 4
	minK := uint16(s.minK)
	numLevels := uint8(s.numLevels)
	//end of full preamble
	lvlsArr := s.getLevelsArray()
	minMaxByteArr := s.getMinMaxByteArr()
	itemsByteArr := s.getRetainedItemsByteArr()

	binary.LittleEndian.PutUint64(bytesOut[8:16], n)
	binary.LittleEndian.PutUint16(bytesOut[16:18], minK)
	bytesOut[18] = numLevels
	for i := uint8(0); i < numLevels; i++ {
		binary.LittleEndian.PutUint32(bytesOut[_DATA_START_ADR+i*4:], lvlsArr[i])
	}
	copy(bytesOut[_DATA_START_ADR+(numLevels*4):], minMaxByteArr)
	copy(bytesOut[_DATA_START_ADR+int(numLevels*4)+len(minMaxByteArr):], itemsByteArr)
	return bytesOut, nil
}

// GetSerializedSizeBytes Returns the current number of bytes this Sketch would require if serialized in compact form.
func (s *ItemsSketch[C]) GetSerializedSizeBytes() (int, error) {
	if s.serde == nil {
		return 0, fmt.Errorf("no SerDe provided")
	}
	return s.currentSerializedSizeBytes()
}

// GetIterator returns the iterator for this sketch, which is not sorted.
func (s *ItemsSketch[C]) GetIterator() *ItemsSketchIterator[C] {
	return newItemsSketchIterator[C](
		s.GetTotalItemsArray(),
		s.getLevelsArray(),
		s.getNumLevels(),
	)
}

//
// Private methods
//

func (s *ItemsSketch[C]) currentSerializedSizeBytes() (int, error) {
	srcN := s.n
	var tgtStructure = _COMPACT_FULL
	if srcN == 0 {
		tgtStructure = _COMPACT_EMPTY
	} else if srcN == 1 {
		tgtStructure = _COMPACT_SINGLE
	}

	totalBytes := 0
	if tgtStructure == _COMPACT_EMPTY {
		totalBytes = _N_LONG_ADR
	} else if tgtStructure == _COMPACT_SINGLE {
		v, err := s.getSingleItemSizeBytes()
		if err != nil {
			return 0, err
		}
		totalBytes = _DATA_START_ADR_SINGLE_ITEM + v
	} else if tgtStructure == _COMPACT_FULL {

		totalBytes = _DATA_START_ADR + s.getLevelsArrSizeBytes(tgtStructure) + s.getMinMaxSizeBytes() + s.getRetainedItemsSizeBytes()
	} else { //structure = UPDATABLE
		return 0, fmt.Errorf("updatable serialization not implemented")
	}
	return totalBytes, nil
}

func (s *ItemsSketch[C]) getNumLevels() int {
	return len(s.levels) - 1
}

func (s *ItemsSketch[C]) getLevelsArray() []uint32 {
	levels := make([]uint32, len(s.levels))
	copy(levels, s.levels)
	return levels
}

func (s *ItemsSketch[C]) getLevelsArrSizeBytes(structure sketchStructure) int {
	if structure == _UPDATABLE {
		return len(s.levels) * 4 // * Integer.BYTES
	} else if structure == _COMPACT_FULL {
		return (len(s.levels) - 1) * 4 // // * Integer.BYTES
	} else {
		return 0
	}
}

func (s *ItemsSketch[C]) getMinMaxSizeBytes() int {
	return s.serde.SizeOf(*s.minItem) + s.serde.SizeOf(*s.maxItem)
}

func (s *ItemsSketch[C]) getMinMaxByteArr() []byte {
	minBytes := s.serde.SerializeOneToSlice(*s.minItem)
	maxBytes := s.serde.SerializeOneToSlice(*s.maxItem)
	minMaxBytes := make([]byte, len(minBytes)+len(maxBytes))
	copy(minMaxBytes, minBytes)
	copy(minMaxBytes[len(minBytes):], maxBytes)
	return minMaxBytes
}

func (s *ItemsSketch[C]) getSingleItemSizeBytes() (int, error) {
	v, err := s.getSingleItem()
	if err != nil {
		return 0, err
	}
	return s.serde.SizeOf(v), nil
}

func (s *ItemsSketch[C]) getSingleItemByteArr() ([]byte, error) {
	v, err := s.getSingleItem()
	if err != nil {
		return nil, err
	}
	return s.serde.SerializeOneToSlice(v), nil
}

func (s *ItemsSketch[C]) getSingleItem() (C, error) {
	if s.n != 1 {
		return *new(C), fmt.Errorf("sketch must have exactly one item")
	}
	return s.items[s.k-1], nil
}

func (s *ItemsSketch[C]) getRetainedItemsArray() []C {
	numRet := s.GetNumRetained()
	outArr := make([]C, numRet)
	copy(outArr, s.items[s.levels[0]:])
	return outArr
}

func (s *ItemsSketch[C]) getRetainedItemsByteArr() []byte {
	retArr := s.getRetainedItemsArray()
	return s.serde.SerializeManyToSlice(retArr)
}

func (s *ItemsSketch[C]) getRetainedItemsSizeBytes() int {
	return len(s.getRetainedItemsByteArr())
}

func (s *ItemsSketch[C]) setupSortedView() error {
	if s.sortedView == nil {
		sView, err := newItemsSketchSortedView[C](s)
		if err != nil {
			return err
		}
		s.sortedView = sView
	}
	return nil
}

func (s *ItemsSketch[C]) updateItem(item C, compareFn common.CompareFn[C]) {
	if internal.IsNil(item) {
		return
	}
	if s.IsEmpty() {
		s.minItem = &item
		s.maxItem = &item
	} else {
		if compareFn(item, *s.minItem) {
			s.minItem = &item
		}
		if compareFn(*s.maxItem, item) {
			s.maxItem = &item
		}
	}
	level0space := s.levels[0]
	if level0space == 0 {
		s.compressWhileUpdatingSketch()
		level0space = s.levels[0]
	}
	s.n++
	s.isLevelZeroSorted = false
	nextPos := level0space - 1
	s.levels[0] = nextPos
	s.items[nextPos] = item
}

func (s *ItemsSketch[C]) mergeItemsSketch(other *ItemsSketch[C]) {
	if other.IsEmpty() {
		return
	}
	// capture my key mutable fields before doing any merging
	myEmpty := s.IsEmpty()
	var myMin, myMax C
	var err error
	if !myEmpty {
		myMin, err = s.GetMinItem()
		if err != nil {
			panic(err)
		}
		myMax, err = s.GetMaxItem()
		if err != nil {
			panic(err)
		}
	}
	myMinK := s.minK
	finalN := s.n + other.n

	// buffers that are referenced multiple times
	otherNumLevels := other.numLevels
	otherLevelsArr := other.levels
	var otherItemsArr []C

	// MERGE: update this sketch with level0 items from the other sketch
	otherItemsArr = other.GetTotalItemsArray()
	for i := otherLevelsArr[0]; i < otherLevelsArr[1]; i++ {
		s.updateItem(otherItemsArr[i], s.compareFn)
	}

	// After the level 0 update, we capture the intermediate state of levels and items arrays...
	myCurNumLevels := s.numLevels
	myCurLevelsArr := s.levels
	myCurItemsArr := s.GetTotalItemsArray()

	// then rename them and initialize in case there are no higher levels
	myNewNumLevels := myCurNumLevels
	myNewLevelsArr := myCurLevelsArr
	myNewItemsArr := myCurItemsArr

	//merge higher levels if they exist
	if otherNumLevels > 1 {
		tmpSpaceNeeded := s.GetNumRetained() + getNumRetainedAboveLevelZero(otherNumLevels, otherLevelsArr)
		workbuf := make([]C, tmpSpaceNeeded)
		ub := ubOnNumLevels(finalN)
		worklevels := make([]uint32, ub+2) // ub+1 does not work
		outlevels := make([]uint32, ub+2)

		provisionalNumLevels := max(myCurNumLevels, otherNumLevels)

		populateItemWorkArrays(workbuf, worklevels, provisionalNumLevels,
			myCurNumLevels, myCurLevelsArr, myCurItemsArr,
			otherNumLevels, otherLevelsArr, otherItemsArr, s.compareFn)

		// notice that workbuf is being used as both the input and output
		result := generalItemsCompress(s.k, s.m, provisionalNumLevels, workbuf, worklevels, workbuf, outlevels, s.isLevelZeroSorted, s.compareFn, s.deterministicOffsetForTest)
		targetItemCount := result[1] //was finalCapacity. Max size given k, m, numLevels
		curItemCount := result[2]    //was finalPop

		// now we need to finalize the results for mySketch

		//THE NEW NUM LEVELS
		myNewNumLevels = uint8(result[0])

		// THE NEW ITEMS ARRAY
		if int(targetItemCount) == len(myCurItemsArr) {
			myNewItemsArr = myCurItemsArr
		} else {
			myNewItemsArr = make([]C, targetItemCount)
		}
		freeSpaceAtBottom := targetItemCount - curItemCount

		//shift the new items array create space at bottom
		for i := uint32(0); i < uint32(curItemCount); i++ {
			myNewItemsArr[uint32(freeSpaceAtBottom)+i] = workbuf[outlevels[0]+i]
		}
		theShift := uint32(freeSpaceAtBottom) - outlevels[0]

		//calculate the new levels array length
		var finalLevelsArrLen uint32
		if uint32(len(myCurLevelsArr)) < uint32(myNewNumLevels+1) {
			finalLevelsArrLen = uint32(myNewNumLevels + 1)
		} else {
			finalLevelsArrLen = uint32(len(myCurLevelsArr))
		}

		//THE NEW LEVELS ARRAY
		myNewLevelsArr = make([]uint32, finalLevelsArrLen)
		for lvl := uint8(0); lvl < myNewNumLevels+1; lvl++ { // includes the "extra" index
			myNewLevelsArr[lvl] = outlevels[lvl] + theShift
		}

		//MEMORY SPACE MANAGEMENT
		//not used
	}

	// Update Preamble:
	s.n = finalN
	if other.IsEstimationMode() { //otherwise the merge brings over exact items.
		s.minK = min(myMinK, other.minK)
	}

	// Update numLevels, levelsArray, items
	s.numLevels = myNewNumLevels
	s.levels = myNewLevelsArr
	s.items = myNewItemsArr

	// Update min, max items
	if myEmpty {
		s.minItem = other.minItem
		s.maxItem = other.maxItem
	} else {
		if s.compareFn(myMin, *other.minItem) {
			s.minItem = &myMin
		} else {
			s.minItem = other.minItem
		}

		if s.compareFn(*other.maxItem, myMax) {
			s.maxItem = &myMax
		} else {
			s.maxItem = other.maxItem
		}
	}
}

func (s *ItemsSketch[C]) compressWhileUpdatingSketch() {
	level := findLevelToCompact(s.k, s.m, s.numLevels, s.levels)
	if level == s.numLevels-1 {
		//The level to compact is the top level, thus we need to add a level.
		//Be aware that this operation grows the items array,
		//shifts the items data and the level boundaries of the data,
		//and grows the levels array and increments numLevels_.
		s.addEmptyTopLevelToCompletelyFullSketch()
	}
	myLevelsArr := s.levels
	rawBeg := myLevelsArr[level]
	rawEnd := myLevelsArr[level+1]
	// +2 is OK because we already added a new top level if necessary
	popAbove := myLevelsArr[level+2] - rawEnd
	rawPop := rawEnd - rawBeg
	oddPop := rawPop%2 == 1
	adjBeg := rawBeg
	if oddPop {
		adjBeg++
	}
	adjPop := rawPop
	if oddPop {
		adjPop--
	}
	halfAdjPop := adjPop / 2

	//the following is specific to generic Items
	myItemsArr := s.GetTotalItemsArray()
	if level == 0 { // level zero might not be sorted, so we must sort it if we wish to compact it
		tmpSlice := myItemsArr[adjBeg : adjBeg+adjPop]
		slices.SortFunc(tmpSlice, func(a, b C) int {
			if s.compareFn(a, b) {
				return -1
			}
			return 1
		})
	}
	if popAbove == 0 {
		randomlyHalveUpItems(myItemsArr, adjBeg, adjPop, s.deterministicOffsetForTest)
	} else {
		randomlyHalveDownItems(myItemsArr, adjBeg, adjPop, s.deterministicOffsetForTest)
		mergeSortedItemsArrays(
			myItemsArr, adjBeg, halfAdjPop,
			myItemsArr, rawEnd, popAbove,
			myItemsArr, adjBeg+halfAdjPop, s.compareFn)
	}
	newIndex := myLevelsArr[level+1] - halfAdjPop // adjust boundaries of the level above
	s.levels[level+1] = newIndex

	if oddPop {
		s.levels[level] = myLevelsArr[level+1] - 1          // the current level now contains one item
		myItemsArr[myLevelsArr[level]] = myItemsArr[rawBeg] // namely this leftover guy
	} else {
		s.levels[level] = myLevelsArr[level+1] // the current level is now empty
	}

	if level > 0 {
		amount := rawBeg - myLevelsArr[0] // adjust boundary

		for i := amount; i > 0; i-- {
			// Start from the end as we are shifting to the right,
			// failing to do so will corrupt the items array.
			tgtInx := myLevelsArr[0] + halfAdjPop + i - 1
			stcInx := myLevelsArr[0] + i - 1
			myItemsArr[tgtInx] = myItemsArr[stcInx]
		}
	}
	for lvl := uint8(0); lvl < level; lvl++ {
		newIndex = myLevelsArr[lvl] + halfAdjPop //adjust boundary
		s.levels[lvl] = newIndex
	}
	s.items = myItemsArr
}

func (s *ItemsSketch[C]) addEmptyTopLevelToCompletelyFullSketch() {
	myCurLevelsArr := s.getLevelsArray()
	myCurNumLevels := s.numLevels
	myCurTotalItemsCapacity := myCurLevelsArr[myCurNumLevels]

	myCurItemsArr := s.GetTotalItemsArray()
	minItem := s.minItem
	maxItem := s.maxItem

	deltaItemsCap := levelCapacity(s.k, myCurNumLevels+1, 0, s.m)
	myNewTotalItemsCapacity := myCurTotalItemsCapacity + deltaItemsCap

	// Check if growing the levels arr if required.
	// Note that merging MIGHT over-grow levels_, in which case we might not have to grow it
	growLevelsArr := len(myCurLevelsArr) < int(myCurNumLevels+2)

	var (
		myNewLevelsArr []uint32
		myNewNumLevels uint8
	)

	//myNewLevelsArr := make([]uint32, myCurNumLevels+2)
	// GROW LEVELS ARRAY
	if growLevelsArr {
		//grow levels arr by one and copy the old data to the new array, extra space at the top.
		myNewLevelsArr = make([]uint32, myCurNumLevels+2)
		copy(myNewLevelsArr, myCurLevelsArr)
		myNewNumLevels = myCurNumLevels + 1
		s.numLevels++ //increment for off-heap
	} else {
		myNewLevelsArr = myCurLevelsArr
		myNewNumLevels = myCurNumLevels
	}

	// This loop updates all level indices EXCLUDING the "extra" index at the top
	for level := uint8(0); level <= myNewNumLevels-1; level++ {
		myNewLevelsArr[level] += deltaItemsCap
	}
	myNewLevelsArr[myNewNumLevels] = myNewTotalItemsCapacity // initialize the new "extra" index at the top

	// GROW items ARRAY
	myNewItemsArr := make([]C, myNewTotalItemsCapacity)
	for i := uint32(0); i < myCurTotalItemsCapacity; i++ {
		myNewItemsArr[i+deltaItemsCap] = myCurItemsArr[i]
	}

	// update our sketch with new expanded spaces
	s.numLevels = myNewNumLevels
	s.levels = myNewLevelsArr

	s.minItem = minItem
	s.maxItem = maxItem
	s.items = myNewItemsArr
}

func findLevelToCompact(k uint16, m uint8, numLevels uint8, levels []uint32) uint8 {
	level := uint8(0)
	for {
		pop := levels[level+1] - levels[level]
		capacity := levelCapacity(k, numLevels, level, m)
		if pop >= capacity {
			return level
		}
		level++
	}
}

func computeTotalItemCapacity(k uint16, m uint8, numLevels uint8) uint32 {
	var total uint32 = 0
	for level := uint8(0); level < numLevels; level++ {
		total += levelCapacity(k, numLevels, level, m)
	}
	return total
}

func levelCapacity(k uint16, numLevels uint8, level uint8, m uint8) uint32 {
	depth := numLevels - level - 1
	return max(uint32(m), intCapAux(k, depth))
}

func intCapAux(k uint16, depth uint8) uint32 {
	if depth <= 30 {
		return intCapAuxAux(k, depth)
	}
	half := depth / 2
	rest := depth - half
	tmp := intCapAuxAux(k, half)
	return intCapAuxAux(uint16(tmp), rest)
}

func intCapAuxAux(k uint16, depth uint8) uint32 {
	twok := uint64(k << 1)                        // for rounding at the end, pre-multiply by 2 here, divide by 2 during rounding.
	tmp := (twok << depth) / powersOfThree[depth] //2k* (2/3)^depth. 2k also keeps the fraction larger.
	result := (tmp + 1) >> 1                      // (tmp + 1)/2. If odd, round up. This guarantees an integer.
	if result <= uint64(k) {
		return uint32(result)
	}
	return uint32(k)
}

func randomlyHalveUpItems[C comparable](buf []C, start uint32, length uint32, deterministicOffsetForTest bool) {
	halfLength := length / 2
	offset := rand.Intn(2)
	if deterministicOffsetForTest {
		offset = deterministicOffset()
	}
	j := (start + length) - 1 - uint32(offset)
	for i := (start + length) - 1; i >= (start + halfLength); i-- {
		buf[i] = buf[j]
		j -= 2
	}
}

func randomlyHalveDownItems[C comparable](buf []C, start uint32, length uint32, deterministicOffsetForTest bool) {
	halfLength := length / 2
	offset := rand.Intn(2)
	if deterministicOffsetForTest {
		offset = deterministicOffset()
	}
	j := start + uint32(offset)
	for i := start; i < (start + halfLength); i++ {
		buf[i] = buf[j]
		j += 2
	}
}

func mergeSortedItemsArrays[C comparable](bufA []C, startA uint32, lenA uint32,
	bufB []C, startB uint32, lenB uint32,
	bufC []C, startC uint32, compareFn common.CompareFn[C]) {
	lenC := lenA + lenB
	limA := startA + lenA
	limB := startB + lenB
	limC := startC + lenC

	a := startA
	b := startB

	for c := startC; c < limC; c++ {
		if a == limA {
			bufC[c] = bufB[b]
			b++
		} else if b == limB {
			bufC[c] = bufA[a]
			a++
		} else if compareFn(bufA[a], bufB[b]) {
			bufC[c] = bufA[a]
			a++
		} else {
			bufC[c] = bufB[b]
			b++
		}
	}
}

func populateItemWorkArrays[C comparable](workbuf []C, worklevels []uint32, provisionalNumLevels uint8,
	myCurNumLevels uint8, myCurLevelsArr []uint32, myCurItemsArr []C,
	otherNumLevels uint8, otherLevelsArr []uint32, otherItemsArr []C,
	compareFn common.CompareFn[C]) {

	worklevels[0] = 0
	// Note: the level zero data from "other" was already inserted into "self"
	selfPopZero := currentLevelSizeItems(0, myCurNumLevels, myCurLevelsArr)
	for i := uint32(0); i < selfPopZero; i++ {
		workbuf[worklevels[0]+i] = myCurItemsArr[myCurLevelsArr[0]+i]
	}
	worklevels[1] = worklevels[0] + selfPopZero

	for lvl := uint8(1); lvl < provisionalNumLevels; lvl++ {
		selfPop := currentLevelSizeItems(lvl, myCurNumLevels, myCurLevelsArr)
		otherPop := currentLevelSizeItems(lvl, otherNumLevels, otherLevelsArr)
		worklevels[lvl+1] = worklevels[lvl] + selfPop + otherPop

		if selfPop > 0 && otherPop == 0 {
			for i := uint32(0); i < selfPop; i++ {
				workbuf[worklevels[lvl]+i] = myCurItemsArr[myCurLevelsArr[lvl]+i]
			}
		} else if selfPop == 0 && otherPop > 0 {
			for i := uint32(0); i < otherPop; i++ {
				workbuf[worklevels[lvl]+i] = otherItemsArr[otherLevelsArr[lvl]+i]
			}
		} else if selfPop > 0 && otherPop > 0 {
			mergeSortedItemsArrays(
				myCurItemsArr, myCurLevelsArr[lvl], selfPop,
				otherItemsArr, otherLevelsArr[lvl], otherPop,
				workbuf, worklevels[lvl], compareFn)
		}
	}
}

func generalItemsCompress[C comparable](
	k uint16,
	m uint8,
	numLevelsIn uint8,
	inBuf []C,
	inLevels []uint32,
	outBuf []C,
	outLevels []uint32,
	isLevelZeroSorted bool,
	compareFn common.CompareFn[C],
	deterministicOffsetForTest bool,
) []uint32 {
	numLevels := numLevelsIn
	currentItemCount := inLevels[numLevels] - inLevels[0]        // decreases with each compaction
	targetItemCount := computeTotalItemCapacity(k, m, numLevels) // increases if we add levels
	doneYet := false
	outLevels[0] = 0
	curLevel := -1
	for !doneYet {
		curLevel++ // start out at level 0

		// If we are at the current top level, add an empty level above it for convenience,
		// but do not increment numLevels until later
		if curLevel == (int(numLevels) - 1) {
			inLevels[curLevel+2] = inLevels[curLevel+1]
		}

		rawBeg := inLevels[curLevel]
		rawLim := inLevels[curLevel+1]
		rawPop := rawLim - rawBeg

		if (uint32(currentItemCount) < targetItemCount) || (rawPop < levelCapacity(k, numLevels, uint8(curLevel), m)) {
			for i := uint32(0); i < rawPop; i++ {
				outBuf[outLevels[curLevel]+i] = inBuf[rawBeg+i]
			}
			outLevels[curLevel+1] = outLevels[curLevel] + rawPop
		} else {
			// The sketch is too full AND this level is too full, so we compact it
			// Note: this can add a level and thus change the sketch's capacity

			popAbove := inLevels[curLevel+2] - rawLim
			oddPop := rawPop%2 == 1
			adjBeg := rawBeg
			if oddPop {
				adjBeg++
			}
			adjPop := rawPop
			if oddPop {
				adjPop--
			}
			halfAdjPop := adjPop / 2

			if oddPop {
				outBuf[outLevels[curLevel]] = inBuf[rawBeg]
				outLevels[curLevel+1] = outLevels[curLevel] + 1
			} else {
				outLevels[curLevel+1] = outLevels[curLevel]
			}

			// level zero might not be sorted, so we must sort it if we wish to compact it
			if (curLevel == 0) && !isLevelZeroSorted {
				tmpSlice := inBuf[adjBeg : adjBeg+adjPop]
				slices.SortFunc(tmpSlice, func(a, b C) int {
					if compareFn(a, b) {
						return -1
					}
					return 1
				})
			}

			if popAbove == 0 {
				randomlyHalveUpItems(inBuf, adjBeg, adjPop, deterministicOffsetForTest)
			} else {
				randomlyHalveDownItems(inBuf, adjBeg, adjPop, deterministicOffsetForTest)
				mergeSortedItemsArrays(
					inBuf, adjBeg, halfAdjPop,
					inBuf, rawLim, popAbove,
					inBuf, adjBeg+halfAdjPop, compareFn)
			}

			// track the fact that we just eliminated some data
			currentItemCount -= halfAdjPop

			// Adjust the boundaries of the level above
			inLevels[curLevel+1] = inLevels[curLevel+1] - halfAdjPop

			// Increment numLevels if we just compacted the old top level
			// This creates some more capacity (the size of the new bottom level)
			if curLevel == (int(numLevels) - 1) {
				numLevels++
				targetItemCount += levelCapacity(k, numLevels, 0, m)
			}
		} // end of code for compacting a level

		// determine whether we have processed all levels yet (including any new levels that we created)
		if curLevel == (int(numLevels) - 1) {
			doneYet = true
		}
	} // end of loop over levels

	return []uint32{uint32(numLevels), targetItemCount, currentItemCount}
}

func deterministicOffset() int {
	result := nextOffsetForTest
	nextOffsetForTest = 1 - nextOffsetForTest
	return result
}
