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

package kll

import "errors"

type ItemsSketchPartitionBoundaries[C comparable] struct {
	totalN     uint64    //totalN of source sketch
	boundaries []C       //quantiles at the boundaries
	natRanks   []int64   //natural ranks at the boundaries
	normRanks  []float64 //normalized ranks at the boundaries
	maxItem    C         //of the source sketch
	minItem    C         //of the source sketch
	inclusive  bool      //of the source sketch query to getPartitionBoundaries.
	//computed
	numDeltaItems []int64 //num of items in each part
	numPartitions int     //num of partitions
}

func newItemsSketchPartitionBoundaries[C comparable](totalN uint64, boundaries []C, natRanks []int64, normRanks []float64, maxItem C, minItem C, inclusive bool) (*ItemsSketchPartitionBoundaries[C], error) {
	if len(boundaries) < 2 {
		return nil, errors.New("boundaries must have at least 2 items")
	}
	numDeltaItems := make([]int64, len(boundaries))
	numDeltaItems[0] = 0
	for i := 1; i < len(boundaries); i++ {
		addOne := 0
		if (i == 1 && inclusive) || (i == len(boundaries)-1 && !inclusive) {
			addOne = 1
		}
		numDeltaItems[i] = natRanks[i] - natRanks[i-1] + int64(addOne)
	}
	return &ItemsSketchPartitionBoundaries[C]{
		totalN:        totalN,
		boundaries:    boundaries,
		natRanks:      natRanks,
		normRanks:     normRanks,
		maxItem:       maxItem,
		minItem:       minItem,
		inclusive:     inclusive,
		numDeltaItems: numDeltaItems,
		numPartitions: len(boundaries) - 1,
	}, nil
}

func (b *ItemsSketchPartitionBoundaries[C]) GetBoundaries() []C {
	return b.boundaries
}
