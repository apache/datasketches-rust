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

import "github.com/apache/datasketches-go/common"

type ItemsSketchIterator[C comparable] struct {
	quantiles     []C
	levelsArr     []uint32
	numLevels     int
	index         uint32
	level         int
	weight        int64
	isInitialized bool
	itemsSketchOp common.ItemSketchSerde[C]
}

func newItemsSketchIterator[C comparable](
	quantiles []C,
	levelsArr []uint32,
	numLevels int,
) *ItemsSketchIterator[C] {
	return &ItemsSketchIterator[C]{
		quantiles: quantiles,
		levelsArr: levelsArr,
		numLevels: numLevels,
	}
}

func (s *ItemsSketchIterator[C]) Next() bool {

	if !s.isInitialized {
		s.level = 0
		s.index = s.levelsArr[s.level]
		s.weight = 1
		s.isInitialized = true
	} else {
		s.index++
	}
	if s.index < s.levelsArr[s.level+1] {
		return true
	}
	// go to next non-empty level
	for {
		s.level++
		if s.level == s.numLevels {
			return false
		}
		s.weight *= 2
		if s.levelsArr[s.level] != s.levelsArr[s.level+1] {
			break
		}
	}
	s.index = s.levelsArr[s.level]
	return true
}

// GetQuantile return the generic quantile at the current index.
//
// Don't call this before calling next() for the first time
// or after getting false from next().
func (s *ItemsSketchIterator[C]) GetQuantile() C {
	return s.quantiles[s.index]
}

func (s *ItemsSketchIterator[C]) GetWeight() int64 {
	return s.weight
}
