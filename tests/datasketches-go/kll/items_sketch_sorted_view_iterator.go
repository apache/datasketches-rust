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

type ItemsSketchSortedViewIterator[C comparable] struct {
	quantiles  []C
	cumWeights []int64
	totalN     int64
	index      int
}

func newItemsSketchSortedViewIterator[C comparable](quantiles []C, cumWeights []int64) *ItemsSketchSortedViewIterator[C] {
	totalN := int64(0)
	if len(cumWeights) > 0 {
		totalN = cumWeights[len(cumWeights)-1]
	}
	return &ItemsSketchSortedViewIterator[C]{
		quantiles:  quantiles,
		cumWeights: cumWeights,
		totalN:     totalN,
		index:      -1,
	}
}

func (i *ItemsSketchSortedViewIterator[C]) Next() bool {
	i.index++
	return i.index < len(i.cumWeights)
}

// GetQuantile returns the quantile at the current index
//
// Don't call this before calling next() for the first time
// or after getting false from next().
func (i *ItemsSketchSortedViewIterator[C]) GetQuantile() C {
	return i.quantiles[i.index]
}

func (i *ItemsSketchSortedViewIterator[C]) GetWeight() int64 {
	if i.index == 0 {
		return i.cumWeights[0]
	}
	return i.cumWeights[i.index] - i.cumWeights[i.index-1]
}

func (i *ItemsSketchSortedViewIterator[C]) GetNaturalRank(inclusive bool) int64 {
	if inclusive {
		return i.cumWeights[i.index]
	}
	if i.index == 0 {
		return 0
	}
	return i.cumWeights[i.index-1]
}

func (i *ItemsSketchSortedViewIterator[C]) GetNormalizedRank(inclusive bool) float64 {
	return float64(i.GetNaturalRank(inclusive)) / float64(i.totalN)
}
