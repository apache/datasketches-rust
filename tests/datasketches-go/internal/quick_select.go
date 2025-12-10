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

package internal

import "cmp"

func QuickSelect[T cmp.Ordered](arr []T, lo int, hi int, pivot int) T {
	for hi > lo {
		j := partition(arr, lo, hi)
		if j == pivot {
			return arr[pivot]
		}
		if j > pivot {
			hi = j - 1
		} else {
			lo = j + 1
		}
	}
	return arr[pivot]
}

func partition[T cmp.Ordered](arr []T, lo int, hi int) int {
	i := lo
	j := hi + 1
	v := arr[lo]
	for {
		for arr[i+1] < v {
			i++
			if i == hi {
				break
			}
		}
		i++
		for v < arr[j-1] {
			j--
			if j == lo {
				break
			}
		}
		j--
		if i >= j {
			break
		}
		arr[i], arr[j] = arr[j], arr[i]
	}
	arr[lo], arr[j] = arr[j], arr[lo]
	return j
}

// QuickSelectFunc finds the k-th smallest element in a slice using the Quickselect algorithm with a custom comparator.
// The slice is partially partitioned and may not maintain full order.
// It modifies the input slice in-place.
// T is a generic type, and the comparison logic is provided by the `compare` function.
// The `lo` and `hi` parameters define the range in the slice to consider for the selection.
func QuickSelectFunc[T any](arr []T, lo int, hi int, pivot int, compare func(a, b T) int) T {
	for hi > lo {
		j := partitionFunc(arr, lo, hi, compare)
		if j == pivot {
			return arr[pivot]
		}
		if j > pivot {
			hi = j - 1
		} else {
			lo = j + 1
		}
	}
	return arr[pivot]
}

func partitionFunc[T any](arr []T, lo int, hi int, compare func(a, b T) int) int {
	i := lo
	j := hi + 1
	v := arr[lo]
	for {
		for compare(arr[i+1], v) < 0 {
			i++
			if i == hi {
				break
			}
		}
		i++
		for compare(v, arr[j-1]) < 0 {
			j--
			if j == lo {
				break
			}
		}
		j--
		if i >= j {
			break
		}
		arr[i], arr[j] = arr[j], arr[i]
	}
	arr[lo], arr[j] = arr[j], arr[lo]
	return j
}
