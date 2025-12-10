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

import (
	"github.com/apache/datasketches-go/common"
)

type Inequality int64

const (
	InequalityLT Inequality = iota
	InequalityLE
	InequalityGE
	InequalityGT
)

func FindWithInequality[C comparable](arr []C, low int, high int, v C, crit Inequality, comparator common.CompareFn[C]) int {
	if len(arr) == 0 {
		return -1
	}
	lo := low
	hi := high
	for lo <= hi {
		if hi-lo <= 1 {
			return resolve(arr, lo, hi, v, crit, comparator)
		}
		mid := lo + (hi-lo)/2
		ret := compare(arr, mid, mid+1, v, crit, comparator)
		if ret == -1 {
			hi = mid
		} else if ret == 1 {
			lo = mid + 1
		} else {
			return getIndex(arr, mid, mid+1, v, crit, comparator)
		}
	}
	return -1
}

func resolve[C comparable](arr []C, lo int, hi int, v C, crit Inequality, compareFn common.CompareFn[C]) int {
	result := 0
	switch crit {
	case InequalityLT:
		if lo == hi {
			if compareFn(v, arr[hi]) == false && v != arr[hi] {
				result = lo
			} else {
				result = -1
			}
		} else {
			if compareFn(v, arr[hi]) == false && v != arr[hi] {
				result = hi
			} else if compareFn(v, arr[lo]) == false && v != arr[lo] {
				result = lo
			} else {
				result = -1
			}
		}
	case InequalityLE:
		if lo == hi {
			if compareFn(v, arr[lo]) == false {
				result = lo
			} else {
				result = -1
			}
		} else {
			if compareFn(v, arr[hi]) == false {
				result = hi
			} else if compareFn(v, arr[lo]) == false {
				result = lo
			} else {
				result = -1
			}
		}

	case InequalityGE:
		if lo == hi {
			if compareFn(v, arr[lo]) || v == arr[lo] {
				result = lo
			} else {
				result = -1
			}
		} else {
			if compareFn(v, arr[lo]) || v == arr[lo] {
				result = lo
			} else if compareFn(v, arr[hi]) || v == arr[hi] {
				result = hi
			} else {
				result = -1
			}
		}
	case InequalityGT:
		if lo == hi {
			if compareFn(v, arr[lo]) {
				result = lo
			} else {
				result = -1
			}
		} else {
			if compareFn(v, arr[lo]) {
				result = lo
			} else if compareFn(v, arr[hi]) {
				result = hi
			} else {
				result = -1
			}
		}
	default:
		panic("invalid inequality")
	}

	return result
}

func compare[C comparable](arr []C, a int, b int, v C, crit Inequality, compareFn common.CompareFn[C]) int {
	result := 0
	switch crit {
	case InequalityLT, InequalityGE:
		if compareFn(v, arr[a]) || arr[a] == v {
			result = -1
		} else if compareFn(arr[b], v) {
			result = 1
		} else {
			result = 0
		}
	case InequalityLE, InequalityGT:
		if compareFn(v, arr[a]) {
			result = -1
		} else if compareFn(arr[b], v) || arr[b] == v {
			result = 1
		} else {
			result = 0
		}
	default:
		panic("invalid inequality")
	}
	return result
}

func getIndex[C comparable](arr []C, a int, b int, v C, crit Inequality, compareFn common.CompareFn[C]) int {
	result := 0
	switch crit {
	case InequalityLT, InequalityLE:
		result = a
	case InequalityGE, InequalityGT:
		result = b
	default:
		panic("invalid inequality")
	}
	return result
}
