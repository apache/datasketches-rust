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

package cpc

import (
	"fmt"
	"math"
)

// rtAssert panics if b is false.
func rtAssert(b bool) {
	if !b {
		panic("False, expected True.")
	}
}

// rtAssertFalse panics if b is true.
func rtAssertFalse(b bool) {
	if b {
		panic("True, expected False.")
	}
}

// rtAssertEqualsInt panics if a != b.
func rtAssertEqualsInt(a, b int) {
	if a != b {
		panic(fmt.Sprintf("%d != %d", a, b))
	}
}

// rtAssertEqualsUint64 panics if a != b.
func rtAssertEqualsUint64(a, b uint64) {
	if a != b {
		panic(fmt.Sprintf("%d != %d", a, b))
	}
}

// rtAssertEqualsFloat64 panics if the absolute difference between a and b is greater than eps.
func rtAssertEqualsFloat64(a, b, eps float64) {
	if math.Abs(a-b) > eps {
		panic(fmt.Sprintf("abs(%f - %f) > %f", a, b, eps))
	}
}

// rtAssertEqualsBool panics if a != b.
func rtAssertEqualsBool(a, b bool) {
	if a != b {
		panic(fmt.Sprintf("%v != %v", a, b))
	}
}

// rtAssertEqualsBytes panics if two byte slices are not equal.
func rtAssertEqualsBytes(a, b []byte) {
	if a == nil && b == nil {
		return
	}
	if a != nil && b != nil {
		if len(a) != len(b) {
			panic(fmt.Sprintf("Array lengths not equal: %d, %d", len(a), len(b)))
		}
		for i := 0; i < len(a); i++ {
			if a[i] != b[i] {
				panic(fmt.Sprintf("%d != %d at index %d", a[i], b[i], i))
			}
		}
		return
	}
	if a == nil {
		panic("Array a is nil")
	}
	panic("Array b is nil")
}

// rtAssertEqualsIntSlice checks if two int slices are equal.
func rtAssertEqualsIntSlice(a, b []int) {
	if a == nil && b == nil {
		return
	}
	if a != nil && b != nil {
		if len(a) != len(b) {
			panic(fmt.Sprintf("Array lengths not equal: %d, %d", len(a), len(b)))
		}
		for i := 0; i < len(a); i++ {
			if a[i] != b[i] {
				panic(fmt.Sprintf("%d != %d at index %d", a[i], b[i], i))
			}
		}
		return
	}
	panic("One of the arrays is nil")
}

// rtAssertEqualsShortSlice checks if two short (int16) slices are equal.
func rtAssertEqualsShortSlice(a, b []int16) {
	if a == nil && b == nil {
		return
	}
	if a != nil && b != nil {
		if len(a) != len(b) {
			panic(fmt.Sprintf("Array lengths not equal: %d, %d", len(a), len(b)))
		}
		for i := 0; i < len(a); i++ {
			if a[i] != b[i] {
				panic(fmt.Sprintf("%d != %d at index %d", a[i], b[i], i))
			}
		}
		return
	}
	panic("One of the arrays is nil")
}

// rtAssertEqualsLongSlice checks if two long (int64) slices are equal.
func rtAssertEqualsLongSlice(a, b []int64) {
	if a == nil && b == nil {
		return
	}
	if a != nil && b != nil {
		if len(a) != len(b) {
			panic(fmt.Sprintf("Array lengths not equal: %d, %d", len(a), len(b)))
		}
		for i := 0; i < len(a); i++ {
			if a[i] != b[i] {
				panic(fmt.Sprintf("%d != %d at index %d", a[i], b[i], i))
			}
		}
		return
	}
	panic("One of the arrays is nil")
}

// rtAssertEqualsFloatSlice checks if two float32 slices are equal within a given epsilon.
func rtAssertEqualsFloatSlice(a, b []float32, eps float32) {
	if a == nil && b == nil {
		return
	}
	if a != nil && b != nil {
		if len(a) != len(b) {
			panic(fmt.Sprintf("Array lengths not equal: %d, %d", len(a), len(b)))
		}
		for i := 0; i < len(a); i++ {
			if math.Abs(float64(a[i]-b[i])) > float64(eps) {
				panic(fmt.Sprintf("abs(%f - %f) > %f at index %d", a[i], b[i], eps, i))
			}
		}
		return
	}
	panic("One of the arrays is nil")
}

// rtAssertEqualsDoubleSlice checks if two float64 slices are equal within a given epsilon.
func rtAssertEqualsDoubleSlice(a, b []float64, eps float64) {
	if a == nil && b == nil {
		return
	}
	if a != nil && b != nil {
		if len(a) != len(b) {
			panic(fmt.Sprintf("Array lengths not equal: %d, %d", len(a), len(b)))
		}
		for i := 0; i < len(a); i++ {
			if math.Abs(a[i]-b[i]) > eps {
				panic(fmt.Sprintf("abs(%f - %f) > %f at index %d", a[i], b[i], eps, i))
			}
		}
		return
	}
	panic("One of the arrays is nil")
}
