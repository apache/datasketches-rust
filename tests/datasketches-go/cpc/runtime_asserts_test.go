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
	"testing"
)

func TestRuntimeAsserts_PositiveCases(t *testing.T) {
	rtAssertFalse(false)

	shortArr1 := []int16{1, 2, 3}
	shortArr2 := append([]int16(nil), shortArr1...) // Clone slice
	rtAssertEqualsShortSlice(shortArr1, shortArr2)
	shortArr1, shortArr2 = nil, nil
	rtAssertEqualsShortSlice(shortArr1, shortArr2)

	floatArr1 := []float32{1, 2, 3}
	floatArr2 := append([]float32(nil), floatArr1...)
	rtAssertEqualsFloatSlice(floatArr1, floatArr2, 0)
	floatArr1, floatArr2 = nil, nil
	rtAssertEqualsFloatSlice(floatArr1, floatArr2, 0)

	doubleArr1 := []float64{1, 2, 3}
	doubleArr2 := append([]float64(nil), doubleArr1...)
	rtAssertEqualsDoubleSlice(doubleArr1, doubleArr2, 0)
	doubleArr1, doubleArr2 = nil, nil
	rtAssertEqualsDoubleSlice(doubleArr1, doubleArr2, 0)
}

func TestRuntimeAsserts_SimpleExceptions(t *testing.T) {
	tests := []struct {
		name string
		fn   func()
	}{
		{"rtAssert(false)", func() { rtAssert(false) }},
		{"rtAssertFalse(true)", func() { rtAssertFalse(true) }},
		{"rtAssertEquals(1, 2)", func() { rtAssertEqualsUint64(1, 2) }},
		{"rtAssertEquals(1.0, 2.0, 0)", func() { rtAssertEqualsFloat64(1.0, 2.0, 0) }},
		{"rtAssertEquals(true, false)", func() { rtAssertEqualsBool(true, false) }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("Expected panic but none occurred for: %s", test.name)
				}
			}()
			test.fn()
		})
	}
}

func TestRuntimeAsserts_ByteArray(t *testing.T) {
	tests := []struct {
		arr1, arr2  []byte
		shouldPanic bool
	}{
		{[]byte{1, 2}, []byte{1}, true},
		{[]byte{1, 2}, []byte{1, 3}, true},
		{[]byte{1, 2}, nil, true},
		{nil, []byte{1, 2}, true},
		{nil, nil, false},
	}

	runArrayTests(t, tests, rtAssertEqualsBytes)
}

func TestRuntimeAsserts_ShortArray(t *testing.T) {
	tests := []struct {
		arr1, arr2  []int16
		shouldPanic bool
	}{
		{[]int16{1, 2}, []int16{1}, true},
		{[]int16{1, 2}, []int16{1, 3}, true},
		{[]int16{1, 2}, nil, true},
		{nil, []int16{1, 2}, true},
		{nil, nil, false},
	}

	runArrayTests(t, tests, rtAssertEqualsShortSlice)
}

func TestRuntimeAsserts_IntArray(t *testing.T) {
	tests := []struct {
		arr1, arr2  []int
		shouldPanic bool
	}{
		{[]int{1, 2}, []int{1}, true},
		{[]int{1, 2}, []int{1, 3}, true},
		{[]int{1, 2}, nil, true},
		{nil, []int{1, 2}, true},
		{nil, nil, false},
	}

	runArrayTests(t, tests, rtAssertEqualsIntSlice)
}

func TestRuntimeAsserts_LongArray(t *testing.T) {
	tests := []struct {
		arr1, arr2  []int64
		shouldPanic bool
	}{
		{[]int64{1, 2}, []int64{1}, true},
		{[]int64{1, 2}, []int64{1, 3}, true},
		{[]int64{1, 2}, nil, true},
		{nil, []int64{1, 2}, true},
		{nil, nil, false},
	}

	runArrayTests(t, tests, rtAssertEqualsLongSlice)
}

func TestRuntimeAsserts_FloatArray(t *testing.T) {
	tests := []struct {
		arr1, arr2  []float32
		shouldPanic bool
	}{
		{[]float32{1, 2}, []float32{1}, true},
		{[]float32{1, 2}, []float32{1, 3}, true},
		{[]float32{1, 2}, nil, true},
		{nil, []float32{1, 2}, true},
		{nil, nil, false},
	}

	runArrayTestsWithEpsilon32(t, tests, rtAssertEqualsFloatSlice, 0)
}

func TestRuntimeAsserts_DoubleArray(t *testing.T) {
	tests := []struct {
		arr1, arr2  []float64
		shouldPanic bool
	}{
		{[]float64{1, 2}, []float64{1}, true},
		{[]float64{1, 2}, []float64{1, 3}, true},
		{[]float64{1, 2}, nil, true},
		{nil, []float64{1, 2}, true},
		{nil, nil, false},
	}

	runArrayTestsWithEpsilon64(t, tests, rtAssertEqualsDoubleSlice, 0)
}

// runArrayTestsWithEpsilon32 handles float32 assertions.
func runArrayTestsWithEpsilon32(t *testing.T, tests []struct {
	arr1, arr2  []float32
	shouldPanic bool
}, assertFunc func([]float32, []float32, float32), eps float32) {
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil && test.shouldPanic {
					t.Fatalf("Expected panic but none occurred")
				} else if r != nil && !test.shouldPanic {
					t.Fatalf("Unexpected panic: %v", r)
				}
			}()
			assertFunc(test.arr1, test.arr2, eps)
		})
	}
}

// runArrayTestsWithEpsilon64 handles float64 assertions.
func runArrayTestsWithEpsilon64(t *testing.T, tests []struct {
	arr1, arr2  []float64
	shouldPanic bool
}, assertFunc func([]float64, []float64, float64), eps float64) {
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil && test.shouldPanic {
					t.Fatalf("Expected panic but none occurred")
				} else if r != nil && !test.shouldPanic {
					t.Fatalf("Unexpected panic: %v", r)
				}
			}()
			assertFunc(test.arr1, test.arr2, eps)
		})
	}
}

// runArrayTests helps test different array assertion functions.
func runArrayTests[T any](t *testing.T, tests []struct {
	arr1, arr2  []T
	shouldPanic bool
}, assertFunc func([]T, []T)) {
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil && test.shouldPanic {
					t.Fatalf("Expected panic but none occurred")
				} else if r != nil && !test.shouldPanic {
					t.Fatalf("Unexpected panic: %v", r)
				}
			}()
			assertFunc(test.arr1, test.arr2)
		})
	}
}
