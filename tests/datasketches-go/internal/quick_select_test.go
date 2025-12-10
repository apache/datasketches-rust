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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuickSelect(t *testing.T) {
	testCases := []struct {
		name     string
		arr      []int
		lo       int
		hi       int
		pivot    int
		expected int
	}{
		{
			name:     "regression test PR48: two elements first smaller",
			arr:      []int{50, 100},
			lo:       0,
			hi:       1,
			pivot:    1,
			expected: 100,
		},
		{
			name:     "find median in odd length array",
			arr:      []int{3, 1, 4, 1, 5, 9, 2, 6},
			lo:       0,
			hi:       7,
			pivot:    4,
			expected: 4,
		},
		{
			name:     "find minimum",
			arr:      []int{3, 1, 4, 1, 5, 9, 2, 6},
			lo:       0,
			hi:       7,
			pivot:    0,
			expected: 1,
		},
		{
			name:     "find maximum",
			arr:      []int{3, 1, 4, 1, 5, 9, 2, 6},
			lo:       0,
			hi:       7,
			pivot:    7,
			expected: 9,
		},
		{
			name:     "single element array",
			arr:      []int{42},
			lo:       0,
			hi:       0,
			pivot:    0,
			expected: 42,
		},
		{
			name:     "two element array - first",
			arr:      []int{5, 3},
			lo:       0,
			hi:       1,
			pivot:    0,
			expected: 3,
		},
		{
			name:     "two element array - second",
			arr:      []int{5, 3},
			lo:       0,
			hi:       1,
			pivot:    1,
			expected: 5,
		},
		{
			name:     "already sorted array",
			arr:      []int{1, 2, 3, 4, 5},
			lo:       0,
			hi:       4,
			pivot:    2,
			expected: 3,
		},
		{
			name:     "reverse sorted array",
			arr:      []int{5, 4, 3, 2, 1},
			lo:       0,
			hi:       4,
			pivot:    2,
			expected: 3,
		},
		{
			name:     "array with duplicates",
			arr:      []int{3, 3, 3, 3, 3},
			lo:       0,
			hi:       4,
			pivot:    2,
			expected: 3,
		},
		{
			name:     "partial range - middle elements",
			arr:      []int{9, 8, 7, 6, 5, 4, 3, 2, 1},
			lo:       2,
			hi:       6,
			pivot:    4,
			expected: 5,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Make a copy to avoid modifying the test case
			arrCopy := make([]int, len(tc.arr))
			copy(arrCopy, tc.arr)

			result := QuickSelect(arrCopy, tc.lo, tc.hi, tc.pivot)

			assert.Equal(t, tc.expected, result, "want: %v\ngot: %v", tc.expected, result)
		})
	}
}

func TestQuickSelectFloat64(t *testing.T) {
	arr := []float64{3.14, 1.41, 2.71, 0.57, 1.61}
	result := QuickSelect(arr, 0, 4, 2)
	expected := 1.61

	assert.Equal(t, expected, result, "want: %v\ngot: %v", expected, result)
}

func TestQuickSelectString(t *testing.T) {
	arr := []string{"dog", "cat", "elephant", "ant", "bear"}
	result := QuickSelect(arr, 0, 4, 2)
	expected := "cat"

	assert.Equal(t, expected, result, "want: %v\ngot: %v", expected, result)
}

type testEntry struct {
	hash    uint64
	summary any
}

func TestQuickSelectFunc(t *testing.T) {
	testCases := []struct {
		name     string
		arr      []testEntry
		lo       int
		hi       int
		pivot    int
		expected uint64
	}{
		{
			name:     "two elements first smaller",
			arr:      []testEntry{{hash: 50}, {hash: 100}},
			lo:       0,
			hi:       1,
			pivot:    1,
			expected: 100,
		},
		{
			name:     "find median",
			arr:      []testEntry{{hash: 3}, {hash: 1}, {hash: 4}, {hash: 1}, {hash: 5}, {hash: 9}, {hash: 2}, {hash: 6}},
			lo:       0,
			hi:       7,
			pivot:    4,
			expected: 4,
		},
		{
			name:     "find minimum",
			arr:      []testEntry{{hash: 3}, {hash: 1}, {hash: 4}, {hash: 1}, {hash: 5}, {hash: 9}, {hash: 2}, {hash: 6}},
			lo:       0,
			hi:       7,
			pivot:    0,
			expected: 1,
		},
		{
			name:     "find maximum",
			arr:      []testEntry{{hash: 3}, {hash: 1}, {hash: 4}, {hash: 1}, {hash: 5}, {hash: 9}, {hash: 2}, {hash: 6}},
			lo:       0,
			hi:       7,
			pivot:    7,
			expected: 9,
		},
		{
			name:     "single element",
			arr:      []testEntry{{hash: 42}},
			lo:       0,
			hi:       0,
			pivot:    0,
			expected: 42,
		},
		{
			name:     "two elements descending",
			arr:      []testEntry{{hash: 5}, {hash: 3}},
			lo:       0,
			hi:       1,
			pivot:    0,
			expected: 3,
		},
		{
			name:     "with summary data",
			arr:      []testEntry{{hash: 30, summary: "a"}, {hash: 10, summary: "b"}, {hash: 20, summary: "c"}},
			lo:       0,
			hi:       2,
			pivot:    1,
			expected: 20,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			arrCopy := make([]testEntry, len(tc.arr))
			copy(arrCopy, tc.arr)

			result := QuickSelectFunc(arrCopy, tc.lo, tc.hi, tc.pivot, func(a, b testEntry) int {
				if a.hash < b.hash {
					return -1
				} else if a.hash > b.hash {
					return 1
				}
				return 0
			})

			assert.Equal(t, tc.expected, result.hash, "want: %v\ngot: %v", tc.expected, result.hash)
		})
	}
}
