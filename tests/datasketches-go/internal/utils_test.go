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

func TestInvPow2(t *testing.T) {
	_, err := InvPow2(0)
	assert.NoError(t, err)
}

func TestFloorPowerOf2(t *testing.T) {
	assert.Equal(t, FloorPowerOf2(-1), int64(1))
	assert.Equal(t, FloorPowerOf2(0), int64(1))
	assert.Equal(t, FloorPowerOf2(1), int64(1))
	assert.Equal(t, FloorPowerOf2(2), int64(2))
	assert.Equal(t, FloorPowerOf2(3), int64(2))
	assert.Equal(t, FloorPowerOf2(4), int64(4))

	assert.Equal(t, FloorPowerOf2((1<<63)-1), int64(1<<62))
	assert.Equal(t, FloorPowerOf2(1<<62), int64(1<<62))
	assert.Equal(t, FloorPowerOf2((1<<62)+1), int64(1<<62))
}

func TestLog2Floor(t *testing.T) {
	testCases := []struct {
		name     string
		input    uint32
		expected uint8
	}{
		{name: "n=0", input: 0, expected: 0},
		{name: "n=1", input: 1, expected: 0},
		{name: "n=2", input: 2, expected: 1},
		{name: "n=3", input: 3, expected: 1},
		{name: "n=4", input: 4, expected: 2},
		{name: "n=5", input: 5, expected: 2},
		{name: "n=6", input: 6, expected: 2},
		{name: "n=7", input: 7, expected: 2},
		{name: "n=8", input: 8, expected: 3},
		{name: "n=15", input: 15, expected: 3},
		{name: "n=16", input: 16, expected: 4},
		{name: "n=31", input: 31, expected: 4},
		{name: "n=32", input: 32, expected: 5},
		{name: "n=63", input: 63, expected: 5},
		{name: "n=64", input: 64, expected: 6},
		{name: "n=127", input: 127, expected: 6},
		{name: "n=128", input: 128, expected: 7},
		{name: "n=255", input: 255, expected: 7},
		{name: "n=256", input: 256, expected: 8},
		{name: "n=1000", input: 1000, expected: 9},
		{name: "n=1024", input: 1024, expected: 10},
		{name: "n=1025", input: 1025, expected: 10},
		{name: "n=4096", input: 4096, expected: 12},
		{name: "n=65535", input: 65535, expected: 15},
		{name: "n=65536", input: 65536, expected: 16},
		{name: "n=1000000", input: 1000000, expected: 19},
		{name: "n=1048576 (2^20)", input: 1048576, expected: 20},
		{name: "n=16777216 (2^24)", input: 16777216, expected: 24},
		{name: "n=max uint32", input: 0xFFFFFFFF, expected: 31},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := Log2Floor(tc.input)
			assert.Equal(t, tc.expected, result)

			if tc.input > 0 {
				assert.LessOrEqual(t, uint32(1)<<result, tc.input)
				if result < 31 {
					assert.Greater(t, uint32(1)<<(result+1), tc.input)
				}
			}
		})
	}
}

func TestLgSizeFromCount(t *testing.T) {
	testCases := []struct {
		name       string
		n          uint32
		loadFactor float64
		expected   uint8
	}{
		{name: "n=0, load=0.75", n: 0, loadFactor: 0.75, expected: 1},
		{name: "n=1, load=0.75", n: 1, loadFactor: 0.75, expected: 1},
		{name: "n=6, load=0.75 (at threshold)", n: 6, loadFactor: 0.75, expected: 3},
		{name: "n=7, load=0.75 (over threshold)", n: 7, loadFactor: 0.75, expected: 4},
		{name: "n=13, load=0.75", n: 13, loadFactor: 0.75, expected: 5},
		{name: "n=4, load=0.5", n: 4, loadFactor: 0.5, expected: 3},
		{name: "n=5, load=0.5", n: 5, loadFactor: 0.5, expected: 4},
		{name: "n=8, load=1.0", n: 8, loadFactor: 1.0, expected: 4},
		{name: "n=1000, load=0.75", n: 1000, loadFactor: 0.75, expected: 11},
		{name: "n=1000000, load=0.75", n: 1000000, loadFactor: 0.75, expected: 21},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := LgSizeFromCount(tc.n, tc.loadFactor)
			assert.Equal(t, tc.expected, result)

			size := uint32(1) << result
			capacity := uint32(float64(size) * tc.loadFactor)
			assert.GreaterOrEqual(t, capacity, tc.n)
		})
	}
}
