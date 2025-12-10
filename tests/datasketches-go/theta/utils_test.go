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

package theta

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckSerialVersionEqual(t *testing.T) {
	err := CheckSerialVersionEqual(3, 3)
	assert.NoError(t, err)

	err = CheckSerialVersionEqual(3, 4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "serial version")
}

func TestCheckSketchFamilyEqual(t *testing.T) {
	err := CheckSketchFamilyEqual(1, 1)
	assert.NoError(t, err)

	err = CheckSketchFamilyEqual(1, 2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sketch family")
}

func TestCheckSketchTypeEqual(t *testing.T) {
	err := CheckSketchTypeEqual(3, 3)
	assert.NoError(t, err)

	err = CheckSketchTypeEqual(3, 2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sketch type")
}

func TestCheckSeedHashEqual(t *testing.T) {
	err := CheckSeedHashEqual(0x1234, 0x1234)
	assert.NoError(t, err)

	err = CheckSeedHashEqual(0x1234, 0x5678)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "seed hash")
}

func TestStartingThetaFromP(t *testing.T) {
	testCases := []struct {
		name     string
		p        float32
		expected uint64
	}{
		{
			name:     "p equals 1.0",
			p:        1.0,
			expected: MaxTheta,
		},
		{
			name:     "p less than 1.0",
			p:        0.5,
			expected: uint64(float64(MaxTheta) * 0.5),
		},
		{
			name:     "p slightly greater than 1.0",
			p:        1.01,
			expected: MaxTheta,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := startingThetaFromP(tc.p)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestStartingSubMultiple(t *testing.T) {
	testCases := []struct {
		name     string
		lgTgt    uint8
		lgMin    uint8
		lgRf     uint8
		expected uint8
	}{
		{
			name:     "lgTgt less than lgMin",
			lgTgt:    3,
			lgMin:    5,
			lgRf:     2,
			expected: 5,
		},
		{
			name:     "lgTgt equals lgMin",
			lgTgt:    5,
			lgMin:    5,
			lgRf:     2,
			expected: 5,
		},
		{
			name:     "lgRf is zero",
			lgTgt:    10,
			lgMin:    5,
			lgRf:     0,
			expected: 10,
		},
		{
			name:     "lgTgt - lgMin divisible by lgRf",
			lgTgt:    11,
			lgMin:    5,
			lgRf:     3,
			expected: 5,
		},
		{
			name:     "lgTgt - lgMin not divisible by lgRf",
			lgTgt:    12,
			lgMin:    5,
			lgRf:     3,
			expected: 6,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := startingSubMultiple(tc.lgTgt, tc.lgMin, tc.lgRf)
			assert.Equal(t, tc.expected, result)
		})
	}
}
