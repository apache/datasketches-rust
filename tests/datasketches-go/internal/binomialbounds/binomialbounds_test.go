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

package binomialbounds

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLowerBound(t *testing.T) {
	testCases := []struct {
		name         string
		numSamples   uint64
		theta        float64
		numStdDevs   uint
		wantErrorMsg string
		validate     func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint)
	}{
		{
			name:       "numSamples == 0",
			numSamples: 0,
			theta:      0.5,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.Zero(t, result)
			},
		},
		{
			name:       "theta == 1.0",
			numSamples: 100,
			theta:      1.0,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.Equal(t, result, float64(numSamples))
			},
		},
		{
			name:       "numSamples == 1",
			numSamples: 1,
			theta:      0.5,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "numSamples == 1, stddev=2",
			numSamples: 1,
			theta:      0.5,
			numStdDevs: 2,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "numSamples == 1, stddev=3",
			numSamples: 1,
			theta:      0.5,
			numStdDevs: 3,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "numSamples > 120",
			numSamples: 121,
			theta:      0.5,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "numSamples > 120, stddev=2",
			numSamples: 200,
			theta:      0.5,
			numStdDevs: 2,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "numSamples > 120, stddev=3",
			numSamples: 500,
			theta:      0.5,
			numStdDevs: 3,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "2 <= numSamples <= 120 AND theta > (1-1e-5)",
			numSamples: 50,
			theta:      1.0 - 1e-6,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.Greater(t, float64(numSamples)*0.01, math.Abs(result-float64(numSamples)))
			},
		},
		{
			name:       "2 <= numSamples <= 120 AND theta > (1-1e-5), stddev=2",
			numSamples: 50,
			theta:      1.0 - 1e-6,
			numStdDevs: 2,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.Greater(t, float64(numSamples)*0.01, math.Abs(result-float64(numSamples)))
			},
		},
		{
			name:       "2 <= numSamples <= 120 AND theta > (1-1e-5), stddev=3",
			numSamples: 50,
			theta:      1.0 - 1e-6,
			numStdDevs: 3,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.Greater(t, float64(numSamples)*0.01, math.Abs(result-float64(numSamples)))
			},
		},
		{
			name:       "2 <= numSamples <= 120 AND theta < numSamples/360",
			numSamples: 100,
			theta:      0.001,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "2 <= numSamples <= 120 AND theta < numSamples/360, stddev=2",
			numSamples: 100,
			theta:      0.001,
			numStdDevs: 2,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "2 <= numSamples <= 120 AND theta < numSamples/360, stddev=3",
			numSamples: 100,
			theta:      0.001,
			numStdDevs: 3,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "2 <= numSamples <= 120 AND middle range theta (exact calculation)",
			numSamples: 10,
			theta:      0.5,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "2 <= numSamples <= 120 AND middle range theta, stddev=2",
			numSamples: 10,
			theta:      0.5,
			numStdDevs: 2,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "2 <= numSamples <= 120 AND middle range theta, stddev=3",
			numSamples: 10,
			theta:      0.5,
			numStdDevs: 3,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "theta=0",
			numSamples: 10,
			theta:      0.0,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				// When theta=0, result will be NaN due to division by zero
				assert.True(t, math.IsNaN(result) || math.IsInf(result, 1))
			},
		},
		{
			name:       "theta very close to 0",
			numSamples: 10,
			theta:      1e-10,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "numSamples=2 boundary",
			numSamples: 2,
			theta:      0.5,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "numSamples=120 boundary",
			numSamples: 120,
			theta:      0.5,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "estimate clamping case",
			numSamples: 10,
			theta:      0.9,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				estimate := float64(numSamples) / theta
				assert.LessOrEqual(t, result, estimate)
			},
		},
		{
			name:         "invalid theta < 0",
			numSamples:   100,
			theta:        -0.1,
			numStdDevs:   1,
			wantErrorMsg: "theta must be in [0, 1]",
		},
		{
			name:         "invalid theta > 1",
			numSamples:   100,
			theta:        1.1,
			numStdDevs:   1,
			wantErrorMsg: "theta must be in [0, 1]",
		},
		{
			name:         "invalid stddev = 0",
			numSamples:   100,
			theta:        0.5,
			numStdDevs:   0,
			wantErrorMsg: "numStdDevs must be 1, 2 or 3",
		},
		{
			name:         "invalid stddev = 4",
			numSamples:   100,
			theta:        0.5,
			numStdDevs:   4,
			wantErrorMsg: "numStdDevs must be 1, 2 or 3",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := LowerBound(tc.numSamples, tc.theta, tc.numStdDevs)
			if tc.wantErrorMsg != "" {
				assert.ErrorContains(t, err, tc.wantErrorMsg)
				return
			}
			tc.validate(t, result, tc.numSamples, tc.theta, tc.numStdDevs)
		})
	}
}

func TestUpperBound(t *testing.T) {
	testCases := []struct {
		name         string
		numSamples   uint64
		theta        float64
		numStdDevs   uint
		wantErrorMsg string
		validate     func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint)
	}{
		{
			name:       "theta == 1.0",
			numSamples: 100,
			theta:      1.0,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.Equal(t, float64(numSamples), result)
			},
		},
		{
			name:       "numSamples == 0",
			numSamples: 0,
			theta:      0.5,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.Greater(t, result, 0.0)
			},
		},
		{
			name:       "numSamples == 0, stddev=2",
			numSamples: 0,
			theta:      0.5,
			numStdDevs: 2,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.Greater(t, result, 0.0)
			},
		},
		{
			name:       "numSamples == 0, stddev=3",
			numSamples: 0,
			theta:      0.5,
			numStdDevs: 3,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.Greater(t, result, 0.0)
			},
		},
		{
			name:       "numSamples > 120",
			numSamples: 121,
			theta:      0.5,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "numSamples > 120, stddev=2",
			numSamples: 200,
			theta:      0.5,
			numStdDevs: 2,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "numSamples > 120, stddev=3",
			numSamples: 500,
			theta:      0.5,
			numStdDevs: 3,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "1 <= numSamples <= 120 AND theta > (1-1e-5)",
			numSamples: 50,
			theta:      1.0 - 1e-6,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.Equal(t, float64(numSamples+1), result)
			},
		},
		{
			name:       "1 <= numSamples <= 120 AND theta > (1-1e-5), stddev=2",
			numSamples: 50,
			theta:      1.0 - 1e-6,
			numStdDevs: 2,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.Equal(t, float64(numSamples+1), result)
			},
		},
		{
			name:       "1 <= numSamples <= 120 AND theta > (1-1e-5), stddev=3",
			numSamples: 50,
			theta:      1.0 - 1e-6,
			numStdDevs: 3,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.Equal(t, float64(numSamples+1), result)
			},
		},
		{
			name:       "1 <= numSamples <= 120 AND theta < numSamples/360",
			numSamples: 100,
			theta:      0.001,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "1 <= numSamples <= 120 AND theta < numSamples/360, stddev=2",
			numSamples: 100,
			theta:      0.001,
			numStdDevs: 2,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "1 <= numSamples <= 120 AND theta < numSamples/360, stddev=3",
			numSamples: 100,
			theta:      0.001,
			numStdDevs: 3,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "1 <= numSamples <= 120 AND middle range theta (exact calculation)",
			numSamples: 10,
			theta:      0.5,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "1 <= numSamples <= 120 AND middle range theta, stddev=2",
			numSamples: 10,
			theta:      0.5,
			numStdDevs: 2,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "1 <= numSamples <= 120 AND middle range theta, stddev=3",
			numSamples: 10,
			theta:      0.5,
			numStdDevs: 3,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "theta=0",
			numSamples: 10,
			theta:      0.0,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				// When theta=0, result will be NaN due to division by zero
				assert.True(t, math.IsNaN(result) || math.IsInf(result, 1))
			},
		},
		{
			name:       "theta very close to 0",
			numSamples: 10,
			theta:      1e-10,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "numSamples=1 boundary",
			numSamples: 1,
			theta:      0.5,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "numSamples=120 boundary",
			numSamples: 120,
			theta:      0.5,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				assert.GreaterOrEqual(t, result, 0.0)
			},
		},
		{
			name:       "estimate clamping case",
			numSamples: 10,
			theta:      0.9,
			numStdDevs: 1,
			validate: func(t *testing.T, result float64, numSamples uint64, theta float64, numStdDevs uint) {
				estimate := float64(numSamples) / theta
				assert.GreaterOrEqual(t, result, estimate)
			},
		},
		{
			name:         "invalid theta < 0",
			numSamples:   100,
			theta:        -0.1,
			numStdDevs:   1,
			wantErrorMsg: "theta must be in [0, 1]",
		},
		{
			name:         "invalid theta > 1",
			numSamples:   100,
			theta:        1.1,
			numStdDevs:   1,
			wantErrorMsg: "theta must be in [0, 1]",
		},
		{
			name:         "invalid stddev = 0",
			numSamples:   100,
			theta:        0.5,
			numStdDevs:   0,
			wantErrorMsg: "numStdDevs must be 1, 2 or 3",
		},
		{
			name:         "invalid stddev = 4",
			numSamples:   100,
			theta:        0.5,
			numStdDevs:   4,
			wantErrorMsg: "numStdDevs must be 1, 2 or 3",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := UpperBound(tc.numSamples, tc.theta, tc.numStdDevs)
			if tc.wantErrorMsg != "" {
				assert.ErrorContains(t, err, tc.wantErrorMsg)
				return
			}
			tc.validate(t, result, tc.numSamples, tc.theta, tc.numStdDevs)
		})
	}
}
