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

package binomialproportionsbounds

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestApproximateLowerBoundOnP(t *testing.T) {
	testCases := []struct {
		name       string
		n          uint64
		k          uint64
		numStdDevs float64
		wantErrMsg string
	}{
		{
			name:       "n=0",
			n:          0,
			k:          0,
			numStdDevs: 2.0,
			wantErrMsg: "",
		},
		{
			name:       "k=0",
			n:          100,
			k:          0,
			numStdDevs: 2.0,
			wantErrMsg: "",
		},
		{
			name:       "k=1",
			n:          100,
			k:          1,
			numStdDevs: 2.0,
			wantErrMsg: "",
		}, // uses exact formula
		{
			name:       "k=n",
			n:          100,
			k:          100,
			numStdDevs: 2.0,
			wantErrMsg: "",
		}, // uses exact formula
		{
			name:       "normal case",
			n:          100,
			k:          50,
			numStdDevs: 2.0,
			wantErrMsg: "",
		},
		{
			name:       "k > n",
			n:          100,
			k:          101,
			numStdDevs: 2.0,
			wantErrMsg: "K cannot exceed N",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ApproximateLowerBoundOnP(tc.n, tc.k, tc.numStdDevs)

			if tc.wantErrMsg != "" {
				assert.ErrorContains(t, err, tc.wantErrMsg)
				return
			}

			if got < 0.0 || got > 1.0 {
				assert.Fail(t, "got: %v, want value in [0, 1]", got)
			}
		})
	}
}

func TestApproximateUpperBoundOnP(t *testing.T) {
	testCases := []struct {
		name       string
		n          uint64
		k          uint64
		numStdDevs float64
		checkExact bool
		exactValue float64
		checkRange bool
		wantErrMsg string
	}{
		{
			name:       "n=0",
			n:          0,
			k:          0,
			numStdDevs: 2.0,
			checkExact: true,
			exactValue: 1.0,
			checkRange: false,
			wantErrMsg: "",
		},
		{
			name:       "k=n",
			n:          100,
			k:          100,
			numStdDevs: 2.0,
			checkExact: true,
			exactValue: 1.0,
			checkRange: false,
			wantErrMsg: "",
		},
		{
			name:       "k=n-1",
			n:          100,
			k:          99,
			numStdDevs: 2.0,
			checkExact: false,
			exactValue: 0.0,
			checkRange: true,
			wantErrMsg: "",
		}, // uses exact formula
		{
			name:       "k=0",
			n:          100,
			k:          0,
			numStdDevs: 2.0,
			checkExact: false,
			exactValue: 0.0,
			checkRange: true,
			wantErrMsg: "",
		}, // uses exact formula
		{
			name:       "normal case",
			n:          100,
			k:          50,
			numStdDevs: 2.0,
			checkExact: false,
			exactValue: 0.0,
			checkRange: true,
			wantErrMsg: "",
		},
		{
			name:       "k > n",
			n:          100,
			k:          101,
			numStdDevs: 2.0,
			checkExact: false,
			exactValue: 0.0,
			checkRange: false,
			wantErrMsg: "K cannot exceed N",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ApproximateUpperBoundOnP(tc.n, tc.k, tc.numStdDevs)

			if tc.wantErrMsg != "" {
				assert.ErrorContains(t, err, tc.wantErrMsg)
				return
			}

			if got < 0.0 || got > 1.0 {
				assert.Fail(t, "got: %v, want value in [0, 1]", got)
			}
		})
	}
}

func TestErf(t *testing.T) {
	testCases := []struct {
		name string
		x    float64
	}{
		{
			name: "erf(0)",
			x:    0.0,
		},
		{
			name: "erf(1)",
			x:    1.0,
		}, // approximate value
		{
			name: "erf(-1)",
			x:    -1.0,
		},
		{
			name: "erf(2)",
			x:    2.0,
		},
		{
			name: "erf(-2)",
			x:    -2.0,
		},
		{
			name: "erf(0.5)",
			x:    0.5,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := Erf(tc.x)

			if got < -1.0 || got > 1.0 {
				assert.Fail(t, "got: %v, want value in [-1, 1]", got)
			}
		})
	}
}

func TestNormalCDF(t *testing.T) {
	testCases := []struct {
		name string
		x    float64
	}{
		{
			name: "normalCDF(0)",
			x:    0.0,
		},
		{
			name: "normalCDF(1)",
			x:    1.0,
		},
		{
			name: "normalCDF(-1)",
			x:    -1.0,
		},
		{
			name: "normalCDF(2)",
			x:    2.0,
		},
		{
			name: "normalCDF(-2)",
			x:    -2.0,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := NormalCDF(tc.x)

			if got < 0.0 || got > 1.0 {
				assert.Fail(t, "got: %v, want value in [0, 1]", got)
			}
		})
	}
}
