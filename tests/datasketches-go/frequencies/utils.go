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

package frequencies

import (
	"math"
	"math/rand"
)

const (
	// _LG_MIN_MAP_SIZE constant controle the size of the initial data structure for the
	// frequencies sketches and its value is somewhat arbitrary.
	_LG_MIN_MAP_SIZE = 3
	// _SAMPLE_SIZE constant is large enough so that computing the median of SAMPLE_SIZE
	// randomly selected entries from a list of numbers and outputting
	// the empirical median will give a constant-factor approximation to the
	// true median with high probability.
	_SAMPLE_SIZE = 1024
)

type errorType struct {
	id   int
	Name string
}

type errorTypes struct {
	NoFalsePositives errorType
	NoFalseNegatives errorType
}

var ErrorTypeEnum = &errorTypes{
	NoFalsePositives: errorType{
		id:   1,
		Name: "NO_FALSE_POSITIVES",
	},
	NoFalseNegatives: errorType{
		id:   2,
		Name: "NO_FALSE_NEGATIVES",
	},
}

// hashFn returns an index into the hashFn table.
// This hashFn function is taken from the internals of Austin Appleby's MurmurHash3 algorithm.
// It is also used by the Trove for Java libraries.
func hashFn(okey int64) int64 {
	key := uint64(okey)
	key ^= key >> 33
	key *= 0xff51afd7ed558ccd
	key ^= key >> 33
	key *= 0xc4ceb9fe1a85ec53
	key ^= key >> 33
	return int64(key)
}

func randomGeometricDist(prob float64) int64 {
	if prob <= 0.0 || prob >= 1.0 {
		panic("prob must be in (0, 1)")
	}
	return int64(1 + math.Log(rand.Float64())/math.Log(1.0-prob))
}
