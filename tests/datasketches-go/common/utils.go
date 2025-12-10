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

package common

import (
	"math"
	"math/bits"
)

const (
	defaultSerdeHashSeed = uint64(9001)
	InverseGoldenU64     = 0x9e3779b97f4a7c13
)

func checkBounds(offset int, reqLen int, memCap int) bool {
	return !((offset | reqLen | (offset + reqLen) | (memCap - (offset + reqLen))) < 0)
}

func PowerSeriesNextDouble(ppb int, curPoint float64, roundToLong bool, logBase float64) float64 {
	// If curPoint < 1.0, force cur to 1.0
	cur := curPoint
	if cur < 1.0 {
		cur = 1.0
	}

	// Compute the "generating index" (gi) by rounding logBaseOfX(logBase, cur)*ppb
	gi := math.Round(logBaseOfX(logBase, cur) * float64(ppb))

	for {
		// Increment gi, compute n = logBase^(gi/ppb)
		gi += 1.0
		n := math.Pow(logBase, gi/float64(ppb))

		// If roundToLong is true, round to nearest integer; otherwise keep the float
		var next float64
		if roundToLong {
			next = math.Round(n)
		} else {
			next = n
		}

		// Repeat until next > cur
		if next > cur {
			return next
		}
	}
}

func logBaseOfX(base, x float64) float64 {
	return math.Log(x) / math.Log(base)
}

func CeilingPowerOf2(n int) int {
	if n <= 1 {
		return 1
	}
	const topIntPwrOf2 = 1 << 30
	if n >= topIntPwrOf2 {
		return topIntPwrOf2
	}
	x := (n - 1) << 1
	return 1 << (bits.Len32(uint32(x)) - 1)
}
