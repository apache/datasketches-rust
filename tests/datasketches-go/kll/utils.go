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

package kll

import (
	"errors"
	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
	"math"
	"math/bits"
	"strconv"
)

const (
	tailRoundingFactor = 1e7

	_PMF_COEF = 2.446
	_PMF_EXP  = 0.9433
	_CDF_COEF = 2.296
	_CDF_EXP  = 0.9723
)

func convertToCumulative(array []int64) int64 {
	subtotal := int64(0)
	for i := range array {
		subtotal += array[i]
		array[i] = subtotal
	}
	return subtotal
}

func getNaturalRank(normalizedRank float64, totalN uint64, inclusive bool) int64 {
	naturalRank := normalizedRank * float64(totalN)
	if totalN <= tailRoundingFactor {
		naturalRank = math.Round(naturalRank*tailRoundingFactor) / tailRoundingFactor
	}
	if inclusive {
		return int64(math.Ceil(naturalRank))
	}
	return int64(math.Floor(naturalRank))
}

func checkK(k uint16, m uint8) error {
	if k < uint16(m) || k > _MAX_K {
		return errors.New("K must be >= " + strconv.Itoa(int(m)) + " and <= " + strconv.Itoa(_MAX_K) + ": " + strconv.Itoa(int(k)))
	}
	return nil
}

func checkM(m uint8) error {
	if m < _MIN_M || m > _MAX_M || ((m & 1) == 1) {
		return errors.New("M must be >= 2, <= 8 and even: " + strconv.Itoa(int(m)))
	}
	return nil
}

func checkNormalizedRankBounds(rank float64) error {
	if rank < 0 || rank > 1 {
		return errors.New("rank must be between 0 and 1 inclusive")
	}
	return nil
}

func checkItems[C comparable](items []C, compareFn common.CompareFn[C]) error {
	if len(items) == 1 && internal.IsNil(items[0]) {
		return errors.New("items must be unique, monotonically increasing and not nil")
	}

	for i := 0; i < len(items)-1; i++ {
		if !internal.IsNil(items[i]) && !internal.IsNil(items[i+1]) && compareFn(items[i], items[i+1]) {
			continue
		}
		return errors.New("items must be unique, monotonically increasing and not nil")
	}
	return nil
}

func numDigits(n int) int {
	if n%10 == 0 {
		n++
	}
	l := math.Log(float64(n))
	return int(math.Ceil(l / math.Log(10)))
}

func intToFixedLengthString(number int, length int) string {
	num := strconv.Itoa(number)
	return characterPad(num, length, ' ', false)
}

func characterPad(s string, fieldLength int, padChar byte, postpend bool) string {
	sLen := len(s)
	if sLen < fieldLength {
		addstr := ""
		for i := 0; i < fieldLength-sLen; i++ {
			addstr += string(padChar)
		}
		if postpend {
			return s + addstr
		}
		return addstr + s
	}
	return s
}

func ubOnNumLevels(n uint64) int {
	v := internal.FloorPowerOf2(int64(n))
	return 1 + bits.TrailingZeros64(uint64(v))
}

func getNumRetainedAboveLevelZero(numLevels uint8, levels []uint32) uint32 {
	return levels[numLevels] - levels[1]
}

func currentLevelSizeItems(level uint8, numLevels uint8, levels []uint32) uint32 {
	if level >= numLevels {
		return 0
	}
	return levels[level+1] - levels[level]
}

func getNormalizedRankError(k uint16, pmf bool) float64 {
	if pmf {
		return _PMF_COEF / math.Pow(float64(k), _PMF_EXP)
	}
	return _CDF_COEF / math.Pow(float64(k), _CDF_EXP)
}

func evenlySpacedDoubles(value1 float64, value2 float64, num int) ([]float64, error) {
	if num < 2 {
		return nil, errors.New("num must be >= 2")
	}
	out := make([]float64, num)
	out[0] = value1
	out[num-1] = value2
	if num == 2 {
		return out, nil
	}

	delta := (value2 - value1) / float64(num-1)

	for i := 1; i < num-1; i++ {
		out[i] = float64(i)*delta + value1
	}
	return out, nil
}
