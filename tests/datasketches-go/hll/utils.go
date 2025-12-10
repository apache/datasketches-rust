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

package hll

import (
	"fmt"
	"math"

	"github.com/apache/datasketches-go/internal"
)

const (
	defaultLgK     = 12
	lgInitListSize = 3
	lgInitSetSize  = 5
)

const (
	minLogK         = 4
	maxLogK         = 21
	empty           = 0
	keyBits26       = 26
	valBits6        = 6
	keyMask26       = (1 << keyBits26) - 1
	valMask6        = (1 << valBits6) - 1
	resizeNumber    = 3
	resizeDenom     = 4
	couponRSEFactor = .409 //at transition point not the asymptote
	couponRSE       = couponRSEFactor / (1 << 13)
	hiNibbleMask    = 0xf0
	loNibbleMask    = 0x0f

	auxToken = 0xf
)

var (
	hllNonHipRSEFactor = math.Sqrt((3.0 * math.Log(2.0)) - 1.0) //1.03896
	hllHipRSEFActor    = math.Sqrt(math.Log(2.0))               //.8325546
)

type TgtHllType int
type curMode int

const (
	curModeList curMode = 0
	curModeSet  curMode = 1
	curModeHll  curMode = 2
)

// Specifies the target type of HLL sketch to be created. It is a target in that the actual
// allocation of the HLL array is deferred until sufficient number of items have been received by
// the warm-up phases.
//
// These three target types are isomorphic representations of the same underlying HLL algorithm.
// Thus, given the same value of <i>lgConfigK</i> and the same input, all three HLL target types
// will produce identical estimates and have identical error distributions.
//
// The memory (and also the serialization) of the sketch during this early warmup phase starts
// out very small (8 bytes, when empty) and then grows in increments of 4 bytes as required
// until the full HLL array is allocated.  This transition point occurs at about 10% of K for
// sketches where lgConfigK is > 8.
//
//   - Hll 8 This uses an 8-bit byte per HLL bucket. It is generally the
//     fastest in terms of update time, but has the largest storage footprint of about
//     K bytes.
//
//   - Hll 6 This uses a 6-bit field per HLL bucket. It is the generally the next fastest
//     in terms of update time with a storage footprint of about 3/4 * K bytes.
//
//   - Hll 4 This uses a 4-bit field per HLL bucket and for large counts may require
//     the use of a small internal auxiliary array for storing statistical exceptions, which are rare.
//     For the values of lgConfigK > 13 (K = 8192),
//     this additional array adds about 3% to the overall storage. It is generally the slowest in
//     terms of update time, but has the smallest storage footprint of about
//     K/2 * 1.03 bytes.
const (
	TgtHllTypeHll4    = TgtHllType(0)
	TgtHllTypeHll6    = TgtHllType(1)
	TgtHllTypeHll8    = TgtHllType(2)
	TgtHllTypeDefault = TgtHllTypeHll4
)

var (
	// lgAuxArrInts is the Log2 table sizes for exceptions based on lgK from 0 to 26.
	//However, only lgK from 4 to 21 are used.
	lgAuxArrInts = []int{
		0, 2, 2, 2, 2, 2, 2, 3, 3, 3, //0 - 9
		4, 4, 5, 5, 6, 7, 8, 9, 10, 11, //10 - 19
		12, 13, 14, 15, 16, 17, 18, //20 - 26
	}
)

// CheckLgK checks the given lgK and returns it if it is valid and return an error otherwise.
func checkLgK(lgK int) (int, error) {
	if lgK >= minLogK && lgK <= maxLogK {
		return lgK, nil
	}
	return 0, fmt.Errorf("log K must be between 4 and 21, inclusive: %d", lgK)
}

// pair returns a value where the lower 26 bits are the slotNo and the upper 6 bits are the value.
func pair(slotNo int, value int) int {
	return (value << keyBits26) | (slotNo & keyMask26)
}

// pairString returns a string representation of the pair.
func pairString(pair int) string {
	return fmt.Sprintf("SlotNo: %d, Value: %d", getPairLow26(pair), getPairValue(pair))
}

// getPairLow26 returns the pair, the lower 26 bits of the pair.
func getPairLow26(pair int) int {
	return pair & keyMask26
}

// getPairValue returns the value of the pair.
// The value is the upper 6 bits of the pair.
func getPairValue(pair int) int {
	return pair >> keyBits26
}

func checkNumStdDev(numStdDev int) error {
	if numStdDev < 1 || numStdDev > 3 {
		return fmt.Errorf("NumStdDev may not be less than 1 or greater than 3: %d", numStdDev)
	}
	return nil
}

// checkPreamble checks the given preamble and returns the curMode if it is valid and return an error otherwise.
func checkPreamble(preamble []byte) (curMode, error) {
	if len(preamble) == 0 {
		return 0, fmt.Errorf("preamble cannot be nil or empty")
	}
	preInts := extractPreInts(preamble)
	if len(preamble) < (preInts * 4) {
		return 0, fmt.Errorf("preamble length mismatch: %d, %d", len(preamble), preInts)
	}
	serVer := extractSerVer(preamble)
	famId := extractFamilyID(preamble)
	curMode := extractCurMode(preamble)

	if famId != internal.FamilyEnum.HLL.Id {
		return 0, fmt.Errorf("possible Corruption: Invalid Family: %d", famId)
	}
	if serVer != 1 {
		return 0, fmt.Errorf("possible Corruption: Invalid Serialization Version: %d", serVer)
	}

	if preInts != listPreInts && preInts != hashSetPreInts && preInts != hllPreInts {
		return 0, fmt.Errorf("possible Corruption: Invalid Preamble Ints: %d", preInts)
	}

	if curMode == curModeList && preInts != listPreInts {
		return 0, fmt.Errorf("possible Corruption: Invalid Preamble Ints: %d", preInts)
	}

	if curMode == curModeSet && preInts != hashSetPreInts {
		return 0, fmt.Errorf("possible Corruption: Invalid Preamble Ints: %d", preInts)
	}

	if curMode == curModeHll && preInts != hllPreInts {
		return 0, fmt.Errorf("possible Corruption: Invalid Preamble Ints: %d", preInts)
	}

	return curMode, nil
}

func getMaxUpdatableSerializationBytes(lgConfigK int, tgtHllType TgtHllType) int {
	var arrBytes int
	if tgtHllType == TgtHllTypeHll4 {
		auxBytes := 4 << lgAuxArrInts[lgConfigK]
		arrBytes = (1 << (lgConfigK - 1)) + auxBytes
	} else if tgtHllType == TgtHllTypeHll6 {
		numSlots := 1 << lgConfigK
		arrBytes = ((numSlots * 3) >> 2) + 1
	} else {
		arrBytes = 1 << lgConfigK
	}
	return hllByteArrStart + arrBytes
}
