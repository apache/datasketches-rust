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

import "encoding/binary"

const (
	_PREAMBLE_INTS_BYTE_ADR = 0
	_SER_VER_BYTE_ADR       = 1
	_FAMILY_BYTE_ADR        = 2
	_FLAGS_BYTE_ADR         = 3
	_K_SHORT_ADR            = 4 // to 5
	_M_BYTE_ADR             = 6

	// SINGLE ITEM ONLY
	_DATA_START_ADR_SINGLE_ITEM = 8 //also ok for empty

	// MULTI-ITEM
	_N_LONG_ADR      = 8  // to 15
	_MIN_K_SHORT_ADR = 16 // to 17

	_NUM_LEVELS_BYTE_ADR = 18

	// 19 is reserved for future use
	_DATA_START_ADR = 20 // Full Sketch, not single item

	// Other static members
	_SERIAL_VERSION_EMPTY_FULL  = 1 // Empty or full preamble, NOT single item format, NOT updatable
	_SERIAL_VERSION_SINGLE      = 2 // only single-item format, NOT updatable
	_SERIAL_VERSION_UPDATABLE   = 3 // PreInts=5, Full preamble + LevelsArr + min, max + empty space
	_PREAMBLE_INTS_EMPTY_SINGLE = 2 // for empty or single item
	_PREAMBLE_INTS_FULL         = 5 // Full preamble, not empty nor single item.

	// Flag bit masks
	_EMPTY_BIT_MASK             = 1
	_LEVEL_ZERO_SORTED_BIT_MASK = 2
	_SINGLE_ITEM_BIT_MASK       = 4
)

func getPreInts(mem []byte) int {
	return int(mem[_PREAMBLE_INTS_BYTE_ADR] & 0xFF)
}

func getSerVer(mem []byte) int {
	return int(mem[_SER_VER_BYTE_ADR] & 0xFF)
}

func getFamilyID(mem []byte) int {
	return int(mem[_FAMILY_BYTE_ADR] & 0xFF)
}

func getFlags(mem []byte) int {
	return int(mem[_FLAGS_BYTE_ADR] & 0xFF)
}

func getEmptyFlag(mem []byte) bool {
	return (getFlags(mem) & _EMPTY_BIT_MASK) != 0
}

func getK(mem []byte) uint16 {
	return binary.LittleEndian.Uint16(mem[_K_SHORT_ADR : _K_SHORT_ADR+2])
}

func getM(mem []byte) uint8 {
	return mem[_M_BYTE_ADR] & 0xFF
}

func getN(mem []byte) uint64 {
	return binary.LittleEndian.Uint64(mem[_N_LONG_ADR : _N_LONG_ADR+8])
}

func getMinK(mem []byte) uint16 {
	return binary.LittleEndian.Uint16(mem[_MIN_K_SHORT_ADR : _MIN_K_SHORT_ADR+2])
}

func getNumLevels(mem []byte) uint8 {
	return mem[_NUM_LEVELS_BYTE_ADR] & 0xFF
}

func getLevelZeroSortedFlag(mem []byte) bool {
	return (getFlags(mem) & _LEVEL_ZERO_SORTED_BIT_MASK) != 0
}
