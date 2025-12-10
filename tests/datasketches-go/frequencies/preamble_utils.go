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
	"encoding/binary"
	"errors"
)

const (
	// ###### DO NOT MESS WITH THIS FROM HERE ...
	// Preamble byte Addresses
	_PREAMBLE_LONGS_BYTE  = 0
	_SER_VER_BYTE         = 1
	_FAMILY_BYTE          = 2
	_LG_MAX_MAP_SIZE_BYTE = 3
	_LG_CUR_MAP_SIZE_BYTE = 4
	_FLAGS_BYTE           = 5

	// _EMPTY_FLAG_MASK flag bit masks
	// due to a mistake different bits were used in C++ and Java to indicate empty sketch
	// therefore both are set and checked for compatibility with historical binary format
	_EMPTY_FLAG_MASK = 5

	_SER_VER = 1
)

func checkPreambleSize(preamble []byte) (int64, error) {
	if len(preamble) < 8 {
		return 0, errors.New("preamble is too small")
	}
	pre0 := int64(binary.LittleEndian.Uint64(preamble))
	preLongs := int(pre0 & 0x3F)
	required := max(preLongs<<3, 8)
	if len(preamble) < required {
		return 0, errors.New("preamble is too small")
	}
	return pre0, nil
}

func insertPreLongs(preLongs, pre0 int64) int64 {
	mask := int64(0x3F)
	return (preLongs & mask) | (^mask & pre0)
}

func insertSerVer(serVer, pre0 int64) int64 {
	shift := _SER_VER_BYTE << 3
	mask := int64(0xFF)
	return ((serVer & mask) << shift) | (^(mask << shift) & pre0)
}

func insertFamilyID(familyID, pre0 int64) int64 {
	shift := _FAMILY_BYTE << 3
	mask := int64(0xFF)
	return ((familyID & mask) << shift) | (^(mask << shift) & pre0)
}

func insertLgMaxMapSize(lgMaxMapSize, pre0 int64) int64 {
	shift := _LG_MAX_MAP_SIZE_BYTE << 3
	mask := int64(0xFF)
	return ((lgMaxMapSize & mask) << shift) | (^(mask << shift) & pre0)
}

func insertLgCurMapSize(lgCurMapSize, pre0 int64) int64 {
	shift := _LG_CUR_MAP_SIZE_BYTE << 3
	mask := int64(0xFF)
	return ((lgCurMapSize & mask) << shift) | (^(mask << shift) & pre0)
}

func insertFlags(flags, pre0 int64) int64 {
	shift := _FLAGS_BYTE << 3
	mask := int64(0xFF)
	return ((flags & mask) << shift) | (^(mask << shift) & pre0)
}

func insertActiveItems(activeItems, pre1 int64) int64 {
	mask := int64(0xFFFFFFFF)
	return (activeItems & mask) | (^mask & pre1)
}

func extractPreLongs(pre0 int64) int {
	mask := int64(0x3F)
	return int(pre0 & mask)
}

func extractSerVer(pre0 int64) int {
	shift := _SER_VER_BYTE << 3
	mask := int64(0xFF)
	return int((pre0 >> shift) & mask)
}

func extractFamilyID(pre0 int64) int {
	shift := _FAMILY_BYTE << 3
	mask := int64(0xFF)
	return int((pre0 >> shift) & mask)
}

func extractLgMaxMapSize(pre0 int64) int {
	shift := _LG_MAX_MAP_SIZE_BYTE << 3
	mask := int64(0xFF)
	return int((pre0 >> shift) & mask)
}

func extractLgCurMapSize(pre0 int64) int {
	shift := _LG_CUR_MAP_SIZE_BYTE << 3
	mask := int64(0xFF)
	return int((pre0 >> shift) & mask)
}

func extractFlags(pre0 int64) int {
	shift := _FLAGS_BYTE << 3
	mask := int64(0xFF)
	return int((pre0 >> shift) & mask)
}

func extractActiveItems(pre1 int64) int {
	mask := int64(0xFFFFFFFF)
	return int(pre1 & mask)
}
