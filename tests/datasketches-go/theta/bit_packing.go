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

import "fmt"

// packBits packs a value with a given number of bits into a byte slice
// value: the value to pack
// bits: the number of bits to pack from the value
// bytes: the byte slice
// bytesIdx: current index in the byte slice
// offset: bit offset within the current byte (0-7)
// Returns: (newByteIdx, newOffset) - the updated index and bit offset
func packBits(value uint64, bits uint8, bytes []byte, bytesIdx int, offset uint8) (int, uint8) {
	if offset > 0 {
		chunkBits := 8 - offset
		mask := uint8((1 << chunkBits) - 1)

		if bits < chunkBits {
			bytes[bytesIdx] |= uint8((value << (chunkBits - bits)) & uint64(mask))
			return bytesIdx, offset + bits
		}

		bytes[bytesIdx] |= uint8((value >> (bits - chunkBits)) & uint64(mask))
		bytesIdx++
		bits -= chunkBits
	}

	for bits >= 8 {
		bytes[bytesIdx] = uint8(value >> (bits - 8))
		bytesIdx++
		bits -= 8
	}

	if bits > 0 {
		bytes[bytesIdx] = uint8(value << (8 - bits))
		return bytesIdx, bits
	}

	return bytesIdx, 0
}

// unpackBits unpacks a specific number of bits from a byte slice into a uint64 value
// bits: the number of bits to unpack
// bytes: the byte slice
// bytesIdx: current index in the byte slice
// offset: bit offset within the current byte (0-7)
// Returns: (value, newPtrIdx, newOffset) - the unpacked value, updated index and bit offset
func unpackBits(bits uint8, bytes []byte, bytesIdx int, offset uint8) (uint64, int, uint8) {
	availBits := 8 - offset
	chunkBits := min(availBits, bits)
	mask := uint8((1 << chunkBits) - 1)
	value := uint64((bytes[bytesIdx] >> (availBits - chunkBits)) & mask)

	if availBits == chunkBits {
		bytesIdx++
	}
	offset = (offset + chunkBits) & 7
	bits -= chunkBits

	for bits >= 8 {
		value <<= 8
		value |= uint64(bytes[bytesIdx])
		bytesIdx++
		bits -= 8
	}

	if bits > 0 {
		value <<= bits
		value |= uint64(bytes[bytesIdx] >> (8 - bits))
		return value, bytesIdx, bits
	}

	return value, bytesIdx, offset
}

func packBits1(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] << 7)
	bytes[0] |= uint8(values[1] << 6)
	bytes[0] |= uint8(values[2] << 5)
	bytes[0] |= uint8(values[3] << 4)
	bytes[0] |= uint8(values[4] << 3)
	bytes[0] |= uint8(values[5] << 2)
	bytes[0] |= uint8(values[6] << 1)
	bytes[0] |= uint8(values[7])
}

func packBits2(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] << 6)
	bytes[0] |= uint8(values[1] << 4)
	bytes[0] |= uint8(values[2] << 2)
	bytes[0] |= uint8(values[3])

	bytes[1] = uint8(values[4] << 6)
	bytes[1] |= uint8(values[5] << 4)
	bytes[1] |= uint8(values[6] << 2)
	bytes[1] |= uint8(values[7])
}

func packBits3(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] << 5)
	bytes[0] |= uint8(values[1] << 2)
	bytes[0] |= uint8(values[2] >> 1)

	bytes[1] = uint8(values[2] << 7)
	bytes[1] |= uint8(values[3] << 4)
	bytes[1] |= uint8(values[4] << 1)
	bytes[1] |= uint8(values[5] >> 2)

	bytes[2] = uint8(values[5] << 6)
	bytes[2] |= uint8(values[6] << 3)
	bytes[2] |= uint8(values[7])
}

func packBits4(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] << 4)
	bytes[0] |= uint8(values[1])

	bytes[1] = uint8(values[2] << 4)
	bytes[1] |= uint8(values[3])

	bytes[2] = uint8(values[4] << 4)
	bytes[2] |= uint8(values[5])

	bytes[3] = uint8(values[6] << 4)
	bytes[3] |= uint8(values[7])
}

func packBits5(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] << 3)
	bytes[0] |= uint8(values[1] >> 2)

	bytes[1] = uint8(values[1] << 6)
	bytes[1] |= uint8(values[2] << 1)
	bytes[1] |= uint8(values[3] >> 4)

	bytes[2] = uint8(values[3] << 4)
	bytes[2] |= uint8(values[4] >> 1)

	bytes[3] = uint8(values[4] << 7)
	bytes[3] |= uint8(values[5] << 2)
	bytes[3] |= uint8(values[6] >> 3)

	bytes[4] = uint8(values[6] << 5)
	bytes[4] |= uint8(values[7])
}

func packBits6(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] << 2)
	bytes[0] |= uint8(values[1] >> 4)

	bytes[1] = uint8(values[1] << 4)
	bytes[1] |= uint8(values[2] >> 2)

	bytes[2] = uint8(values[2] << 6)
	bytes[2] |= uint8(values[3])

	bytes[3] = uint8(values[4] << 2)
	bytes[3] |= uint8(values[5] >> 4)

	bytes[4] = uint8(values[5] << 4)
	bytes[4] |= uint8(values[6] >> 2)

	bytes[5] = uint8(values[6] << 6)
	bytes[5] |= uint8(values[7])
}

func packBits7(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] << 1)
	bytes[0] |= uint8(values[1] >> 6)

	bytes[1] = uint8(values[1] << 2)
	bytes[1] |= uint8(values[2] >> 5)

	bytes[2] = uint8(values[2] << 3)
	bytes[2] |= uint8(values[3] >> 4)

	bytes[3] = uint8(values[3] << 4)
	bytes[3] |= uint8(values[4] >> 3)

	bytes[4] = uint8(values[4] << 5)
	bytes[4] |= uint8(values[5] >> 2)

	bytes[5] = uint8(values[5] << 6)
	bytes[5] |= uint8(values[6] >> 1)

	bytes[6] = uint8(values[6] << 7)
	bytes[6] |= uint8(values[7])
}

func packBits8(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0])
	bytes[1] = uint8(values[1])
	bytes[2] = uint8(values[2])
	bytes[3] = uint8(values[3])
	bytes[4] = uint8(values[4])
	bytes[5] = uint8(values[5])
	bytes[6] = uint8(values[6])
	bytes[7] = uint8(values[7])
}

func packBits9(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 1)

	bytes[1] = uint8(values[0] << 7)
	bytes[1] |= uint8(values[1] >> 2)

	bytes[2] = uint8(values[1] << 6)
	bytes[2] |= uint8(values[2] >> 3)

	bytes[3] = uint8(values[2] << 5)
	bytes[3] |= uint8(values[3] >> 4)

	bytes[4] = uint8(values[3] << 4)
	bytes[4] |= uint8(values[4] >> 5)

	bytes[5] = uint8(values[4] << 3)
	bytes[5] |= uint8(values[5] >> 6)

	bytes[6] = uint8(values[5] << 2)
	bytes[6] |= uint8(values[6] >> 7)

	bytes[7] = uint8(values[6] << 1)
	bytes[7] |= uint8(values[7] >> 8)

	bytes[8] = uint8(values[7])
}

func packBits10(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 2)

	bytes[1] = uint8(values[0] << 6)
	bytes[1] |= uint8(values[1] >> 4)

	bytes[2] = uint8(values[1] << 4)
	bytes[2] |= uint8(values[2] >> 6)

	bytes[3] = uint8(values[2] << 2)
	bytes[3] |= uint8(values[3] >> 8)

	bytes[4] = uint8(values[3])

	bytes[5] = uint8(values[4] >> 2)

	bytes[6] = uint8(values[4] << 6)
	bytes[6] |= uint8(values[5] >> 4)

	bytes[7] = uint8(values[5] << 4)
	bytes[7] |= uint8(values[6] >> 6)

	bytes[8] = uint8(values[6] << 2)
	bytes[8] |= uint8(values[7] >> 8)

	bytes[9] = uint8(values[7])
}

func packBits11(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 3)

	bytes[1] = uint8(values[0] << 5)
	bytes[1] |= uint8(values[1] >> 6)

	bytes[2] = uint8(values[1] << 2)
	bytes[2] |= uint8(values[2] >> 9)

	bytes[3] = uint8(values[2] >> 1)

	bytes[4] = uint8(values[2] << 7)
	bytes[4] |= uint8(values[3] >> 4)

	bytes[5] = uint8(values[3] << 4)
	bytes[5] |= uint8(values[4] >> 7)

	bytes[6] = uint8(values[4] << 1)
	bytes[6] |= uint8(values[5] >> 10)

	bytes[7] = uint8(values[5] >> 2)

	bytes[8] = uint8(values[5] << 6)
	bytes[8] |= uint8(values[6] >> 5)

	bytes[9] = uint8(values[6] << 3)
	bytes[9] |= uint8(values[7] >> 8)

	bytes[10] = uint8(values[7])
}

func packBits12(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 4)

	bytes[1] = uint8(values[0] << 4)
	bytes[1] |= uint8(values[1] >> 8)

	bytes[2] = uint8(values[1])

	bytes[3] = uint8(values[2] >> 4)

	bytes[4] = uint8(values[2] << 4)
	bytes[4] |= uint8(values[3] >> 8)

	bytes[5] = uint8(values[3])

	bytes[6] = uint8(values[4] >> 4)

	bytes[7] = uint8(values[4] << 4)
	bytes[7] |= uint8(values[5] >> 8)

	bytes[8] = uint8(values[5])

	bytes[9] = uint8(values[6] >> 4)

	bytes[10] = uint8(values[6] << 4)
	bytes[10] |= uint8(values[7] >> 8)

	bytes[11] = uint8(values[7])
}

func packBits13(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 5)

	bytes[1] = uint8(values[0] << 3)
	bytes[1] |= uint8(values[1] >> 10)

	bytes[2] = uint8(values[1] >> 2)

	bytes[3] = uint8(values[1] << 6)
	bytes[3] |= uint8(values[2] >> 7)

	bytes[4] = uint8(values[2] << 1)
	bytes[4] |= uint8(values[3] >> 12)

	bytes[5] = uint8(values[3] >> 4)

	bytes[6] = uint8(values[3] << 4)
	bytes[6] |= uint8(values[4] >> 9)

	bytes[7] = uint8(values[4] >> 1)

	bytes[8] = uint8(values[4] << 7)
	bytes[8] |= uint8(values[5] >> 6)

	bytes[9] = uint8(values[5] << 2)
	bytes[9] |= uint8(values[6] >> 11)

	bytes[10] = uint8(values[6] >> 3)

	bytes[11] = uint8(values[6] << 5)
	bytes[11] |= uint8(values[7] >> 8)

	bytes[12] = uint8(values[7])
}

func packBits14(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 6)

	bytes[1] = uint8(values[0] << 2)
	bytes[1] |= uint8(values[1] >> 12)

	bytes[2] = uint8(values[1] >> 4)

	bytes[3] = uint8(values[1] << 4)
	bytes[3] |= uint8(values[2] >> 10)

	bytes[4] = uint8(values[2] >> 2)

	bytes[5] = uint8(values[2] << 6)
	bytes[5] |= uint8(values[3] >> 8)

	bytes[6] = uint8(values[3])

	bytes[7] = uint8(values[4] >> 6)

	bytes[8] = uint8(values[4] << 2)
	bytes[8] |= uint8(values[5] >> 12)

	bytes[9] = uint8(values[5] >> 4)

	bytes[10] = uint8(values[5] << 4)
	bytes[10] |= uint8(values[6] >> 10)

	bytes[11] = uint8(values[6] >> 2)

	bytes[12] = uint8(values[6] << 6)
	bytes[12] |= uint8(values[7] >> 8)

	bytes[13] = uint8(values[7])
}

func packBits15(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 7)

	bytes[1] = uint8(values[0] << 1)
	bytes[1] |= uint8(values[1] >> 14)

	bytes[2] = uint8(values[1] >> 6)

	bytes[3] = uint8(values[1] << 2)
	bytes[3] |= uint8(values[2] >> 13)

	bytes[4] = uint8(values[2] >> 5)

	bytes[5] = uint8(values[2] << 3)
	bytes[5] |= uint8(values[3] >> 12)

	bytes[6] = uint8(values[3] >> 4)

	bytes[7] = uint8(values[3] << 4)
	bytes[7] |= uint8(values[4] >> 11)

	bytes[8] = uint8(values[4] >> 3)

	bytes[9] = uint8(values[4] << 5)
	bytes[9] |= uint8(values[5] >> 10)

	bytes[10] = uint8(values[5] >> 2)

	bytes[11] = uint8(values[5] << 6)
	bytes[11] |= uint8(values[6] >> 9)

	bytes[12] = uint8(values[6] >> 1)

	bytes[13] = uint8(values[6] << 7)
	bytes[13] |= uint8(values[7] >> 8)

	bytes[14] = uint8(values[7])
}

func packBits16(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 8)
	bytes[1] = uint8(values[0])

	bytes[2] = uint8(values[1] >> 8)
	bytes[3] = uint8(values[1])

	bytes[4] = uint8(values[2] >> 8)
	bytes[5] = uint8(values[2])

	bytes[6] = uint8(values[3] >> 8)
	bytes[7] = uint8(values[3])

	bytes[8] = uint8(values[4] >> 8)
	bytes[9] = uint8(values[4])

	bytes[10] = uint8(values[5] >> 8)
	bytes[11] = uint8(values[5])

	bytes[12] = uint8(values[6] >> 8)
	bytes[13] = uint8(values[6])

	bytes[14] = uint8(values[7] >> 8)
	bytes[15] = uint8(values[7])
}

func packBits17(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 9)

	bytes[1] = uint8(values[0] >> 1)

	bytes[2] = uint8(values[0] << 7)
	bytes[2] |= uint8(values[1] >> 10)

	bytes[3] = uint8(values[1] >> 2)

	bytes[4] = uint8(values[1] << 6)
	bytes[4] |= uint8(values[2] >> 11)

	bytes[5] = uint8(values[2] >> 3)

	bytes[6] = uint8(values[2] << 5)
	bytes[6] |= uint8(values[3] >> 12)

	bytes[7] = uint8(values[3] >> 4)

	bytes[8] = uint8(values[3] << 4)
	bytes[8] |= uint8(values[4] >> 13)

	bytes[9] = uint8(values[4] >> 5)

	bytes[10] = uint8(values[4] << 3)
	bytes[10] |= uint8(values[5] >> 14)

	bytes[11] = uint8(values[5] >> 6)

	bytes[12] = uint8(values[5] << 2)
	bytes[12] |= uint8(values[6] >> 15)

	bytes[13] = uint8(values[6] >> 7)

	bytes[14] = uint8(values[6] << 1)
	bytes[14] |= uint8(values[7] >> 16)

	bytes[15] = uint8(values[7] >> 8)

	bytes[16] = uint8(values[7])
}

func packBits18(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 10)

	bytes[1] = uint8(values[0] >> 2)

	bytes[2] = uint8(values[0] << 6)
	bytes[2] |= uint8(values[1] >> 12)

	bytes[3] = uint8(values[1] >> 4)

	bytes[4] = uint8(values[1] << 4)
	bytes[4] |= uint8(values[2] >> 14)

	bytes[5] = uint8(values[2] >> 6)

	bytes[6] = uint8(values[2] << 2)
	bytes[6] |= uint8(values[3] >> 16)

	bytes[7] = uint8(values[3] >> 8)

	bytes[8] = uint8(values[3])

	bytes[9] = uint8(values[4] >> 10)

	bytes[10] = uint8(values[4] >> 2)

	bytes[11] = uint8(values[4] << 6)
	bytes[11] |= uint8(values[5] >> 12)

	bytes[12] = uint8(values[5] >> 4)

	bytes[13] = uint8(values[5] << 4)
	bytes[13] |= uint8(values[6] >> 14)

	bytes[14] = uint8(values[6] >> 6)

	bytes[15] = uint8(values[6] << 2)
	bytes[15] |= uint8(values[7] >> 16)

	bytes[16] = uint8(values[7] >> 8)

	bytes[17] = uint8(values[7])
}

func packBits19(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 11)

	bytes[1] = uint8(values[0] >> 3)

	bytes[2] = uint8(values[0] << 5)
	bytes[2] |= uint8(values[1] >> 14)

	bytes[3] = uint8(values[1] >> 6)

	bytes[4] = uint8(values[1] << 2)
	bytes[4] |= uint8(values[2] >> 17)

	bytes[5] = uint8(values[2] >> 9)

	bytes[6] = uint8(values[2] >> 1)

	bytes[7] = uint8(values[2] << 7)
	bytes[7] |= uint8(values[3] >> 12)

	bytes[8] = uint8(values[3] >> 4)

	bytes[9] = uint8(values[3] << 4)
	bytes[9] |= uint8(values[4] >> 15)

	bytes[10] = uint8(values[4] >> 7)

	bytes[11] = uint8(values[4] << 1)
	bytes[11] |= uint8(values[5] >> 18)

	bytes[12] = uint8(values[5] >> 10)

	bytes[13] = uint8(values[5] >> 2)

	bytes[14] = uint8(values[5] << 6)
	bytes[14] |= uint8(values[6] >> 13)

	bytes[15] = uint8(values[6] >> 5)

	bytes[16] = uint8(values[6] << 3)
	bytes[16] |= uint8(values[7] >> 16)

	bytes[17] = uint8(values[7] >> 8)

	bytes[18] = uint8(values[7])
}

func packBits20(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 12)

	bytes[1] = uint8(values[0] >> 4)

	bytes[2] = uint8(values[0] << 4)
	bytes[2] |= uint8(values[1] >> 16)

	bytes[3] = uint8(values[1] >> 8)

	bytes[4] = uint8(values[1])

	bytes[5] = uint8(values[2] >> 12)

	bytes[6] = uint8(values[2] >> 4)

	bytes[7] = uint8(values[2] << 4)
	bytes[7] |= uint8(values[3] >> 16)

	bytes[8] = uint8(values[3] >> 8)

	bytes[9] = uint8(values[3])

	bytes[10] = uint8(values[4] >> 12)

	bytes[11] = uint8(values[4] >> 4)

	bytes[12] = uint8(values[4] << 4)
	bytes[12] |= uint8(values[5] >> 16)

	bytes[13] = uint8(values[5] >> 8)

	bytes[14] = uint8(values[5])

	bytes[15] = uint8(values[6] >> 12)

	bytes[16] = uint8(values[6] >> 4)

	bytes[17] = uint8(values[6] << 4)
	bytes[17] |= uint8(values[7] >> 16)

	bytes[18] = uint8(values[7] >> 8)

	bytes[19] = uint8(values[7])
}

func packBits21(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 13)

	bytes[1] = uint8(values[0] >> 5)

	bytes[2] = uint8(values[0] << 3)
	bytes[2] |= uint8(values[1] >> 18)

	bytes[3] = uint8(values[1] >> 10)

	bytes[4] = uint8(values[1] >> 2)

	bytes[5] = uint8(values[1] << 6)
	bytes[5] |= uint8(values[2] >> 15)

	bytes[6] = uint8(values[2] >> 7)

	bytes[7] = uint8(values[2] << 1)
	bytes[7] |= uint8(values[3] >> 20)

	bytes[8] = uint8(values[3] >> 12)

	bytes[9] = uint8(values[3] >> 4)

	bytes[10] = uint8(values[3] << 4)
	bytes[10] |= uint8(values[4] >> 17)

	bytes[11] = uint8(values[4] >> 9)

	bytes[12] = uint8(values[4] >> 1)

	bytes[13] = uint8(values[4] << 7)
	bytes[13] |= uint8(values[5] >> 14)

	bytes[14] = uint8(values[5] >> 6)

	bytes[15] = uint8(values[5] << 2)
	bytes[15] |= uint8(values[6] >> 19)

	bytes[16] = uint8(values[6] >> 11)

	bytes[17] = uint8(values[6] >> 3)

	bytes[18] = uint8(values[6] << 5)
	bytes[18] |= uint8(values[7] >> 16)

	bytes[19] = uint8(values[7] >> 8)

	bytes[20] = uint8(values[7])
}

func packBits22(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 14)

	bytes[1] = uint8(values[0] >> 6)

	bytes[2] = uint8(values[0] << 2)
	bytes[2] |= uint8(values[1] >> 20)

	bytes[3] = uint8(values[1] >> 12)

	bytes[4] = uint8(values[1] >> 4)

	bytes[5] = uint8(values[1] << 4)
	bytes[5] |= uint8(values[2] >> 18)

	bytes[6] = uint8(values[2] >> 10)

	bytes[7] = uint8(values[2] >> 2)

	bytes[8] = uint8(values[2] << 6)
	bytes[8] |= uint8(values[3] >> 16)

	bytes[9] = uint8(values[3] >> 8)

	bytes[10] = uint8(values[3])

	bytes[11] = uint8(values[4] >> 14)

	bytes[12] = uint8(values[4] >> 6)

	bytes[13] = uint8(values[4] << 2)
	bytes[13] |= uint8(values[5] >> 20)

	bytes[14] = uint8(values[5] >> 12)

	bytes[15] = uint8(values[5] >> 4)

	bytes[16] = uint8(values[5] << 4)
	bytes[16] |= uint8(values[6] >> 18)

	bytes[17] = uint8(values[6] >> 10)

	bytes[18] = uint8(values[6] >> 2)

	bytes[19] = uint8(values[6] << 6)
	bytes[19] |= uint8(values[7] >> 16)

	bytes[20] = uint8(values[7] >> 8)

	bytes[21] = uint8(values[7])
}

func packBits23(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 15)

	bytes[1] = uint8(values[0] >> 7)

	bytes[2] = uint8(values[0] << 1)
	bytes[2] |= uint8(values[1] >> 22)

	bytes[3] = uint8(values[1] >> 14)

	bytes[4] = uint8(values[1] >> 6)

	bytes[5] = uint8(values[1] << 2)
	bytes[5] |= uint8(values[2] >> 21)

	bytes[6] = uint8(values[2] >> 13)

	bytes[7] = uint8(values[2] >> 5)

	bytes[8] = uint8(values[2] << 3)
	bytes[8] |= uint8(values[3] >> 20)

	bytes[9] = uint8(values[3] >> 12)

	bytes[10] = uint8(values[3] >> 4)

	bytes[11] = uint8(values[3] << 4)
	bytes[11] |= uint8(values[4] >> 19)

	bytes[12] = uint8(values[4] >> 11)

	bytes[13] = uint8(values[4] >> 3)

	bytes[14] = uint8(values[4] << 5)
	bytes[14] |= uint8(values[5] >> 18)

	bytes[15] = uint8(values[5] >> 10)

	bytes[16] = uint8(values[5] >> 2)

	bytes[17] = uint8(values[5] << 6)
	bytes[17] |= uint8(values[6] >> 17)

	bytes[18] = uint8(values[6] >> 9)

	bytes[19] = uint8(values[6] >> 1)

	bytes[20] = uint8(values[6] << 7)
	bytes[20] |= uint8(values[7] >> 16)

	bytes[21] = uint8(values[7] >> 8)

	bytes[22] = uint8(values[7])
}

func packBits24(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 16)
	bytes[1] = uint8(values[0] >> 8)
	bytes[2] = uint8(values[0])

	bytes[3] = uint8(values[1] >> 16)
	bytes[4] = uint8(values[1] >> 8)
	bytes[5] = uint8(values[1])

	bytes[6] = uint8(values[2] >> 16)
	bytes[7] = uint8(values[2] >> 8)
	bytes[8] = uint8(values[2])

	bytes[9] = uint8(values[3] >> 16)
	bytes[10] = uint8(values[3] >> 8)
	bytes[11] = uint8(values[3])

	bytes[12] = uint8(values[4] >> 16)
	bytes[13] = uint8(values[4] >> 8)
	bytes[14] = uint8(values[4])

	bytes[15] = uint8(values[5] >> 16)
	bytes[16] = uint8(values[5] >> 8)
	bytes[17] = uint8(values[5])

	bytes[18] = uint8(values[6] >> 16)
	bytes[19] = uint8(values[6] >> 8)
	bytes[20] = uint8(values[6])

	bytes[21] = uint8(values[7] >> 16)
	bytes[22] = uint8(values[7] >> 8)
	bytes[23] = uint8(values[7])
}

func packBits25(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 17)

	bytes[1] = uint8(values[0] >> 9)

	bytes[2] = uint8(values[0] >> 1)

	bytes[3] = uint8(values[0] << 7)
	bytes[3] |= uint8(values[1] >> 18)

	bytes[4] = uint8(values[1] >> 10)

	bytes[5] = uint8(values[1] >> 2)

	bytes[6] = uint8(values[1] << 6)
	bytes[6] |= uint8(values[2] >> 19)

	bytes[7] = uint8(values[2] >> 11)

	bytes[8] = uint8(values[2] >> 3)

	bytes[9] = uint8(values[2] << 5)
	bytes[9] |= uint8(values[3] >> 20)

	bytes[10] = uint8(values[3] >> 12)

	bytes[11] = uint8(values[3] >> 4)

	bytes[12] = uint8(values[3] << 4)
	bytes[12] |= uint8(values[4] >> 21)

	bytes[13] = uint8(values[4] >> 13)

	bytes[14] = uint8(values[4] >> 5)

	bytes[15] = uint8(values[4] << 3)
	bytes[15] |= uint8(values[5] >> 22)

	bytes[16] = uint8(values[5] >> 14)

	bytes[17] = uint8(values[5] >> 6)

	bytes[18] = uint8(values[5] << 2)
	bytes[18] |= uint8(values[6] >> 23)

	bytes[19] = uint8(values[6] >> 15)

	bytes[20] = uint8(values[6] >> 7)

	bytes[21] = uint8(values[6] << 1)
	bytes[21] |= uint8(values[7] >> 24)

	bytes[22] = uint8(values[7] >> 16)

	bytes[23] = uint8(values[7] >> 8)

	bytes[24] = uint8(values[7])
}

func packBits26(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 18)

	bytes[1] = uint8(values[0] >> 10)

	bytes[2] = uint8(values[0] >> 2)

	bytes[3] = uint8(values[0] << 6)
	bytes[3] |= uint8(values[1] >> 20)

	bytes[4] = uint8(values[1] >> 12)

	bytes[5] = uint8(values[1] >> 4)

	bytes[6] = uint8(values[1] << 4)
	bytes[6] |= uint8(values[2] >> 22)

	bytes[7] = uint8(values[2] >> 14)

	bytes[8] = uint8(values[2] >> 6)

	bytes[9] = uint8(values[2] << 2)
	bytes[9] |= uint8(values[3] >> 24)

	bytes[10] = uint8(values[3] >> 16)

	bytes[11] = uint8(values[3] >> 8)

	bytes[12] = uint8(values[3])

	bytes[13] = uint8(values[4] >> 18)

	bytes[14] = uint8(values[4] >> 10)

	bytes[15] = uint8(values[4] >> 2)

	bytes[16] = uint8(values[4] << 6)
	bytes[16] |= uint8(values[5] >> 20)

	bytes[17] = uint8(values[5] >> 12)

	bytes[18] = uint8(values[5] >> 4)

	bytes[19] = uint8(values[5] << 4)
	bytes[19] |= uint8(values[6] >> 22)

	bytes[20] = uint8(values[6] >> 14)

	bytes[21] = uint8(values[6] >> 6)

	bytes[22] = uint8(values[6] << 2)
	bytes[22] |= uint8(values[7] >> 24)

	bytes[23] = uint8(values[7] >> 16)

	bytes[24] = uint8(values[7] >> 8)

	bytes[25] = uint8(values[7])
}

func packBits27(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 19)

	bytes[1] = uint8(values[0] >> 11)

	bytes[2] = uint8(values[0] >> 3)

	bytes[3] = uint8(values[0] << 5)
	bytes[3] |= uint8(values[1] >> 22)

	bytes[4] = uint8(values[1] >> 14)

	bytes[5] = uint8(values[1] >> 6)

	bytes[6] = uint8(values[1] << 2)
	bytes[6] |= uint8(values[2] >> 25)

	bytes[7] = uint8(values[2] >> 17)

	bytes[8] = uint8(values[2] >> 9)

	bytes[9] = uint8(values[2] >> 1)

	bytes[10] = uint8(values[2] << 7)
	bytes[10] |= uint8(values[3] >> 20)

	bytes[11] = uint8(values[3] >> 12)

	bytes[12] = uint8(values[3] >> 4)

	bytes[13] = uint8(values[3] << 4)
	bytes[13] |= uint8(values[4] >> 23)

	bytes[14] = uint8(values[4] >> 15)

	bytes[15] = uint8(values[4] >> 7)

	bytes[16] = uint8(values[4] << 1)
	bytes[16] |= uint8(values[5] >> 26)

	bytes[17] = uint8(values[5] >> 18)

	bytes[18] = uint8(values[5] >> 10)

	bytes[19] = uint8(values[5] >> 2)

	bytes[20] = uint8(values[5] << 6)
	bytes[20] |= uint8(values[6] >> 21)

	bytes[21] = uint8(values[6] >> 13)

	bytes[22] = uint8(values[6] >> 5)

	bytes[23] = uint8(values[6] << 3)
	bytes[23] |= uint8(values[7] >> 24)

	bytes[24] = uint8(values[7] >> 16)

	bytes[25] = uint8(values[7] >> 8)

	bytes[26] = uint8(values[7])
}

func packBits28(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 20)
	bytes[1] = uint8(values[0] >> 12)
	bytes[2] = uint8(values[0] >> 4)
	bytes[3] = uint8(values[0] << 4)
	bytes[3] |= uint8(values[1] >> 24)
	bytes[4] = uint8(values[1] >> 16)
	bytes[5] = uint8(values[1] >> 8)
	bytes[6] = uint8(values[1])
	bytes[7] = uint8(values[2] >> 20)
	bytes[8] = uint8(values[2] >> 12)
	bytes[9] = uint8(values[2] >> 4)
	bytes[10] = uint8(values[2] << 4)
	bytes[10] |= uint8(values[3] >> 24)
	bytes[11] = uint8(values[3] >> 16)
	bytes[12] = uint8(values[3] >> 8)
	bytes[13] = uint8(values[3])
	bytes[14] = uint8(values[4] >> 20)
	bytes[15] = uint8(values[4] >> 12)
	bytes[16] = uint8(values[4] >> 4)
	bytes[17] = uint8(values[4] << 4)
	bytes[17] |= uint8(values[5] >> 24)
	bytes[18] = uint8(values[5] >> 16)
	bytes[19] = uint8(values[5] >> 8)
	bytes[20] = uint8(values[5])
	bytes[21] = uint8(values[6] >> 20)
	bytes[22] = uint8(values[6] >> 12)
	bytes[23] = uint8(values[6] >> 4)
	bytes[24] = uint8(values[6] << 4)
	bytes[24] |= uint8(values[7] >> 24)
	bytes[25] = uint8(values[7] >> 16)
	bytes[26] = uint8(values[7] >> 8)
	bytes[27] = uint8(values[7])
}

func packBits29(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 21)

	bytes[1] = uint8(values[0] >> 13)

	bytes[2] = uint8(values[0] >> 5)

	bytes[3] = uint8(values[0] << 3)
	bytes[3] |= uint8(values[1] >> 26)

	bytes[4] = uint8(values[1] >> 18)

	bytes[5] = uint8(values[1] >> 10)

	bytes[6] = uint8(values[1] >> 2)

	bytes[7] = uint8(values[1] << 6)
	bytes[7] |= uint8(values[2] >> 23)

	bytes[8] = uint8(values[2] >> 15)

	bytes[9] = uint8(values[2] >> 7)

	bytes[10] = uint8(values[2] << 1)
	bytes[10] |= uint8(values[3] >> 28)

	bytes[11] = uint8(values[3] >> 20)

	bytes[12] = uint8(values[3] >> 12)

	bytes[13] = uint8(values[3] >> 4)

	bytes[14] = uint8(values[3] << 4)
	bytes[14] |= uint8(values[4] >> 25)

	bytes[15] = uint8(values[4] >> 17)

	bytes[16] = uint8(values[4] >> 9)

	bytes[17] = uint8(values[4] >> 1)

	bytes[18] = uint8(values[4] << 7)
	bytes[18] |= uint8(values[5] >> 22)

	bytes[19] = uint8(values[5] >> 14)

	bytes[20] = uint8(values[5] >> 6)

	bytes[21] = uint8(values[5] << 2)
	bytes[21] |= uint8(values[6] >> 27)

	bytes[22] = uint8(values[6] >> 19)

	bytes[23] = uint8(values[6] >> 11)

	bytes[24] = uint8(values[6] >> 3)

	bytes[25] = uint8(values[6] << 5)
	bytes[25] |= uint8(values[7] >> 24)

	bytes[26] = uint8(values[7] >> 16)

	bytes[27] = uint8(values[7] >> 8)

	bytes[28] = uint8(values[7])
}

func packBits30(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 22)
	bytes[1] = uint8(values[0] >> 14)
	bytes[2] = uint8(values[0] >> 6)

	bytes[3] = uint8(values[0] << 2)
	bytes[3] |= uint8(values[1] >> 28)
	bytes[4] = uint8(values[1] >> 20)
	bytes[5] = uint8(values[1] >> 12)
	bytes[6] = uint8(values[1] >> 4)

	bytes[7] = uint8(values[1] << 4)
	bytes[7] |= uint8(values[2] >> 26)
	bytes[8] = uint8(values[2] >> 18)
	bytes[9] = uint8(values[2] >> 10)
	bytes[10] = uint8(values[2] >> 2)

	bytes[11] = uint8(values[2] << 6)
	bytes[11] |= uint8(values[3] >> 24)
	bytes[12] = uint8(values[3] >> 16)
	bytes[13] = uint8(values[3] >> 8)
	bytes[14] = uint8(values[3])

	bytes[15] = uint8(values[4] >> 22)
	bytes[16] = uint8(values[4] >> 14)
	bytes[17] = uint8(values[4] >> 6)

	bytes[18] = uint8(values[4] << 2)
	bytes[18] |= uint8(values[5] >> 28)
	bytes[19] = uint8(values[5] >> 20)
	bytes[20] = uint8(values[5] >> 12)
	bytes[21] = uint8(values[5] >> 4)

	bytes[22] = uint8(values[5] << 4)
	bytes[22] |= uint8(values[6] >> 26)
	bytes[23] = uint8(values[6] >> 18)
	bytes[24] = uint8(values[6] >> 10)
	bytes[25] = uint8(values[6] >> 2)

	bytes[26] = uint8(values[6] << 6)
	bytes[26] |= uint8(values[7] >> 24)
	bytes[27] = uint8(values[7] >> 16)
	bytes[28] = uint8(values[7] >> 8)
	bytes[29] = uint8(values[7])
}

func packBits31(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 23)
	bytes[1] = uint8(values[0] >> 15)
	bytes[2] = uint8(values[0] >> 7)

	bytes[3] = uint8(values[0] << 1)
	bytes[3] |= uint8(values[1] >> 30)
	bytes[4] = uint8(values[1] >> 22)
	bytes[5] = uint8(values[1] >> 14)
	bytes[6] = uint8(values[1] >> 6)

	bytes[7] = uint8(values[1] << 2)
	bytes[7] |= uint8(values[2] >> 29)
	bytes[8] = uint8(values[2] >> 21)
	bytes[9] = uint8(values[2] >> 13)
	bytes[10] = uint8(values[2] >> 5)

	bytes[11] = uint8(values[2] << 3)
	bytes[11] |= uint8(values[3] >> 28)
	bytes[12] = uint8(values[3] >> 20)
	bytes[13] = uint8(values[3] >> 12)
	bytes[14] = uint8(values[3] >> 4)

	bytes[15] = uint8(values[3] << 4)
	bytes[15] |= uint8(values[4] >> 27)
	bytes[16] = uint8(values[4] >> 19)
	bytes[17] = uint8(values[4] >> 11)
	bytes[18] = uint8(values[4] >> 3)

	bytes[19] = uint8(values[4] << 5)
	bytes[19] |= uint8(values[5] >> 26)
	bytes[20] = uint8(values[5] >> 18)
	bytes[21] = uint8(values[5] >> 10)
	bytes[22] = uint8(values[5] >> 2)

	bytes[23] = uint8(values[5] << 6)
	bytes[23] |= uint8(values[6] >> 25)
	bytes[24] = uint8(values[6] >> 17)
	bytes[25] = uint8(values[6] >> 9)
	bytes[26] = uint8(values[6] >> 1)

	bytes[27] = uint8(values[6] << 7)
	bytes[27] |= uint8(values[7] >> 24)
	bytes[28] = uint8(values[7] >> 16)
	bytes[29] = uint8(values[7] >> 8)
	bytes[30] = uint8(values[7])
}

func packBits32(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 24)
	bytes[1] = uint8(values[0] >> 16)
	bytes[2] = uint8(values[0] >> 8)
	bytes[3] = uint8(values[0])

	bytes[4] = uint8(values[1] >> 24)
	bytes[5] = uint8(values[1] >> 16)
	bytes[6] = uint8(values[1] >> 8)
	bytes[7] = uint8(values[1])

	bytes[8] = uint8(values[2] >> 24)
	bytes[9] = uint8(values[2] >> 16)
	bytes[10] = uint8(values[2] >> 8)
	bytes[11] = uint8(values[2])

	bytes[12] = uint8(values[3] >> 24)
	bytes[13] = uint8(values[3] >> 16)
	bytes[14] = uint8(values[3] >> 8)
	bytes[15] = uint8(values[3])

	bytes[16] = uint8(values[4] >> 24)
	bytes[17] = uint8(values[4] >> 16)
	bytes[18] = uint8(values[4] >> 8)
	bytes[19] = uint8(values[4])

	bytes[20] = uint8(values[5] >> 24)
	bytes[21] = uint8(values[5] >> 16)
	bytes[22] = uint8(values[5] >> 8)
	bytes[23] = uint8(values[5])

	bytes[24] = uint8(values[6] >> 24)
	bytes[25] = uint8(values[6] >> 16)
	bytes[26] = uint8(values[6] >> 8)
	bytes[27] = uint8(values[6])

	bytes[28] = uint8(values[7] >> 24)
	bytes[29] = uint8(values[7] >> 16)
	bytes[30] = uint8(values[7] >> 8)
	bytes[31] = uint8(values[7])
}

func packBits33(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 25)
	bytes[1] = uint8(values[0] >> 17)
	bytes[2] = uint8(values[0] >> 9)
	bytes[3] = uint8(values[0] >> 1)

	bytes[4] = uint8(values[0] << 7)
	bytes[4] |= uint8(values[1] >> 26)
	bytes[5] = uint8(values[1] >> 18)
	bytes[6] = uint8(values[1] >> 10)
	bytes[7] = uint8(values[1] >> 2)

	bytes[8] = uint8(values[1] << 6)
	bytes[8] |= uint8(values[2] >> 27)
	bytes[9] = uint8(values[2] >> 19)
	bytes[10] = uint8(values[2] >> 11)
	bytes[11] = uint8(values[2] >> 3)

	bytes[12] = uint8(values[2] << 5)
	bytes[12] |= uint8(values[3] >> 28)
	bytes[13] = uint8(values[3] >> 20)
	bytes[14] = uint8(values[3] >> 12)
	bytes[15] = uint8(values[3] >> 4)

	bytes[16] = uint8(values[3] << 4)
	bytes[16] |= uint8(values[4] >> 29)
	bytes[17] = uint8(values[4] >> 21)
	bytes[18] = uint8(values[4] >> 13)
	bytes[19] = uint8(values[4] >> 5)

	bytes[20] = uint8(values[4] << 3)
	bytes[20] |= uint8(values[5] >> 30)
	bytes[21] = uint8(values[5] >> 22)
	bytes[22] = uint8(values[5] >> 14)
	bytes[23] = uint8(values[5] >> 6)

	bytes[24] = uint8(values[5] << 2)
	bytes[24] |= uint8(values[6] >> 31)
	bytes[25] = uint8(values[6] >> 23)
	bytes[26] = uint8(values[6] >> 15)
	bytes[27] = uint8(values[6] >> 7)

	bytes[28] = uint8(values[6] << 1)
	bytes[28] |= uint8(values[7] >> 32)
	bytes[29] = uint8(values[7] >> 24)
	bytes[30] = uint8(values[7] >> 16)
	bytes[31] = uint8(values[7] >> 8)
	bytes[32] = uint8(values[7])
}

func packBits34(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 26)
	bytes[1] = uint8(values[0] >> 18)
	bytes[2] = uint8(values[0] >> 10)
	bytes[3] = uint8(values[0] >> 2)

	bytes[4] = uint8(values[0] << 6)
	bytes[4] |= uint8(values[1] >> 28)
	bytes[5] = uint8(values[1] >> 20)
	bytes[6] = uint8(values[1] >> 12)
	bytes[7] = uint8(values[1] >> 4)

	bytes[8] = uint8(values[1] << 4)
	bytes[8] |= uint8(values[2] >> 30)
	bytes[9] = uint8(values[2] >> 22)
	bytes[10] = uint8(values[2] >> 14)
	bytes[11] = uint8(values[2] >> 6)

	bytes[12] = uint8(values[2] << 2)
	bytes[12] |= uint8(values[3] >> 32)
	bytes[13] = uint8(values[3] >> 24)
	bytes[14] = uint8(values[3] >> 16)
	bytes[15] = uint8(values[3] >> 8)
	bytes[16] = uint8(values[3])

	bytes[17] = uint8(values[4] >> 26)
	bytes[18] = uint8(values[4] >> 18)
	bytes[19] = uint8(values[4] >> 10)
	bytes[20] = uint8(values[4] >> 2)

	bytes[21] = uint8(values[4] << 6)
	bytes[21] |= uint8(values[5] >> 28)
	bytes[22] = uint8(values[5] >> 20)
	bytes[23] = uint8(values[5] >> 12)
	bytes[24] = uint8(values[5] >> 4)

	bytes[25] = uint8(values[5] << 4)
	bytes[25] |= uint8(values[6] >> 30)
	bytes[26] = uint8(values[6] >> 22)
	bytes[27] = uint8(values[6] >> 14)
	bytes[28] = uint8(values[6] >> 6)

	bytes[29] = uint8(values[6] << 2)
	bytes[29] |= uint8(values[7] >> 32)
	bytes[30] = uint8(values[7] >> 24)
	bytes[31] = uint8(values[7] >> 16)
	bytes[32] = uint8(values[7] >> 8)
	bytes[33] = uint8(values[7])
}

func packBits35(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 27)
	bytes[1] = uint8(values[0] >> 19)
	bytes[2] = uint8(values[0] >> 11)
	bytes[3] = uint8(values[0] >> 3)

	bytes[4] = uint8(values[0] << 5)
	bytes[4] |= uint8(values[1] >> 30)
	bytes[5] = uint8(values[1] >> 22)
	bytes[6] = uint8(values[1] >> 14)
	bytes[7] = uint8(values[1] >> 6)

	bytes[8] = uint8(values[1] << 2)
	bytes[8] |= uint8(values[2] >> 33)
	bytes[9] = uint8(values[2] >> 25)
	bytes[10] = uint8(values[2] >> 17)
	bytes[11] = uint8(values[2] >> 9)
	bytes[12] = uint8(values[2] >> 1)

	bytes[13] = uint8(values[2] << 7)
	bytes[13] |= uint8(values[3] >> 28)
	bytes[14] = uint8(values[3] >> 20)
	bytes[15] = uint8(values[3] >> 12)
	bytes[16] = uint8(values[3] >> 4)

	bytes[17] = uint8(values[3] << 4)
	bytes[17] |= uint8(values[4] >> 31)
	bytes[18] = uint8(values[4] >> 23)
	bytes[19] = uint8(values[4] >> 15)
	bytes[20] = uint8(values[4] >> 7)

	bytes[21] = uint8(values[4] << 1)
	bytes[21] |= uint8(values[5] >> 34)
	bytes[22] = uint8(values[5] >> 26)
	bytes[23] = uint8(values[5] >> 18)
	bytes[24] = uint8(values[5] >> 10)
	bytes[25] = uint8(values[5] >> 2)

	bytes[26] = uint8(values[5] << 6)
	bytes[26] |= uint8(values[6] >> 29)
	bytes[27] = uint8(values[6] >> 21)
	bytes[28] = uint8(values[6] >> 13)
	bytes[29] = uint8(values[6] >> 5)

	bytes[30] = uint8(values[6] << 3)
	bytes[30] |= uint8(values[7] >> 32)
	bytes[31] = uint8(values[7] >> 24)
	bytes[32] = uint8(values[7] >> 16)
	bytes[33] = uint8(values[7] >> 8)
	bytes[34] = uint8(values[7])
}

func packBits36(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 28)
	bytes[1] = uint8(values[0] >> 20)
	bytes[2] = uint8(values[0] >> 12)
	bytes[3] = uint8(values[0] >> 4)

	bytes[4] = uint8(values[0] << 4)
	bytes[4] |= uint8(values[1] >> 32)
	bytes[5] = uint8(values[1] >> 24)
	bytes[6] = uint8(values[1] >> 16)
	bytes[7] = uint8(values[1] >> 8)
	bytes[8] = uint8(values[1])

	bytes[9] = uint8(values[2] >> 28)
	bytes[10] = uint8(values[2] >> 20)
	bytes[11] = uint8(values[2] >> 12)
	bytes[12] = uint8(values[2] >> 4)

	bytes[13] = uint8(values[2] << 4)
	bytes[13] |= uint8(values[3] >> 32)
	bytes[14] = uint8(values[3] >> 24)
	bytes[15] = uint8(values[3] >> 16)
	bytes[16] = uint8(values[3] >> 8)
	bytes[17] = uint8(values[3])

	bytes[18] = uint8(values[4] >> 28)
	bytes[19] = uint8(values[4] >> 20)
	bytes[20] = uint8(values[4] >> 12)
	bytes[21] = uint8(values[4] >> 4)

	bytes[22] = uint8(values[4] << 4)
	bytes[22] |= uint8(values[5] >> 32)
	bytes[23] = uint8(values[5] >> 24)
	bytes[24] = uint8(values[5] >> 16)
	bytes[25] = uint8(values[5] >> 8)
	bytes[26] = uint8(values[5])

	bytes[27] = uint8(values[6] >> 28)
	bytes[28] = uint8(values[6] >> 20)
	bytes[29] = uint8(values[6] >> 12)
	bytes[30] = uint8(values[6] >> 4)

	bytes[31] = uint8(values[6] << 4)
	bytes[31] |= uint8(values[7] >> 32)
	bytes[32] = uint8(values[7] >> 24)
	bytes[33] = uint8(values[7] >> 16)
	bytes[34] = uint8(values[7] >> 8)
	bytes[35] = uint8(values[7])
}

func packBits37(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 29)
	bytes[1] = uint8(values[0] >> 21)
	bytes[2] = uint8(values[0] >> 13)
	bytes[3] = uint8(values[0] >> 5)

	bytes[4] = uint8(values[0] << 3)
	bytes[4] |= uint8(values[1] >> 34)
	bytes[5] = uint8(values[1] >> 26)
	bytes[6] = uint8(values[1] >> 18)
	bytes[7] = uint8(values[1] >> 10)
	bytes[8] = uint8(values[1] >> 2)

	bytes[9] = uint8(values[1] << 6)
	bytes[9] |= uint8(values[2] >> 31)
	bytes[10] = uint8(values[2] >> 23)
	bytes[11] = uint8(values[2] >> 15)
	bytes[12] = uint8(values[2] >> 7)

	bytes[13] = uint8(values[2] << 1)
	bytes[13] |= uint8(values[3] >> 36)
	bytes[14] = uint8(values[3] >> 28)
	bytes[15] = uint8(values[3] >> 20)
	bytes[16] = uint8(values[3] >> 12)
	bytes[17] = uint8(values[3] >> 4)

	bytes[18] = uint8(values[3] << 4)
	bytes[18] |= uint8(values[4] >> 33)
	bytes[19] = uint8(values[4] >> 25)
	bytes[20] = uint8(values[4] >> 17)
	bytes[21] = uint8(values[4] >> 9)
	bytes[22] = uint8(values[4] >> 1)

	bytes[23] = uint8(values[4] << 7)
	bytes[23] |= uint8(values[5] >> 30)
	bytes[24] = uint8(values[5] >> 22)
	bytes[25] = uint8(values[5] >> 14)
	bytes[26] = uint8(values[5] >> 6)

	bytes[27] = uint8(values[5] << 2)
	bytes[27] |= uint8(values[6] >> 35)
	bytes[28] = uint8(values[6] >> 27)
	bytes[29] = uint8(values[6] >> 19)
	bytes[30] = uint8(values[6] >> 11)
	bytes[31] = uint8(values[6] >> 3)

	bytes[32] = uint8(values[6] << 5)
	bytes[32] |= uint8(values[7] >> 32)
	bytes[33] = uint8(values[7] >> 24)
	bytes[34] = uint8(values[7] >> 16)
	bytes[35] = uint8(values[7] >> 8)
	bytes[36] = uint8(values[7])
}

func packBits38(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 30)
	bytes[1] = uint8(values[0] >> 22)
	bytes[2] = uint8(values[0] >> 14)
	bytes[3] = uint8(values[0] >> 6)

	bytes[4] = uint8(values[0] << 2)
	bytes[4] |= uint8(values[1] >> 36)
	bytes[5] = uint8(values[1] >> 28)
	bytes[6] = uint8(values[1] >> 20)
	bytes[7] = uint8(values[1] >> 12)
	bytes[8] = uint8(values[1] >> 4)

	bytes[9] = uint8(values[1] << 4)
	bytes[9] |= uint8(values[2] >> 34)
	bytes[10] = uint8(values[2] >> 26)
	bytes[11] = uint8(values[2] >> 18)
	bytes[12] = uint8(values[2] >> 10)
	bytes[13] = uint8(values[2] >> 2)

	bytes[14] = uint8(values[2] << 6)
	bytes[14] |= uint8(values[3] >> 32)
	bytes[15] = uint8(values[3] >> 24)
	bytes[16] = uint8(values[3] >> 16)
	bytes[17] = uint8(values[3] >> 8)
	bytes[18] = uint8(values[3])

	bytes[19] = uint8(values[4] >> 30)
	bytes[20] = uint8(values[4] >> 22)
	bytes[21] = uint8(values[4] >> 14)
	bytes[22] = uint8(values[4] >> 6)

	bytes[23] = uint8(values[4] << 2)
	bytes[23] |= uint8(values[5] >> 36)
	bytes[24] = uint8(values[5] >> 28)
	bytes[25] = uint8(values[5] >> 20)
	bytes[26] = uint8(values[5] >> 12)
	bytes[27] = uint8(values[5] >> 4)

	bytes[28] = uint8(values[5] << 4)
	bytes[28] |= uint8(values[6] >> 34)
	bytes[29] = uint8(values[6] >> 26)
	bytes[30] = uint8(values[6] >> 18)
	bytes[31] = uint8(values[6] >> 10)
	bytes[32] = uint8(values[6] >> 2)

	bytes[33] = uint8(values[6] << 6)
	bytes[33] |= uint8(values[7] >> 32)
	bytes[34] = uint8(values[7] >> 24)
	bytes[35] = uint8(values[7] >> 16)
	bytes[36] = uint8(values[7] >> 8)
	bytes[37] = uint8(values[7])
}

func packBits39(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 31)
	bytes[1] = uint8(values[0] >> 23)
	bytes[2] = uint8(values[0] >> 15)
	bytes[3] = uint8(values[0] >> 7)

	bytes[4] = uint8(values[0] << 1)
	bytes[4] |= uint8(values[1] >> 38)
	bytes[5] = uint8(values[1] >> 30)
	bytes[6] = uint8(values[1] >> 22)
	bytes[7] = uint8(values[1] >> 14)
	bytes[8] = uint8(values[1] >> 6)

	bytes[9] = uint8(values[1] << 2)
	bytes[9] |= uint8(values[2] >> 37)
	bytes[10] = uint8(values[2] >> 29)
	bytes[11] = uint8(values[2] >> 21)
	bytes[12] = uint8(values[2] >> 13)
	bytes[13] = uint8(values[2] >> 5)

	bytes[14] = uint8(values[2] << 3)
	bytes[14] |= uint8(values[3] >> 36)
	bytes[15] = uint8(values[3] >> 28)
	bytes[16] = uint8(values[3] >> 20)
	bytes[17] = uint8(values[3] >> 12)
	bytes[18] = uint8(values[3] >> 4)

	bytes[19] = uint8(values[3] << 4)
	bytes[19] |= uint8(values[4] >> 35)
	bytes[20] = uint8(values[4] >> 27)
	bytes[21] = uint8(values[4] >> 19)
	bytes[22] = uint8(values[4] >> 11)
	bytes[23] = uint8(values[4] >> 3)

	bytes[24] = uint8(values[4] << 5)
	bytes[24] |= uint8(values[5] >> 34)
	bytes[25] = uint8(values[5] >> 26)
	bytes[26] = uint8(values[5] >> 18)
	bytes[27] = uint8(values[5] >> 10)
	bytes[28] = uint8(values[5] >> 2)

	bytes[29] = uint8(values[5] << 6)
	bytes[29] |= uint8(values[6] >> 33)
	bytes[30] = uint8(values[6] >> 25)
	bytes[31] = uint8(values[6] >> 17)
	bytes[32] = uint8(values[6] >> 9)
	bytes[33] = uint8(values[6] >> 1)

	bytes[34] = uint8(values[6] << 7)
	bytes[34] |= uint8(values[7] >> 32)
	bytes[35] = uint8(values[7] >> 24)
	bytes[36] = uint8(values[7] >> 16)
	bytes[37] = uint8(values[7] >> 8)
	bytes[38] = uint8(values[7])
}

func packBits40(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 32)
	bytes[1] = uint8(values[0] >> 24)
	bytes[2] = uint8(values[0] >> 16)
	bytes[3] = uint8(values[0] >> 8)
	bytes[4] = uint8(values[0])

	bytes[5] = uint8(values[1] >> 32)
	bytes[6] = uint8(values[1] >> 24)
	bytes[7] = uint8(values[1] >> 16)
	bytes[8] = uint8(values[1] >> 8)
	bytes[9] = uint8(values[1])

	bytes[10] = uint8(values[2] >> 32)
	bytes[11] = uint8(values[2] >> 24)
	bytes[12] = uint8(values[2] >> 16)
	bytes[13] = uint8(values[2] >> 8)
	bytes[14] = uint8(values[2])

	bytes[15] = uint8(values[3] >> 32)
	bytes[16] = uint8(values[3] >> 24)
	bytes[17] = uint8(values[3] >> 16)
	bytes[18] = uint8(values[3] >> 8)
	bytes[19] = uint8(values[3])

	bytes[20] = uint8(values[4] >> 32)
	bytes[21] = uint8(values[4] >> 24)
	bytes[22] = uint8(values[4] >> 16)
	bytes[23] = uint8(values[4] >> 8)
	bytes[24] = uint8(values[4])

	bytes[25] = uint8(values[5] >> 32)
	bytes[26] = uint8(values[5] >> 24)
	bytes[27] = uint8(values[5] >> 16)
	bytes[28] = uint8(values[5] >> 8)
	bytes[29] = uint8(values[5])

	bytes[30] = uint8(values[6] >> 32)
	bytes[31] = uint8(values[6] >> 24)
	bytes[32] = uint8(values[6] >> 16)
	bytes[33] = uint8(values[6] >> 8)
	bytes[34] = uint8(values[6])

	bytes[35] = uint8(values[7] >> 32)
	bytes[36] = uint8(values[7] >> 24)
	bytes[37] = uint8(values[7] >> 16)
	bytes[38] = uint8(values[7] >> 8)
	bytes[39] = uint8(values[7])
}

func packBits41(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 33)
	bytes[1] = uint8(values[0] >> 25)
	bytes[2] = uint8(values[0] >> 17)
	bytes[3] = uint8(values[0] >> 9)
	bytes[4] = uint8(values[0] >> 1)

	bytes[5] = uint8(values[0] << 7)
	bytes[5] |= uint8(values[1] >> 34)
	bytes[6] = uint8(values[1] >> 26)
	bytes[7] = uint8(values[1] >> 18)
	bytes[8] = uint8(values[1] >> 10)
	bytes[9] = uint8(values[1] >> 2)

	bytes[10] = uint8(values[1] << 6)
	bytes[10] |= uint8(values[2] >> 35)
	bytes[11] = uint8(values[2] >> 27)
	bytes[12] = uint8(values[2] >> 19)
	bytes[13] = uint8(values[2] >> 11)
	bytes[14] = uint8(values[2] >> 3)

	bytes[15] = uint8(values[2] << 5)
	bytes[15] |= uint8(values[3] >> 36)
	bytes[16] = uint8(values[3] >> 28)
	bytes[17] = uint8(values[3] >> 20)
	bytes[18] = uint8(values[3] >> 12)
	bytes[19] = uint8(values[3] >> 4)

	bytes[20] = uint8(values[3] << 4)
	bytes[20] |= uint8(values[4] >> 37)
	bytes[21] = uint8(values[4] >> 29)
	bytes[22] = uint8(values[4] >> 21)
	bytes[23] = uint8(values[4] >> 13)
	bytes[24] = uint8(values[4] >> 5)

	bytes[25] = uint8(values[4] << 3)
	bytes[25] |= uint8(values[5] >> 38)
	bytes[26] = uint8(values[5] >> 30)
	bytes[27] = uint8(values[5] >> 22)
	bytes[28] = uint8(values[5] >> 14)
	bytes[29] = uint8(values[5] >> 6)

	bytes[30] = uint8(values[5] << 2)
	bytes[30] |= uint8(values[6] >> 39)
	bytes[31] = uint8(values[6] >> 31)
	bytes[32] = uint8(values[6] >> 23)
	bytes[33] = uint8(values[6] >> 15)
	bytes[34] = uint8(values[6] >> 7)

	bytes[35] = uint8(values[6] << 1)
	bytes[35] |= uint8(values[7] >> 40)
	bytes[36] = uint8(values[7] >> 32)
	bytes[37] = uint8(values[7] >> 24)
	bytes[38] = uint8(values[7] >> 16)
	bytes[39] = uint8(values[7] >> 8)
	bytes[40] = uint8(values[7])
}

func packBits42(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 34)
	bytes[1] = uint8(values[0] >> 26)
	bytes[2] = uint8(values[0] >> 18)
	bytes[3] = uint8(values[0] >> 10)
	bytes[4] = uint8(values[0] >> 2)

	bytes[5] = uint8(values[0] << 6)
	bytes[5] |= uint8(values[1] >> 36)
	bytes[6] = uint8(values[1] >> 28)
	bytes[7] = uint8(values[1] >> 20)
	bytes[8] = uint8(values[1] >> 12)
	bytes[9] = uint8(values[1] >> 4)

	bytes[10] = uint8(values[1] << 4)
	bytes[10] |= uint8(values[2] >> 38)
	bytes[11] = uint8(values[2] >> 30)
	bytes[12] = uint8(values[2] >> 22)
	bytes[13] = uint8(values[2] >> 14)
	bytes[14] = uint8(values[2] >> 6)

	bytes[15] = uint8(values[2] << 2)
	bytes[15] |= uint8(values[3] >> 40)
	bytes[16] = uint8(values[3] >> 32)
	bytes[17] = uint8(values[3] >> 24)
	bytes[18] = uint8(values[3] >> 16)
	bytes[19] = uint8(values[3] >> 8)
	bytes[20] = uint8(values[3])

	bytes[21] = uint8(values[4] >> 34)
	bytes[22] = uint8(values[4] >> 26)
	bytes[23] = uint8(values[4] >> 18)
	bytes[24] = uint8(values[4] >> 10)
	bytes[25] = uint8(values[4] >> 2)

	bytes[26] = uint8(values[4] << 6)
	bytes[26] |= uint8(values[5] >> 36)
	bytes[27] = uint8(values[5] >> 28)
	bytes[28] = uint8(values[5] >> 20)
	bytes[29] = uint8(values[5] >> 12)
	bytes[30] = uint8(values[5] >> 4)

	bytes[31] = uint8(values[5] << 4)
	bytes[31] |= uint8(values[6] >> 38)
	bytes[32] = uint8(values[6] >> 30)
	bytes[33] = uint8(values[6] >> 22)
	bytes[34] = uint8(values[6] >> 14)
	bytes[35] = uint8(values[6] >> 6)

	bytes[36] = uint8(values[6] << 2)
	bytes[36] |= uint8(values[7] >> 40)
	bytes[37] = uint8(values[7] >> 32)
	bytes[38] = uint8(values[7] >> 24)
	bytes[39] = uint8(values[7] >> 16)
	bytes[40] = uint8(values[7] >> 8)
	bytes[41] = uint8(values[7])
}

func packBits43(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 35)
	bytes[1] = uint8(values[0] >> 27)
	bytes[2] = uint8(values[0] >> 19)
	bytes[3] = uint8(values[0] >> 11)
	bytes[4] = uint8(values[0] >> 3)

	bytes[5] = uint8(values[0] << 5)
	bytes[5] |= uint8(values[1] >> 38)
	bytes[6] = uint8(values[1] >> 30)
	bytes[7] = uint8(values[1] >> 22)
	bytes[8] = uint8(values[1] >> 14)
	bytes[9] = uint8(values[1] >> 6)

	bytes[10] = uint8(values[1] << 2)
	bytes[10] |= uint8(values[2] >> 41)
	bytes[11] = uint8(values[2] >> 33)
	bytes[12] = uint8(values[2] >> 25)
	bytes[13] = uint8(values[2] >> 17)
	bytes[14] = uint8(values[2] >> 9)
	bytes[15] = uint8(values[2] >> 1)

	bytes[16] = uint8(values[2] << 7)
	bytes[16] |= uint8(values[3] >> 36)
	bytes[17] = uint8(values[3] >> 28)
	bytes[18] = uint8(values[3] >> 20)
	bytes[19] = uint8(values[3] >> 12)
	bytes[20] = uint8(values[3] >> 4)

	bytes[21] = uint8(values[3] << 4)
	bytes[21] |= uint8(values[4] >> 39)
	bytes[22] = uint8(values[4] >> 31)
	bytes[23] = uint8(values[4] >> 23)
	bytes[24] = uint8(values[4] >> 15)
	bytes[25] = uint8(values[4] >> 7)

	bytes[26] = uint8(values[4] << 1)
	bytes[26] |= uint8(values[5] >> 42)
	bytes[27] = uint8(values[5] >> 34)
	bytes[28] = uint8(values[5] >> 26)
	bytes[29] = uint8(values[5] >> 18)
	bytes[30] = uint8(values[5] >> 10)
	bytes[31] = uint8(values[5] >> 2)

	bytes[32] = uint8(values[5] << 6)
	bytes[32] |= uint8(values[6] >> 37)
	bytes[33] = uint8(values[6] >> 29)
	bytes[34] = uint8(values[6] >> 21)
	bytes[35] = uint8(values[6] >> 13)
	bytes[36] = uint8(values[6] >> 5)

	bytes[37] = uint8(values[6] << 3)
	bytes[37] |= uint8(values[7] >> 40)
	bytes[38] = uint8(values[7] >> 32)
	bytes[39] = uint8(values[7] >> 24)
	bytes[40] = uint8(values[7] >> 16)
	bytes[41] = uint8(values[7] >> 8)
	bytes[42] = uint8(values[7])
}

func packBits44(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 36)
	bytes[1] = uint8(values[0] >> 28)
	bytes[2] = uint8(values[0] >> 20)
	bytes[3] = uint8(values[0] >> 12)
	bytes[4] = uint8(values[0] >> 4)

	bytes[5] = uint8(values[0] << 4)
	bytes[5] |= uint8(values[1] >> 40)
	bytes[6] = uint8(values[1] >> 32)
	bytes[7] = uint8(values[1] >> 24)
	bytes[8] = uint8(values[1] >> 16)
	bytes[9] = uint8(values[1] >> 8)
	bytes[10] = uint8(values[1])

	bytes[11] = uint8(values[2] >> 36)
	bytes[12] = uint8(values[2] >> 28)
	bytes[13] = uint8(values[2] >> 20)
	bytes[14] = uint8(values[2] >> 12)
	bytes[15] = uint8(values[2] >> 4)

	bytes[16] = uint8(values[2] << 4)
	bytes[16] |= uint8(values[3] >> 40)
	bytes[17] = uint8(values[3] >> 32)
	bytes[18] = uint8(values[3] >> 24)
	bytes[19] = uint8(values[3] >> 16)
	bytes[20] = uint8(values[3] >> 8)
	bytes[21] = uint8(values[3])

	bytes[22] = uint8(values[4] >> 36)
	bytes[23] = uint8(values[4] >> 28)
	bytes[24] = uint8(values[4] >> 20)
	bytes[25] = uint8(values[4] >> 12)
	bytes[26] = uint8(values[4] >> 4)

	bytes[27] = uint8(values[4] << 4)
	bytes[27] |= uint8(values[5] >> 40)
	bytes[28] = uint8(values[5] >> 32)
	bytes[29] = uint8(values[5] >> 24)
	bytes[30] = uint8(values[5] >> 16)
	bytes[31] = uint8(values[5] >> 8)
	bytes[32] = uint8(values[5])

	bytes[33] = uint8(values[6] >> 36)
	bytes[34] = uint8(values[6] >> 28)
	bytes[35] = uint8(values[6] >> 20)
	bytes[36] = uint8(values[6] >> 12)
	bytes[37] = uint8(values[6] >> 4)

	bytes[38] = uint8(values[6] << 4)
	bytes[38] |= uint8(values[7] >> 40)
	bytes[39] = uint8(values[7] >> 32)
	bytes[40] = uint8(values[7] >> 24)
	bytes[41] = uint8(values[7] >> 16)
	bytes[42] = uint8(values[7] >> 8)
	bytes[43] = uint8(values[7])
}

func packBits45(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 37)
	bytes[1] = uint8(values[0] >> 29)
	bytes[2] = uint8(values[0] >> 21)
	bytes[3] = uint8(values[0] >> 13)
	bytes[4] = uint8(values[0] >> 5)

	bytes[5] = uint8(values[0] << 3)
	bytes[5] |= uint8(values[1] >> 42)
	bytes[6] = uint8(values[1] >> 34)
	bytes[7] = uint8(values[1] >> 26)
	bytes[8] = uint8(values[1] >> 18)
	bytes[9] = uint8(values[1] >> 10)
	bytes[10] = uint8(values[1] >> 2)

	bytes[11] = uint8(values[1] << 6)
	bytes[11] |= uint8(values[2] >> 39)
	bytes[12] = uint8(values[2] >> 31)
	bytes[13] = uint8(values[2] >> 23)
	bytes[14] = uint8(values[2] >> 15)
	bytes[15] = uint8(values[2] >> 7)

	bytes[16] = uint8(values[2] << 1)
	bytes[16] |= uint8(values[3] >> 44)
	bytes[17] = uint8(values[3] >> 36)
	bytes[18] = uint8(values[3] >> 28)
	bytes[19] = uint8(values[3] >> 20)
	bytes[20] = uint8(values[3] >> 12)
	bytes[21] = uint8(values[3] >> 4)

	bytes[22] = uint8(values[3] << 4)
	bytes[22] |= uint8(values[4] >> 41)
	bytes[23] = uint8(values[4] >> 33)
	bytes[24] = uint8(values[4] >> 25)
	bytes[25] = uint8(values[4] >> 17)
	bytes[26] = uint8(values[4] >> 9)
	bytes[27] = uint8(values[4] >> 1)

	bytes[28] = uint8(values[4] << 7)
	bytes[28] |= uint8(values[5] >> 38)
	bytes[29] = uint8(values[5] >> 30)
	bytes[30] = uint8(values[5] >> 22)
	bytes[31] = uint8(values[5] >> 14)
	bytes[32] = uint8(values[5] >> 6)

	bytes[33] = uint8(values[5] << 2)
	bytes[33] |= uint8(values[6] >> 43)
	bytes[34] = uint8(values[6] >> 35)
	bytes[35] = uint8(values[6] >> 27)
	bytes[36] = uint8(values[6] >> 19)
	bytes[37] = uint8(values[6] >> 11)
	bytes[38] = uint8(values[6] >> 3)

	bytes[39] = uint8(values[6] << 5)
	bytes[39] |= uint8(values[7] >> 40)
	bytes[40] = uint8(values[7] >> 32)
	bytes[41] = uint8(values[7] >> 24)
	bytes[42] = uint8(values[7] >> 16)
	bytes[43] = uint8(values[7] >> 8)
	bytes[44] = uint8(values[7])
}

func packBits46(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 38)
	bytes[1] = uint8(values[0] >> 30)
	bytes[2] = uint8(values[0] >> 22)
	bytes[3] = uint8(values[0] >> 14)
	bytes[4] = uint8(values[0] >> 6)

	bytes[5] = uint8(values[0] << 2)
	bytes[5] |= uint8(values[1] >> 44)
	bytes[6] = uint8(values[1] >> 36)
	bytes[7] = uint8(values[1] >> 28)
	bytes[8] = uint8(values[1] >> 20)
	bytes[9] = uint8(values[1] >> 12)
	bytes[10] = uint8(values[1] >> 4)

	bytes[11] = uint8(values[1] << 4)
	bytes[11] |= uint8(values[2] >> 42)
	bytes[12] = uint8(values[2] >> 34)
	bytes[13] = uint8(values[2] >> 26)
	bytes[14] = uint8(values[2] >> 18)
	bytes[15] = uint8(values[2] >> 10)
	bytes[16] = uint8(values[2] >> 2)

	bytes[17] = uint8(values[2] << 6)
	bytes[17] |= uint8(values[3] >> 40)
	bytes[18] = uint8(values[3] >> 32)
	bytes[19] = uint8(values[3] >> 24)
	bytes[20] = uint8(values[3] >> 16)
	bytes[21] = uint8(values[3] >> 8)
	bytes[22] = uint8(values[3])

	bytes[23] = uint8(values[4] >> 38)
	bytes[24] = uint8(values[4] >> 30)
	bytes[25] = uint8(values[4] >> 22)
	bytes[26] = uint8(values[4] >> 14)
	bytes[27] = uint8(values[4] >> 6)

	bytes[28] = uint8(values[4] << 2)
	bytes[28] |= uint8(values[5] >> 44)
	bytes[29] = uint8(values[5] >> 36)
	bytes[30] = uint8(values[5] >> 28)
	bytes[31] = uint8(values[5] >> 20)
	bytes[32] = uint8(values[5] >> 12)
	bytes[33] = uint8(values[5] >> 4)

	bytes[34] = uint8(values[5] << 4)
	bytes[34] |= uint8(values[6] >> 42)
	bytes[35] = uint8(values[6] >> 34)
	bytes[36] = uint8(values[6] >> 26)
	bytes[37] = uint8(values[6] >> 18)
	bytes[38] = uint8(values[6] >> 10)
	bytes[39] = uint8(values[6] >> 2)

	bytes[40] = uint8(values[6] << 6)
	bytes[40] |= uint8(values[7] >> 40)
	bytes[41] = uint8(values[7] >> 32)
	bytes[42] = uint8(values[7] >> 24)
	bytes[43] = uint8(values[7] >> 16)
	bytes[44] = uint8(values[7] >> 8)
	bytes[45] = uint8(values[7])
}

func packBits47(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 39)
	bytes[1] = uint8(values[0] >> 31)
	bytes[2] = uint8(values[0] >> 23)
	bytes[3] = uint8(values[0] >> 15)
	bytes[4] = uint8(values[0] >> 7)

	bytes[5] = uint8(values[0] << 1)
	bytes[5] |= uint8(values[1] >> 46)
	bytes[6] = uint8(values[1] >> 38)
	bytes[7] = uint8(values[1] >> 30)
	bytes[8] = uint8(values[1] >> 22)
	bytes[9] = uint8(values[1] >> 14)
	bytes[10] = uint8(values[1] >> 6)

	bytes[11] = uint8(values[1] << 2)
	bytes[11] |= uint8(values[2] >> 45)
	bytes[12] = uint8(values[2] >> 37)
	bytes[13] = uint8(values[2] >> 29)
	bytes[14] = uint8(values[2] >> 21)
	bytes[15] = uint8(values[2] >> 13)
	bytes[16] = uint8(values[2] >> 5)

	bytes[17] = uint8(values[2] << 3)
	bytes[17] |= uint8(values[3] >> 44)
	bytes[18] = uint8(values[3] >> 36)
	bytes[19] = uint8(values[3] >> 28)
	bytes[20] = uint8(values[3] >> 20)
	bytes[21] = uint8(values[3] >> 12)
	bytes[22] = uint8(values[3] >> 4)

	bytes[23] = uint8(values[3] << 4)
	bytes[23] |= uint8(values[4] >> 43)
	bytes[24] = uint8(values[4] >> 35)
	bytes[25] = uint8(values[4] >> 27)
	bytes[26] = uint8(values[4] >> 19)
	bytes[27] = uint8(values[4] >> 11)
	bytes[28] = uint8(values[4] >> 3)

	bytes[29] = uint8(values[4] << 5)
	bytes[29] |= uint8(values[5] >> 42)
	bytes[30] = uint8(values[5] >> 34)
	bytes[31] = uint8(values[5] >> 26)
	bytes[32] = uint8(values[5] >> 18)
	bytes[33] = uint8(values[5] >> 10)
	bytes[34] = uint8(values[5] >> 2)

	bytes[35] = uint8(values[5] << 6)
	bytes[35] |= uint8(values[6] >> 41)
	bytes[36] = uint8(values[6] >> 33)
	bytes[37] = uint8(values[6] >> 25)
	bytes[38] = uint8(values[6] >> 17)
	bytes[39] = uint8(values[6] >> 9)
	bytes[40] = uint8(values[6] >> 1)

	bytes[41] = uint8(values[6] << 7)
	bytes[41] |= uint8(values[7] >> 40)
	bytes[42] = uint8(values[7] >> 32)
	bytes[43] = uint8(values[7] >> 24)
	bytes[44] = uint8(values[7] >> 16)
	bytes[45] = uint8(values[7] >> 8)
	bytes[46] = uint8(values[7])
}

func packBits48(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 40)
	bytes[1] = uint8(values[0] >> 32)
	bytes[2] = uint8(values[0] >> 24)
	bytes[3] = uint8(values[0] >> 16)
	bytes[4] = uint8(values[0] >> 8)
	bytes[5] = uint8(values[0])

	bytes[6] = uint8(values[1] >> 40)
	bytes[7] = uint8(values[1] >> 32)
	bytes[8] = uint8(values[1] >> 24)
	bytes[9] = uint8(values[1] >> 16)
	bytes[10] = uint8(values[1] >> 8)
	bytes[11] = uint8(values[1])

	bytes[12] = uint8(values[2] >> 40)
	bytes[13] = uint8(values[2] >> 32)
	bytes[14] = uint8(values[2] >> 24)
	bytes[15] = uint8(values[2] >> 16)
	bytes[16] = uint8(values[2] >> 8)
	bytes[17] = uint8(values[2])

	bytes[18] = uint8(values[3] >> 40)
	bytes[19] = uint8(values[3] >> 32)
	bytes[20] = uint8(values[3] >> 24)
	bytes[21] = uint8(values[3] >> 16)
	bytes[22] = uint8(values[3] >> 8)
	bytes[23] = uint8(values[3])

	bytes[24] = uint8(values[4] >> 40)
	bytes[25] = uint8(values[4] >> 32)
	bytes[26] = uint8(values[4] >> 24)
	bytes[27] = uint8(values[4] >> 16)
	bytes[28] = uint8(values[4] >> 8)
	bytes[29] = uint8(values[4])

	bytes[30] = uint8(values[5] >> 40)
	bytes[31] = uint8(values[5] >> 32)
	bytes[32] = uint8(values[5] >> 24)
	bytes[33] = uint8(values[5] >> 16)
	bytes[34] = uint8(values[5] >> 8)
	bytes[35] = uint8(values[5])

	bytes[36] = uint8(values[6] >> 40)
	bytes[37] = uint8(values[6] >> 32)
	bytes[38] = uint8(values[6] >> 24)
	bytes[39] = uint8(values[6] >> 16)
	bytes[40] = uint8(values[6] >> 8)
	bytes[41] = uint8(values[6])

	bytes[42] = uint8(values[7] >> 40)
	bytes[43] = uint8(values[7] >> 32)
	bytes[44] = uint8(values[7] >> 24)
	bytes[45] = uint8(values[7] >> 16)
	bytes[46] = uint8(values[7] >> 8)
	bytes[47] = uint8(values[7])
}

func packBits49(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 41)
	bytes[1] = uint8(values[0] >> 33)
	bytes[2] = uint8(values[0] >> 25)
	bytes[3] = uint8(values[0] >> 17)
	bytes[4] = uint8(values[0] >> 9)
	bytes[5] = uint8(values[0] >> 1)

	bytes[6] = uint8(values[0] << 7)
	bytes[6] |= uint8(values[1] >> 42)
	bytes[7] = uint8(values[1] >> 34)
	bytes[8] = uint8(values[1] >> 26)
	bytes[9] = uint8(values[1] >> 18)
	bytes[10] = uint8(values[1] >> 10)
	bytes[11] = uint8(values[1] >> 2)

	bytes[12] = uint8(values[1] << 6)
	bytes[12] |= uint8(values[2] >> 43)
	bytes[13] = uint8(values[2] >> 35)
	bytes[14] = uint8(values[2] >> 27)
	bytes[15] = uint8(values[2] >> 19)
	bytes[16] = uint8(values[2] >> 11)
	bytes[17] = uint8(values[2] >> 3)

	bytes[18] = uint8(values[2] << 5)
	bytes[18] |= uint8(values[3] >> 44)
	bytes[19] = uint8(values[3] >> 36)
	bytes[20] = uint8(values[3] >> 28)
	bytes[21] = uint8(values[3] >> 20)
	bytes[22] = uint8(values[3] >> 12)
	bytes[23] = uint8(values[3] >> 4)

	bytes[24] = uint8(values[3] << 4)
	bytes[24] |= uint8(values[4] >> 45)
	bytes[25] = uint8(values[4] >> 37)
	bytes[26] = uint8(values[4] >> 29)
	bytes[27] = uint8(values[4] >> 21)
	bytes[28] = uint8(values[4] >> 13)
	bytes[29] = uint8(values[4] >> 5)

	bytes[30] = uint8(values[4] << 3)
	bytes[30] |= uint8(values[5] >> 46)
	bytes[31] = uint8(values[5] >> 38)
	bytes[32] = uint8(values[5] >> 30)
	bytes[33] = uint8(values[5] >> 22)
	bytes[34] = uint8(values[5] >> 14)
	bytes[35] = uint8(values[5] >> 6)

	bytes[36] = uint8(values[5] << 2)
	bytes[36] |= uint8(values[6] >> 47)
	bytes[37] = uint8(values[6] >> 39)
	bytes[38] = uint8(values[6] >> 31)
	bytes[39] = uint8(values[6] >> 23)
	bytes[40] = uint8(values[6] >> 15)
	bytes[41] = uint8(values[6] >> 7)

	bytes[42] = uint8(values[6] << 1)
	bytes[42] |= uint8(values[7] >> 48)
	bytes[43] = uint8(values[7] >> 40)
	bytes[44] = uint8(values[7] >> 32)
	bytes[45] = uint8(values[7] >> 24)
	bytes[46] = uint8(values[7] >> 16)
	bytes[47] = uint8(values[7] >> 8)
	bytes[48] = uint8(values[7])
}

func packBits50(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 42)
	bytes[1] = uint8(values[0] >> 34)
	bytes[2] = uint8(values[0] >> 26)
	bytes[3] = uint8(values[0] >> 18)
	bytes[4] = uint8(values[0] >> 10)
	bytes[5] = uint8(values[0] >> 2)

	bytes[6] = uint8(values[0] << 6)
	bytes[6] |= uint8(values[1] >> 44)
	bytes[7] = uint8(values[1] >> 36)
	bytes[8] = uint8(values[1] >> 28)
	bytes[9] = uint8(values[1] >> 20)
	bytes[10] = uint8(values[1] >> 12)
	bytes[11] = uint8(values[1] >> 4)

	bytes[12] = uint8(values[1] << 4)
	bytes[12] |= uint8(values[2] >> 46)
	bytes[13] = uint8(values[2] >> 38)
	bytes[14] = uint8(values[2] >> 30)
	bytes[15] = uint8(values[2] >> 22)
	bytes[16] = uint8(values[2] >> 14)
	bytes[17] = uint8(values[2] >> 6)

	bytes[18] = uint8(values[2] << 2)
	bytes[18] |= uint8(values[3] >> 48)
	bytes[19] = uint8(values[3] >> 40)
	bytes[20] = uint8(values[3] >> 32)
	bytes[21] = uint8(values[3] >> 24)
	bytes[22] = uint8(values[3] >> 16)
	bytes[23] = uint8(values[3] >> 8)
	bytes[24] = uint8(values[3])

	bytes[25] = uint8(values[4] >> 42)
	bytes[26] = uint8(values[4] >> 34)
	bytes[27] = uint8(values[4] >> 26)
	bytes[28] = uint8(values[4] >> 18)
	bytes[29] = uint8(values[4] >> 10)
	bytes[30] = uint8(values[4] >> 2)

	bytes[31] = uint8(values[4] << 6)
	bytes[31] |= uint8(values[5] >> 44)
	bytes[32] = uint8(values[5] >> 36)
	bytes[33] = uint8(values[5] >> 28)
	bytes[34] = uint8(values[5] >> 20)
	bytes[35] = uint8(values[5] >> 12)
	bytes[36] = uint8(values[5] >> 4)

	bytes[37] = uint8(values[5] << 4)
	bytes[37] |= uint8(values[6] >> 46)
	bytes[38] = uint8(values[6] >> 38)
	bytes[39] = uint8(values[6] >> 30)
	bytes[40] = uint8(values[6] >> 22)
	bytes[41] = uint8(values[6] >> 14)
	bytes[42] = uint8(values[6] >> 6)

	bytes[43] = uint8(values[6] << 2)
	bytes[43] |= uint8(values[7] >> 48)
	bytes[44] = uint8(values[7] >> 40)
	bytes[45] = uint8(values[7] >> 32)
	bytes[46] = uint8(values[7] >> 24)
	bytes[47] = uint8(values[7] >> 16)
	bytes[48] = uint8(values[7] >> 8)
	bytes[49] = uint8(values[7])
}

func packBits51(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 43)
	bytes[1] = uint8(values[0] >> 35)
	bytes[2] = uint8(values[0] >> 27)
	bytes[3] = uint8(values[0] >> 19)
	bytes[4] = uint8(values[0] >> 11)
	bytes[5] = uint8(values[0] >> 3)

	bytes[6] = uint8(values[0] << 5)
	bytes[6] |= uint8(values[1] >> 46)
	bytes[7] = uint8(values[1] >> 38)
	bytes[8] = uint8(values[1] >> 30)
	bytes[9] = uint8(values[1] >> 22)
	bytes[10] = uint8(values[1] >> 14)
	bytes[11] = uint8(values[1] >> 6)

	bytes[12] = uint8(values[1] << 2)
	bytes[12] |= uint8(values[2] >> 49)
	bytes[13] = uint8(values[2] >> 41)
	bytes[14] = uint8(values[2] >> 33)
	bytes[15] = uint8(values[2] >> 25)
	bytes[16] = uint8(values[2] >> 17)
	bytes[17] = uint8(values[2] >> 9)
	bytes[18] = uint8(values[2] >> 1)

	bytes[19] = uint8(values[2] << 7)
	bytes[19] |= uint8(values[3] >> 44)
	bytes[20] = uint8(values[3] >> 36)
	bytes[21] = uint8(values[3] >> 28)
	bytes[22] = uint8(values[3] >> 20)
	bytes[23] = uint8(values[3] >> 12)
	bytes[24] = uint8(values[3] >> 4)

	bytes[25] = uint8(values[3] << 4)
	bytes[25] |= uint8(values[4] >> 47)
	bytes[26] = uint8(values[4] >> 39)
	bytes[27] = uint8(values[4] >> 31)
	bytes[28] = uint8(values[4] >> 23)
	bytes[29] = uint8(values[4] >> 15)
	bytes[30] = uint8(values[4] >> 7)

	bytes[31] = uint8(values[4] << 1)
	bytes[31] |= uint8(values[5] >> 50)
	bytes[32] = uint8(values[5] >> 42)
	bytes[33] = uint8(values[5] >> 34)
	bytes[34] = uint8(values[5] >> 26)
	bytes[35] = uint8(values[5] >> 18)
	bytes[36] = uint8(values[5] >> 10)
	bytes[37] = uint8(values[5] >> 2)

	bytes[38] = uint8(values[5] << 6)
	bytes[38] |= uint8(values[6] >> 45)
	bytes[39] = uint8(values[6] >> 37)
	bytes[40] = uint8(values[6] >> 29)
	bytes[41] = uint8(values[6] >> 21)
	bytes[42] = uint8(values[6] >> 13)
	bytes[43] = uint8(values[6] >> 5)

	bytes[44] = uint8(values[6] << 3)
	bytes[44] |= uint8(values[7] >> 48)
	bytes[45] = uint8(values[7] >> 40)
	bytes[46] = uint8(values[7] >> 32)
	bytes[47] = uint8(values[7] >> 24)
	bytes[48] = uint8(values[7] >> 16)
	bytes[49] = uint8(values[7] >> 8)
	bytes[50] = uint8(values[7])
}

func packBits52(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 44)
	bytes[1] = uint8(values[0] >> 36)
	bytes[2] = uint8(values[0] >> 28)
	bytes[3] = uint8(values[0] >> 20)
	bytes[4] = uint8(values[0] >> 12)
	bytes[5] = uint8(values[0] >> 4)

	bytes[6] = uint8(values[0] << 4)
	bytes[6] |= uint8(values[1] >> 48)
	bytes[7] = uint8(values[1] >> 40)
	bytes[8] = uint8(values[1] >> 32)
	bytes[9] = uint8(values[1] >> 24)
	bytes[10] = uint8(values[1] >> 16)
	bytes[11] = uint8(values[1] >> 8)
	bytes[12] = uint8(values[1])

	bytes[13] = uint8(values[2] >> 44)
	bytes[14] = uint8(values[2] >> 36)
	bytes[15] = uint8(values[2] >> 28)
	bytes[16] = uint8(values[2] >> 20)
	bytes[17] = uint8(values[2] >> 12)
	bytes[18] = uint8(values[2] >> 4)

	bytes[19] = uint8(values[2] << 4)
	bytes[19] |= uint8(values[3] >> 48)
	bytes[20] = uint8(values[3] >> 40)
	bytes[21] = uint8(values[3] >> 32)
	bytes[22] = uint8(values[3] >> 24)
	bytes[23] = uint8(values[3] >> 16)
	bytes[24] = uint8(values[3] >> 8)
	bytes[25] = uint8(values[3])

	bytes[26] = uint8(values[4] >> 44)
	bytes[27] = uint8(values[4] >> 36)
	bytes[28] = uint8(values[4] >> 28)
	bytes[29] = uint8(values[4] >> 20)
	bytes[30] = uint8(values[4] >> 12)
	bytes[31] = uint8(values[4] >> 4)

	bytes[32] = uint8(values[4] << 4)
	bytes[32] |= uint8(values[5] >> 48)
	bytes[33] = uint8(values[5] >> 40)
	bytes[34] = uint8(values[5] >> 32)
	bytes[35] = uint8(values[5] >> 24)
	bytes[36] = uint8(values[5] >> 16)
	bytes[37] = uint8(values[5] >> 8)
	bytes[38] = uint8(values[5])

	bytes[39] = uint8(values[6] >> 44)
	bytes[40] = uint8(values[6] >> 36)
	bytes[41] = uint8(values[6] >> 28)
	bytes[42] = uint8(values[6] >> 20)
	bytes[43] = uint8(values[6] >> 12)
	bytes[44] = uint8(values[6] >> 4)

	bytes[45] = uint8(values[6] << 4)
	bytes[45] |= uint8(values[7] >> 48)
	bytes[46] = uint8(values[7] >> 40)
	bytes[47] = uint8(values[7] >> 32)
	bytes[48] = uint8(values[7] >> 24)
	bytes[49] = uint8(values[7] >> 16)
	bytes[50] = uint8(values[7] >> 8)
	bytes[51] = uint8(values[7])
}

func packBits53(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 45)
	bytes[1] = uint8(values[0] >> 37)
	bytes[2] = uint8(values[0] >> 29)
	bytes[3] = uint8(values[0] >> 21)
	bytes[4] = uint8(values[0] >> 13)
	bytes[5] = uint8(values[0] >> 5)

	bytes[6] = uint8(values[0] << 3)
	bytes[6] |= uint8(values[1] >> 50)
	bytes[7] = uint8(values[1] >> 42)
	bytes[8] = uint8(values[1] >> 34)
	bytes[9] = uint8(values[1] >> 26)
	bytes[10] = uint8(values[1] >> 18)
	bytes[11] = uint8(values[1] >> 10)
	bytes[12] = uint8(values[1] >> 2)

	bytes[13] = uint8(values[1] << 6)
	bytes[13] |= uint8(values[2] >> 47)
	bytes[14] = uint8(values[2] >> 39)
	bytes[15] = uint8(values[2] >> 31)
	bytes[16] = uint8(values[2] >> 23)
	bytes[17] = uint8(values[2] >> 15)
	bytes[18] = uint8(values[2] >> 7)

	bytes[19] = uint8(values[2] << 1)
	bytes[19] |= uint8(values[3] >> 52)
	bytes[20] = uint8(values[3] >> 44)
	bytes[21] = uint8(values[3] >> 36)
	bytes[22] = uint8(values[3] >> 28)
	bytes[23] = uint8(values[3] >> 20)
	bytes[24] = uint8(values[3] >> 12)
	bytes[25] = uint8(values[3] >> 4)

	bytes[26] = uint8(values[3] << 4)
	bytes[26] |= uint8(values[4] >> 49)
	bytes[27] = uint8(values[4] >> 41)
	bytes[28] = uint8(values[4] >> 33)
	bytes[29] = uint8(values[4] >> 25)
	bytes[30] = uint8(values[4] >> 17)
	bytes[31] = uint8(values[4] >> 9)
	bytes[32] = uint8(values[4] >> 1)

	bytes[33] = uint8(values[4] << 7)
	bytes[33] |= uint8(values[5] >> 46)
	bytes[34] = uint8(values[5] >> 38)
	bytes[35] = uint8(values[5] >> 30)
	bytes[36] = uint8(values[5] >> 22)
	bytes[37] = uint8(values[5] >> 14)
	bytes[38] = uint8(values[5] >> 6)

	bytes[39] = uint8(values[5] << 2)
	bytes[39] |= uint8(values[6] >> 51)
	bytes[40] = uint8(values[6] >> 43)
	bytes[41] = uint8(values[6] >> 35)
	bytes[42] = uint8(values[6] >> 27)
	bytes[43] = uint8(values[6] >> 19)
	bytes[44] = uint8(values[6] >> 11)
	bytes[45] = uint8(values[6] >> 3)

	bytes[46] = uint8(values[6] << 5)
	bytes[46] |= uint8(values[7] >> 48)
	bytes[47] = uint8(values[7] >> 40)
	bytes[48] = uint8(values[7] >> 32)
	bytes[49] = uint8(values[7] >> 24)
	bytes[50] = uint8(values[7] >> 16)
	bytes[51] = uint8(values[7] >> 8)
	bytes[52] = uint8(values[7])
}

func packBits54(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 46)
	bytes[1] = uint8(values[0] >> 38)
	bytes[2] = uint8(values[0] >> 30)
	bytes[3] = uint8(values[0] >> 22)
	bytes[4] = uint8(values[0] >> 14)
	bytes[5] = uint8(values[0] >> 6)

	bytes[6] = uint8(values[0] << 2)
	bytes[6] |= uint8(values[1] >> 52)
	bytes[7] = uint8(values[1] >> 44)
	bytes[8] = uint8(values[1] >> 36)
	bytes[9] = uint8(values[1] >> 28)
	bytes[10] = uint8(values[1] >> 20)
	bytes[11] = uint8(values[1] >> 12)
	bytes[12] = uint8(values[1] >> 4)

	bytes[13] = uint8(values[1] << 4)
	bytes[13] |= uint8(values[2] >> 50)
	bytes[14] = uint8(values[2] >> 42)
	bytes[15] = uint8(values[2] >> 34)
	bytes[16] = uint8(values[2] >> 26)
	bytes[17] = uint8(values[2] >> 18)
	bytes[18] = uint8(values[2] >> 10)
	bytes[19] = uint8(values[2] >> 2)

	bytes[20] = uint8(values[2] << 6)
	bytes[20] |= uint8(values[3] >> 48)
	bytes[21] = uint8(values[3] >> 40)
	bytes[22] = uint8(values[3] >> 32)
	bytes[23] = uint8(values[3] >> 24)
	bytes[24] = uint8(values[3] >> 16)
	bytes[25] = uint8(values[3] >> 8)
	bytes[26] = uint8(values[3])

	bytes[27] = uint8(values[4] >> 46)
	bytes[28] = uint8(values[4] >> 38)
	bytes[29] = uint8(values[4] >> 30)
	bytes[30] = uint8(values[4] >> 22)
	bytes[31] = uint8(values[4] >> 14)
	bytes[32] = uint8(values[4] >> 6)

	bytes[33] = uint8(values[4] << 2)
	bytes[33] |= uint8(values[5] >> 52)
	bytes[34] = uint8(values[5] >> 44)
	bytes[35] = uint8(values[5] >> 36)
	bytes[36] = uint8(values[5] >> 28)
	bytes[37] = uint8(values[5] >> 20)
	bytes[38] = uint8(values[5] >> 12)
	bytes[39] = uint8(values[5] >> 4)

	bytes[40] = uint8(values[5] << 4)
	bytes[40] |= uint8(values[6] >> 50)
	bytes[41] = uint8(values[6] >> 42)
	bytes[42] = uint8(values[6] >> 34)
	bytes[43] = uint8(values[6] >> 26)
	bytes[44] = uint8(values[6] >> 18)
	bytes[45] = uint8(values[6] >> 10)
	bytes[46] = uint8(values[6] >> 2)

	bytes[47] = uint8(values[6] << 6)
	bytes[47] |= uint8(values[7] >> 48)
	bytes[48] = uint8(values[7] >> 40)
	bytes[49] = uint8(values[7] >> 32)
	bytes[50] = uint8(values[7] >> 24)
	bytes[51] = uint8(values[7] >> 16)
	bytes[52] = uint8(values[7] >> 8)
	bytes[53] = uint8(values[7])
}

func packBits55(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 47)
	bytes[1] = uint8(values[0] >> 39)
	bytes[2] = uint8(values[0] >> 31)
	bytes[3] = uint8(values[0] >> 23)
	bytes[4] = uint8(values[0] >> 15)
	bytes[5] = uint8(values[0] >> 7)

	bytes[6] = uint8(values[0] << 1)
	bytes[6] |= uint8(values[1] >> 54)
	bytes[7] = uint8(values[1] >> 46)
	bytes[8] = uint8(values[1] >> 38)
	bytes[9] = uint8(values[1] >> 30)
	bytes[10] = uint8(values[1] >> 22)
	bytes[11] = uint8(values[1] >> 14)
	bytes[12] = uint8(values[1] >> 6)

	bytes[13] = uint8(values[1] << 2)
	bytes[13] |= uint8(values[2] >> 53)
	bytes[14] = uint8(values[2] >> 45)
	bytes[15] = uint8(values[2] >> 37)
	bytes[16] = uint8(values[2] >> 29)
	bytes[17] = uint8(values[2] >> 21)
	bytes[18] = uint8(values[2] >> 13)
	bytes[19] = uint8(values[2] >> 5)

	bytes[20] = uint8(values[2] << 3)
	bytes[20] |= uint8(values[3] >> 52)
	bytes[21] = uint8(values[3] >> 44)
	bytes[22] = uint8(values[3] >> 36)
	bytes[23] = uint8(values[3] >> 28)
	bytes[24] = uint8(values[3] >> 20)
	bytes[25] = uint8(values[3] >> 12)
	bytes[26] = uint8(values[3] >> 4)

	bytes[27] = uint8(values[3] << 4)
	bytes[27] |= uint8(values[4] >> 51)
	bytes[28] = uint8(values[4] >> 43)
	bytes[29] = uint8(values[4] >> 35)
	bytes[30] = uint8(values[4] >> 27)
	bytes[31] = uint8(values[4] >> 19)
	bytes[32] = uint8(values[4] >> 11)
	bytes[33] = uint8(values[4] >> 3)

	bytes[34] = uint8(values[4] << 5)
	bytes[34] |= uint8(values[5] >> 50)
	bytes[35] = uint8(values[5] >> 42)
	bytes[36] = uint8(values[5] >> 34)
	bytes[37] = uint8(values[5] >> 26)
	bytes[38] = uint8(values[5] >> 18)
	bytes[39] = uint8(values[5] >> 10)
	bytes[40] = uint8(values[5] >> 2)

	bytes[41] = uint8(values[5] << 6)
	bytes[41] |= uint8(values[6] >> 49)
	bytes[42] = uint8(values[6] >> 41)
	bytes[43] = uint8(values[6] >> 33)
	bytes[44] = uint8(values[6] >> 25)
	bytes[45] = uint8(values[6] >> 17)
	bytes[46] = uint8(values[6] >> 9)
	bytes[47] = uint8(values[6] >> 1)

	bytes[48] = uint8(values[6] << 7)
	bytes[48] |= uint8(values[7] >> 48)
	bytes[49] = uint8(values[7] >> 40)
	bytes[50] = uint8(values[7] >> 32)
	bytes[51] = uint8(values[7] >> 24)
	bytes[52] = uint8(values[7] >> 16)
	bytes[53] = uint8(values[7] >> 8)
	bytes[54] = uint8(values[7])
}

func packBits56(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 48)
	bytes[1] = uint8(values[0] >> 40)
	bytes[2] = uint8(values[0] >> 32)
	bytes[3] = uint8(values[0] >> 24)
	bytes[4] = uint8(values[0] >> 16)
	bytes[5] = uint8(values[0] >> 8)
	bytes[6] = uint8(values[0])

	bytes[7] = uint8(values[1] >> 48)
	bytes[8] = uint8(values[1] >> 40)
	bytes[9] = uint8(values[1] >> 32)
	bytes[10] = uint8(values[1] >> 24)
	bytes[11] = uint8(values[1] >> 16)
	bytes[12] = uint8(values[1] >> 8)
	bytes[13] = uint8(values[1])

	bytes[14] = uint8(values[2] >> 48)
	bytes[15] = uint8(values[2] >> 40)
	bytes[16] = uint8(values[2] >> 32)
	bytes[17] = uint8(values[2] >> 24)
	bytes[18] = uint8(values[2] >> 16)
	bytes[19] = uint8(values[2] >> 8)
	bytes[20] = uint8(values[2])

	bytes[21] = uint8(values[3] >> 48)
	bytes[22] = uint8(values[3] >> 40)
	bytes[23] = uint8(values[3] >> 32)
	bytes[24] = uint8(values[3] >> 24)
	bytes[25] = uint8(values[3] >> 16)
	bytes[26] = uint8(values[3] >> 8)
	bytes[27] = uint8(values[3])

	bytes[28] = uint8(values[4] >> 48)
	bytes[29] = uint8(values[4] >> 40)
	bytes[30] = uint8(values[4] >> 32)
	bytes[31] = uint8(values[4] >> 24)
	bytes[32] = uint8(values[4] >> 16)
	bytes[33] = uint8(values[4] >> 8)
	bytes[34] = uint8(values[4])

	bytes[35] = uint8(values[5] >> 48)
	bytes[36] = uint8(values[5] >> 40)
	bytes[37] = uint8(values[5] >> 32)
	bytes[38] = uint8(values[5] >> 24)
	bytes[39] = uint8(values[5] >> 16)
	bytes[40] = uint8(values[5] >> 8)
	bytes[41] = uint8(values[5])

	bytes[42] = uint8(values[6] >> 48)
	bytes[43] = uint8(values[6] >> 40)
	bytes[44] = uint8(values[6] >> 32)
	bytes[45] = uint8(values[6] >> 24)
	bytes[46] = uint8(values[6] >> 16)
	bytes[47] = uint8(values[6] >> 8)
	bytes[48] = uint8(values[6])

	bytes[49] = uint8(values[7] >> 48)
	bytes[50] = uint8(values[7] >> 40)
	bytes[51] = uint8(values[7] >> 32)
	bytes[52] = uint8(values[7] >> 24)
	bytes[53] = uint8(values[7] >> 16)
	bytes[54] = uint8(values[7] >> 8)
	bytes[55] = uint8(values[7])
}

func packBits57(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 49)
	bytes[1] = uint8(values[0] >> 41)
	bytes[2] = uint8(values[0] >> 33)
	bytes[3] = uint8(values[0] >> 25)
	bytes[4] = uint8(values[0] >> 17)
	bytes[5] = uint8(values[0] >> 9)
	bytes[6] = uint8(values[0] >> 1)

	bytes[7] = uint8(values[0] << 7)
	bytes[7] |= uint8(values[1] >> 50)
	bytes[8] = uint8(values[1] >> 42)
	bytes[9] = uint8(values[1] >> 34)
	bytes[10] = uint8(values[1] >> 26)
	bytes[11] = uint8(values[1] >> 18)
	bytes[12] = uint8(values[1] >> 10)
	bytes[13] = uint8(values[1] >> 2)

	bytes[14] = uint8(values[1] << 6)
	bytes[14] |= uint8(values[2] >> 51)
	bytes[15] = uint8(values[2] >> 43)
	bytes[16] = uint8(values[2] >> 35)
	bytes[17] = uint8(values[2] >> 27)
	bytes[18] = uint8(values[2] >> 19)
	bytes[19] = uint8(values[2] >> 11)
	bytes[20] = uint8(values[2] >> 3)

	bytes[21] = uint8(values[2] << 5)
	bytes[21] |= uint8(values[3] >> 52)
	bytes[22] = uint8(values[3] >> 44)
	bytes[23] = uint8(values[3] >> 36)
	bytes[24] = uint8(values[3] >> 28)
	bytes[25] = uint8(values[3] >> 20)
	bytes[26] = uint8(values[3] >> 12)
	bytes[27] = uint8(values[3] >> 4)

	bytes[28] = uint8(values[3] << 4)
	bytes[28] |= uint8(values[4] >> 53)
	bytes[29] = uint8(values[4] >> 45)
	bytes[30] = uint8(values[4] >> 37)
	bytes[31] = uint8(values[4] >> 29)
	bytes[32] = uint8(values[4] >> 21)
	bytes[33] = uint8(values[4] >> 13)
	bytes[34] = uint8(values[4] >> 5)

	bytes[35] = uint8(values[4] << 3)
	bytes[35] |= uint8(values[5] >> 54)
	bytes[36] = uint8(values[5] >> 46)
	bytes[37] = uint8(values[5] >> 38)
	bytes[38] = uint8(values[5] >> 30)
	bytes[39] = uint8(values[5] >> 22)
	bytes[40] = uint8(values[5] >> 14)
	bytes[41] = uint8(values[5] >> 6)

	bytes[42] = uint8(values[5] << 2)
	bytes[42] |= uint8(values[6] >> 55)
	bytes[43] = uint8(values[6] >> 47)
	bytes[44] = uint8(values[6] >> 39)
	bytes[45] = uint8(values[6] >> 31)
	bytes[46] = uint8(values[6] >> 23)
	bytes[47] = uint8(values[6] >> 15)
	bytes[48] = uint8(values[6] >> 7)

	bytes[49] = uint8(values[6] << 1)
	bytes[49] |= uint8(values[7] >> 56)
	bytes[50] = uint8(values[7] >> 48)
	bytes[51] = uint8(values[7] >> 40)
	bytes[52] = uint8(values[7] >> 32)
	bytes[53] = uint8(values[7] >> 24)
	bytes[54] = uint8(values[7] >> 16)
	bytes[55] = uint8(values[7] >> 8)
	bytes[56] = uint8(values[7])
}

func packBits58(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 50)
	bytes[1] = uint8(values[0] >> 42)
	bytes[2] = uint8(values[0] >> 34)
	bytes[3] = uint8(values[0] >> 26)
	bytes[4] = uint8(values[0] >> 18)
	bytes[5] = uint8(values[0] >> 10)
	bytes[6] = uint8(values[0] >> 2)

	bytes[7] = uint8(values[0] << 6)
	bytes[7] |= uint8(values[1] >> 52)
	bytes[8] = uint8(values[1] >> 44)
	bytes[9] = uint8(values[1] >> 36)
	bytes[10] = uint8(values[1] >> 28)
	bytes[11] = uint8(values[1] >> 20)
	bytes[12] = uint8(values[1] >> 12)
	bytes[13] = uint8(values[1] >> 4)

	bytes[14] = uint8(values[1] << 4)
	bytes[14] |= uint8(values[2] >> 54)
	bytes[15] = uint8(values[2] >> 46)
	bytes[16] = uint8(values[2] >> 38)
	bytes[17] = uint8(values[2] >> 30)
	bytes[18] = uint8(values[2] >> 22)
	bytes[19] = uint8(values[2] >> 14)
	bytes[20] = uint8(values[2] >> 6)

	bytes[21] = uint8(values[2] << 2)
	bytes[21] |= uint8(values[3] >> 56)
	bytes[22] = uint8(values[3] >> 48)
	bytes[23] = uint8(values[3] >> 40)
	bytes[24] = uint8(values[3] >> 32)
	bytes[25] = uint8(values[3] >> 24)
	bytes[26] = uint8(values[3] >> 16)
	bytes[27] = uint8(values[3] >> 8)
	bytes[28] = uint8(values[3])

	bytes[29] = uint8(values[4] >> 50)
	bytes[30] = uint8(values[4] >> 42)
	bytes[31] = uint8(values[4] >> 34)
	bytes[32] = uint8(values[4] >> 26)
	bytes[33] = uint8(values[4] >> 18)
	bytes[34] = uint8(values[4] >> 10)
	bytes[35] = uint8(values[4] >> 2)

	bytes[36] = uint8(values[4] << 6)
	bytes[36] |= uint8(values[5] >> 52)
	bytes[37] = uint8(values[5] >> 44)
	bytes[38] = uint8(values[5] >> 36)
	bytes[39] = uint8(values[5] >> 28)
	bytes[40] = uint8(values[5] >> 20)
	bytes[41] = uint8(values[5] >> 12)
	bytes[42] = uint8(values[5] >> 4)

	bytes[43] = uint8(values[5] << 4)
	bytes[43] |= uint8(values[6] >> 54)
	bytes[44] = uint8(values[6] >> 46)
	bytes[45] = uint8(values[6] >> 38)
	bytes[46] = uint8(values[6] >> 30)
	bytes[47] = uint8(values[6] >> 22)
	bytes[48] = uint8(values[6] >> 14)
	bytes[49] = uint8(values[6] >> 6)

	bytes[50] = uint8(values[6] << 2)
	bytes[50] |= uint8(values[7] >> 56)
	bytes[51] = uint8(values[7] >> 48)
	bytes[52] = uint8(values[7] >> 40)
	bytes[53] = uint8(values[7] >> 32)
	bytes[54] = uint8(values[7] >> 24)
	bytes[55] = uint8(values[7] >> 16)
	bytes[56] = uint8(values[7] >> 8)
	bytes[57] = uint8(values[7])
}

func packBits59(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 51)
	bytes[1] = uint8(values[0] >> 43)
	bytes[2] = uint8(values[0] >> 35)
	bytes[3] = uint8(values[0] >> 27)
	bytes[4] = uint8(values[0] >> 19)
	bytes[5] = uint8(values[0] >> 11)
	bytes[6] = uint8(values[0] >> 3)

	bytes[7] = uint8(values[0] << 5)
	bytes[7] |= uint8(values[1] >> 54)
	bytes[8] = uint8(values[1] >> 46)
	bytes[9] = uint8(values[1] >> 38)
	bytes[10] = uint8(values[1] >> 30)
	bytes[11] = uint8(values[1] >> 22)
	bytes[12] = uint8(values[1] >> 14)
	bytes[13] = uint8(values[1] >> 6)

	bytes[14] = uint8(values[1] << 2)
	bytes[14] |= uint8(values[2] >> 57)
	bytes[15] = uint8(values[2] >> 49)
	bytes[16] = uint8(values[2] >> 41)
	bytes[17] = uint8(values[2] >> 33)
	bytes[18] = uint8(values[2] >> 25)
	bytes[19] = uint8(values[2] >> 17)
	bytes[20] = uint8(values[2] >> 9)
	bytes[21] = uint8(values[2] >> 1)

	bytes[22] = uint8(values[2] << 7)
	bytes[22] |= uint8(values[3] >> 52)
	bytes[23] = uint8(values[3] >> 44)
	bytes[24] = uint8(values[3] >> 36)
	bytes[25] = uint8(values[3] >> 28)
	bytes[26] = uint8(values[3] >> 20)
	bytes[27] = uint8(values[3] >> 12)
	bytes[28] = uint8(values[3] >> 4)

	bytes[29] = uint8(values[3] << 4)
	bytes[29] |= uint8(values[4] >> 55)
	bytes[30] = uint8(values[4] >> 47)
	bytes[31] = uint8(values[4] >> 39)
	bytes[32] = uint8(values[4] >> 31)
	bytes[33] = uint8(values[4] >> 23)
	bytes[34] = uint8(values[4] >> 15)
	bytes[35] = uint8(values[4] >> 7)

	bytes[36] = uint8(values[4] << 1)
	bytes[36] |= uint8(values[5] >> 58)
	bytes[37] = uint8(values[5] >> 50)
	bytes[38] = uint8(values[5] >> 42)
	bytes[39] = uint8(values[5] >> 34)
	bytes[40] = uint8(values[5] >> 26)
	bytes[41] = uint8(values[5] >> 18)
	bytes[42] = uint8(values[5] >> 10)
	bytes[43] = uint8(values[5] >> 2)

	bytes[44] = uint8(values[5] << 6)
	bytes[44] |= uint8(values[6] >> 53)
	bytes[45] = uint8(values[6] >> 45)
	bytes[46] = uint8(values[6] >> 37)
	bytes[47] = uint8(values[6] >> 29)
	bytes[48] = uint8(values[6] >> 21)
	bytes[49] = uint8(values[6] >> 13)
	bytes[50] = uint8(values[6] >> 5)

	bytes[51] = uint8(values[6] << 3)
	bytes[51] |= uint8(values[7] >> 56)
	bytes[52] = uint8(values[7] >> 48)
	bytes[53] = uint8(values[7] >> 40)
	bytes[54] = uint8(values[7] >> 32)
	bytes[55] = uint8(values[7] >> 24)
	bytes[56] = uint8(values[7] >> 16)
	bytes[57] = uint8(values[7] >> 8)
	bytes[58] = uint8(values[7])
}

func packBits60(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 52)
	bytes[1] = uint8(values[0] >> 44)
	bytes[2] = uint8(values[0] >> 36)
	bytes[3] = uint8(values[0] >> 28)
	bytes[4] = uint8(values[0] >> 20)
	bytes[5] = uint8(values[0] >> 12)
	bytes[6] = uint8(values[0] >> 4)

	bytes[7] = uint8(values[0] << 4)
	bytes[7] |= uint8(values[1] >> 56)
	bytes[8] = uint8(values[1] >> 48)
	bytes[9] = uint8(values[1] >> 40)
	bytes[10] = uint8(values[1] >> 32)
	bytes[11] = uint8(values[1] >> 24)
	bytes[12] = uint8(values[1] >> 16)
	bytes[13] = uint8(values[1] >> 8)
	bytes[14] = uint8(values[1])

	bytes[15] = uint8(values[2] >> 52)
	bytes[16] = uint8(values[2] >> 44)
	bytes[17] = uint8(values[2] >> 36)
	bytes[18] = uint8(values[2] >> 28)
	bytes[19] = uint8(values[2] >> 20)
	bytes[20] = uint8(values[2] >> 12)
	bytes[21] = uint8(values[2] >> 4)

	bytes[22] = uint8(values[2] << 4)
	bytes[22] |= uint8(values[3] >> 56)
	bytes[23] = uint8(values[3] >> 48)
	bytes[24] = uint8(values[3] >> 40)
	bytes[25] = uint8(values[3] >> 32)
	bytes[26] = uint8(values[3] >> 24)
	bytes[27] = uint8(values[3] >> 16)
	bytes[28] = uint8(values[3] >> 8)
	bytes[29] = uint8(values[3])

	bytes[30] = uint8(values[4] >> 52)
	bytes[31] = uint8(values[4] >> 44)
	bytes[32] = uint8(values[4] >> 36)
	bytes[33] = uint8(values[4] >> 28)
	bytes[34] = uint8(values[4] >> 20)
	bytes[35] = uint8(values[4] >> 12)
	bytes[36] = uint8(values[4] >> 4)

	bytes[37] = uint8(values[4] << 4)
	bytes[37] |= uint8(values[5] >> 56)
	bytes[38] = uint8(values[5] >> 48)
	bytes[39] = uint8(values[5] >> 40)
	bytes[40] = uint8(values[5] >> 32)
	bytes[41] = uint8(values[5] >> 24)
	bytes[42] = uint8(values[5] >> 16)
	bytes[43] = uint8(values[5] >> 8)
	bytes[44] = uint8(values[5])

	bytes[45] = uint8(values[6] >> 52)
	bytes[46] = uint8(values[6] >> 44)
	bytes[47] = uint8(values[6] >> 36)
	bytes[48] = uint8(values[6] >> 28)
	bytes[49] = uint8(values[6] >> 20)
	bytes[50] = uint8(values[6] >> 12)
	bytes[51] = uint8(values[6] >> 4)

	bytes[52] = uint8(values[6] << 4)
	bytes[52] |= uint8(values[7] >> 56)
	bytes[53] = uint8(values[7] >> 48)
	bytes[54] = uint8(values[7] >> 40)
	bytes[55] = uint8(values[7] >> 32)
	bytes[56] = uint8(values[7] >> 24)
	bytes[57] = uint8(values[7] >> 16)
	bytes[58] = uint8(values[7] >> 8)
	bytes[59] = uint8(values[7])
}

func packBits61(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 53)
	bytes[1] = uint8(values[0] >> 45)
	bytes[2] = uint8(values[0] >> 37)
	bytes[3] = uint8(values[0] >> 29)
	bytes[4] = uint8(values[0] >> 21)
	bytes[5] = uint8(values[0] >> 13)
	bytes[6] = uint8(values[0] >> 5)

	bytes[7] = uint8(values[0] << 3)
	bytes[7] |= uint8(values[1] >> 58)
	bytes[8] = uint8(values[1] >> 50)
	bytes[9] = uint8(values[1] >> 42)
	bytes[10] = uint8(values[1] >> 34)
	bytes[11] = uint8(values[1] >> 26)
	bytes[12] = uint8(values[1] >> 18)
	bytes[13] = uint8(values[1] >> 10)
	bytes[14] = uint8(values[1] >> 2)

	bytes[15] = uint8(values[1] << 6)
	bytes[15] |= uint8(values[2] >> 55)
	bytes[16] = uint8(values[2] >> 47)
	bytes[17] = uint8(values[2] >> 39)
	bytes[18] = uint8(values[2] >> 31)
	bytes[19] = uint8(values[2] >> 23)
	bytes[20] = uint8(values[2] >> 15)
	bytes[21] = uint8(values[2] >> 7)

	bytes[22] = uint8(values[2] << 1)
	bytes[22] |= uint8(values[3] >> 60)
	bytes[23] = uint8(values[3] >> 52)
	bytes[24] = uint8(values[3] >> 44)
	bytes[25] = uint8(values[3] >> 36)
	bytes[26] = uint8(values[3] >> 28)
	bytes[27] = uint8(values[3] >> 20)
	bytes[28] = uint8(values[3] >> 12)
	bytes[29] = uint8(values[3] >> 4)

	bytes[30] = uint8(values[3] << 4)
	bytes[30] |= uint8(values[4] >> 57)
	bytes[31] = uint8(values[4] >> 49)
	bytes[32] = uint8(values[4] >> 41)
	bytes[33] = uint8(values[4] >> 33)
	bytes[34] = uint8(values[4] >> 25)
	bytes[35] = uint8(values[4] >> 17)
	bytes[36] = uint8(values[4] >> 9)
	bytes[37] = uint8(values[4] >> 1)

	bytes[38] = uint8(values[4] << 7)
	bytes[38] |= uint8(values[5] >> 54)
	bytes[39] = uint8(values[5] >> 46)
	bytes[40] = uint8(values[5] >> 38)
	bytes[41] = uint8(values[5] >> 30)
	bytes[42] = uint8(values[5] >> 22)
	bytes[43] = uint8(values[5] >> 14)
	bytes[44] = uint8(values[5] >> 6)

	bytes[45] = uint8(values[5] << 2)
	bytes[45] |= uint8(values[6] >> 59)
	bytes[46] = uint8(values[6] >> 51)
	bytes[47] = uint8(values[6] >> 43)
	bytes[48] = uint8(values[6] >> 35)
	bytes[49] = uint8(values[6] >> 27)
	bytes[50] = uint8(values[6] >> 19)
	bytes[51] = uint8(values[6] >> 11)
	bytes[52] = uint8(values[6] >> 3)

	bytes[53] = uint8(values[6] << 5)
	bytes[53] |= uint8(values[7] >> 56)
	bytes[54] = uint8(values[7] >> 48)
	bytes[55] = uint8(values[7] >> 40)
	bytes[56] = uint8(values[7] >> 32)
	bytes[57] = uint8(values[7] >> 24)
	bytes[58] = uint8(values[7] >> 16)
	bytes[59] = uint8(values[7] >> 8)
	bytes[60] = uint8(values[7])
}

func packBits62(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 54)
	bytes[1] = uint8(values[0] >> 46)
	bytes[2] = uint8(values[0] >> 38)
	bytes[3] = uint8(values[0] >> 30)
	bytes[4] = uint8(values[0] >> 22)
	bytes[5] = uint8(values[0] >> 14)
	bytes[6] = uint8(values[0] >> 6)

	bytes[7] = uint8(values[0] << 2)
	bytes[7] |= uint8(values[1] >> 60)
	bytes[8] = uint8(values[1] >> 52)
	bytes[9] = uint8(values[1] >> 44)
	bytes[10] = uint8(values[1] >> 36)
	bytes[11] = uint8(values[1] >> 28)
	bytes[12] = uint8(values[1] >> 20)
	bytes[13] = uint8(values[1] >> 12)
	bytes[14] = uint8(values[1] >> 4)

	bytes[15] = uint8(values[1] << 4)
	bytes[15] |= uint8(values[2] >> 58)
	bytes[16] = uint8(values[2] >> 50)
	bytes[17] = uint8(values[2] >> 42)
	bytes[18] = uint8(values[2] >> 34)
	bytes[19] = uint8(values[2] >> 26)
	bytes[20] = uint8(values[2] >> 18)
	bytes[21] = uint8(values[2] >> 10)
	bytes[22] = uint8(values[2] >> 2)

	bytes[23] = uint8(values[2] << 6)
	bytes[23] |= uint8(values[3] >> 56)
	bytes[24] = uint8(values[3] >> 48)
	bytes[25] = uint8(values[3] >> 40)
	bytes[26] = uint8(values[3] >> 32)
	bytes[27] = uint8(values[3] >> 24)
	bytes[28] = uint8(values[3] >> 16)
	bytes[29] = uint8(values[3] >> 8)
	bytes[30] = uint8(values[3])

	bytes[31] = uint8(values[4] >> 54)
	bytes[32] = uint8(values[4] >> 46)
	bytes[33] = uint8(values[4] >> 38)
	bytes[34] = uint8(values[4] >> 30)
	bytes[35] = uint8(values[4] >> 22)
	bytes[36] = uint8(values[4] >> 14)
	bytes[37] = uint8(values[4] >> 6)

	bytes[38] = uint8(values[4] << 2)
	bytes[38] |= uint8(values[5] >> 60)
	bytes[39] = uint8(values[5] >> 52)
	bytes[40] = uint8(values[5] >> 44)
	bytes[41] = uint8(values[5] >> 36)
	bytes[42] = uint8(values[5] >> 28)
	bytes[43] = uint8(values[5] >> 20)
	bytes[44] = uint8(values[5] >> 12)
	bytes[45] = uint8(values[5] >> 4)

	bytes[46] = uint8(values[5] << 4)
	bytes[46] |= uint8(values[6] >> 58)
	bytes[47] = uint8(values[6] >> 50)
	bytes[48] = uint8(values[6] >> 42)
	bytes[49] = uint8(values[6] >> 34)
	bytes[50] = uint8(values[6] >> 26)
	bytes[51] = uint8(values[6] >> 18)
	bytes[52] = uint8(values[6] >> 10)
	bytes[53] = uint8(values[6] >> 2)

	bytes[54] = uint8(values[6] << 6)
	bytes[54] |= uint8(values[7] >> 56)
	bytes[55] = uint8(values[7] >> 48)
	bytes[56] = uint8(values[7] >> 40)
	bytes[57] = uint8(values[7] >> 32)
	bytes[58] = uint8(values[7] >> 24)
	bytes[59] = uint8(values[7] >> 16)
	bytes[60] = uint8(values[7] >> 8)
	bytes[61] = uint8(values[7])
}

func packBits63(values []uint64, bytes []byte) {
	bytes[0] = uint8(values[0] >> 55)
	bytes[1] = uint8(values[0] >> 47)
	bytes[2] = uint8(values[0] >> 39)
	bytes[3] = uint8(values[0] >> 31)
	bytes[4] = uint8(values[0] >> 23)
	bytes[5] = uint8(values[0] >> 15)
	bytes[6] = uint8(values[0] >> 7)

	bytes[7] = uint8(values[0] << 1)
	bytes[7] |= uint8(values[1] >> 62)
	bytes[8] = uint8(values[1] >> 54)
	bytes[9] = uint8(values[1] >> 46)
	bytes[10] = uint8(values[1] >> 38)
	bytes[11] = uint8(values[1] >> 30)
	bytes[12] = uint8(values[1] >> 22)
	bytes[13] = uint8(values[1] >> 14)
	bytes[14] = uint8(values[1] >> 6)

	bytes[15] = uint8(values[1] << 2)
	bytes[15] |= uint8(values[2] >> 61)
	bytes[16] = uint8(values[2] >> 53)
	bytes[17] = uint8(values[2] >> 45)
	bytes[18] = uint8(values[2] >> 37)
	bytes[19] = uint8(values[2] >> 29)
	bytes[20] = uint8(values[2] >> 21)
	bytes[21] = uint8(values[2] >> 13)
	bytes[22] = uint8(values[2] >> 5)

	bytes[23] = uint8(values[2] << 3)
	bytes[23] |= uint8(values[3] >> 60)
	bytes[24] = uint8(values[3] >> 52)
	bytes[25] = uint8(values[3] >> 44)
	bytes[26] = uint8(values[3] >> 36)
	bytes[27] = uint8(values[3] >> 28)
	bytes[28] = uint8(values[3] >> 20)
	bytes[29] = uint8(values[3] >> 12)
	bytes[30] = uint8(values[3] >> 4)

	bytes[31] = uint8(values[3] << 4)
	bytes[31] |= uint8(values[4] >> 59)
	bytes[32] = uint8(values[4] >> 51)
	bytes[33] = uint8(values[4] >> 43)
	bytes[34] = uint8(values[4] >> 35)
	bytes[35] = uint8(values[4] >> 27)
	bytes[36] = uint8(values[4] >> 19)
	bytes[37] = uint8(values[4] >> 11)
	bytes[38] = uint8(values[4] >> 3)

	bytes[39] = uint8(values[4] << 5)
	bytes[39] |= uint8(values[5] >> 58)
	bytes[40] = uint8(values[5] >> 50)
	bytes[41] = uint8(values[5] >> 42)
	bytes[42] = uint8(values[5] >> 34)
	bytes[43] = uint8(values[5] >> 26)
	bytes[44] = uint8(values[5] >> 18)
	bytes[45] = uint8(values[5] >> 10)
	bytes[46] = uint8(values[5] >> 2)

	bytes[47] = uint8(values[5] << 6)
	bytes[47] |= uint8(values[6] >> 57)
	bytes[48] = uint8(values[6] >> 49)
	bytes[49] = uint8(values[6] >> 41)
	bytes[50] = uint8(values[6] >> 33)
	bytes[51] = uint8(values[6] >> 25)
	bytes[52] = uint8(values[6] >> 17)
	bytes[53] = uint8(values[6] >> 9)
	bytes[54] = uint8(values[6] >> 1)

	bytes[55] = uint8(values[6] << 7)
	bytes[55] |= uint8(values[7] >> 56)
	bytes[56] = uint8(values[7] >> 48)
	bytes[57] = uint8(values[7] >> 40)
	bytes[58] = uint8(values[7] >> 32)
	bytes[59] = uint8(values[7] >> 24)
	bytes[60] = uint8(values[7] >> 16)
	bytes[61] = uint8(values[7] >> 8)
	bytes[62] = uint8(values[7])
}

// packBitsBlock8 packs 8 uint64 values with a given number of bits into bytes
// values: array of 8 uint64 values
// bytes: byte slice to write to
// bits: number of bits to pack from each value (1-63)
func packBitsBlock8(values []uint64, bytes []byte, bits uint8) error {
	switch bits {
	case 1:
		packBits1(values, bytes)
	case 2:
		packBits2(values, bytes)
	case 3:
		packBits3(values, bytes)
	case 4:
		packBits4(values, bytes)
	case 5:
		packBits5(values, bytes)
	case 6:
		packBits6(values, bytes)
	case 7:
		packBits7(values, bytes)
	case 8:
		packBits8(values, bytes)
	case 9:
		packBits9(values, bytes)
	case 10:
		packBits10(values, bytes)
	case 11:
		packBits11(values, bytes)
	case 12:
		packBits12(values, bytes)
	case 13:
		packBits13(values, bytes)
	case 14:
		packBits14(values, bytes)
	case 15:
		packBits15(values, bytes)
	case 16:
		packBits16(values, bytes)
	case 17:
		packBits17(values, bytes)
	case 18:
		packBits18(values, bytes)
	case 19:
		packBits19(values, bytes)
	case 20:
		packBits20(values, bytes)
	case 21:
		packBits21(values, bytes)
	case 22:
		packBits22(values, bytes)
	case 23:
		packBits23(values, bytes)
	case 24:
		packBits24(values, bytes)
	case 25:
		packBits25(values, bytes)
	case 26:
		packBits26(values, bytes)
	case 27:
		packBits27(values, bytes)
	case 28:
		packBits28(values, bytes)
	case 29:
		packBits29(values, bytes)
	case 30:
		packBits30(values, bytes)
	case 31:
		packBits31(values, bytes)
	case 32:
		packBits32(values, bytes)
	case 33:
		packBits33(values, bytes)
	case 34:
		packBits34(values, bytes)
	case 35:
		packBits35(values, bytes)
	case 36:
		packBits36(values, bytes)
	case 37:
		packBits37(values, bytes)
	case 38:
		packBits38(values, bytes)
	case 39:
		packBits39(values, bytes)
	case 40:
		packBits40(values, bytes)
	case 41:
		packBits41(values, bytes)
	case 42:
		packBits42(values, bytes)
	case 43:
		packBits43(values, bytes)
	case 44:
		packBits44(values, bytes)
	case 45:
		packBits45(values, bytes)
	case 46:
		packBits46(values, bytes)
	case 47:
		packBits47(values, bytes)
	case 48:
		packBits48(values, bytes)
	case 49:
		packBits49(values, bytes)
	case 50:
		packBits50(values, bytes)
	case 51:
		packBits51(values, bytes)
	case 52:
		packBits52(values, bytes)
	case 53:
		packBits53(values, bytes)
	case 54:
		packBits54(values, bytes)
	case 55:
		packBits55(values, bytes)
	case 56:
		packBits56(values, bytes)
	case 57:
		packBits57(values, bytes)
	case 58:
		packBits58(values, bytes)
	case 59:
		packBits59(values, bytes)
	case 60:
		packBits60(values, bytes)
	case 61:
		packBits61(values, bytes)
	case 62:
		packBits62(values, bytes)
	case 63:
		packBits63(values, bytes)
	default:
		return fmt.Errorf("wrong number of bits in packBitsBlock8: %d", bits)
	}
	return nil
}

func unpackBits1(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0] >> 7)
	values[1] = uint64((bytes[0] >> 6) & 1)
	values[2] = uint64((bytes[0] >> 5) & 1)
	values[3] = uint64((bytes[0] >> 4) & 1)
	values[4] = uint64((bytes[0] >> 3) & 1)
	values[5] = uint64((bytes[0] >> 2) & 1)
	values[6] = uint64((bytes[0] >> 1) & 1)
	values[7] = uint64(bytes[0] & 1)
}

func unpackBits2(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0] >> 6)
	values[1] = uint64((bytes[0] >> 4) & 3)
	values[2] = uint64((bytes[0] >> 2) & 3)
	values[3] = uint64(bytes[0] & 3)
	values[4] = uint64(bytes[1] >> 6)
	values[5] = uint64((bytes[1] >> 4) & 3)
	values[6] = uint64((bytes[1] >> 2) & 3)
	values[7] = uint64(bytes[1] & 3)
}

func unpackBits3(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0] >> 5)
	values[1] = uint64((bytes[0] >> 2) & 7)
	values[2] = uint64(bytes[0]&3) << 1
	values[2] |= uint64(bytes[1] >> 7)
	values[3] = uint64((bytes[1] >> 4) & 7)
	values[4] = uint64((bytes[1] >> 1) & 7)
	values[5] = uint64(bytes[1]&1) << 2
	values[5] |= uint64(bytes[2] >> 6)
	values[6] = uint64((bytes[2] >> 3) & 7)
	values[7] = uint64(bytes[2] & 7)
}

func unpackBits4(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0] >> 4)
	values[1] = uint64(bytes[0] & 0xf)
	values[2] = uint64(bytes[1] >> 4)
	values[3] = uint64(bytes[1] & 0xf)
	values[4] = uint64(bytes[2] >> 4)
	values[5] = uint64(bytes[2] & 0xf)
	values[6] = uint64(bytes[3] >> 4)
	values[7] = uint64(bytes[3] & 0xf)
}

func unpackBits5(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0] >> 3)

	values[1] = uint64(bytes[0]&7) << 2
	values[1] |= uint64(bytes[1] >> 6)

	values[2] = uint64((bytes[1] >> 1) & 0x1f)

	values[3] = uint64(bytes[1]&1) << 4
	values[3] |= uint64(bytes[2] >> 4)

	values[4] = uint64(bytes[2]&0xf) << 1
	values[4] |= uint64(bytes[3] >> 7)

	values[5] = uint64((bytes[3] >> 2) & 0x1f)

	values[6] = uint64(bytes[3]&3) << 3
	values[6] |= uint64(bytes[4] >> 5)

	values[7] = uint64(bytes[4] & 0x1f)
}

func unpackBits6(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0] >> 2)

	values[1] = uint64(bytes[0]&3) << 4
	values[1] |= uint64(bytes[1] >> 4)

	values[2] = uint64(bytes[1]&0xf) << 2
	values[2] |= uint64(bytes[2] >> 6)

	values[3] = uint64(bytes[2] & 0x3f)

	values[4] = uint64(bytes[3] >> 2)

	values[5] = uint64(bytes[3]&3) << 4
	values[5] |= uint64(bytes[4] >> 4)

	values[6] = uint64(bytes[4]&0xf) << 2
	values[6] |= uint64(bytes[5] >> 6)

	values[7] = uint64(bytes[5] & 0x3f)
}

func unpackBits7(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0] >> 1)

	values[1] = uint64(bytes[0]&1) << 6
	values[1] |= uint64(bytes[1] >> 2)

	values[2] = uint64(bytes[1]&3) << 5
	values[2] |= uint64(bytes[2] >> 3)

	values[3] = uint64(bytes[2]&7) << 4
	values[3] |= uint64(bytes[3] >> 4)

	values[4] = uint64(bytes[3]&0xf) << 3
	values[4] |= uint64(bytes[4] >> 5)

	values[5] = uint64(bytes[4]&0x1f) << 2
	values[5] |= uint64(bytes[5] >> 6)

	values[6] = uint64(bytes[5]&0x3f) << 1
	values[6] |= uint64(bytes[6] >> 7)

	values[7] = uint64(bytes[6] & 0x7f)
}

func unpackBits8(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0])
	values[1] = uint64(bytes[1])
	values[2] = uint64(bytes[2])
	values[3] = uint64(bytes[3])
	values[4] = uint64(bytes[4])
	values[5] = uint64(bytes[5])
	values[6] = uint64(bytes[6])
	values[7] = uint64(bytes[7])
}

func unpackBits9(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 1
	values[0] |= uint64(bytes[1] >> 7)

	values[1] = uint64(bytes[1]&0x7f) << 2
	values[1] |= uint64(bytes[2] >> 6)

	values[2] = uint64(bytes[2]&0x3f) << 3
	values[2] |= uint64(bytes[3] >> 5)

	values[3] = uint64(bytes[3]&0x1f) << 4
	values[3] |= uint64(bytes[4] >> 4)

	values[4] = uint64(bytes[4]&0xf) << 5
	values[4] |= uint64(bytes[5] >> 3)

	values[5] = uint64(bytes[5]&7) << 6
	values[5] |= uint64(bytes[6] >> 2)

	values[6] = uint64(bytes[6]&3) << 7
	values[6] |= uint64(bytes[7] >> 1)

	values[7] = uint64(bytes[7]&1) << 8
	values[7] |= uint64(bytes[8])
}

func unpackBits10(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 2
	values[0] |= uint64(bytes[1] >> 6)

	values[1] = uint64(bytes[1]&0x3f) << 4
	values[1] |= uint64(bytes[2] >> 4)

	values[2] = uint64(bytes[2]&0xf) << 6
	values[2] |= uint64(bytes[3] >> 2)

	values[3] = uint64(bytes[3]&3) << 8
	values[3] |= uint64(bytes[4])

	values[4] = uint64(bytes[5]) << 2
	values[4] |= uint64(bytes[6] >> 6)

	values[5] = uint64(bytes[6]&0x3f) << 4
	values[5] |= uint64(bytes[7] >> 4)

	values[6] = uint64(bytes[7]&0xf) << 6
	values[6] |= uint64(bytes[8] >> 2)

	values[7] = uint64(bytes[8]&3) << 8
	values[7] |= uint64(bytes[9])
}

func unpackBits11(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 3
	values[0] |= uint64(bytes[1] >> 5)

	values[1] = uint64(bytes[1]&0x1f) << 6
	values[1] |= uint64(bytes[2] >> 2)

	values[2] = uint64(bytes[2]&3) << 9
	values[2] |= uint64(bytes[3]) << 1
	values[2] |= uint64(bytes[4] >> 7)

	values[3] = uint64(bytes[4]&0x7f) << 4
	values[3] |= uint64(bytes[5] >> 4)

	values[4] = uint64(bytes[5]&0xf) << 7
	values[4] |= uint64(bytes[6] >> 1)

	values[5] = uint64(bytes[6]&1) << 10
	values[5] |= uint64(bytes[7]) << 2
	values[5] |= uint64(bytes[8] >> 6)

	values[6] = uint64(bytes[8]&0x3f) << 5
	values[6] |= uint64(bytes[9] >> 3)

	values[7] = uint64(bytes[9]&7) << 8
	values[7] |= uint64(bytes[10])
}

func unpackBits12(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 4
	values[0] |= uint64(bytes[1] >> 4)

	values[1] = uint64(bytes[1]&0xf) << 8
	values[1] |= uint64(bytes[2])

	values[2] = uint64(bytes[3]) << 4
	values[2] |= uint64(bytes[4] >> 4)

	values[3] = uint64(bytes[4]&0xf) << 8
	values[3] |= uint64(bytes[5])

	values[4] = uint64(bytes[6]) << 4
	values[4] |= uint64(bytes[7] >> 4)

	values[5] = uint64(bytes[7]&0xf) << 8
	values[5] |= uint64(bytes[8])

	values[6] = uint64(bytes[9]) << 4
	values[6] |= uint64(bytes[10] >> 4)

	values[7] = uint64(bytes[10]&0xf) << 8
	values[7] |= uint64(bytes[11])
}

func unpackBits13(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 5
	values[0] |= uint64(bytes[1] >> 3)

	values[1] = uint64(bytes[1]&7) << 10
	values[1] |= uint64(bytes[2]) << 2
	values[1] |= uint64(bytes[3] >> 6)

	values[2] = uint64(bytes[3]&0x3f) << 7
	values[2] |= uint64(bytes[4] >> 1)

	values[3] = uint64(bytes[4]&1) << 12
	values[3] |= uint64(bytes[5]) << 4
	values[3] |= uint64(bytes[6] >> 4)

	values[4] = uint64(bytes[6]&0xf) << 9
	values[4] |= uint64(bytes[7]) << 1
	values[4] |= uint64(bytes[8] >> 7)

	values[5] = uint64(bytes[8]&0x7f) << 6
	values[5] |= uint64(bytes[9] >> 2)

	values[6] = uint64(bytes[9]&3) << 11
	values[6] |= uint64(bytes[10]) << 3
	values[6] |= uint64(bytes[11] >> 5)

	values[7] = uint64(bytes[11]&0x1f) << 8
	values[7] |= uint64(bytes[12])
}

func unpackBits14(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 6
	values[0] |= uint64(bytes[1] >> 2)

	values[1] = uint64(bytes[1]&3) << 12
	values[1] |= uint64(bytes[2]) << 4
	values[1] |= uint64(bytes[3] >> 4)

	values[2] = uint64(bytes[3]&0xf) << 10
	values[2] |= uint64(bytes[4]) << 2
	values[2] |= uint64(bytes[5] >> 6)

	values[3] = uint64(bytes[5]&0x3f) << 8
	values[3] |= uint64(bytes[6])

	values[4] = uint64(bytes[7]) << 6
	values[4] |= uint64(bytes[8] >> 2)

	values[5] = uint64(bytes[8]&3) << 12
	values[5] |= uint64(bytes[9]) << 4
	values[5] |= uint64(bytes[10] >> 4)

	values[6] = uint64(bytes[10]&0xf) << 10
	values[6] |= uint64(bytes[11]) << 2
	values[6] |= uint64(bytes[12] >> 6)

	values[7] = uint64(bytes[12]&0x3f) << 8
	values[7] |= uint64(bytes[13])
}

func unpackBits15(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 7
	values[0] |= uint64(bytes[1] >> 1)

	values[1] = uint64(bytes[1]&1) << 14
	values[1] |= uint64(bytes[2]) << 6
	values[1] |= uint64(bytes[3] >> 2)

	values[2] = uint64(bytes[3]&3) << 13
	values[2] |= uint64(bytes[4]) << 5
	values[2] |= uint64(bytes[5] >> 3)

	values[3] = uint64(bytes[5]&7) << 12
	values[3] |= uint64(bytes[6]) << 4
	values[3] |= uint64(bytes[7] >> 4)

	values[4] = uint64(bytes[7]&0xf) << 11
	values[4] |= uint64(bytes[8]) << 3
	values[4] |= uint64(bytes[9] >> 5)

	values[5] = uint64(bytes[9]&0x1f) << 10
	values[5] |= uint64(bytes[10]) << 2
	values[5] |= uint64(bytes[11] >> 6)

	values[6] = uint64(bytes[11]&0x3f) << 9
	values[6] |= uint64(bytes[12]) << 1
	values[6] |= uint64(bytes[13] >> 7)

	values[7] = uint64(bytes[13]&0x7f) << 8
	values[7] |= uint64(bytes[14])
}

func unpackBits16(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 8
	values[0] |= uint64(bytes[1])
	values[1] = uint64(bytes[2]) << 8
	values[1] |= uint64(bytes[3])
	values[2] = uint64(bytes[4]) << 8
	values[2] |= uint64(bytes[5])
	values[3] = uint64(bytes[6]) << 8
	values[3] |= uint64(bytes[7])
	values[4] = uint64(bytes[8]) << 8
	values[4] |= uint64(bytes[9])
	values[5] = uint64(bytes[10]) << 8
	values[5] |= uint64(bytes[11])
	values[6] = uint64(bytes[12]) << 8
	values[6] |= uint64(bytes[13])
	values[7] = uint64(bytes[14]) << 8
	values[7] |= uint64(bytes[15])
}

func unpackBits17(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 9
	values[0] |= uint64(bytes[1]) << 1
	values[0] |= uint64(bytes[2] >> 7)

	values[1] = uint64(bytes[2]&0x7f) << 10
	values[1] |= uint64(bytes[3]) << 2
	values[1] |= uint64(bytes[4] >> 6)

	values[2] = uint64(bytes[4]&0x3f) << 11
	values[2] |= uint64(bytes[5]) << 3
	values[2] |= uint64(bytes[6] >> 5)

	values[3] = uint64(bytes[6]&0x1f) << 12
	values[3] |= uint64(bytes[7]) << 4
	values[3] |= uint64(bytes[8] >> 4)

	values[4] = uint64(bytes[8]&0xf) << 13
	values[4] |= uint64(bytes[9]) << 5
	values[4] |= uint64(bytes[10] >> 3)

	values[5] = uint64(bytes[10]&7) << 14
	values[5] |= uint64(bytes[11]) << 6
	values[5] |= uint64(bytes[12] >> 2)

	values[6] = uint64(bytes[12]&3) << 15
	values[6] |= uint64(bytes[13]) << 7
	values[6] |= uint64(bytes[14] >> 1)

	values[7] = uint64(bytes[14]&1) << 16
	values[7] |= uint64(bytes[15]) << 8
	values[7] |= uint64(bytes[16])
}

func unpackBits18(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 10
	values[0] |= uint64(bytes[1]) << 2
	values[0] |= uint64(bytes[2] >> 6)

	values[1] = uint64(bytes[2]&0x3f) << 12
	values[1] |= uint64(bytes[3]) << 4
	values[1] |= uint64(bytes[4] >> 4)

	values[2] = uint64(bytes[4]&0xf) << 14
	values[2] |= uint64(bytes[5]) << 6
	values[2] |= uint64(bytes[6] >> 2)

	values[3] = uint64(bytes[6]&3) << 16
	values[3] |= uint64(bytes[7]) << 8
	values[3] |= uint64(bytes[8])

	values[4] = uint64(bytes[9]) << 10
	values[4] |= uint64(bytes[10]) << 2
	values[4] |= uint64(bytes[11] >> 6)

	values[5] = uint64(bytes[11]&0x3f) << 12
	values[5] |= uint64(bytes[12]) << 4
	values[5] |= uint64(bytes[13] >> 4)

	values[6] = uint64(bytes[13]&0xf) << 14
	values[6] |= uint64(bytes[14]) << 6
	values[6] |= uint64(bytes[15] >> 2)

	values[7] = uint64(bytes[15]&3) << 16
	values[7] |= uint64(bytes[16]) << 8
	values[7] |= uint64(bytes[17])
}

func unpackBits19(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 11
	values[0] |= uint64(bytes[1]) << 3
	values[0] |= uint64(bytes[2] >> 5)

	values[1] = uint64(bytes[2]&0x1f) << 14
	values[1] |= uint64(bytes[3]) << 6
	values[1] |= uint64(bytes[4] >> 2)

	values[2] = uint64(bytes[4]&3) << 17
	values[2] |= uint64(bytes[5]) << 9
	values[2] |= uint64(bytes[6]) << 1
	values[2] |= uint64(bytes[7] >> 7)

	values[3] = uint64(bytes[7]&0x7f) << 12
	values[3] |= uint64(bytes[8]) << 4
	values[3] |= uint64(bytes[9] >> 4)

	values[4] = uint64(bytes[9]&0xf) << 15
	values[4] |= uint64(bytes[10]) << 7
	values[4] |= uint64(bytes[11] >> 1)

	values[5] = uint64(bytes[11]&1) << 18
	values[5] |= uint64(bytes[12]) << 10
	values[5] |= uint64(bytes[13]) << 2
	values[5] |= uint64(bytes[14] >> 6)

	values[6] = uint64(bytes[14]&0x3f) << 13
	values[6] |= uint64(bytes[15]) << 5
	values[6] |= uint64(bytes[16] >> 3)

	values[7] = uint64(bytes[16]&7) << 16
	values[7] |= uint64(bytes[17]) << 8
	values[7] |= uint64(bytes[18])
}

func unpackBits20(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 12
	values[0] |= uint64(bytes[1]) << 4
	values[0] |= uint64(bytes[2] >> 4)

	values[1] = uint64(bytes[2]&0xf) << 16
	values[1] |= uint64(bytes[3]) << 8
	values[1] |= uint64(bytes[4])

	values[2] = uint64(bytes[5]) << 12
	values[2] |= uint64(bytes[6]) << 4
	values[2] |= uint64(bytes[7] >> 4)

	values[3] = uint64(bytes[7]&0xf) << 16
	values[3] |= uint64(bytes[8]) << 8
	values[3] |= uint64(bytes[9])

	values[4] = uint64(bytes[10]) << 12
	values[4] |= uint64(bytes[11]) << 4
	values[4] |= uint64(bytes[12] >> 4)

	values[5] = uint64(bytes[12]&0xf) << 16
	values[5] |= uint64(bytes[13]) << 8
	values[5] |= uint64(bytes[14])

	values[6] = uint64(bytes[15]) << 12
	values[6] |= uint64(bytes[16]) << 4
	values[6] |= uint64(bytes[17] >> 4)

	values[7] = uint64(bytes[17]&0xf) << 16
	values[7] |= uint64(bytes[18]) << 8
	values[7] |= uint64(bytes[19])
}

func unpackBits21(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 13
	values[0] |= uint64(bytes[1]) << 5
	values[0] |= uint64(bytes[2] >> 3)

	values[1] = uint64(bytes[2]&7) << 18
	values[1] |= uint64(bytes[3]) << 10
	values[1] |= uint64(bytes[4]) << 2
	values[1] |= uint64(bytes[5] >> 6)

	values[2] = uint64(bytes[5]&0x3f) << 15
	values[2] |= uint64(bytes[6]) << 7
	values[2] |= uint64(bytes[7] >> 1)

	values[3] = uint64(bytes[7]&1) << 20
	values[3] |= uint64(bytes[8]) << 12
	values[3] |= uint64(bytes[9]) << 4
	values[3] |= uint64(bytes[10] >> 4)

	values[4] = uint64(bytes[10]&0xf) << 17
	values[4] |= uint64(bytes[11]) << 9
	values[4] |= uint64(bytes[12]) << 1
	values[4] |= uint64(bytes[13] >> 7)

	values[5] = uint64(bytes[13]&0x7f) << 14
	values[5] |= uint64(bytes[14]) << 6
	values[5] |= uint64(bytes[15] >> 2)

	values[6] = uint64(bytes[15]&3) << 19
	values[6] |= uint64(bytes[16]) << 11
	values[6] |= uint64(bytes[17]) << 3
	values[6] |= uint64(bytes[18] >> 5)

	values[7] = uint64(bytes[18]&0x1f) << 16
	values[7] |= uint64(bytes[19]) << 8
	values[7] |= uint64(bytes[20])
}

func unpackBits22(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 14
	values[0] |= uint64(bytes[1]) << 6
	values[0] |= uint64(bytes[2] >> 2)

	values[1] = uint64(bytes[2]&3) << 20
	values[1] |= uint64(bytes[3]) << 12
	values[1] |= uint64(bytes[4]) << 4
	values[1] |= uint64(bytes[5] >> 4)

	values[2] = uint64(bytes[5]&0xf) << 18
	values[2] |= uint64(bytes[6]) << 10
	values[2] |= uint64(bytes[7]) << 2
	values[2] |= uint64(bytes[8] >> 6)

	values[3] = uint64(bytes[8]&0x3f) << 16
	values[3] |= uint64(bytes[9]) << 8
	values[3] |= uint64(bytes[10])

	values[4] = uint64(bytes[11]) << 14
	values[4] |= uint64(bytes[12]) << 6
	values[4] |= uint64(bytes[13] >> 2)

	values[5] = uint64(bytes[13]&3) << 20
	values[5] |= uint64(bytes[14]) << 12
	values[5] |= uint64(bytes[15]) << 4
	values[5] |= uint64(bytes[16] >> 4)

	values[6] = uint64(bytes[16]&0xf) << 18
	values[6] |= uint64(bytes[17]) << 10
	values[6] |= uint64(bytes[18]) << 2
	values[6] |= uint64(bytes[19] >> 6)

	values[7] = uint64(bytes[19]&0x3f) << 16
	values[7] |= uint64(bytes[20]) << 8
	values[7] |= uint64(bytes[21])
}

func unpackBits23(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 15
	values[0] |= uint64(bytes[1]) << 7
	values[0] |= uint64(bytes[2] >> 1)

	values[1] = uint64(bytes[2]&1) << 22
	values[1] |= uint64(bytes[3]) << 14
	values[1] |= uint64(bytes[4]) << 6
	values[1] |= uint64(bytes[5] >> 2)

	values[2] = uint64(bytes[5]&3) << 21
	values[2] |= uint64(bytes[6]) << 13
	values[2] |= uint64(bytes[7]) << 5
	values[2] |= uint64(bytes[8] >> 3)

	values[3] = uint64(bytes[8]&7) << 20
	values[3] |= uint64(bytes[9]) << 12
	values[3] |= uint64(bytes[10]) << 4
	values[3] |= uint64(bytes[11] >> 4)

	values[4] = uint64(bytes[11]&0xf) << 19
	values[4] |= uint64(bytes[12]) << 11
	values[4] |= uint64(bytes[13]) << 3
	values[4] |= uint64(bytes[14] >> 5)

	values[5] = uint64(bytes[14]&0x1f) << 18
	values[5] |= uint64(bytes[15]) << 10
	values[5] |= uint64(bytes[16]) << 2
	values[5] |= uint64(bytes[17] >> 6)

	values[6] = uint64(bytes[17]&0x3f) << 17
	values[6] |= uint64(bytes[18]) << 9
	values[6] |= uint64(bytes[19]) << 1
	values[6] |= uint64(bytes[20] >> 7)

	values[7] = uint64(bytes[20]&0x7f) << 16
	values[7] |= uint64(bytes[21]) << 8
	values[7] |= uint64(bytes[22])
}

func unpackBits24(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 16
	values[0] |= uint64(bytes[1]) << 8
	values[0] |= uint64(bytes[2])
	values[1] = uint64(bytes[3]) << 16
	values[1] |= uint64(bytes[4]) << 8
	values[1] |= uint64(bytes[5])
	values[2] = uint64(bytes[6]) << 16
	values[2] |= uint64(bytes[7]) << 8
	values[2] |= uint64(bytes[8])
	values[3] = uint64(bytes[9]) << 16
	values[3] |= uint64(bytes[10]) << 8
	values[3] |= uint64(bytes[11])
	values[4] = uint64(bytes[12]) << 16
	values[4] |= uint64(bytes[13]) << 8
	values[4] |= uint64(bytes[14])
	values[5] = uint64(bytes[15]) << 16
	values[5] |= uint64(bytes[16]) << 8
	values[5] |= uint64(bytes[17])
	values[6] = uint64(bytes[18]) << 16
	values[6] |= uint64(bytes[19]) << 8
	values[6] |= uint64(bytes[20])
	values[7] = uint64(bytes[21]) << 16
	values[7] |= uint64(bytes[22]) << 8
	values[7] |= uint64(bytes[23])
}

func unpackBits25(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 17
	values[0] |= uint64(bytes[1]) << 9
	values[0] |= uint64(bytes[2]) << 1
	values[0] |= uint64(bytes[3] >> 7)

	values[1] = uint64(bytes[3]&0x7f) << 18
	values[1] |= uint64(bytes[4]) << 10
	values[1] |= uint64(bytes[5]) << 2
	values[1] |= uint64(bytes[6] >> 6)

	values[2] = uint64(bytes[6]&0x3f) << 19
	values[2] |= uint64(bytes[7]) << 11
	values[2] |= uint64(bytes[8]) << 3
	values[2] |= uint64(bytes[9] >> 5)

	values[3] = uint64(bytes[9]&0x1f) << 20
	values[3] |= uint64(bytes[10]) << 12
	values[3] |= uint64(bytes[11]) << 4
	values[3] |= uint64(bytes[12] >> 4)

	values[4] = uint64(bytes[12]&0xf) << 21
	values[4] |= uint64(bytes[13]) << 13
	values[4] |= uint64(bytes[14]) << 5
	values[4] |= uint64(bytes[15] >> 3)

	values[5] = uint64(bytes[15]&7) << 22
	values[5] |= uint64(bytes[16]) << 14
	values[5] |= uint64(bytes[17]) << 6
	values[5] |= uint64(bytes[18] >> 2)

	values[6] = uint64(bytes[18]&3) << 23
	values[6] |= uint64(bytes[19]) << 15
	values[6] |= uint64(bytes[20]) << 7
	values[6] |= uint64(bytes[21] >> 1)

	values[7] = uint64(bytes[21]&1) << 24
	values[7] |= uint64(bytes[22]) << 16
	values[7] |= uint64(bytes[23]) << 8
	values[7] |= uint64(bytes[24])
}

func unpackBits26(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 18
	values[0] |= uint64(bytes[1]) << 10
	values[0] |= uint64(bytes[2]) << 2
	values[0] |= uint64(bytes[3] >> 6)

	values[1] = uint64(bytes[3]&0x3f) << 20
	values[1] |= uint64(bytes[4]) << 12
	values[1] |= uint64(bytes[5]) << 4
	values[1] |= uint64(bytes[6] >> 4)

	values[2] = uint64(bytes[6]&0xf) << 22
	values[2] |= uint64(bytes[7]) << 14
	values[2] |= uint64(bytes[8]) << 6
	values[2] |= uint64(bytes[9] >> 2)

	values[3] = uint64(bytes[9]&3) << 24
	values[3] |= uint64(bytes[10]) << 16
	values[3] |= uint64(bytes[11]) << 8
	values[3] |= uint64(bytes[12])

	values[4] = uint64(bytes[13]) << 18
	values[4] |= uint64(bytes[14]) << 10
	values[4] |= uint64(bytes[15]) << 2
	values[4] |= uint64(bytes[16] >> 6)

	values[5] = uint64(bytes[16]&0x3f) << 20
	values[5] |= uint64(bytes[17]) << 12
	values[5] |= uint64(bytes[18]) << 4
	values[5] |= uint64(bytes[19] >> 4)

	values[6] = uint64(bytes[19]&0xf) << 22
	values[6] |= uint64(bytes[20]) << 14
	values[6] |= uint64(bytes[21]) << 6
	values[6] |= uint64(bytes[22] >> 2)

	values[7] = uint64(bytes[22]&3) << 24
	values[7] |= uint64(bytes[23]) << 16
	values[7] |= uint64(bytes[24]) << 8
	values[7] |= uint64(bytes[25])
}

func unpackBits27(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 19
	values[0] |= uint64(bytes[1]) << 11
	values[0] |= uint64(bytes[2]) << 3
	values[0] |= uint64(bytes[3] >> 5)

	values[1] = uint64(bytes[3]&0x1f) << 22
	values[1] |= uint64(bytes[4]) << 14
	values[1] |= uint64(bytes[5]) << 6
	values[1] |= uint64(bytes[6] >> 2)

	values[2] = uint64(bytes[6]&3) << 25
	values[2] |= uint64(bytes[7]) << 17
	values[2] |= uint64(bytes[8]) << 9
	values[2] |= uint64(bytes[9]) << 1
	values[2] |= uint64(bytes[10] >> 7)

	values[3] = uint64(bytes[10]&0x7f) << 20
	values[3] |= uint64(bytes[11]) << 12
	values[3] |= uint64(bytes[12]) << 4
	values[3] |= uint64(bytes[13] >> 4)

	values[4] = uint64(bytes[13]&0xf) << 23
	values[4] |= uint64(bytes[14]) << 15
	values[4] |= uint64(bytes[15]) << 7
	values[4] |= uint64(bytes[16] >> 1)

	values[5] = uint64(bytes[16]&1) << 26
	values[5] |= uint64(bytes[17]) << 18
	values[5] |= uint64(bytes[18]) << 10
	values[5] |= uint64(bytes[19]) << 2
	values[5] |= uint64(bytes[20] >> 6)

	values[6] = uint64(bytes[20]&0x3f) << 21
	values[6] |= uint64(bytes[21]) << 13
	values[6] |= uint64(bytes[22]) << 5
	values[6] |= uint64(bytes[23] >> 3)

	values[7] = uint64(bytes[23]&7) << 24
	values[7] |= uint64(bytes[24]) << 16
	values[7] |= uint64(bytes[25]) << 8
	values[7] |= uint64(bytes[26])
}

func unpackBits28(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 20
	values[0] |= uint64(bytes[1]) << 12
	values[0] |= uint64(bytes[2]) << 4
	values[0] |= uint64(bytes[3] >> 4)

	values[1] = uint64(bytes[3]&0xf) << 24
	values[1] |= uint64(bytes[4]) << 16
	values[1] |= uint64(bytes[5]) << 8
	values[1] |= uint64(bytes[6])

	values[2] = uint64(bytes[7]) << 20
	values[2] |= uint64(bytes[8]) << 12
	values[2] |= uint64(bytes[9]) << 4
	values[2] |= uint64(bytes[10] >> 4)

	values[3] = uint64(bytes[10]&0xf) << 24
	values[3] |= uint64(bytes[11]) << 16
	values[3] |= uint64(bytes[12]) << 8
	values[3] |= uint64(bytes[13])

	values[4] = uint64(bytes[14]) << 20
	values[4] |= uint64(bytes[15]) << 12
	values[4] |= uint64(bytes[16]) << 4
	values[4] |= uint64(bytes[17] >> 4)

	values[5] = uint64(bytes[17]&0xf) << 24
	values[5] |= uint64(bytes[18]) << 16
	values[5] |= uint64(bytes[19]) << 8
	values[5] |= uint64(bytes[20])

	values[6] = uint64(bytes[21]) << 20
	values[6] |= uint64(bytes[22]) << 12
	values[6] |= uint64(bytes[23]) << 4
	values[6] |= uint64(bytes[24] >> 4)

	values[7] = uint64(bytes[24]&0xf) << 24
	values[7] |= uint64(bytes[25]) << 16
	values[7] |= uint64(bytes[26]) << 8
	values[7] |= uint64(bytes[27])
}

func unpackBits29(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 21
	values[0] |= uint64(bytes[1]) << 13
	values[0] |= uint64(bytes[2]) << 5
	values[0] |= uint64(bytes[3] >> 3)

	values[1] = uint64(bytes[3]&7) << 26
	values[1] |= uint64(bytes[4]) << 18
	values[1] |= uint64(bytes[5]) << 10
	values[1] |= uint64(bytes[6]) << 2
	values[1] |= uint64(bytes[7] >> 6)

	values[2] = uint64(bytes[7]&0x3f) << 23
	values[2] |= uint64(bytes[8]) << 15
	values[2] |= uint64(bytes[9]) << 7
	values[2] |= uint64(bytes[10] >> 1)

	values[3] = uint64(bytes[10]&1) << 28
	values[3] |= uint64(bytes[11]) << 20
	values[3] |= uint64(bytes[12]) << 12
	values[3] |= uint64(bytes[13]) << 4
	values[3] |= uint64(bytes[14] >> 4)

	values[4] = uint64(bytes[14]&0xf) << 25
	values[4] |= uint64(bytes[15]) << 17
	values[4] |= uint64(bytes[16]) << 9
	values[4] |= uint64(bytes[17]) << 1
	values[4] |= uint64(bytes[18] >> 7)

	values[5] = uint64(bytes[18]&0x7f) << 22
	values[5] |= uint64(bytes[19]) << 14
	values[5] |= uint64(bytes[20]) << 6
	values[5] |= uint64(bytes[21] >> 2)

	values[6] = uint64(bytes[21]&3) << 27
	values[6] |= uint64(bytes[22]) << 19
	values[6] |= uint64(bytes[23]) << 11
	values[6] |= uint64(bytes[24]) << 3
	values[6] |= uint64(bytes[25] >> 5)

	values[7] = uint64(bytes[25]&0x1f) << 24
	values[7] |= uint64(bytes[26]) << 16
	values[7] |= uint64(bytes[27]) << 8
	values[7] |= uint64(bytes[28])
}

func unpackBits30(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 22
	values[0] |= uint64(bytes[1]) << 14
	values[0] |= uint64(bytes[2]) << 6
	values[0] |= uint64(bytes[3] >> 2)

	values[1] = uint64(bytes[3]&3) << 28
	values[1] |= uint64(bytes[4]) << 20
	values[1] |= uint64(bytes[5]) << 12
	values[1] |= uint64(bytes[6]) << 4
	values[1] |= uint64(bytes[7] >> 4)

	values[2] = uint64(bytes[7]&0xf) << 26
	values[2] |= uint64(bytes[8]) << 18
	values[2] |= uint64(bytes[9]) << 10
	values[2] |= uint64(bytes[10]) << 2
	values[2] |= uint64(bytes[11] >> 6)

	values[3] = uint64(bytes[11]&0x3f) << 24
	values[3] |= uint64(bytes[12]) << 16
	values[3] |= uint64(bytes[13]) << 8
	values[3] |= uint64(bytes[14])

	values[4] = uint64(bytes[15]) << 22
	values[4] |= uint64(bytes[16]) << 14
	values[4] |= uint64(bytes[17]) << 6
	values[4] |= uint64(bytes[18] >> 2)

	values[5] = uint64(bytes[18]&3) << 28
	values[5] |= uint64(bytes[19]) << 20
	values[5] |= uint64(bytes[20]) << 12
	values[5] |= uint64(bytes[21]) << 4
	values[5] |= uint64(bytes[22] >> 4)

	values[6] = uint64(bytes[22]&0xf) << 26
	values[6] |= uint64(bytes[23]) << 18
	values[6] |= uint64(bytes[24]) << 10
	values[6] |= uint64(bytes[25]) << 2
	values[6] |= uint64(bytes[26] >> 6)

	values[7] = uint64(bytes[26]&0x3f) << 24
	values[7] |= uint64(bytes[27]) << 16
	values[7] |= uint64(bytes[28]) << 8
	values[7] |= uint64(bytes[29])
}

func unpackBits31(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 23
	values[0] |= uint64(bytes[1]) << 15
	values[0] |= uint64(bytes[2]) << 7
	values[0] |= uint64(bytes[3] >> 1)

	values[1] = uint64(bytes[3]&1) << 30
	values[1] |= uint64(bytes[4]) << 22
	values[1] |= uint64(bytes[5]) << 14
	values[1] |= uint64(bytes[6]) << 6
	values[1] |= uint64(bytes[7] >> 2)

	values[2] = uint64(bytes[7]&3) << 29
	values[2] |= uint64(bytes[8]) << 21
	values[2] |= uint64(bytes[9]) << 13
	values[2] |= uint64(bytes[10]) << 5
	values[2] |= uint64(bytes[11] >> 3)

	values[3] = uint64(bytes[11]&7) << 28
	values[3] |= uint64(bytes[12]) << 20
	values[3] |= uint64(bytes[13]) << 12
	values[3] |= uint64(bytes[14]) << 4
	values[3] |= uint64(bytes[15] >> 4)

	values[4] = uint64(bytes[15]&0xf) << 27
	values[4] |= uint64(bytes[16]) << 19
	values[4] |= uint64(bytes[17]) << 11
	values[4] |= uint64(bytes[18]) << 3
	values[4] |= uint64(bytes[19] >> 5)

	values[5] = uint64(bytes[19]&0x1f) << 26
	values[5] |= uint64(bytes[20]) << 18
	values[5] |= uint64(bytes[21]) << 10
	values[5] |= uint64(bytes[22]) << 2
	values[5] |= uint64(bytes[23] >> 6)

	values[6] = uint64(bytes[23]&0x3f) << 25
	values[6] |= uint64(bytes[24]) << 17
	values[6] |= uint64(bytes[25]) << 9
	values[6] |= uint64(bytes[26]) << 1
	values[6] |= uint64(bytes[27] >> 7)

	values[7] = uint64(bytes[27]&0x7f) << 24
	values[7] |= uint64(bytes[28]) << 16
	values[7] |= uint64(bytes[29]) << 8
	values[7] |= uint64(bytes[30])
}

func unpackBits32(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 24
	values[0] |= uint64(bytes[1]) << 16
	values[0] |= uint64(bytes[2]) << 8
	values[0] |= uint64(bytes[3])
	values[1] = uint64(bytes[4]) << 24
	values[1] |= uint64(bytes[5]) << 16
	values[1] |= uint64(bytes[6]) << 8
	values[1] |= uint64(bytes[7])
	values[2] = uint64(bytes[8]) << 24
	values[2] |= uint64(bytes[9]) << 16
	values[2] |= uint64(bytes[10]) << 8
	values[2] |= uint64(bytes[11])
	values[3] = uint64(bytes[12]) << 24
	values[3] |= uint64(bytes[13]) << 16
	values[3] |= uint64(bytes[14]) << 8
	values[3] |= uint64(bytes[15])
	values[4] = uint64(bytes[16]) << 24
	values[4] |= uint64(bytes[17]) << 16
	values[4] |= uint64(bytes[18]) << 8
	values[4] |= uint64(bytes[19])
	values[5] = uint64(bytes[20]) << 24
	values[5] |= uint64(bytes[21]) << 16
	values[5] |= uint64(bytes[22]) << 8
	values[5] |= uint64(bytes[23])
	values[6] = uint64(bytes[24]) << 24
	values[6] |= uint64(bytes[25]) << 16
	values[6] |= uint64(bytes[26]) << 8
	values[6] |= uint64(bytes[27])
	values[7] = uint64(bytes[28]) << 24
	values[7] |= uint64(bytes[29]) << 16
	values[7] |= uint64(bytes[30]) << 8
	values[7] |= uint64(bytes[31])
}

func unpackBits33(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 25
	values[0] |= uint64(bytes[1]) << 17
	values[0] |= uint64(bytes[2]) << 9
	values[0] |= uint64(bytes[3]) << 1
	values[0] |= uint64(bytes[4] >> 7)

	values[1] = uint64(bytes[4]&0x7f) << 26
	values[1] |= uint64(bytes[5]) << 18
	values[1] |= uint64(bytes[6]) << 10
	values[1] |= uint64(bytes[7]) << 2
	values[1] |= uint64(bytes[8] >> 6)

	values[2] = uint64(bytes[8]&0x3f) << 27
	values[2] |= uint64(bytes[9]) << 19
	values[2] |= uint64(bytes[10]) << 11
	values[2] |= uint64(bytes[11]) << 3
	values[2] |= uint64(bytes[12] >> 5)

	values[3] = uint64(bytes[12]&0x1f) << 28
	values[3] |= uint64(bytes[13]) << 20
	values[3] |= uint64(bytes[14]) << 12
	values[3] |= uint64(bytes[15]) << 4
	values[3] |= uint64(bytes[16] >> 4)

	values[4] = uint64(bytes[16]&0xf) << 29
	values[4] |= uint64(bytes[17]) << 21
	values[4] |= uint64(bytes[18]) << 13
	values[4] |= uint64(bytes[19]) << 5
	values[4] |= uint64(bytes[20] >> 3)

	values[5] = uint64(bytes[20]&7) << 30
	values[5] |= uint64(bytes[21]) << 22
	values[5] |= uint64(bytes[22]) << 14
	values[5] |= uint64(bytes[23]) << 6
	values[5] |= uint64(bytes[24] >> 2)

	values[6] = uint64(bytes[24]&3) << 31
	values[6] |= uint64(bytes[25]) << 23
	values[6] |= uint64(bytes[26]) << 15
	values[6] |= uint64(bytes[27]) << 7
	values[6] |= uint64(bytes[28] >> 1)

	values[7] = uint64(bytes[28]&1) << 32
	values[7] |= uint64(bytes[29]) << 24
	values[7] |= uint64(bytes[30]) << 16
	values[7] |= uint64(bytes[31]) << 8
	values[7] |= uint64(bytes[32])
}

func unpackBits34(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 26
	values[0] |= uint64(bytes[1]) << 18
	values[0] |= uint64(bytes[2]) << 10
	values[0] |= uint64(bytes[3]) << 2
	values[0] |= uint64(bytes[4] >> 6)

	values[1] = uint64(bytes[4]&0x3f) << 28
	values[1] |= uint64(bytes[5]) << 20
	values[1] |= uint64(bytes[6]) << 12
	values[1] |= uint64(bytes[7]) << 4
	values[1] |= uint64(bytes[8] >> 4)

	values[2] = uint64(bytes[8]&0xf) << 30
	values[2] |= uint64(bytes[9]) << 22
	values[2] |= uint64(bytes[10]) << 14
	values[2] |= uint64(bytes[11]) << 6
	values[2] |= uint64(bytes[12] >> 2)

	values[3] = uint64(bytes[12]&3) << 32
	values[3] |= uint64(bytes[13]) << 24
	values[3] |= uint64(bytes[14]) << 16
	values[3] |= uint64(bytes[15]) << 8
	values[3] |= uint64(bytes[16])

	values[4] = uint64(bytes[17]) << 26
	values[4] |= uint64(bytes[18]) << 18
	values[4] |= uint64(bytes[19]) << 10
	values[4] |= uint64(bytes[20]) << 2
	values[4] |= uint64(bytes[21] >> 6)

	values[5] = uint64(bytes[21]&0x3f) << 28
	values[5] |= uint64(bytes[22]) << 20
	values[5] |= uint64(bytes[23]) << 12
	values[5] |= uint64(bytes[24]) << 4
	values[5] |= uint64(bytes[25] >> 4)

	values[6] = uint64(bytes[25]&0xf) << 30
	values[6] |= uint64(bytes[26]) << 22
	values[6] |= uint64(bytes[27]) << 14
	values[6] |= uint64(bytes[28]) << 6
	values[6] |= uint64(bytes[29] >> 2)

	values[7] = uint64(bytes[29]&3) << 32
	values[7] |= uint64(bytes[30]) << 24
	values[7] |= uint64(bytes[31]) << 16
	values[7] |= uint64(bytes[32]) << 8
	values[7] |= uint64(bytes[33])
}

func unpackBits35(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 27
	values[0] |= uint64(bytes[1]) << 19
	values[0] |= uint64(bytes[2]) << 11
	values[0] |= uint64(bytes[3]) << 3
	values[0] |= uint64(bytes[4] >> 5)

	values[1] = uint64(bytes[4]&0x1f) << 30
	values[1] |= uint64(bytes[5]) << 22
	values[1] |= uint64(bytes[6]) << 14
	values[1] |= uint64(bytes[7]) << 6
	values[1] |= uint64(bytes[8] >> 2)

	values[2] = uint64(bytes[8]&3) << 33
	values[2] |= uint64(bytes[9]) << 25
	values[2] |= uint64(bytes[10]) << 17
	values[2] |= uint64(bytes[11]) << 9
	values[2] |= uint64(bytes[12]) << 1
	values[2] |= uint64(bytes[13] >> 7)

	values[3] = uint64(bytes[13]&0x7f) << 28
	values[3] |= uint64(bytes[14]) << 20
	values[3] |= uint64(bytes[15]) << 12
	values[3] |= uint64(bytes[16]) << 4
	values[3] |= uint64(bytes[17] >> 4)

	values[4] = uint64(bytes[17]&0xf) << 31
	values[4] |= uint64(bytes[18]) << 23
	values[4] |= uint64(bytes[19]) << 15
	values[4] |= uint64(bytes[20]) << 7
	values[4] |= uint64(bytes[21] >> 1)

	values[5] = uint64(bytes[21]&1) << 34
	values[5] |= uint64(bytes[22]) << 26
	values[5] |= uint64(bytes[23]) << 18
	values[5] |= uint64(bytes[24]) << 10
	values[5] |= uint64(bytes[25]) << 2
	values[5] |= uint64(bytes[26] >> 6)

	values[6] = uint64(bytes[26]&0x3f) << 29
	values[6] |= uint64(bytes[27]) << 21
	values[6] |= uint64(bytes[28]) << 13
	values[6] |= uint64(bytes[29]) << 5
	values[6] |= uint64(bytes[30] >> 3)

	values[7] = uint64(bytes[30]&7) << 32
	values[7] |= uint64(bytes[31]) << 24
	values[7] |= uint64(bytes[32]) << 16
	values[7] |= uint64(bytes[33]) << 8
	values[7] |= uint64(bytes[34])
}

func unpackBits36(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 28
	values[0] |= uint64(bytes[1]) << 20
	values[0] |= uint64(bytes[2]) << 12
	values[0] |= uint64(bytes[3]) << 4
	values[0] |= uint64(bytes[4] >> 4)

	values[1] = uint64(bytes[4]&0xf) << 32
	values[1] |= uint64(bytes[5]) << 24
	values[1] |= uint64(bytes[6]) << 16
	values[1] |= uint64(bytes[7]) << 8
	values[1] |= uint64(bytes[8])

	values[2] = uint64(bytes[9]) << 28
	values[2] |= uint64(bytes[10]) << 20
	values[2] |= uint64(bytes[11]) << 12
	values[2] |= uint64(bytes[12]) << 4
	values[2] |= uint64(bytes[13] >> 4)

	values[3] = uint64(bytes[13]&0xf) << 32
	values[3] |= uint64(bytes[14]) << 24
	values[3] |= uint64(bytes[15]) << 16
	values[3] |= uint64(bytes[16]) << 8
	values[3] |= uint64(bytes[17])

	values[4] = uint64(bytes[18]) << 28
	values[4] |= uint64(bytes[19]) << 20
	values[4] |= uint64(bytes[20]) << 12
	values[4] |= uint64(bytes[21]) << 4
	values[4] |= uint64(bytes[22] >> 4)

	values[5] = uint64(bytes[22]&0xf) << 32
	values[5] |= uint64(bytes[23]) << 24
	values[5] |= uint64(bytes[24]) << 16
	values[5] |= uint64(bytes[25]) << 8
	values[5] |= uint64(bytes[26])

	values[6] = uint64(bytes[27]) << 28
	values[6] |= uint64(bytes[28]) << 20
	values[6] |= uint64(bytes[29]) << 12
	values[6] |= uint64(bytes[30]) << 4
	values[6] |= uint64(bytes[31] >> 4)

	values[7] = uint64(bytes[31]&0xf) << 32
	values[7] |= uint64(bytes[32]) << 24
	values[7] |= uint64(bytes[33]) << 16
	values[7] |= uint64(bytes[34]) << 8
	values[7] |= uint64(bytes[35])
}

func unpackBits37(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 29
	values[0] |= uint64(bytes[1]) << 21
	values[0] |= uint64(bytes[2]) << 13
	values[0] |= uint64(bytes[3]) << 5
	values[0] |= uint64(bytes[4] >> 3)

	values[1] = uint64(bytes[4]&7) << 34
	values[1] |= uint64(bytes[5]) << 26
	values[1] |= uint64(bytes[6]) << 18
	values[1] |= uint64(bytes[7]) << 10
	values[1] |= uint64(bytes[8]) << 2
	values[1] |= uint64(bytes[9] >> 6)

	values[2] = uint64(bytes[9]&0x3f) << 31
	values[2] |= uint64(bytes[10]) << 23
	values[2] |= uint64(bytes[11]) << 15
	values[2] |= uint64(bytes[12]) << 7
	values[2] |= uint64(bytes[13] >> 1)

	values[3] = uint64(bytes[13]&1) << 36
	values[3] |= uint64(bytes[14]) << 28
	values[3] |= uint64(bytes[15]) << 20
	values[3] |= uint64(bytes[16]) << 12
	values[3] |= uint64(bytes[17]) << 4
	values[3] |= uint64(bytes[18] >> 4)

	values[4] = uint64(bytes[18]&0xf) << 33
	values[4] |= uint64(bytes[19]) << 25
	values[4] |= uint64(bytes[20]) << 17
	values[4] |= uint64(bytes[21]) << 9
	values[4] |= uint64(bytes[22]) << 1
	values[4] |= uint64(bytes[23] >> 7)

	values[5] = uint64(bytes[23]&0x7f) << 30
	values[5] |= uint64(bytes[24]) << 22
	values[5] |= uint64(bytes[25]) << 14
	values[5] |= uint64(bytes[26]) << 6
	values[5] |= uint64(bytes[27] >> 2)

	values[6] = uint64(bytes[27]&3) << 35
	values[6] |= uint64(bytes[28]) << 27
	values[6] |= uint64(bytes[29]) << 19
	values[6] |= uint64(bytes[30]) << 11
	values[6] |= uint64(bytes[31]) << 3
	values[6] |= uint64(bytes[32] >> 5)

	values[7] = uint64(bytes[32]&0x1f) << 32
	values[7] |= uint64(bytes[33]) << 24
	values[7] |= uint64(bytes[34]) << 16
	values[7] |= uint64(bytes[35]) << 8
	values[7] |= uint64(bytes[36])
}

func unpackBits38(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 30
	values[0] |= uint64(bytes[1]) << 22
	values[0] |= uint64(bytes[2]) << 14
	values[0] |= uint64(bytes[3]) << 6
	values[0] |= uint64(bytes[4] >> 2)

	values[1] = uint64(bytes[4]&3) << 36
	values[1] |= uint64(bytes[5]) << 28
	values[1] |= uint64(bytes[6]) << 20
	values[1] |= uint64(bytes[7]) << 12
	values[1] |= uint64(bytes[8]) << 4
	values[1] |= uint64(bytes[9] >> 4)

	values[2] = uint64(bytes[9]&0xf) << 34
	values[2] |= uint64(bytes[10]) << 26
	values[2] |= uint64(bytes[11]) << 18
	values[2] |= uint64(bytes[12]) << 10
	values[2] |= uint64(bytes[13]) << 2
	values[2] |= uint64(bytes[14] >> 6)

	values[3] = uint64(bytes[14]&0x3f) << 32
	values[3] |= uint64(bytes[15]) << 24
	values[3] |= uint64(bytes[16]) << 16
	values[3] |= uint64(bytes[17]) << 8
	values[3] |= uint64(bytes[18])

	values[4] = uint64(bytes[19]) << 30
	values[4] |= uint64(bytes[20]) << 22
	values[4] |= uint64(bytes[21]) << 14
	values[4] |= uint64(bytes[22]) << 6
	values[4] |= uint64(bytes[23] >> 2)

	values[5] = uint64(bytes[23]&3) << 36
	values[5] |= uint64(bytes[24]) << 28
	values[5] |= uint64(bytes[25]) << 20
	values[5] |= uint64(bytes[26]) << 12
	values[5] |= uint64(bytes[27]) << 4
	values[5] |= uint64(bytes[28] >> 4)

	values[6] = uint64(bytes[28]&0xf) << 34
	values[6] |= uint64(bytes[29]) << 26
	values[6] |= uint64(bytes[30]) << 18
	values[6] |= uint64(bytes[31]) << 10
	values[6] |= uint64(bytes[32]) << 2
	values[6] |= uint64(bytes[33] >> 6)

	values[7] = uint64(bytes[33]&0x3f) << 32
	values[7] |= uint64(bytes[34]) << 24
	values[7] |= uint64(bytes[35]) << 16
	values[7] |= uint64(bytes[36]) << 8
	values[7] |= uint64(bytes[37])
}

func unpackBits39(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 31
	values[0] |= uint64(bytes[1]) << 23
	values[0] |= uint64(bytes[2]) << 15
	values[0] |= uint64(bytes[3]) << 7
	values[0] |= uint64(bytes[4] >> 1)

	values[1] = uint64(bytes[4]&1) << 38
	values[1] |= uint64(bytes[5]) << 30
	values[1] |= uint64(bytes[6]) << 22
	values[1] |= uint64(bytes[7]) << 14
	values[1] |= uint64(bytes[8]) << 6
	values[1] |= uint64(bytes[9] >> 2)

	values[2] = uint64(bytes[9]&3) << 37
	values[2] |= uint64(bytes[10]) << 29
	values[2] |= uint64(bytes[11]) << 21
	values[2] |= uint64(bytes[12]) << 13
	values[2] |= uint64(bytes[13]) << 5
	values[2] |= uint64(bytes[14] >> 3)

	values[3] = uint64(bytes[14]&7) << 36
	values[3] |= uint64(bytes[15]) << 28
	values[3] |= uint64(bytes[16]) << 20
	values[3] |= uint64(bytes[17]) << 12
	values[3] |= uint64(bytes[18]) << 4
	values[3] |= uint64(bytes[19] >> 4)

	values[4] = uint64(bytes[19]&0xf) << 35
	values[4] |= uint64(bytes[20]) << 27
	values[4] |= uint64(bytes[21]) << 19
	values[4] |= uint64(bytes[22]) << 11
	values[4] |= uint64(bytes[23]) << 3
	values[4] |= uint64(bytes[24] >> 5)

	values[5] = uint64(bytes[24]&0x1f) << 34
	values[5] |= uint64(bytes[25]) << 26
	values[5] |= uint64(bytes[26]) << 18
	values[5] |= uint64(bytes[27]) << 10
	values[5] |= uint64(bytes[28]) << 2
	values[5] |= uint64(bytes[29] >> 6)

	values[6] = uint64(bytes[29]&0x3f) << 33
	values[6] |= uint64(bytes[30]) << 25
	values[6] |= uint64(bytes[31]) << 17
	values[6] |= uint64(bytes[32]) << 9
	values[6] |= uint64(bytes[33]) << 1
	values[6] |= uint64(bytes[34] >> 7)

	values[7] = uint64(bytes[34]&0x7f) << 32
	values[7] |= uint64(bytes[35]) << 24
	values[7] |= uint64(bytes[36]) << 16
	values[7] |= uint64(bytes[37]) << 8
	values[7] |= uint64(bytes[38])
}

func unpackBits40(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 32
	values[0] |= uint64(bytes[1]) << 24
	values[0] |= uint64(bytes[2]) << 16
	values[0] |= uint64(bytes[3]) << 8
	values[0] |= uint64(bytes[4])
	values[1] = uint64(bytes[5]) << 32
	values[1] |= uint64(bytes[6]) << 24
	values[1] |= uint64(bytes[7]) << 16
	values[1] |= uint64(bytes[8]) << 8
	values[1] |= uint64(bytes[9])
	values[2] = uint64(bytes[10]) << 32
	values[2] |= uint64(bytes[11]) << 24
	values[2] |= uint64(bytes[12]) << 16
	values[2] |= uint64(bytes[13]) << 8
	values[2] |= uint64(bytes[14])
	values[3] = uint64(bytes[15]) << 32
	values[3] |= uint64(bytes[16]) << 24
	values[3] |= uint64(bytes[17]) << 16
	values[3] |= uint64(bytes[18]) << 8
	values[3] |= uint64(bytes[19])
	values[4] = uint64(bytes[20]) << 32
	values[4] |= uint64(bytes[21]) << 24
	values[4] |= uint64(bytes[22]) << 16
	values[4] |= uint64(bytes[23]) << 8
	values[4] |= uint64(bytes[24])
	values[5] = uint64(bytes[25]) << 32
	values[5] |= uint64(bytes[26]) << 24
	values[5] |= uint64(bytes[27]) << 16
	values[5] |= uint64(bytes[28]) << 8
	values[5] |= uint64(bytes[29])
	values[6] = uint64(bytes[30]) << 32
	values[6] |= uint64(bytes[31]) << 24
	values[6] |= uint64(bytes[32]) << 16
	values[6] |= uint64(bytes[33]) << 8
	values[6] |= uint64(bytes[34])
	values[7] = uint64(bytes[35]) << 32
	values[7] |= uint64(bytes[36]) << 24
	values[7] |= uint64(bytes[37]) << 16
	values[7] |= uint64(bytes[38]) << 8
	values[7] |= uint64(bytes[39])
}

func unpackBits41(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 33
	values[0] |= uint64(bytes[1]) << 25
	values[0] |= uint64(bytes[2]) << 17
	values[0] |= uint64(bytes[3]) << 9
	values[0] |= uint64(bytes[4]) << 1
	values[0] |= uint64(bytes[5] >> 7)

	values[1] = uint64(bytes[5]&0x7f) << 34
	values[1] |= uint64(bytes[6]) << 26
	values[1] |= uint64(bytes[7]) << 18
	values[1] |= uint64(bytes[8]) << 10
	values[1] |= uint64(bytes[9]) << 2
	values[1] |= uint64(bytes[10] >> 6)

	values[2] = uint64(bytes[10]&0x3f) << 35
	values[2] |= uint64(bytes[11]) << 27
	values[2] |= uint64(bytes[12]) << 19
	values[2] |= uint64(bytes[13]) << 11
	values[2] |= uint64(bytes[14]) << 3
	values[2] |= uint64(bytes[15] >> 5)

	values[3] = uint64(bytes[15]&0x1f) << 36
	values[3] |= uint64(bytes[16]) << 28
	values[3] |= uint64(bytes[17]) << 20
	values[3] |= uint64(bytes[18]) << 12
	values[3] |= uint64(bytes[19]) << 4
	values[3] |= uint64(bytes[20] >> 4)

	values[4] = uint64(bytes[20]&0xf) << 37
	values[4] |= uint64(bytes[21]) << 29
	values[4] |= uint64(bytes[22]) << 21
	values[4] |= uint64(bytes[23]) << 13
	values[4] |= uint64(bytes[24]) << 5
	values[4] |= uint64(bytes[25] >> 3)

	values[5] = uint64(bytes[25]&7) << 38
	values[5] |= uint64(bytes[26]) << 30
	values[5] |= uint64(bytes[27]) << 22
	values[5] |= uint64(bytes[28]) << 14
	values[5] |= uint64(bytes[29]) << 6
	values[5] |= uint64(bytes[30] >> 2)

	values[6] = uint64(bytes[30]&3) << 39
	values[6] |= uint64(bytes[31]) << 31
	values[6] |= uint64(bytes[32]) << 23
	values[6] |= uint64(bytes[33]) << 15
	values[6] |= uint64(bytes[34]) << 7
	values[6] |= uint64(bytes[35] >> 1)

	values[7] = uint64(bytes[35]&1) << 40
	values[7] |= uint64(bytes[36]) << 32
	values[7] |= uint64(bytes[37]) << 24
	values[7] |= uint64(bytes[38]) << 16
	values[7] |= uint64(bytes[39]) << 8
	values[7] |= uint64(bytes[40])
}

func unpackBits42(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 34
	values[0] |= uint64(bytes[1]) << 26
	values[0] |= uint64(bytes[2]) << 18
	values[0] |= uint64(bytes[3]) << 10
	values[0] |= uint64(bytes[4]) << 2
	values[0] |= uint64(bytes[5] >> 6)

	values[1] = uint64(bytes[5]&0x3f) << 36
	values[1] |= uint64(bytes[6]) << 28
	values[1] |= uint64(bytes[7]) << 20
	values[1] |= uint64(bytes[8]) << 12
	values[1] |= uint64(bytes[9]) << 4
	values[1] |= uint64(bytes[10] >> 4)

	values[2] = uint64(bytes[10]&0xf) << 38
	values[2] |= uint64(bytes[11]) << 30
	values[2] |= uint64(bytes[12]) << 22
	values[2] |= uint64(bytes[13]) << 14
	values[2] |= uint64(bytes[14]) << 6
	values[2] |= uint64(bytes[15] >> 2)

	values[3] = uint64(bytes[15]&3) << 40
	values[3] |= uint64(bytes[16]) << 32
	values[3] |= uint64(bytes[17]) << 24
	values[3] |= uint64(bytes[18]) << 16
	values[3] |= uint64(bytes[19]) << 8
	values[3] |= uint64(bytes[20])

	values[4] = uint64(bytes[21]) << 34
	values[4] |= uint64(bytes[22]) << 26
	values[4] |= uint64(bytes[23]) << 18
	values[4] |= uint64(bytes[24]) << 10
	values[4] |= uint64(bytes[25]) << 2
	values[4] |= uint64(bytes[26] >> 6)

	values[5] = uint64(bytes[26]&0x3f) << 36
	values[5] |= uint64(bytes[27]) << 28
	values[5] |= uint64(bytes[28]) << 20
	values[5] |= uint64(bytes[29]) << 12
	values[5] |= uint64(bytes[30]) << 4
	values[5] |= uint64(bytes[31] >> 4)

	values[6] = uint64(bytes[31]&0xf) << 38
	values[6] |= uint64(bytes[32]) << 30
	values[6] |= uint64(bytes[33]) << 22
	values[6] |= uint64(bytes[34]) << 14
	values[6] |= uint64(bytes[35]) << 6
	values[6] |= uint64(bytes[36] >> 2)

	values[7] = uint64(bytes[36]&3) << 40
	values[7] |= uint64(bytes[37]) << 32
	values[7] |= uint64(bytes[38]) << 24
	values[7] |= uint64(bytes[39]) << 16
	values[7] |= uint64(bytes[40]) << 8
	values[7] |= uint64(bytes[41])
}

func unpackBits43(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 35
	values[0] |= uint64(bytes[1]) << 27
	values[0] |= uint64(bytes[2]) << 19
	values[0] |= uint64(bytes[3]) << 11
	values[0] |= uint64(bytes[4]) << 3
	values[0] |= uint64(bytes[5] >> 5)

	values[1] = uint64(bytes[5]&0x1f) << 38
	values[1] |= uint64(bytes[6]) << 30
	values[1] |= uint64(bytes[7]) << 22
	values[1] |= uint64(bytes[8]) << 14
	values[1] |= uint64(bytes[9]) << 6
	values[1] |= uint64(bytes[10] >> 2)

	values[2] = uint64(bytes[10]&3) << 41
	values[2] |= uint64(bytes[11]) << 33
	values[2] |= uint64(bytes[12]) << 25
	values[2] |= uint64(bytes[13]) << 17
	values[2] |= uint64(bytes[14]) << 9
	values[2] |= uint64(bytes[15]) << 1
	values[2] |= uint64(bytes[16] >> 7)

	values[3] = uint64(bytes[16]&0x7f) << 36
	values[3] |= uint64(bytes[17]) << 28
	values[3] |= uint64(bytes[18]) << 20
	values[3] |= uint64(bytes[19]) << 12
	values[3] |= uint64(bytes[20]) << 4
	values[3] |= uint64(bytes[21] >> 4)

	values[4] = uint64(bytes[21]&0xf) << 39
	values[4] |= uint64(bytes[22]) << 31
	values[4] |= uint64(bytes[23]) << 23
	values[4] |= uint64(bytes[24]) << 15
	values[4] |= uint64(bytes[25]) << 7
	values[4] |= uint64(bytes[26] >> 1)

	values[5] = uint64(bytes[26]&1) << 42
	values[5] |= uint64(bytes[27]) << 34
	values[5] |= uint64(bytes[28]) << 26
	values[5] |= uint64(bytes[29]) << 18
	values[5] |= uint64(bytes[30]) << 10
	values[5] |= uint64(bytes[31]) << 2
	values[5] |= uint64(bytes[32] >> 6)

	values[6] = uint64(bytes[32]&0x3f) << 37
	values[6] |= uint64(bytes[33]) << 29
	values[6] |= uint64(bytes[34]) << 21
	values[6] |= uint64(bytes[35]) << 13
	values[6] |= uint64(bytes[36]) << 5
	values[6] |= uint64(bytes[37] >> 3)

	values[7] = uint64(bytes[37]&7) << 40
	values[7] |= uint64(bytes[38]) << 32
	values[7] |= uint64(bytes[39]) << 24
	values[7] |= uint64(bytes[40]) << 16
	values[7] |= uint64(bytes[41]) << 8
	values[7] |= uint64(bytes[42])
}

func unpackBits44(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 36
	values[0] |= uint64(bytes[1]) << 28
	values[0] |= uint64(bytes[2]) << 20
	values[0] |= uint64(bytes[3]) << 12
	values[0] |= uint64(bytes[4]) << 4
	values[0] |= uint64(bytes[5] >> 4)

	values[1] = uint64(bytes[5]&0xf) << 40
	values[1] |= uint64(bytes[6]) << 32
	values[1] |= uint64(bytes[7]) << 24
	values[1] |= uint64(bytes[8]) << 16
	values[1] |= uint64(bytes[9]) << 8
	values[1] |= uint64(bytes[10])

	values[2] = uint64(bytes[11]) << 36
	values[2] |= uint64(bytes[12]) << 28
	values[2] |= uint64(bytes[13]) << 20
	values[2] |= uint64(bytes[14]) << 12
	values[2] |= uint64(bytes[15]) << 4
	values[2] |= uint64(bytes[16] >> 4)

	values[3] = uint64(bytes[16]&0xf) << 40
	values[3] |= uint64(bytes[17]) << 32
	values[3] |= uint64(bytes[18]) << 24
	values[3] |= uint64(bytes[19]) << 16
	values[3] |= uint64(bytes[20]) << 8
	values[3] |= uint64(bytes[21])

	values[4] = uint64(bytes[22]) << 36
	values[4] |= uint64(bytes[23]) << 28
	values[4] |= uint64(bytes[24]) << 20
	values[4] |= uint64(bytes[25]) << 12
	values[4] |= uint64(bytes[26]) << 4
	values[4] |= uint64(bytes[27] >> 4)

	values[5] = uint64(bytes[27]&0xf) << 40
	values[5] |= uint64(bytes[28]) << 32
	values[5] |= uint64(bytes[29]) << 24
	values[5] |= uint64(bytes[30]) << 16
	values[5] |= uint64(bytes[31]) << 8
	values[5] |= uint64(bytes[32])

	values[6] = uint64(bytes[33]) << 36
	values[6] |= uint64(bytes[34]) << 28
	values[6] |= uint64(bytes[35]) << 20
	values[6] |= uint64(bytes[36]) << 12
	values[6] |= uint64(bytes[37]) << 4
	values[6] |= uint64(bytes[38] >> 4)

	values[7] = uint64(bytes[38]&0xf) << 40
	values[7] |= uint64(bytes[39]) << 32
	values[7] |= uint64(bytes[40]) << 24
	values[7] |= uint64(bytes[41]) << 16
	values[7] |= uint64(bytes[42]) << 8
	values[7] |= uint64(bytes[43])
}

func unpackBits45(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 37
	values[0] |= uint64(bytes[1]) << 29
	values[0] |= uint64(bytes[2]) << 21
	values[0] |= uint64(bytes[3]) << 13
	values[0] |= uint64(bytes[4]) << 5
	values[0] |= uint64(bytes[5] >> 3)

	values[1] = uint64(bytes[5]&7) << 42
	values[1] |= uint64(bytes[6]) << 34
	values[1] |= uint64(bytes[7]) << 26
	values[1] |= uint64(bytes[8]) << 18
	values[1] |= uint64(bytes[9]) << 10
	values[1] |= uint64(bytes[10]) << 2
	values[1] |= uint64(bytes[11] >> 6)

	values[2] = uint64(bytes[11]&0x3f) << 39
	values[2] |= uint64(bytes[12]) << 31
	values[2] |= uint64(bytes[13]) << 23
	values[2] |= uint64(bytes[14]) << 15
	values[2] |= uint64(bytes[15]) << 7
	values[2] |= uint64(bytes[16] >> 1)

	values[3] = uint64(bytes[16]&1) << 44
	values[3] |= uint64(bytes[17]) << 36
	values[3] |= uint64(bytes[18]) << 28
	values[3] |= uint64(bytes[19]) << 20
	values[3] |= uint64(bytes[20]) << 12
	values[3] |= uint64(bytes[21]) << 4
	values[3] |= uint64(bytes[22] >> 4)

	values[4] = uint64(bytes[22]&0xf) << 41
	values[4] |= uint64(bytes[23]) << 33
	values[4] |= uint64(bytes[24]) << 25
	values[4] |= uint64(bytes[25]) << 17
	values[4] |= uint64(bytes[26]) << 9
	values[4] |= uint64(bytes[27]) << 1
	values[4] |= uint64(bytes[28] >> 7)

	values[5] = uint64(bytes[28]&0x7f) << 38
	values[5] |= uint64(bytes[29]) << 30
	values[5] |= uint64(bytes[30]) << 22
	values[5] |= uint64(bytes[31]) << 14
	values[5] |= uint64(bytes[32]) << 6
	values[5] |= uint64(bytes[33] >> 2)

	values[6] = uint64(bytes[33]&3) << 43
	values[6] |= uint64(bytes[34]) << 35
	values[6] |= uint64(bytes[35]) << 27
	values[6] |= uint64(bytes[36]) << 19
	values[6] |= uint64(bytes[37]) << 11
	values[6] |= uint64(bytes[38]) << 3
	values[6] |= uint64(bytes[39] >> 5)

	values[7] = uint64(bytes[39]&0x1f) << 40
	values[7] |= uint64(bytes[40]) << 32
	values[7] |= uint64(bytes[41]) << 24
	values[7] |= uint64(bytes[42]) << 16
	values[7] |= uint64(bytes[43]) << 8
	values[7] |= uint64(bytes[44])
}

func unpackBits46(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 38
	values[0] |= uint64(bytes[1]) << 30
	values[0] |= uint64(bytes[2]) << 22
	values[0] |= uint64(bytes[3]) << 14
	values[0] |= uint64(bytes[4]) << 6
	values[0] |= uint64(bytes[5] >> 2)

	values[1] = uint64(bytes[5]&3) << 44
	values[1] |= uint64(bytes[6]) << 36
	values[1] |= uint64(bytes[7]) << 28
	values[1] |= uint64(bytes[8]) << 20
	values[1] |= uint64(bytes[9]) << 12
	values[1] |= uint64(bytes[10]) << 4
	values[1] |= uint64(bytes[11] >> 4)

	values[2] = uint64(bytes[11]&0xf) << 42
	values[2] |= uint64(bytes[12]) << 34
	values[2] |= uint64(bytes[13]) << 26
	values[2] |= uint64(bytes[14]) << 18
	values[2] |= uint64(bytes[15]) << 10
	values[2] |= uint64(bytes[16]) << 2
	values[2] |= uint64(bytes[17] >> 6)

	values[3] = uint64(bytes[17]&0x3f) << 40
	values[3] |= uint64(bytes[18]) << 32
	values[3] |= uint64(bytes[19]) << 24
	values[3] |= uint64(bytes[20]) << 16
	values[3] |= uint64(bytes[21]) << 8
	values[3] |= uint64(bytes[22])

	values[4] = uint64(bytes[23]) << 38
	values[4] |= uint64(bytes[24]) << 30
	values[4] |= uint64(bytes[25]) << 22
	values[4] |= uint64(bytes[26]) << 14
	values[4] |= uint64(bytes[27]) << 6
	values[4] |= uint64(bytes[28] >> 2)

	values[5] = uint64(bytes[28]&3) << 44
	values[5] |= uint64(bytes[29]) << 36
	values[5] |= uint64(bytes[30]) << 28
	values[5] |= uint64(bytes[31]) << 20
	values[5] |= uint64(bytes[32]) << 12
	values[5] |= uint64(bytes[33]) << 4
	values[5] |= uint64(bytes[34] >> 4)

	values[6] = uint64(bytes[34]&0xf) << 42
	values[6] |= uint64(bytes[35]) << 34
	values[6] |= uint64(bytes[36]) << 26
	values[6] |= uint64(bytes[37]) << 18
	values[6] |= uint64(bytes[38]) << 10
	values[6] |= uint64(bytes[39]) << 2
	values[6] |= uint64(bytes[40] >> 6)

	values[7] = uint64(bytes[40]&0x3f) << 40
	values[7] |= uint64(bytes[41]) << 32
	values[7] |= uint64(bytes[42]) << 24
	values[7] |= uint64(bytes[43]) << 16
	values[7] |= uint64(bytes[44]) << 8
	values[7] |= uint64(bytes[45])
}

func unpackBits47(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 39
	values[0] |= uint64(bytes[1]) << 31
	values[0] |= uint64(bytes[2]) << 23
	values[0] |= uint64(bytes[3]) << 15
	values[0] |= uint64(bytes[4]) << 7
	values[0] |= uint64(bytes[5] >> 1)

	values[1] = uint64(bytes[5]&1) << 46
	values[1] |= uint64(bytes[6]) << 38
	values[1] |= uint64(bytes[7]) << 30
	values[1] |= uint64(bytes[8]) << 22
	values[1] |= uint64(bytes[9]) << 14
	values[1] |= uint64(bytes[10]) << 6
	values[1] |= uint64(bytes[11] >> 2)

	values[2] = uint64(bytes[11]&3) << 45
	values[2] |= uint64(bytes[12]) << 37
	values[2] |= uint64(bytes[13]) << 29
	values[2] |= uint64(bytes[14]) << 21
	values[2] |= uint64(bytes[15]) << 13
	values[2] |= uint64(bytes[16]) << 5
	values[2] |= uint64(bytes[17] >> 3)

	values[3] = uint64(bytes[17]&7) << 44
	values[3] |= uint64(bytes[18]) << 36
	values[3] |= uint64(bytes[19]) << 28
	values[3] |= uint64(bytes[20]) << 20
	values[3] |= uint64(bytes[21]) << 12
	values[3] |= uint64(bytes[22]) << 4
	values[3] |= uint64(bytes[23] >> 4)

	values[4] = uint64(bytes[23]&0xf) << 43
	values[4] |= uint64(bytes[24]) << 35
	values[4] |= uint64(bytes[25]) << 27
	values[4] |= uint64(bytes[26]) << 19
	values[4] |= uint64(bytes[27]) << 11
	values[4] |= uint64(bytes[28]) << 3
	values[4] |= uint64(bytes[29] >> 5)

	values[5] = uint64(bytes[29]&0x1f) << 42
	values[5] |= uint64(bytes[30]) << 34
	values[5] |= uint64(bytes[31]) << 26
	values[5] |= uint64(bytes[32]) << 18
	values[5] |= uint64(bytes[33]) << 10
	values[5] |= uint64(bytes[34]) << 2
	values[5] |= uint64(bytes[35] >> 6)

	values[6] = uint64(bytes[35]&0x3f) << 41
	values[6] |= uint64(bytes[36]) << 33
	values[6] |= uint64(bytes[37]) << 25
	values[6] |= uint64(bytes[38]) << 17
	values[6] |= uint64(bytes[39]) << 9
	values[6] |= uint64(bytes[40]) << 1
	values[6] |= uint64(bytes[41] >> 7)

	values[7] = uint64(bytes[41]&0x7f) << 40
	values[7] |= uint64(bytes[42]) << 32
	values[7] |= uint64(bytes[43]) << 24
	values[7] |= uint64(bytes[44]) << 16
	values[7] |= uint64(bytes[45]) << 8
	values[7] |= uint64(bytes[46])
}

func unpackBits48(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 40
	values[0] |= uint64(bytes[1]) << 32
	values[0] |= uint64(bytes[2]) << 24
	values[0] |= uint64(bytes[3]) << 16
	values[0] |= uint64(bytes[4]) << 8
	values[0] |= uint64(bytes[5])
	values[1] = uint64(bytes[6]) << 40
	values[1] |= uint64(bytes[7]) << 32
	values[1] |= uint64(bytes[8]) << 24
	values[1] |= uint64(bytes[9]) << 16
	values[1] |= uint64(bytes[10]) << 8
	values[1] |= uint64(bytes[11])
	values[2] = uint64(bytes[12]) << 40
	values[2] |= uint64(bytes[13]) << 32
	values[2] |= uint64(bytes[14]) << 24
	values[2] |= uint64(bytes[15]) << 16
	values[2] |= uint64(bytes[16]) << 8
	values[2] |= uint64(bytes[17])
	values[3] = uint64(bytes[18]) << 40
	values[3] |= uint64(bytes[19]) << 32
	values[3] |= uint64(bytes[20]) << 24
	values[3] |= uint64(bytes[21]) << 16
	values[3] |= uint64(bytes[22]) << 8
	values[3] |= uint64(bytes[23])
	values[4] = uint64(bytes[24]) << 40
	values[4] |= uint64(bytes[25]) << 32
	values[4] |= uint64(bytes[26]) << 24
	values[4] |= uint64(bytes[27]) << 16
	values[4] |= uint64(bytes[28]) << 8
	values[4] |= uint64(bytes[29])
	values[5] = uint64(bytes[30]) << 40
	values[5] |= uint64(bytes[31]) << 32
	values[5] |= uint64(bytes[32]) << 24
	values[5] |= uint64(bytes[33]) << 16
	values[5] |= uint64(bytes[34]) << 8
	values[5] |= uint64(bytes[35])
	values[6] = uint64(bytes[36]) << 40
	values[6] |= uint64(bytes[37]) << 32
	values[6] |= uint64(bytes[38]) << 24
	values[6] |= uint64(bytes[39]) << 16
	values[6] |= uint64(bytes[40]) << 8
	values[6] |= uint64(bytes[41])
	values[7] = uint64(bytes[42]) << 40
	values[7] |= uint64(bytes[43]) << 32
	values[7] |= uint64(bytes[44]) << 24
	values[7] |= uint64(bytes[45]) << 16
	values[7] |= uint64(bytes[46]) << 8
	values[7] |= uint64(bytes[47])
}

func unpackBits49(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 41
	values[0] |= uint64(bytes[1]) << 33
	values[0] |= uint64(bytes[2]) << 25
	values[0] |= uint64(bytes[3]) << 17
	values[0] |= uint64(bytes[4]) << 9
	values[0] |= uint64(bytes[5]) << 1
	values[0] |= uint64(bytes[6] >> 7)

	values[1] = uint64(bytes[6]&0x7f) << 42
	values[1] |= uint64(bytes[7]) << 34
	values[1] |= uint64(bytes[8]) << 26
	values[1] |= uint64(bytes[9]) << 18
	values[1] |= uint64(bytes[10]) << 10
	values[1] |= uint64(bytes[11]) << 2
	values[1] |= uint64(bytes[12] >> 6)

	values[2] = uint64(bytes[12]&0x3f) << 43
	values[2] |= uint64(bytes[13]) << 35
	values[2] |= uint64(bytes[14]) << 27
	values[2] |= uint64(bytes[15]) << 19
	values[2] |= uint64(bytes[16]) << 11
	values[2] |= uint64(bytes[17]) << 3
	values[2] |= uint64(bytes[18] >> 5)

	values[3] = uint64(bytes[18]&0x1f) << 44
	values[3] |= uint64(bytes[19]) << 36
	values[3] |= uint64(bytes[20]) << 28
	values[3] |= uint64(bytes[21]) << 20
	values[3] |= uint64(bytes[22]) << 12
	values[3] |= uint64(bytes[23]) << 4
	values[3] |= uint64(bytes[24] >> 4)

	values[4] = uint64(bytes[24]&0xf) << 45
	values[4] |= uint64(bytes[25]) << 37
	values[4] |= uint64(bytes[26]) << 29
	values[4] |= uint64(bytes[27]) << 21
	values[4] |= uint64(bytes[28]) << 13
	values[4] |= uint64(bytes[29]) << 5
	values[4] |= uint64(bytes[30] >> 3)

	values[5] = uint64(bytes[30]&7) << 46
	values[5] |= uint64(bytes[31]) << 38
	values[5] |= uint64(bytes[32]) << 30
	values[5] |= uint64(bytes[33]) << 22
	values[5] |= uint64(bytes[34]) << 14
	values[5] |= uint64(bytes[35]) << 6
	values[5] |= uint64(bytes[36] >> 2)

	values[6] = uint64(bytes[36]&3) << 47
	values[6] |= uint64(bytes[37]) << 39
	values[6] |= uint64(bytes[38]) << 31
	values[6] |= uint64(bytes[39]) << 23
	values[6] |= uint64(bytes[40]) << 15
	values[6] |= uint64(bytes[41]) << 7
	values[6] |= uint64(bytes[42] >> 1)

	values[7] = uint64(bytes[42]&1) << 48
	values[7] |= uint64(bytes[43]) << 40
	values[7] |= uint64(bytes[44]) << 32
	values[7] |= uint64(bytes[45]) << 24
	values[7] |= uint64(bytes[46]) << 16
	values[7] |= uint64(bytes[47]) << 8
	values[7] |= uint64(bytes[48])
}

func unpackBits50(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 42
	values[0] |= uint64(bytes[1]) << 34
	values[0] |= uint64(bytes[2]) << 26
	values[0] |= uint64(bytes[3]) << 18
	values[0] |= uint64(bytes[4]) << 10
	values[0] |= uint64(bytes[5]) << 2
	values[0] |= uint64(bytes[6] >> 6)

	values[1] = uint64(bytes[6]&0x3f) << 44
	values[1] |= uint64(bytes[7]) << 36
	values[1] |= uint64(bytes[8]) << 28
	values[1] |= uint64(bytes[9]) << 20
	values[1] |= uint64(bytes[10]) << 12
	values[1] |= uint64(bytes[11]) << 4
	values[1] |= uint64(bytes[12] >> 4)

	values[2] = uint64(bytes[12]&0xf) << 46
	values[2] |= uint64(bytes[13]) << 38
	values[2] |= uint64(bytes[14]) << 30
	values[2] |= uint64(bytes[15]) << 22
	values[2] |= uint64(bytes[16]) << 14
	values[2] |= uint64(bytes[17]) << 6
	values[2] |= uint64(bytes[18] >> 2)

	values[3] = uint64(bytes[18]&3) << 48
	values[3] |= uint64(bytes[19]) << 40
	values[3] |= uint64(bytes[20]) << 32
	values[3] |= uint64(bytes[21]) << 24
	values[3] |= uint64(bytes[22]) << 16
	values[3] |= uint64(bytes[23]) << 8
	values[3] |= uint64(bytes[24])

	values[4] = uint64(bytes[25]) << 42
	values[4] |= uint64(bytes[26]) << 34
	values[4] |= uint64(bytes[27]) << 26
	values[4] |= uint64(bytes[28]) << 18
	values[4] |= uint64(bytes[29]) << 10
	values[4] |= uint64(bytes[30]) << 2
	values[4] |= uint64(bytes[31] >> 6)

	values[5] = uint64(bytes[31]&0x3f) << 44
	values[5] |= uint64(bytes[32]) << 36
	values[5] |= uint64(bytes[33]) << 28
	values[5] |= uint64(bytes[34]) << 20
	values[5] |= uint64(bytes[35]) << 12
	values[5] |= uint64(bytes[36]) << 4
	values[5] |= uint64(bytes[37] >> 4)

	values[6] = uint64(bytes[37]&0xf) << 46
	values[6] |= uint64(bytes[38]) << 38
	values[6] |= uint64(bytes[39]) << 30
	values[6] |= uint64(bytes[40]) << 22
	values[6] |= uint64(bytes[41]) << 14
	values[6] |= uint64(bytes[42]) << 6
	values[6] |= uint64(bytes[43] >> 2)

	values[7] = uint64(bytes[43]&3) << 48
	values[7] |= uint64(bytes[44]) << 40
	values[7] |= uint64(bytes[45]) << 32
	values[7] |= uint64(bytes[46]) << 24
	values[7] |= uint64(bytes[47]) << 16
	values[7] |= uint64(bytes[48]) << 8
	values[7] |= uint64(bytes[49])
}

func unpackBits51(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 43
	values[0] |= uint64(bytes[1]) << 35
	values[0] |= uint64(bytes[2]) << 27
	values[0] |= uint64(bytes[3]) << 19
	values[0] |= uint64(bytes[4]) << 11
	values[0] |= uint64(bytes[5]) << 3
	values[0] |= uint64(bytes[6] >> 5)

	values[1] = uint64(bytes[6]&0x1f) << 46
	values[1] |= uint64(bytes[7]) << 38
	values[1] |= uint64(bytes[8]) << 30
	values[1] |= uint64(bytes[9]) << 22
	values[1] |= uint64(bytes[10]) << 14
	values[1] |= uint64(bytes[11]) << 6
	values[1] |= uint64(bytes[12] >> 2)

	values[2] = uint64(bytes[12]&3) << 49
	values[2] |= uint64(bytes[13]) << 41
	values[2] |= uint64(bytes[14]) << 33
	values[2] |= uint64(bytes[15]) << 25
	values[2] |= uint64(bytes[16]) << 17
	values[2] |= uint64(bytes[17]) << 9
	values[2] |= uint64(bytes[18]) << 1
	values[2] |= uint64(bytes[19] >> 7)

	values[3] = uint64(bytes[19]&0x7f) << 44
	values[3] |= uint64(bytes[20]) << 36
	values[3] |= uint64(bytes[21]) << 28
	values[3] |= uint64(bytes[22]) << 20
	values[3] |= uint64(bytes[23]) << 12
	values[3] |= uint64(bytes[24]) << 4
	values[3] |= uint64(bytes[25] >> 4)

	values[4] = uint64(bytes[25]&0xf) << 47
	values[4] |= uint64(bytes[26]) << 39
	values[4] |= uint64(bytes[27]) << 31
	values[4] |= uint64(bytes[28]) << 23
	values[4] |= uint64(bytes[29]) << 15
	values[4] |= uint64(bytes[30]) << 7
	values[4] |= uint64(bytes[31] >> 1)

	values[5] = uint64(bytes[31]&1) << 50
	values[5] |= uint64(bytes[32]) << 42
	values[5] |= uint64(bytes[33]) << 34
	values[5] |= uint64(bytes[34]) << 26
	values[5] |= uint64(bytes[35]) << 18
	values[5] |= uint64(bytes[36]) << 10
	values[5] |= uint64(bytes[37]) << 2
	values[5] |= uint64(bytes[38] >> 6)

	values[6] = uint64(bytes[38]&0x3f) << 45
	values[6] |= uint64(bytes[39]) << 37
	values[6] |= uint64(bytes[40]) << 29
	values[6] |= uint64(bytes[41]) << 21
	values[6] |= uint64(bytes[42]) << 13
	values[6] |= uint64(bytes[43]) << 5
	values[6] |= uint64(bytes[44] >> 3)

	values[7] = uint64(bytes[44]&7) << 48
	values[7] |= uint64(bytes[45]) << 40
	values[7] |= uint64(bytes[46]) << 32
	values[7] |= uint64(bytes[47]) << 24
	values[7] |= uint64(bytes[48]) << 16
	values[7] |= uint64(bytes[49]) << 8
	values[7] |= uint64(bytes[50])
}

func unpackBits52(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 44
	values[0] |= uint64(bytes[1]) << 36
	values[0] |= uint64(bytes[2]) << 28
	values[0] |= uint64(bytes[3]) << 20
	values[0] |= uint64(bytes[4]) << 12
	values[0] |= uint64(bytes[5]) << 4
	values[0] |= uint64(bytes[6] >> 4)

	values[1] = uint64(bytes[6]&0xf) << 48
	values[1] |= uint64(bytes[7]) << 40
	values[1] |= uint64(bytes[8]) << 32
	values[1] |= uint64(bytes[9]) << 24
	values[1] |= uint64(bytes[10]) << 16
	values[1] |= uint64(bytes[11]) << 8
	values[1] |= uint64(bytes[12])

	values[2] = uint64(bytes[13]) << 44
	values[2] |= uint64(bytes[14]) << 36
	values[2] |= uint64(bytes[15]) << 28
	values[2] |= uint64(bytes[16]) << 20
	values[2] |= uint64(bytes[17]) << 12
	values[2] |= uint64(bytes[18]) << 4
	values[2] |= uint64(bytes[19] >> 4)

	values[3] = uint64(bytes[19]&0xf) << 48
	values[3] |= uint64(bytes[20]) << 40
	values[3] |= uint64(bytes[21]) << 32
	values[3] |= uint64(bytes[22]) << 24
	values[3] |= uint64(bytes[23]) << 16
	values[3] |= uint64(bytes[24]) << 8
	values[3] |= uint64(bytes[25])

	values[4] = uint64(bytes[26]) << 44
	values[4] |= uint64(bytes[27]) << 36
	values[4] |= uint64(bytes[28]) << 28
	values[4] |= uint64(bytes[29]) << 20
	values[4] |= uint64(bytes[30]) << 12
	values[4] |= uint64(bytes[31]) << 4
	values[4] |= uint64(bytes[32] >> 4)

	values[5] = uint64(bytes[32]&0xf) << 48
	values[5] |= uint64(bytes[33]) << 40
	values[5] |= uint64(bytes[34]) << 32
	values[5] |= uint64(bytes[35]) << 24
	values[5] |= uint64(bytes[36]) << 16
	values[5] |= uint64(bytes[37]) << 8
	values[5] |= uint64(bytes[38])

	values[6] = uint64(bytes[39]) << 44
	values[6] |= uint64(bytes[40]) << 36
	values[6] |= uint64(bytes[41]) << 28
	values[6] |= uint64(bytes[42]) << 20
	values[6] |= uint64(bytes[43]) << 12
	values[6] |= uint64(bytes[44]) << 4
	values[6] |= uint64(bytes[45] >> 4)

	values[7] = uint64(bytes[45]&0xf) << 48
	values[7] |= uint64(bytes[46]) << 40
	values[7] |= uint64(bytes[47]) << 32
	values[7] |= uint64(bytes[48]) << 24
	values[7] |= uint64(bytes[49]) << 16
	values[7] |= uint64(bytes[50]) << 8
	values[7] |= uint64(bytes[51])
}

func unpackBits53(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 45
	values[0] |= uint64(bytes[1]) << 37
	values[0] |= uint64(bytes[2]) << 29
	values[0] |= uint64(bytes[3]) << 21
	values[0] |= uint64(bytes[4]) << 13
	values[0] |= uint64(bytes[5]) << 5
	values[0] |= uint64(bytes[6] >> 3)

	values[1] = uint64(bytes[6]&7) << 50
	values[1] |= uint64(bytes[7]) << 42
	values[1] |= uint64(bytes[8]) << 34
	values[1] |= uint64(bytes[9]) << 26
	values[1] |= uint64(bytes[10]) << 18
	values[1] |= uint64(bytes[11]) << 10
	values[1] |= uint64(bytes[12]) << 2
	values[1] |= uint64(bytes[13] >> 6)

	values[2] = uint64(bytes[13]&0x3f) << 47
	values[2] |= uint64(bytes[14]) << 39
	values[2] |= uint64(bytes[15]) << 31
	values[2] |= uint64(bytes[16]) << 23
	values[2] |= uint64(bytes[17]) << 15
	values[2] |= uint64(bytes[18]) << 7
	values[2] |= uint64(bytes[19] >> 1)

	values[3] = uint64(bytes[19]&1) << 52
	values[3] |= uint64(bytes[20]) << 44
	values[3] |= uint64(bytes[21]) << 36
	values[3] |= uint64(bytes[22]) << 28
	values[3] |= uint64(bytes[23]) << 20
	values[3] |= uint64(bytes[24]) << 12
	values[3] |= uint64(bytes[25]) << 4
	values[3] |= uint64(bytes[26] >> 4)

	values[4] = uint64(bytes[26]&0xf) << 49
	values[4] |= uint64(bytes[27]) << 41
	values[4] |= uint64(bytes[28]) << 33
	values[4] |= uint64(bytes[29]) << 25
	values[4] |= uint64(bytes[30]) << 17
	values[4] |= uint64(bytes[31]) << 9
	values[4] |= uint64(bytes[32]) << 1
	values[4] |= uint64(bytes[33] >> 7)

	values[5] = uint64(bytes[33]&0x7f) << 46
	values[5] |= uint64(bytes[34]) << 38
	values[5] |= uint64(bytes[35]) << 30
	values[5] |= uint64(bytes[36]) << 22
	values[5] |= uint64(bytes[37]) << 14
	values[5] |= uint64(bytes[38]) << 6
	values[5] |= uint64(bytes[39] >> 2)

	values[6] = uint64(bytes[39]&3) << 51
	values[6] |= uint64(bytes[40]) << 43
	values[6] |= uint64(bytes[41]) << 35
	values[6] |= uint64(bytes[42]) << 27
	values[6] |= uint64(bytes[43]) << 19
	values[6] |= uint64(bytes[44]) << 11
	values[6] |= uint64(bytes[45]) << 3
	values[6] |= uint64(bytes[46] >> 5)

	values[7] = uint64(bytes[46]&0x1f) << 48
	values[7] |= uint64(bytes[47]) << 40
	values[7] |= uint64(bytes[48]) << 32
	values[7] |= uint64(bytes[49]) << 24
	values[7] |= uint64(bytes[50]) << 16
	values[7] |= uint64(bytes[51]) << 8
	values[7] |= uint64(bytes[52])
}

func unpackBits54(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 46
	values[0] |= uint64(bytes[1]) << 38
	values[0] |= uint64(bytes[2]) << 30
	values[0] |= uint64(bytes[3]) << 22
	values[0] |= uint64(bytes[4]) << 14
	values[0] |= uint64(bytes[5]) << 6
	values[0] |= uint64(bytes[6] >> 2)

	values[1] = uint64(bytes[6]&3) << 52
	values[1] |= uint64(bytes[7]) << 44
	values[1] |= uint64(bytes[8]) << 36
	values[1] |= uint64(bytes[9]) << 28
	values[1] |= uint64(bytes[10]) << 20
	values[1] |= uint64(bytes[11]) << 12
	values[1] |= uint64(bytes[12]) << 4
	values[1] |= uint64(bytes[13] >> 4)

	values[2] = uint64(bytes[13]&0xf) << 50
	values[2] |= uint64(bytes[14]) << 42
	values[2] |= uint64(bytes[15]) << 34
	values[2] |= uint64(bytes[16]) << 26
	values[2] |= uint64(bytes[17]) << 18
	values[2] |= uint64(bytes[18]) << 10
	values[2] |= uint64(bytes[19]) << 2
	values[2] |= uint64(bytes[20] >> 6)

	values[3] = uint64(bytes[20]&0x3f) << 48
	values[3] |= uint64(bytes[21]) << 40
	values[3] |= uint64(bytes[22]) << 32
	values[3] |= uint64(bytes[23]) << 24
	values[3] |= uint64(bytes[24]) << 16
	values[3] |= uint64(bytes[25]) << 8
	values[3] |= uint64(bytes[26])

	values[4] = uint64(bytes[27]) << 46
	values[4] |= uint64(bytes[28]) << 38
	values[4] |= uint64(bytes[29]) << 30
	values[4] |= uint64(bytes[30]) << 22
	values[4] |= uint64(bytes[31]) << 14
	values[4] |= uint64(bytes[32]) << 6
	values[4] |= uint64(bytes[33] >> 2)

	values[5] = uint64(bytes[33]&3) << 52
	values[5] |= uint64(bytes[34]) << 44
	values[5] |= uint64(bytes[35]) << 36
	values[5] |= uint64(bytes[36]) << 28
	values[5] |= uint64(bytes[37]) << 20
	values[5] |= uint64(bytes[38]) << 12
	values[5] |= uint64(bytes[39]) << 4
	values[5] |= uint64(bytes[40] >> 4)

	values[6] = uint64(bytes[40]&0xf) << 50
	values[6] |= uint64(bytes[41]) << 42
	values[6] |= uint64(bytes[42]) << 34
	values[6] |= uint64(bytes[43]) << 26
	values[6] |= uint64(bytes[44]) << 18
	values[6] |= uint64(bytes[45]) << 10
	values[6] |= uint64(bytes[46]) << 2
	values[6] |= uint64(bytes[47] >> 6)

	values[7] = uint64(bytes[47]&0x3f) << 48
	values[7] |= uint64(bytes[48]) << 40
	values[7] |= uint64(bytes[49]) << 32
	values[7] |= uint64(bytes[50]) << 24
	values[7] |= uint64(bytes[51]) << 16
	values[7] |= uint64(bytes[52]) << 8
	values[7] |= uint64(bytes[53])
}

func unpackBits55(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 47
	values[0] |= uint64(bytes[1]) << 39
	values[0] |= uint64(bytes[2]) << 31
	values[0] |= uint64(bytes[3]) << 23
	values[0] |= uint64(bytes[4]) << 15
	values[0] |= uint64(bytes[5]) << 7
	values[0] |= uint64(bytes[6] >> 1)

	values[1] = uint64(bytes[6]&1) << 54
	values[1] |= uint64(bytes[7]) << 46
	values[1] |= uint64(bytes[8]) << 38
	values[1] |= uint64(bytes[9]) << 30
	values[1] |= uint64(bytes[10]) << 22
	values[1] |= uint64(bytes[11]) << 14
	values[1] |= uint64(bytes[12]) << 6
	values[1] |= uint64(bytes[13] >> 2)

	values[2] = uint64(bytes[13]&3) << 53
	values[2] |= uint64(bytes[14]) << 45
	values[2] |= uint64(bytes[15]) << 37
	values[2] |= uint64(bytes[16]) << 29
	values[2] |= uint64(bytes[17]) << 21
	values[2] |= uint64(bytes[18]) << 13
	values[2] |= uint64(bytes[19]) << 5
	values[2] |= uint64(bytes[20] >> 3)

	values[3] = uint64(bytes[20]&7) << 52
	values[3] |= uint64(bytes[21]) << 44
	values[3] |= uint64(bytes[22]) << 36
	values[3] |= uint64(bytes[23]) << 28
	values[3] |= uint64(bytes[24]) << 20
	values[3] |= uint64(bytes[25]) << 12
	values[3] |= uint64(bytes[26]) << 4
	values[3] |= uint64(bytes[27] >> 4)

	values[4] = uint64(bytes[27]&0xf) << 51
	values[4] |= uint64(bytes[28]) << 43
	values[4] |= uint64(bytes[29]) << 35
	values[4] |= uint64(bytes[30]) << 27
	values[4] |= uint64(bytes[31]) << 19
	values[4] |= uint64(bytes[32]) << 11
	values[4] |= uint64(bytes[33]) << 3
	values[4] |= uint64(bytes[34] >> 5)

	values[5] = uint64(bytes[34]&0x1f) << 50
	values[5] |= uint64(bytes[35]) << 42
	values[5] |= uint64(bytes[36]) << 34
	values[5] |= uint64(bytes[37]) << 26
	values[5] |= uint64(bytes[38]) << 18
	values[5] |= uint64(bytes[39]) << 10
	values[5] |= uint64(bytes[40]) << 2
	values[5] |= uint64(bytes[41] >> 6)

	values[6] = uint64(bytes[41]&0x3f) << 49
	values[6] |= uint64(bytes[42]) << 41
	values[6] |= uint64(bytes[43]) << 33
	values[6] |= uint64(bytes[44]) << 25
	values[6] |= uint64(bytes[45]) << 17
	values[6] |= uint64(bytes[46]) << 9
	values[6] |= uint64(bytes[47]) << 1
	values[6] |= uint64(bytes[48] >> 7)

	values[7] = uint64(bytes[48]&0x7f) << 48
	values[7] |= uint64(bytes[49]) << 40
	values[7] |= uint64(bytes[50]) << 32
	values[7] |= uint64(bytes[51]) << 24
	values[7] |= uint64(bytes[52]) << 16
	values[7] |= uint64(bytes[53]) << 8
	values[7] |= uint64(bytes[54])
}

func unpackBits56(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 48
	values[0] |= uint64(bytes[1]) << 40
	values[0] |= uint64(bytes[2]) << 32
	values[0] |= uint64(bytes[3]) << 24
	values[0] |= uint64(bytes[4]) << 16
	values[0] |= uint64(bytes[5]) << 8
	values[0] |= uint64(bytes[6])
	values[1] = uint64(bytes[7]) << 48
	values[1] |= uint64(bytes[8]) << 40
	values[1] |= uint64(bytes[9]) << 32
	values[1] |= uint64(bytes[10]) << 24
	values[1] |= uint64(bytes[11]) << 16
	values[1] |= uint64(bytes[12]) << 8
	values[1] |= uint64(bytes[13])
	values[2] = uint64(bytes[14]) << 48
	values[2] |= uint64(bytes[15]) << 40
	values[2] |= uint64(bytes[16]) << 32
	values[2] |= uint64(bytes[17]) << 24
	values[2] |= uint64(bytes[18]) << 16
	values[2] |= uint64(bytes[19]) << 8
	values[2] |= uint64(bytes[20])
	values[3] = uint64(bytes[21]) << 48
	values[3] |= uint64(bytes[22]) << 40
	values[3] |= uint64(bytes[23]) << 32
	values[3] |= uint64(bytes[24]) << 24
	values[3] |= uint64(bytes[25]) << 16
	values[3] |= uint64(bytes[26]) << 8
	values[3] |= uint64(bytes[27])
	values[4] = uint64(bytes[28]) << 48
	values[4] |= uint64(bytes[29]) << 40
	values[4] |= uint64(bytes[30]) << 32
	values[4] |= uint64(bytes[31]) << 24
	values[4] |= uint64(bytes[32]) << 16
	values[4] |= uint64(bytes[33]) << 8
	values[4] |= uint64(bytes[34])
	values[5] = uint64(bytes[35]) << 48
	values[5] |= uint64(bytes[36]) << 40
	values[5] |= uint64(bytes[37]) << 32
	values[5] |= uint64(bytes[38]) << 24
	values[5] |= uint64(bytes[39]) << 16
	values[5] |= uint64(bytes[40]) << 8
	values[5] |= uint64(bytes[41])
	values[6] = uint64(bytes[42]) << 48
	values[6] |= uint64(bytes[43]) << 40
	values[6] |= uint64(bytes[44]) << 32
	values[6] |= uint64(bytes[45]) << 24
	values[6] |= uint64(bytes[46]) << 16
	values[6] |= uint64(bytes[47]) << 8
	values[6] |= uint64(bytes[48])
	values[7] = uint64(bytes[49]) << 48
	values[7] |= uint64(bytes[50]) << 40
	values[7] |= uint64(bytes[51]) << 32
	values[7] |= uint64(bytes[52]) << 24
	values[7] |= uint64(bytes[53]) << 16
	values[7] |= uint64(bytes[54]) << 8
	values[7] |= uint64(bytes[55])
}

func unpackBits57(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 49
	values[0] |= uint64(bytes[1]) << 41
	values[0] |= uint64(bytes[2]) << 33
	values[0] |= uint64(bytes[3]) << 25
	values[0] |= uint64(bytes[4]) << 17
	values[0] |= uint64(bytes[5]) << 9
	values[0] |= uint64(bytes[6]) << 1
	values[0] |= uint64(bytes[7] >> 7)

	values[1] = uint64(bytes[7]&0x7f) << 50
	values[1] |= uint64(bytes[8]) << 42
	values[1] |= uint64(bytes[9]) << 34
	values[1] |= uint64(bytes[10]) << 26
	values[1] |= uint64(bytes[11]) << 18
	values[1] |= uint64(bytes[12]) << 10
	values[1] |= uint64(bytes[13]) << 2
	values[1] |= uint64(bytes[14] >> 6)

	values[2] = uint64(bytes[14]&0x3f) << 51
	values[2] |= uint64(bytes[15]) << 43
	values[2] |= uint64(bytes[16]) << 35
	values[2] |= uint64(bytes[17]) << 27
	values[2] |= uint64(bytes[18]) << 19
	values[2] |= uint64(bytes[19]) << 11
	values[2] |= uint64(bytes[20]) << 3
	values[2] |= uint64(bytes[21] >> 5)

	values[3] = uint64(bytes[21]&0x1f) << 52
	values[3] |= uint64(bytes[22]) << 44
	values[3] |= uint64(bytes[23]) << 36
	values[3] |= uint64(bytes[24]) << 28
	values[3] |= uint64(bytes[25]) << 20
	values[3] |= uint64(bytes[26]) << 12
	values[3] |= uint64(bytes[27]) << 4
	values[3] |= uint64(bytes[28] >> 4)

	values[4] = uint64(bytes[28]&0xf) << 53
	values[4] |= uint64(bytes[29]) << 45
	values[4] |= uint64(bytes[30]) << 37
	values[4] |= uint64(bytes[31]) << 29
	values[4] |= uint64(bytes[32]) << 21
	values[4] |= uint64(bytes[33]) << 13
	values[4] |= uint64(bytes[34]) << 5
	values[4] |= uint64(bytes[35] >> 3)

	values[5] = uint64(bytes[35]&7) << 54
	values[5] |= uint64(bytes[36]) << 46
	values[5] |= uint64(bytes[37]) << 38
	values[5] |= uint64(bytes[38]) << 30
	values[5] |= uint64(bytes[39]) << 22
	values[5] |= uint64(bytes[40]) << 14
	values[5] |= uint64(bytes[41]) << 6
	values[5] |= uint64(bytes[42] >> 2)

	values[6] = uint64(bytes[42]&3) << 55
	values[6] |= uint64(bytes[43]) << 47
	values[6] |= uint64(bytes[44]) << 39
	values[6] |= uint64(bytes[45]) << 31
	values[6] |= uint64(bytes[46]) << 23
	values[6] |= uint64(bytes[47]) << 15
	values[6] |= uint64(bytes[48]) << 7
	values[6] |= uint64(bytes[49] >> 1)

	values[7] = uint64(bytes[49]&1) << 56
	values[7] |= uint64(bytes[50]) << 48
	values[7] |= uint64(bytes[51]) << 40
	values[7] |= uint64(bytes[52]) << 32
	values[7] |= uint64(bytes[53]) << 24
	values[7] |= uint64(bytes[54]) << 16
	values[7] |= uint64(bytes[55]) << 8
	values[7] |= uint64(bytes[56])
}

func unpackBits58(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 50
	values[0] |= uint64(bytes[1]) << 42
	values[0] |= uint64(bytes[2]) << 34
	values[0] |= uint64(bytes[3]) << 26
	values[0] |= uint64(bytes[4]) << 18
	values[0] |= uint64(bytes[5]) << 10
	values[0] |= uint64(bytes[6]) << 2
	values[0] |= uint64(bytes[7] >> 6)

	values[1] = uint64(bytes[7]&0x3f) << 52
	values[1] |= uint64(bytes[8]) << 44
	values[1] |= uint64(bytes[9]) << 36
	values[1] |= uint64(bytes[10]) << 28
	values[1] |= uint64(bytes[11]) << 20
	values[1] |= uint64(bytes[12]) << 12
	values[1] |= uint64(bytes[13]) << 4
	values[1] |= uint64(bytes[14] >> 4)

	values[2] = uint64(bytes[14]&0xf) << 54
	values[2] |= uint64(bytes[15]) << 46
	values[2] |= uint64(bytes[16]) << 38
	values[2] |= uint64(bytes[17]) << 30
	values[2] |= uint64(bytes[18]) << 22
	values[2] |= uint64(bytes[19]) << 14
	values[2] |= uint64(bytes[20]) << 6
	values[2] |= uint64(bytes[21] >> 2)

	values[3] = uint64(bytes[21]&3) << 56
	values[3] |= uint64(bytes[22]) << 48
	values[3] |= uint64(bytes[23]) << 40
	values[3] |= uint64(bytes[24]) << 32
	values[3] |= uint64(bytes[25]) << 24
	values[3] |= uint64(bytes[26]) << 16
	values[3] |= uint64(bytes[27]) << 8
	values[3] |= uint64(bytes[28])

	values[4] = uint64(bytes[29]) << 50
	values[4] |= uint64(bytes[30]) << 42
	values[4] |= uint64(bytes[31]) << 34
	values[4] |= uint64(bytes[32]) << 26
	values[4] |= uint64(bytes[33]) << 18
	values[4] |= uint64(bytes[34]) << 10
	values[4] |= uint64(bytes[35]) << 2
	values[4] |= uint64(bytes[36] >> 6)

	values[5] = uint64(bytes[36]&0x3f) << 52
	values[5] |= uint64(bytes[37]) << 44
	values[5] |= uint64(bytes[38]) << 36
	values[5] |= uint64(bytes[39]) << 28
	values[5] |= uint64(bytes[40]) << 20
	values[5] |= uint64(bytes[41]) << 12
	values[5] |= uint64(bytes[42]) << 4
	values[5] |= uint64(bytes[43] >> 4)

	values[6] = uint64(bytes[43]&0xf) << 54
	values[6] |= uint64(bytes[44]) << 46
	values[6] |= uint64(bytes[45]) << 38
	values[6] |= uint64(bytes[46]) << 30
	values[6] |= uint64(bytes[47]) << 22
	values[6] |= uint64(bytes[48]) << 14
	values[6] |= uint64(bytes[49]) << 6
	values[6] |= uint64(bytes[50] >> 2)

	values[7] = uint64(bytes[50]&3) << 56
	values[7] |= uint64(bytes[51]) << 48
	values[7] |= uint64(bytes[52]) << 40
	values[7] |= uint64(bytes[53]) << 32
	values[7] |= uint64(bytes[54]) << 24
	values[7] |= uint64(bytes[55]) << 16
	values[7] |= uint64(bytes[56]) << 8
	values[7] |= uint64(bytes[57])
}

func unpackBits59(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 51
	values[0] |= uint64(bytes[1]) << 43
	values[0] |= uint64(bytes[2]) << 35
	values[0] |= uint64(bytes[3]) << 27
	values[0] |= uint64(bytes[4]) << 19
	values[0] |= uint64(bytes[5]) << 11
	values[0] |= uint64(bytes[6]) << 3
	values[0] |= uint64(bytes[7] >> 5)

	values[1] = uint64(bytes[7]&0x1f) << 54
	values[1] |= uint64(bytes[8]) << 46
	values[1] |= uint64(bytes[9]) << 38
	values[1] |= uint64(bytes[10]) << 30
	values[1] |= uint64(bytes[11]) << 22
	values[1] |= uint64(bytes[12]) << 14
	values[1] |= uint64(bytes[13]) << 6
	values[1] |= uint64(bytes[14] >> 2)

	values[2] = uint64(bytes[14]&3) << 57
	values[2] |= uint64(bytes[15]) << 49
	values[2] |= uint64(bytes[16]) << 41
	values[2] |= uint64(bytes[17]) << 33
	values[2] |= uint64(bytes[18]) << 25
	values[2] |= uint64(bytes[19]) << 17
	values[2] |= uint64(bytes[20]) << 9
	values[2] |= uint64(bytes[21]) << 1
	values[2] |= uint64(bytes[22] >> 7)

	values[3] = uint64(bytes[22]&0x7f) << 52
	values[3] |= uint64(bytes[23]) << 44
	values[3] |= uint64(bytes[24]) << 36
	values[3] |= uint64(bytes[25]) << 28
	values[3] |= uint64(bytes[26]) << 20
	values[3] |= uint64(bytes[27]) << 12
	values[3] |= uint64(bytes[28]) << 4
	values[3] |= uint64(bytes[29] >> 4)

	values[4] = uint64(bytes[29]&0xf) << 55
	values[4] |= uint64(bytes[30]) << 47
	values[4] |= uint64(bytes[31]) << 39
	values[4] |= uint64(bytes[32]) << 31
	values[4] |= uint64(bytes[33]) << 23
	values[4] |= uint64(bytes[34]) << 15
	values[4] |= uint64(bytes[35]) << 7
	values[4] |= uint64(bytes[36] >> 1)

	values[5] = uint64(bytes[36]&1) << 58
	values[5] |= uint64(bytes[37]) << 50
	values[5] |= uint64(bytes[38]) << 42
	values[5] |= uint64(bytes[39]) << 34
	values[5] |= uint64(bytes[40]) << 26
	values[5] |= uint64(bytes[41]) << 18
	values[5] |= uint64(bytes[42]) << 10
	values[5] |= uint64(bytes[43]) << 2
	values[5] |= uint64(bytes[44] >> 6)

	values[6] = uint64(bytes[44]&0x3f) << 53
	values[6] |= uint64(bytes[45]) << 45
	values[6] |= uint64(bytes[46]) << 37
	values[6] |= uint64(bytes[47]) << 29
	values[6] |= uint64(bytes[48]) << 21
	values[6] |= uint64(bytes[49]) << 13
	values[6] |= uint64(bytes[50]) << 5
	values[6] |= uint64(bytes[51] >> 3)

	values[7] = uint64(bytes[51]&7) << 56
	values[7] |= uint64(bytes[52]) << 48
	values[7] |= uint64(bytes[53]) << 40
	values[7] |= uint64(bytes[54]) << 32
	values[7] |= uint64(bytes[55]) << 24
	values[7] |= uint64(bytes[56]) << 16
	values[7] |= uint64(bytes[57]) << 8
	values[7] |= uint64(bytes[58])
}

func unpackBits60(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 52
	values[0] |= uint64(bytes[1]) << 44
	values[0] |= uint64(bytes[2]) << 36
	values[0] |= uint64(bytes[3]) << 28
	values[0] |= uint64(bytes[4]) << 20
	values[0] |= uint64(bytes[5]) << 12
	values[0] |= uint64(bytes[6]) << 4
	values[0] |= uint64(bytes[7] >> 4)

	values[1] = uint64(bytes[7]&0xf) << 56
	values[1] |= uint64(bytes[8]) << 48
	values[1] |= uint64(bytes[9]) << 40
	values[1] |= uint64(bytes[10]) << 32
	values[1] |= uint64(bytes[11]) << 24
	values[1] |= uint64(bytes[12]) << 16
	values[1] |= uint64(bytes[13]) << 8
	values[1] |= uint64(bytes[14])

	values[2] = uint64(bytes[15]) << 52
	values[2] |= uint64(bytes[16]) << 44
	values[2] |= uint64(bytes[17]) << 36
	values[2] |= uint64(bytes[18]) << 28
	values[2] |= uint64(bytes[19]) << 20
	values[2] |= uint64(bytes[20]) << 12
	values[2] |= uint64(bytes[21]) << 4
	values[2] |= uint64(bytes[22] >> 4)

	values[3] = uint64(bytes[22]&0xf) << 56
	values[3] |= uint64(bytes[23]) << 48
	values[3] |= uint64(bytes[24]) << 40
	values[3] |= uint64(bytes[25]) << 32
	values[3] |= uint64(bytes[26]) << 24
	values[3] |= uint64(bytes[27]) << 16
	values[3] |= uint64(bytes[28]) << 8
	values[3] |= uint64(bytes[29])

	values[4] = uint64(bytes[30]) << 52
	values[4] |= uint64(bytes[31]) << 44
	values[4] |= uint64(bytes[32]) << 36
	values[4] |= uint64(bytes[33]) << 28
	values[4] |= uint64(bytes[34]) << 20
	values[4] |= uint64(bytes[35]) << 12
	values[4] |= uint64(bytes[36]) << 4
	values[4] |= uint64(bytes[37] >> 4)

	values[5] = uint64(bytes[37]&0xf) << 56
	values[5] |= uint64(bytes[38]) << 48
	values[5] |= uint64(bytes[39]) << 40
	values[5] |= uint64(bytes[40]) << 32
	values[5] |= uint64(bytes[41]) << 24
	values[5] |= uint64(bytes[42]) << 16
	values[5] |= uint64(bytes[43]) << 8
	values[5] |= uint64(bytes[44])

	values[6] = uint64(bytes[45]) << 52
	values[6] |= uint64(bytes[46]) << 44
	values[6] |= uint64(bytes[47]) << 36
	values[6] |= uint64(bytes[48]) << 28
	values[6] |= uint64(bytes[49]) << 20
	values[6] |= uint64(bytes[50]) << 12
	values[6] |= uint64(bytes[51]) << 4
	values[6] |= uint64(bytes[52] >> 4)

	values[7] = uint64(bytes[52]&0xf) << 56
	values[7] |= uint64(bytes[53]) << 48
	values[7] |= uint64(bytes[54]) << 40
	values[7] |= uint64(bytes[55]) << 32
	values[7] |= uint64(bytes[56]) << 24
	values[7] |= uint64(bytes[57]) << 16
	values[7] |= uint64(bytes[58]) << 8
	values[7] |= uint64(bytes[59])
}

func unpackBits61(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 53
	values[0] |= uint64(bytes[1]) << 45
	values[0] |= uint64(bytes[2]) << 37
	values[0] |= uint64(bytes[3]) << 29
	values[0] |= uint64(bytes[4]) << 21
	values[0] |= uint64(bytes[5]) << 13
	values[0] |= uint64(bytes[6]) << 5
	values[0] |= uint64(bytes[7] >> 3)

	values[1] = uint64(bytes[7]&7) << 58
	values[1] |= uint64(bytes[8]) << 50
	values[1] |= uint64(bytes[9]) << 42
	values[1] |= uint64(bytes[10]) << 34
	values[1] |= uint64(bytes[11]) << 26
	values[1] |= uint64(bytes[12]) << 18
	values[1] |= uint64(bytes[13]) << 10
	values[1] |= uint64(bytes[14]) << 2
	values[1] |= uint64(bytes[15] >> 6)

	values[2] = uint64(bytes[15]&0x3f) << 55
	values[2] |= uint64(bytes[16]) << 47
	values[2] |= uint64(bytes[17]) << 39
	values[2] |= uint64(bytes[18]) << 31
	values[2] |= uint64(bytes[19]) << 23
	values[2] |= uint64(bytes[20]) << 15
	values[2] |= uint64(bytes[21]) << 7
	values[2] |= uint64(bytes[22] >> 1)

	values[3] = uint64(bytes[22]&1) << 60
	values[3] |= uint64(bytes[23]) << 52
	values[3] |= uint64(bytes[24]) << 44
	values[3] |= uint64(bytes[25]) << 36
	values[3] |= uint64(bytes[26]) << 28
	values[3] |= uint64(bytes[27]) << 20
	values[3] |= uint64(bytes[28]) << 12
	values[3] |= uint64(bytes[29]) << 4
	values[3] |= uint64(bytes[30] >> 4)

	values[4] = uint64(bytes[30]&0xf) << 57
	values[4] |= uint64(bytes[31]) << 49
	values[4] |= uint64(bytes[32]) << 41
	values[4] |= uint64(bytes[33]) << 33
	values[4] |= uint64(bytes[34]) << 25
	values[4] |= uint64(bytes[35]) << 17
	values[4] |= uint64(bytes[36]) << 9
	values[4] |= uint64(bytes[37]) << 1
	values[4] |= uint64(bytes[38] >> 7)

	values[5] = uint64(bytes[38]&0x7f) << 54
	values[5] |= uint64(bytes[39]) << 46
	values[5] |= uint64(bytes[40]) << 38
	values[5] |= uint64(bytes[41]) << 30
	values[5] |= uint64(bytes[42]) << 22
	values[5] |= uint64(bytes[43]) << 14
	values[5] |= uint64(bytes[44]) << 6
	values[5] |= uint64(bytes[45] >> 2)

	values[6] = uint64(bytes[45]&3) << 59
	values[6] |= uint64(bytes[46]) << 51
	values[6] |= uint64(bytes[47]) << 43
	values[6] |= uint64(bytes[48]) << 35
	values[6] |= uint64(bytes[49]) << 27
	values[6] |= uint64(bytes[50]) << 19
	values[6] |= uint64(bytes[51]) << 11
	values[6] |= uint64(bytes[52]) << 3
	values[6] |= uint64(bytes[53] >> 5)

	values[7] = uint64(bytes[53]&0x1f) << 56
	values[7] |= uint64(bytes[54]) << 48
	values[7] |= uint64(bytes[55]) << 40
	values[7] |= uint64(bytes[56]) << 32
	values[7] |= uint64(bytes[57]) << 24
	values[7] |= uint64(bytes[58]) << 16
	values[7] |= uint64(bytes[59]) << 8
	values[7] |= uint64(bytes[60])
}

func unpackBits62(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 54
	values[0] |= uint64(bytes[1]) << 46
	values[0] |= uint64(bytes[2]) << 38
	values[0] |= uint64(bytes[3]) << 30
	values[0] |= uint64(bytes[4]) << 22
	values[0] |= uint64(bytes[5]) << 14
	values[0] |= uint64(bytes[6]) << 6
	values[0] |= uint64(bytes[7] >> 2)

	values[1] = uint64(bytes[7]&3) << 60
	values[1] |= uint64(bytes[8]) << 52
	values[1] |= uint64(bytes[9]) << 44
	values[1] |= uint64(bytes[10]) << 36
	values[1] |= uint64(bytes[11]) << 28
	values[1] |= uint64(bytes[12]) << 20
	values[1] |= uint64(bytes[13]) << 12
	values[1] |= uint64(bytes[14]) << 4
	values[1] |= uint64(bytes[15] >> 4)

	values[2] = uint64(bytes[15]&0xf) << 58
	values[2] |= uint64(bytes[16]) << 50
	values[2] |= uint64(bytes[17]) << 42
	values[2] |= uint64(bytes[18]) << 34
	values[2] |= uint64(bytes[19]) << 26
	values[2] |= uint64(bytes[20]) << 18
	values[2] |= uint64(bytes[21]) << 10
	values[2] |= uint64(bytes[22]) << 2
	values[2] |= uint64(bytes[23] >> 6)

	values[3] = uint64(bytes[23]&0x3f) << 56
	values[3] |= uint64(bytes[24]) << 48
	values[3] |= uint64(bytes[25]) << 40
	values[3] |= uint64(bytes[26]) << 32
	values[3] |= uint64(bytes[27]) << 24
	values[3] |= uint64(bytes[28]) << 16
	values[3] |= uint64(bytes[29]) << 8
	values[3] |= uint64(bytes[30])

	values[4] = uint64(bytes[31]) << 54
	values[4] |= uint64(bytes[32]) << 46
	values[4] |= uint64(bytes[33]) << 38
	values[4] |= uint64(bytes[34]) << 30
	values[4] |= uint64(bytes[35]) << 22
	values[4] |= uint64(bytes[36]) << 14
	values[4] |= uint64(bytes[37]) << 6
	values[4] |= uint64(bytes[38] >> 2)

	values[5] = uint64(bytes[38]&3) << 60
	values[5] |= uint64(bytes[39]) << 52
	values[5] |= uint64(bytes[40]) << 44
	values[5] |= uint64(bytes[41]) << 36
	values[5] |= uint64(bytes[42]) << 28
	values[5] |= uint64(bytes[43]) << 20
	values[5] |= uint64(bytes[44]) << 12
	values[5] |= uint64(bytes[45]) << 4
	values[5] |= uint64(bytes[46] >> 4)

	values[6] = uint64(bytes[46]&0xf) << 58
	values[6] |= uint64(bytes[47]) << 50
	values[6] |= uint64(bytes[48]) << 42
	values[6] |= uint64(bytes[49]) << 34
	values[6] |= uint64(bytes[50]) << 26
	values[6] |= uint64(bytes[51]) << 18
	values[6] |= uint64(bytes[52]) << 10
	values[6] |= uint64(bytes[53]) << 2
	values[6] |= uint64(bytes[54] >> 6)

	values[7] = uint64(bytes[54]&0x3f) << 56
	values[7] |= uint64(bytes[55]) << 48
	values[7] |= uint64(bytes[56]) << 40
	values[7] |= uint64(bytes[57]) << 32
	values[7] |= uint64(bytes[58]) << 24
	values[7] |= uint64(bytes[59]) << 16
	values[7] |= uint64(bytes[60]) << 8
	values[7] |= uint64(bytes[61])
}

func unpackBits63(values []uint64, bytes []byte) {
	values[0] = uint64(bytes[0]) << 55
	values[0] |= uint64(bytes[1]) << 47
	values[0] |= uint64(bytes[2]) << 39
	values[0] |= uint64(bytes[3]) << 31
	values[0] |= uint64(bytes[4]) << 23
	values[0] |= uint64(bytes[5]) << 15
	values[0] |= uint64(bytes[6]) << 7
	values[0] |= uint64(bytes[7] >> 1)

	values[1] = uint64(bytes[7]&1) << 62
	values[1] |= uint64(bytes[8]) << 54
	values[1] |= uint64(bytes[9]) << 46
	values[1] |= uint64(bytes[10]) << 38
	values[1] |= uint64(bytes[11]) << 30
	values[1] |= uint64(bytes[12]) << 22
	values[1] |= uint64(bytes[13]) << 14
	values[1] |= uint64(bytes[14]) << 6
	values[1] |= uint64(bytes[15] >> 2)

	values[2] = uint64(bytes[15]&3) << 61
	values[2] |= uint64(bytes[16]) << 53
	values[2] |= uint64(bytes[17]) << 45
	values[2] |= uint64(bytes[18]) << 37
	values[2] |= uint64(bytes[19]) << 29
	values[2] |= uint64(bytes[20]) << 21
	values[2] |= uint64(bytes[21]) << 13
	values[2] |= uint64(bytes[22]) << 5
	values[2] |= uint64(bytes[23] >> 3)

	values[3] = uint64(bytes[23]&7) << 60
	values[3] |= uint64(bytes[24]) << 52
	values[3] |= uint64(bytes[25]) << 44
	values[3] |= uint64(bytes[26]) << 36
	values[3] |= uint64(bytes[27]) << 28
	values[3] |= uint64(bytes[28]) << 20
	values[3] |= uint64(bytes[29]) << 12
	values[3] |= uint64(bytes[30]) << 4
	values[3] |= uint64(bytes[31] >> 4)

	values[4] = uint64(bytes[31]&0xf) << 59
	values[4] |= uint64(bytes[32]) << 51
	values[4] |= uint64(bytes[33]) << 43
	values[4] |= uint64(bytes[34]) << 35
	values[4] |= uint64(bytes[35]) << 27
	values[4] |= uint64(bytes[36]) << 19
	values[4] |= uint64(bytes[37]) << 11
	values[4] |= uint64(bytes[38]) << 3
	values[4] |= uint64(bytes[39] >> 5)

	values[5] = uint64(bytes[39]&0x1f) << 58
	values[5] |= uint64(bytes[40]) << 50
	values[5] |= uint64(bytes[41]) << 42
	values[5] |= uint64(bytes[42]) << 34
	values[5] |= uint64(bytes[43]) << 26
	values[5] |= uint64(bytes[44]) << 18
	values[5] |= uint64(bytes[45]) << 10
	values[5] |= uint64(bytes[46]) << 2
	values[5] |= uint64(bytes[47] >> 6)

	values[6] = uint64(bytes[47]&0x3f) << 57
	values[6] |= uint64(bytes[48]) << 49
	values[6] |= uint64(bytes[49]) << 41
	values[6] |= uint64(bytes[50]) << 33
	values[6] |= uint64(bytes[51]) << 25
	values[6] |= uint64(bytes[52]) << 17
	values[6] |= uint64(bytes[53]) << 9
	values[6] |= uint64(bytes[54]) << 1
	values[6] |= uint64(bytes[55] >> 7)

	values[7] = uint64(bytes[55]&0x7f) << 56
	values[7] |= uint64(bytes[56]) << 48
	values[7] |= uint64(bytes[57]) << 40
	values[7] |= uint64(bytes[58]) << 32
	values[7] |= uint64(bytes[59]) << 24
	values[7] |= uint64(bytes[60]) << 16
	values[7] |= uint64(bytes[61]) << 8
	values[7] |= uint64(bytes[62])
}

// unpackBitsBlock8 unpacks 8 uint64 values with a given number of bits from bytes
// values: array of 8 uint64 values to write to
// bytes: byte slice to read from
// bits: number of bits to unpack into each value (1-63)
func unpackBitsBlock8(values []uint64, bytes []byte, bits uint8) error {
	switch bits {
	case 1:
		unpackBits1(values, bytes)
	case 2:
		unpackBits2(values, bytes)
	case 3:
		unpackBits3(values, bytes)
	case 4:
		unpackBits4(values, bytes)
	case 5:
		unpackBits5(values, bytes)
	case 6:
		unpackBits6(values, bytes)
	case 7:
		unpackBits7(values, bytes)
	case 8:
		unpackBits8(values, bytes)
	case 9:
		unpackBits9(values, bytes)
	case 10:
		unpackBits10(values, bytes)
	case 11:
		unpackBits11(values, bytes)
	case 12:
		unpackBits12(values, bytes)
	case 13:
		unpackBits13(values, bytes)
	case 14:
		unpackBits14(values, bytes)
	case 15:
		unpackBits15(values, bytes)
	case 16:
		unpackBits16(values, bytes)
	case 17:
		unpackBits17(values, bytes)
	case 18:
		unpackBits18(values, bytes)
	case 19:
		unpackBits19(values, bytes)
	case 20:
		unpackBits20(values, bytes)
	case 21:
		unpackBits21(values, bytes)
	case 22:
		unpackBits22(values, bytes)
	case 23:
		unpackBits23(values, bytes)
	case 24:
		unpackBits24(values, bytes)
	case 25:
		unpackBits25(values, bytes)
	case 26:
		unpackBits26(values, bytes)
	case 27:
		unpackBits27(values, bytes)
	case 28:
		unpackBits28(values, bytes)
	case 29:
		unpackBits29(values, bytes)
	case 30:
		unpackBits30(values, bytes)
	case 31:
		unpackBits31(values, bytes)
	case 32:
		unpackBits32(values, bytes)
	case 33:
		unpackBits33(values, bytes)
	case 34:
		unpackBits34(values, bytes)
	case 35:
		unpackBits35(values, bytes)
	case 36:
		unpackBits36(values, bytes)
	case 37:
		unpackBits37(values, bytes)
	case 38:
		unpackBits38(values, bytes)
	case 39:
		unpackBits39(values, bytes)
	case 40:
		unpackBits40(values, bytes)
	case 41:
		unpackBits41(values, bytes)
	case 42:
		unpackBits42(values, bytes)
	case 43:
		unpackBits43(values, bytes)
	case 44:
		unpackBits44(values, bytes)
	case 45:
		unpackBits45(values, bytes)
	case 46:
		unpackBits46(values, bytes)
	case 47:
		unpackBits47(values, bytes)
	case 48:
		unpackBits48(values, bytes)
	case 49:
		unpackBits49(values, bytes)
	case 50:
		unpackBits50(values, bytes)
	case 51:
		unpackBits51(values, bytes)
	case 52:
		unpackBits52(values, bytes)
	case 53:
		unpackBits53(values, bytes)
	case 54:
		unpackBits54(values, bytes)
	case 55:
		unpackBits55(values, bytes)
	case 56:
		unpackBits56(values, bytes)
	case 57:
		unpackBits57(values, bytes)
	case 58:
		unpackBits58(values, bytes)
	case 59:
		unpackBits59(values, bytes)
	case 60:
		unpackBits60(values, bytes)
	case 61:
		unpackBits61(values, bytes)
	case 62:
		unpackBits62(values, bytes)
	case 63:
		unpackBits63(values, bytes)
	default:
		return fmt.Errorf("wrong number of bits in unpackBitsBlock8: %d", bits)
	}
	return nil
}
