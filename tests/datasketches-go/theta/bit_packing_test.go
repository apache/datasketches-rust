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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const igolden64 = 0x9e3779b97f4a7c13

func TestPackAndUnpack(t *testing.T) {
	packFuncs := []func([]uint64, []byte){
		packBits1, packBits2, packBits3, packBits4, packBits5, packBits6, packBits7, packBits8,
		packBits9, packBits10, packBits11, packBits12, packBits13, packBits14, packBits15, packBits16,
		packBits17, packBits18, packBits19, packBits20, packBits21, packBits22, packBits23, packBits24,
		packBits25, packBits26, packBits27, packBits28, packBits29, packBits30, packBits31, packBits32,
		packBits33, packBits34, packBits35, packBits36, packBits37, packBits38, packBits39, packBits40,
		packBits41, packBits42, packBits43, packBits44, packBits45, packBits46, packBits47, packBits48,
		packBits49, packBits50, packBits51, packBits52, packBits53, packBits54, packBits55, packBits56,
		packBits57, packBits58, packBits59, packBits60, packBits61, packBits62, packBits63,
	}

	unpackFuncs := []func([]uint64, []byte){
		unpackBits1, unpackBits2, unpackBits3, unpackBits4, unpackBits5, unpackBits6, unpackBits7, unpackBits8,
		unpackBits9, unpackBits10, unpackBits11, unpackBits12, unpackBits13, unpackBits14, unpackBits15, unpackBits16,
		unpackBits17, unpackBits18, unpackBits19, unpackBits20, unpackBits21, unpackBits22, unpackBits23, unpackBits24,
		unpackBits25, unpackBits26, unpackBits27, unpackBits28, unpackBits29, unpackBits30, unpackBits31, unpackBits32,
		unpackBits33, unpackBits34, unpackBits35, unpackBits36, unpackBits37, unpackBits38, unpackBits39, unpackBits40,
		unpackBits41, unpackBits42, unpackBits43, unpackBits44, unpackBits45, unpackBits46, unpackBits47, unpackBits48,
		unpackBits49, unpackBits50, unpackBits51, unpackBits52, unpackBits53, unpackBits54, unpackBits55, unpackBits56,
		unpackBits57, unpackBits58, unpackBits59, unpackBits60, unpackBits61, unpackBits62, unpackBits63,
	}

	for bits := uint8(1); bits <= 63; bits++ {
		t.Run(fmt.Sprintf("bits_%d", bits), func(t *testing.T) {
			maxVal := uint64(1<<bits) - 1

			testPatterns := [][8]uint64{
				{0, 0, 0, 0, 0, 0, 0, 0}, // All zeros
				{maxVal, maxVal, maxVal, maxVal, maxVal, maxVal, maxVal, maxVal}, // All max
				{0, 1, 2, 3, 4, 5, 6, 7},                     // Sequential (if fits)
				{maxVal, 0, maxVal, 0, maxVal, 0, maxVal, 0}, // Alternating
				{1, 1, 1, 1, 1, 1, 1, 1},                     // All ones (bit value)
			}

			for patIdx, pattern := range testPatterns {
				values := [8]uint64{}
				for i := 0; i < 8; i++ {
					values[i] = pattern[i] & maxVal
				}

				bufSize := (int(bits) * 8) / 8
				if (int(bits)*8)%8 != 0 {
					bufSize++
				}
				buf := make([]byte, bufSize)

				// Pack
				packFuncs[bits-1](values[:], buf)

				// Unpack
				result := [8]uint64{}
				unpackFuncs[bits-1](result[:], buf)

				// Verify
				for i := 0; i < 8; i++ {
					assert.Equal(t, result[i], values[i], "Pattern %d, bit width %d, index %d", patIdx, bits, i)
				}
			}
		})
	}
}

func TestPackUnpackBits(t *testing.T) {
	value := uint64(0xaa55aa55aa55aa55) // arbitrary starting value

	for m := 0; m < 10000; m++ {
		for bits := uint8(1); bits <= 63; bits++ {
			n := 8
			mask := (uint64(1) << bits) - 1

			// Generate input data
			input := make([]uint64, n)
			for i := 0; i < n; i++ {
				input[i] = value & mask
				value += igolden64
			}

			// Pack data
			bytes := make([]byte, n*8)
			offset := uint8(0)
			ptrIdx := 0
			for i := 0; i < n; i++ {
				ptrIdx, offset = packBits(input[i], bits, bytes, ptrIdx, offset)
			}

			// Unpack data
			output := make([]uint64, n)
			offset = 0
			ptrIdx = 0
			for i := 0; i < n; i++ {
				output[i], ptrIdx, offset = unpackBits(bits, bytes, ptrIdx, offset)
			}

			// Verify
			for i := 0; i < n; i++ {
				assert.Equal(t, input[i], output[i])
			}
		}
	}
}

func TestPackUnpackBlocks(t *testing.T) {
	value := uint64(0xaa55aa55aa55aa55) // arbitrary starting value

	for n := 0; n < 10000; n++ {
		for bits := uint8(1); bits <= 63; bits++ {
			mask := (uint64(1) << bits) - 1

			// Generate input data (block of 8 values)
			input := make([]uint64, 8)
			for i := 0; i < 8; i++ {
				input[i] = value & mask
				value += igolden64
			}

			// Pack block
			bytes := make([]byte, bits)
			packBitsBlock8(input, bytes, bits)

			// Unpack block
			output := make([]uint64, 8)
			unpackBitsBlock8(output, bytes, bits)

			// Verify
			for i := 0; i < 8; i++ {
				assert.Equal(t, input[i], output[i])
			}
		}
	}
}

func TestPackBitsUnpackBlocks(t *testing.T) {
	value := uint64(0) // arbitrary starting value

	for m := 0; m < 10000; m++ {
		for bits := uint8(1); bits <= 63; bits++ {
			mask := (uint64(1) << bits) - 1

			// Generate input data
			input := make([]uint64, 8)
			for i := 0; i < 8; i++ {
				input[i] = value & mask
				value += igolden64
			}

			// Pack with individual bits
			bytes := make([]byte, bits)
			offset := uint8(0)
			ptrIdx := 0
			for i := 0; i < 8; i++ {
				ptrIdx, offset = packBits(input[i], bits, bytes, ptrIdx, offset)
			}

			// Unpack with block function
			output := make([]uint64, 8)
			unpackBitsBlock8(output, bytes, bits)

			// Verify
			for i := 0; i < 8; i++ {
				assert.Equal(t, input[i], output[i])
			}
		}
	}
}

func TestPackBlocksUnpackBits(t *testing.T) {
	value := uint64(111) // arbitrary starting value

	for m := 0; m < 10000; m++ {
		for bits := uint8(1); bits <= 63; bits++ {
			mask := (uint64(1) << bits) - 1

			// Generate input data
			input := make([]uint64, 8)
			for i := 0; i < 8; i++ {
				input[i] = value & mask
				value += igolden64
			}

			// Pack with block function
			bytes := make([]byte, bits)
			packBitsBlock8(input, bytes, bits)

			// Unpack with individual bits
			output := make([]uint64, 8)
			offset := uint8(0)
			ptrIdx := 0
			for i := 0; i < 8; i++ {
				output[i], ptrIdx, offset = unpackBits(bits, bytes, ptrIdx, offset)
			}

			// Verify
			for i := 0; i < 8; i++ {
				assert.Equal(t, input[i], output[i])
			}
		}
	}
}
