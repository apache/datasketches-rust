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

package internal

const (
	C1 = 0x87c37b91114253d5
	C2 = 0x4cf5ad432745937f
)

type SimpleMurmur3 struct {
	h1 uint64
	h2 uint64
}

func HashCharSliceMurmur3(key []byte, offsetChars int, lengthChars int, seed uint64) (uint64, uint64) {
	hashState := SimpleMurmur3{h1: seed, h2: seed}

	// Number of full 128-bit blocks of 8 chars.
	// Possible exclusion of a remainder of up to 7 chars.
	nblocks := lengthChars >> 3 //chars / 8

	// Process the 128-bit blocks (the body) into the hash
	for i := 0; i < nblocks; i++ {
		k1 := getUint64(key, offsetChars+(i<<3), 4)   //offsetChars + 0, 8, 16, ...
		k2 := getUint64(key, offsetChars+(i<<3)+4, 4) //offsetChars + 4, 12, 20, ...
		hashState.blockMix128(k1, k2)
	}

	// Get the tail index wrt hashed portion, remainder length
	tail := nblocks << 3      // 8 chars per block
	rem := lengthChars - tail // remainder chars: 0,1,2,3,4,5,6,7

	// Get the tail
	k1 := uint64(0)
	k2 := uint64(0)
	if rem > 4 {
		k1 = getUint64(key, offsetChars+tail, 4)
		k2 = getUint64(key, offsetChars+tail+4, rem-4)
	} else {
		if rem != 0 {
			k1 = getUint64(key, offsetChars+tail, rem)
		}
	}

	// Mix the tail into the hash and return
	return hashState.finalMix128(k1, k2, uint64(lengthChars)<<1) //convert to bytes

}

func HashInt32SliceMurmur3(key []int32, offsetInts int, lengthInts int, seed uint64) (uint64, uint64) {
	hashState := SimpleMurmur3{h1: seed, h2: seed}

	// Number of full 128-bit blocks of 4 ints.
	// Possible exclusion of a remainder of up to 3 ints.
	nblocks := lengthInts >> 2 //ints / 4

	// Process the 128-bit blocks (the body) into the hash
	for i := 0; i < nblocks; i++ {
		k1 := uint64(key[offsetInts+(i<<2)])   //offsetInts + 0, 4, 8, ...
		k2 := uint64(key[offsetInts+(i<<2)+2]) //offsetInts + 2, 6, 10, ...
		hashState.blockMix128(k1, k2)
	}

	// Get the tail index wrt hashed portion, remainder length
	tail := nblocks << 2     // 4 ints per block
	rem := lengthInts - tail // remainder ints: 0,1,2,3

	// Get the tail
	k1 := uint64(0)
	k2 := uint64(0)
	if rem > 2 {
		k1 = uint64(key[offsetInts+tail]) //k2 -> whole; k1 -> partial
		k2 = uint64(key[offsetInts+tail+2])
	} else {
		if rem != 0 {
			k1 = uint64(key[offsetInts+tail]) //k1 -> whole(2), partial(1) or 0; k2 == 0
		}
	}

	// Mix the tail into the hash and return
	return hashState.finalMix128(k1, k2, uint64(lengthInts)<<2) //convert to bytes
}

func HashInt64SliceMurmur3(key []int64, offsetLongs int, lengthLongs int, seed uint64) (uint64, uint64) {
	hashState := SimpleMurmur3{h1: seed, h2: seed}

	// Number of full 128-bit blocks of 2 longs (the body).
	// Possible exclusion of a remainder of 1 long.
	nblocks := lengthLongs >> 1 //longs / 2

	// Process the 128-bit blocks (the body) into the hash
	for i := 0; i < nblocks; i++ {
		k1 := uint64(key[offsetLongs+(i<<1)])   //offsetLongs + 0, 2, 4, ...
		k2 := uint64(key[offsetLongs+(i<<1)+1]) //offsetLongs + 1, 3, 5, ...
		hashState.blockMix128(k1, k2)
	}

	// Get the tail index wrt hashed portion, remainder length
	tail := nblocks << 1      // 2 longs / block
	rem := lengthLongs - tail // remainder longs: 0,1

	// Get the tail
	k1 := uint64(0)
	if rem != 0 {
		k1 = uint64(key[offsetLongs+tail]) //k2 -> 0
	}

	return hashState.finalMix128(k1, 0, uint64(lengthLongs)<<3)
}

func HashByteArrMurmur3(key []byte, offsetBytes int, lengthBytes int, seed uint64) (uint64, uint64) {
	hashState := SimpleMurmur3{h1: seed, h2: seed}

	// Number of full 128-bit blocks of 16 bytes.
	// Possible exclusion of a remainder of up to 15 bytes.
	nblocks := lengthBytes >> 4 //bytes / 16

	// Process the 128-bit blocks (the body) into the hash
	for i := 0; i < nblocks; i++ {
		k1 := getUint64(key, offsetBytes+(i<<4), 8)   //0, 16, 32, ...
		k2 := getUint64(key, offsetBytes+(i<<4)+8, 8) //8, 24, 40, ...
		hashState.blockMix128(k1, k2)
	}

	// Get the tail index wrt hashed portion, remainder length
	tail := nblocks << 4      // 16 bytes / block
	rem := lengthBytes - tail // remainder bytes: 0,1,...,15

	// Get the tail
	k1 := uint64(0)
	k2 := uint64(0)
	if rem > 8 {
		k1 = getUint64(key, offsetBytes+tail, 8)
		k2 = getUint64(key, offsetBytes+tail+8, rem-8)
	} else {
		if rem != 0 {
			k1 = getUint64(key, offsetBytes+tail, rem)
		}
	}

	// Mix the tail into the hash and return
	return hashState.finalMix128(k1, k2, uint64(lengthBytes))
}

func getUint64(bArr []byte, index int, rem int) uint64 {
	var out uint64
	for i := rem - 1; i >= 0; i-- { //i= 7,6,5,4,3,2,1,0
		b := bArr[index+i]
		out ^= uint64(b&0xFF) << uint(i*8) //equivalent to |=
	}
	return out
}

func mixK1(k1 uint64) uint64 {
	k1 *= C1
	k1 = (k1 << 31) | (k1 >> (64 - 31))
	k1 *= C2
	return k1

}

func mixK2(k2 uint64) uint64 {
	k2 *= C2
	k2 = (k2 << 33) | (k2 >> (64 - 33))
	k2 *= C1
	return k2
}

func finalMix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h

}

func (m *SimpleMurmur3) blockMix128(k1, k2 uint64) {
	m.h1 ^= mixK1(k1)
	m.h1 = (m.h1 << 27) | (m.h1 >> (64 - 27))
	m.h1 += m.h2
	m.h1 = m.h1*5 + 0x52dce729

	m.h2 ^= mixK2(k2)
	m.h2 = (m.h2 << 31) | (m.h2 >> (64 - 31))
	m.h2 += m.h1
	m.h2 = m.h2*5 + 0x38495ab5
}

func (m *SimpleMurmur3) finalMix128(k1, k2, inputLengthBytes uint64) (uint64, uint64) {
	m.h1 ^= mixK1(k1)
	m.h2 ^= mixK2(k2)
	m.h1 ^= inputLengthBytes
	m.h2 ^= inputLengthBytes
	m.h1 += m.h2
	m.h2 += m.h1
	m.h1 = finalMix64(m.h1)
	m.h2 = finalMix64(m.h2)
	m.h1 += m.h2
	m.h2 += m.h1
	return m.h1, m.h2
}
