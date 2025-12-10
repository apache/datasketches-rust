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
	"encoding/binary"
	"errors"
	"unsafe"

	"github.com/twmb/murmur3"
)

type ItemSketchStringHasher struct{}
type ItemSketchStringSerDe struct{}

var ItemSketchStringComparator = func(reverseOrder bool) CompareFn[string] {
	return func(a, b string) bool {
		if reverseOrder {
			return a > b
		}
		return a < b
	}
}

func (f ItemSketchStringHasher) Hash(item string) uint64 {
	datum := unsafe.Slice(unsafe.StringData(item), len(item))
	return murmur3.SeedSum64(defaultSerdeHashSeed, datum[:])
}

func (f ItemSketchStringSerDe) SizeOf(item string) int {
	if len(item) == 0 {
		return int(unsafe.Sizeof(uint32(0)))
	}
	return len(item) + int(unsafe.Sizeof(uint32(0)))
}

func (f ItemSketchStringSerDe) SizeOfMany(mem []byte, offsetBytes int, numItems int) (int, error) {
	if numItems <= 0 {
		return 0, nil
	}
	reqLen := 4
	offset := offsetBytes
	memCap := len(mem)
	for i := 0; i < numItems; i++ {
		if !checkBounds(offset, reqLen, memCap) {
			return 0, errors.New("offset out of bounds")
		}
		itemLenBytes := int(binary.LittleEndian.Uint32(mem[offset:]))
		offset += 4
		if offset+itemLenBytes > memCap {
			return 0, errors.New("offset out of bounds")
		}
		offset += itemLenBytes
	}
	return offset - offsetBytes, nil
}

func (f ItemSketchStringSerDe) SerializeOneToSlice(item string) []byte {
	if len(item) == 0 {
		return []byte{}
	}
	utf8len := len(item)
	bytesOut := make([]byte, utf8len+4)
	binary.LittleEndian.PutUint32(bytesOut, uint32(utf8len))
	copy(bytesOut[4:], []byte(item))
	return bytesOut
}

func (f ItemSketchStringSerDe) SerializeManyToSlice(item []string) []byte {
	if len(item) == 0 {
		return []byte{}
	}
	totalBytes := 0
	numItems := len(item)
	serialized2DArray := make([][]byte, numItems)
	for i := 0; i < numItems; i++ {
		serialized2DArray[i] = []byte(item[i])
		totalBytes += len(serialized2DArray[i]) + 4
	}
	bytesOut := make([]byte, totalBytes)
	offset := 0
	for i := 0; i < numItems; i++ {
		utf8len := len(serialized2DArray[i])
		binary.LittleEndian.PutUint32(bytesOut[offset:], uint32(utf8len))
		offset += 4
		copy(bytesOut[offset:], serialized2DArray[i])
		offset += utf8len
	}
	return bytesOut
}

func (f ItemSketchStringSerDe) DeserializeManyFromSlice(mem []byte, offsetBytes int, numItems int) ([]string, error) {
	if numItems <= 0 {
		return []string{}, nil
	}
	array := make([]string, numItems)
	offset := offsetBytes
	intSize := int(unsafe.Sizeof(uint32(0)))
	memCap := len(mem)
	for i := 0; i < numItems; i++ {
		if !checkBounds(offset, intSize, memCap) {
			return nil, errors.New("offset out of bounds")
		}
		strLength := int(binary.LittleEndian.Uint32(mem[offset:]))
		offset += intSize
		utf8Bytes := make([]byte, strLength)
		if !checkBounds(offset, strLength, memCap) {
			return nil, errors.New("offset out of bounds")
		}
		copy(utf8Bytes, mem[offset:offset+strLength])
		offset += strLength
		array[i] = string(utf8Bytes)
	}
	return array, nil
}
