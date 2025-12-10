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
	"github.com/twmb/murmur3"
)

type ItemSketchLongHasher struct {
	scratch [8]byte
}
type ItemSketchLongSerDe struct{}

var ItemSketchLongComparator = func(reverseOrder bool) CompareFn[int64] {
	return func(a, b int64) bool {
		if reverseOrder {
			return a > b
		}
		return a < b
	}
}

func (f ItemSketchLongHasher) Hash(item int64) uint64 {
	binary.LittleEndian.PutUint64(f.scratch[:], uint64(item))
	return murmur3.SeedSum64(defaultSerdeHashSeed, f.scratch[:])
}

func (f ItemSketchLongSerDe) SizeOf(item int64) int {
	return 8
}

func (f ItemSketchLongSerDe) SizeOfMany(mem []byte, offsetBytes int, numItems int) (int, error) {
	return numItems * 8, nil
}

func (f ItemSketchLongSerDe) SerializeOneToSlice(item int64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(item))
	return bytes
}

func (f ItemSketchLongSerDe) SerializeManyToSlice(item []int64) []byte {
	if len(item) == 0 {
		return []byte{}
	}
	bytes := make([]byte, 8*len(item))
	offset := 0
	for i := 0; i < len(item); i++ {
		binary.LittleEndian.PutUint64(bytes[offset:], uint64(item[i]))
		offset += 8
	}
	return bytes
}

func (f ItemSketchLongSerDe) DeserializeManyFromSlice(mem []byte, offsetBytes int, numItems int) ([]int64, error) {
	if numItems == 0 {
		return []int64{}, nil
	}
	array := make([]int64, 0, numItems)
	for i := 0; i < numItems; i++ {
		array = append(array, int64(binary.LittleEndian.Uint64(mem[offsetBytes:])))
		offsetBytes += 8
	}
	return array, nil
}
