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
	"math"
)

type ItemSketchDoubleHasher struct {
	scratch [8]byte
}
type ItemSketchDoubleSerDe struct{}

var ItemSketchDoubleComparator = func(reverseOrder bool) CompareFn[float64] {
	return func(a float64, b float64) bool {
		if reverseOrder {
			return a > b
		}
		return a < b
	}
}

func (f ItemSketchDoubleHasher) Hash(item float64) uint64 {
	binary.LittleEndian.PutUint64(f.scratch[:], math.Float64bits(item))
	return murmur3.SeedSum64(defaultSerdeHashSeed, f.scratch[:])
}

func (f ItemSketchDoubleSerDe) SizeOf(item float64) int {
	return 8
}

func (f ItemSketchDoubleSerDe) SizeOfMany(mem []byte, offsetBytes int, numItems int) (int, error) {
	return numItems * 8, nil
}

func (f ItemSketchDoubleSerDe) SerializeOneToSlice(item float64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, math.Float64bits(item))
	return bytes
}

func (f ItemSketchDoubleSerDe) SerializeManyToSlice(item []float64) []byte {
	if len(item) == 0 {
		return []byte{}
	}
	bytes := make([]byte, 8*len(item))
	offset := 0
	for i := 0; i < len(item); i++ {
		binary.LittleEndian.PutUint64(bytes[offset:], math.Float64bits(item[i]))
		offset += 8
	}
	return bytes
}

func (f ItemSketchDoubleSerDe) DeserializeManyFromSlice(mem []byte, offsetBytes int, numItems int) ([]float64, error) {
	if numItems == 0 {
		return []float64{}, nil
	}
	array := make([]float64, 0, numItems)
	for i := 0; i < numItems; i++ {
		array = append(array, math.Float64frombits(binary.LittleEndian.Uint64(mem[offsetBytes:])))
		offsetBytes += 8
	}
	return array, nil
}
