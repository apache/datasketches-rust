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

import (
	"fmt"
	"math"
	"math/bits"
	"reflect"
)

const (
	InverseGolden = float64(0.6180339887498949025)
)

const (
	DEFAULT_UPDATE_SEED = uint64(9001)
)

const (
	DSketchTestGenerateGo = "DSKETCH_TEST_GENERATE_GO"
)

const (
	JavaPath = "../serialization_test_data/java_generated_files"
	CppPath  = "../serialization_test_data/cpp_generated_files"
	GoPath   = "../serialization_test_data/go_generated_files"
)

// GetShortLE gets a short value from a byte array in little endian format.
func GetShortLE(array []byte, offset int) int {
	return int(array[offset]&0xFF) | (int(array[offset+1]&0xFF) << 8)
}

// PutShortLE puts a short value into a byte array in little endian format.
func PutShortLE(array []byte, offset int, value int) {
	array[offset] = byte(value)
	array[offset+1] = byte(value >> 8)
}

// InvPow2 returns 2^(-e).
func InvPow2(e int) (float64, error) {
	if (e | 1024 - e - 1) < 0 {
		return 0, fmt.Errorf("e cannot be negative or greater than 1023: %d", e)
	}
	return math.Float64frombits((1023 - uint64(e)) << 52), nil
}

// CeilPowerOf2 returns the smallest power of 2 greater than or equal to n.
func CeilPowerOf2(n int) int {
	if n <= 1 {
		return 1
	}
	topIntPwrOf2 := 1 << 30
	if n >= topIntPwrOf2 {
		return topIntPwrOf2
	}
	return int(math.Pow(2, math.Ceil(math.Log2(float64(n)))))
}

func ExactLog2(powerOf2 int) (int, error) {
	if !IsPowerOf2(powerOf2) {
		return 0, fmt.Errorf("argument 'powerOf2' must be a positive power of 2")
	}
	return bits.TrailingZeros64(uint64(powerOf2)), nil
}

// IsPowerOf2 returns true if the given number is a power of 2.
func IsPowerOf2(powerOf2 int) bool {
	return powerOf2 > 0 && (powerOf2&(powerOf2-1)) == 0
}

func BoolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func FloorPowerOf2(n int64) int64 {
	if n <= 1 {
		return 1
	}

	return n & (math.MinInt64 >> (bits.LeadingZeros64(uint64(n))))
}

func IsNil[T any](t T) bool {
	v := reflect.ValueOf(t)
	kind := v.Kind()
	// Must be one of these types to be nillable
	return (kind == reflect.Ptr ||
		kind == reflect.Interface ||
		kind == reflect.Slice ||
		kind == reflect.Map ||
		kind == reflect.Chan ||
		kind == reflect.Func) &&
		v.IsNil()
}

func ComputeSeedHash(seed int64) (int16, error) {
	seedArr := []int64{seed}
	//seedHash, _ := HashInt64SliceMurmur3(seedArr, 0, 1, uint64(seed))
	seedHash, _ := HashInt64SliceMurmur3(seedArr, 0, len(seedArr), 0)
	seedHash = seedHash & 0xFFFF

	if seedHash == 0 {
		return 0, fmt.Errorf("the given seed: %d produced a seedHash of zero. You must choose a different seed", seed)
	}
	return int16(seedHash), nil
}

func Log2Floor(n uint32) uint8 {
	if n == 0 {
		return 0
	}
	return uint8(bits.Len32(n) - 1)
}

func LgSizeFromCount(n uint32, loadFactor float64) uint8 {
	lgN := Log2Floor(n)
	// Check if n > (2^(lgN+1)) * loadFactor
	// If so, we need lgN + 2, otherwise lgN + 1
	powerOfTwo := uint32(1) << (lgN + 1)
	threshold := uint32(float64(powerOfTwo) * loadFactor)
	if n > threshold {
		return lgN + 2
	}
	return lgN + 1
}
