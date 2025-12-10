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

package cpc

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestCpcCheckSort(t *testing.T) {
	length := 10
	arr1 := []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	rng := rand.New(rand.NewSource(0))
	for i := 0; i < length; i++ {
		arr1[i] = rng.Intn(10000)
	}
	introspectiveInsertionSort(arr1, 0, length-1)
	for _, i := range arr1 {
		println(i)
	}
}

func TestCpcCheckSort2(t *testing.T) {
	length := 10
	arr2 := make([]int, length)
	rng := rand.New(rand.NewSource(0))
	for i := 0; i < length; i++ {
		r1 := rng.Intn(10000)
		r2 := 3_000_000_000
		arr2[i] = r2 + r1
	}
	println("")
	introspectiveInsertionSort(arr2, 0, length-1)
	for _, i := range arr2 {
		println(i & 0xFFFF_FFFF)
	}
}

func TestCpcCheckSort3(t *testing.T) {
	length := 20
	arr3 := make([]int, length)
	for i := 0; i < length; i++ {
		arr3[i] = (length - i) + 1
	}
	println("")
	introspectiveInsertionSort(arr3, 0, length-1)
	for _, i := range arr3 {
		println(i)
	}
}

func TestCpcCheckMerge(t *testing.T) {
	arrA := []int{1, 3, 5}
	arrB := []int{2, 4, 6}
	arrC := make([]int, 6)
	mergePairs(arrA, 0, 3, arrB, 0, 3, arrC, 0)
	for _, i := range arrC {
		println(i)
	}
}

func TestCpcCheckMerge2(t *testing.T) {
	arrA := []int{1, 3, 5}
	arrB := []int{2, 4, 6}
	arrC := make([]int, 6)
	for i := 0; i < 3; i++ {
		arrA[i] = arrA[i] + 3_000_000_000
		arrB[i] = arrB[i] + 3_000_000_000
	}
	mergePairs(arrA, 0, 3, arrB, 0, 3, arrC, 0)
	for _, i := range arrC {
		println(i & 0xFFFF_FFFF)
	}
}

func TestCheckError(t *testing.T) {
	lgK := 10
	a, err := NewPairTable(2, lgK+6)
	assert.NoError(t, err)
	assert.Equal(t, a.validBits, lgK+6)

	_, err = NewPairTable(1, 16)
	assert.Error(t, err)
}
