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

import (
	"fmt"
	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestGenerateGoFiles(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	os.Mkdir(internal.GoPath, 0755)

	nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
	comparatorString := common.ItemSketchStringComparator(false)
	for _, n := range nArr {
		digits := numDigits(n)
		sk, err := NewKllItemsSketchWithDefault[string](comparatorString, common.ItemSketchStringSerDe{})
		sk.deterministicOffsetForTest = true
		assert.NoError(t, err)
		for i := 1; i <= n; i++ {
			sk.Update(intToFixedLengthString(i, digits))
		}
		slc, err := sk.ToSlice()
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/kll_string_n%d_go.sk", internal.GoPath, n), slc, 0644)
		assert.NoError(t, err)
	}

	comparatorDouble := common.ItemSketchDoubleComparator(false)
	for _, n := range nArr {
		sk, err := NewKllItemsSketchWithDefault[float64](comparatorDouble, common.ItemSketchDoubleSerDe{})
		sk.deterministicOffsetForTest = true
		assert.NoError(t, err)
		for i := 1; i <= n; i++ {
			sk.Update(float64(i))
		}
		slc, err := sk.ToSlice()
		assert.NoError(t, err)
		err = os.WriteFile(fmt.Sprintf("%s/kll_double_n%d_go.sk", internal.GoPath, n), slc, 0644)
		assert.NoError(t, err)
	}
}

func TestJavaCompat(t *testing.T) {
	t.Run("Java KLL String", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		serde := common.ItemSketchStringSerDe{}
		comparatorString := common.ItemSketchStringComparator(false)
		for _, n := range nArr {
			digits := numDigits(n)
			bytes, err := os.ReadFile(fmt.Sprintf("%s/kll_string_n%d_java.sk", internal.JavaPath, n))
			assert.NoError(t, err)
			sketch, err := NewKllItemsSketchFromSlice[string](bytes, comparatorString, serde)
			if err != nil {
				return
			}

			assert.Equal(t, sketch.GetK(), uint16(200))
			if n == 0 {
				assert.True(t, sketch.IsEmpty())
			} else {
				assert.False(t, sketch.IsEmpty())
			}

			if n > 100 {
				assert.True(t, sketch.IsEstimationMode())
			} else {
				assert.False(t, sketch.IsEstimationMode())
			}

			if n > 0 {
				minV, err := sketch.GetMinItem()
				assert.NoError(t, err)
				assert.Equal(t, minV, intToFixedLengthString(1, digits))

				maxV, err := sketch.GetMaxItem()
				assert.NoError(t, err)
				assert.Equal(t, maxV, intToFixedLengthString(n, digits))

				weight := int64(0)
				it := sketch.GetIterator()
				compareFn := comparatorString
				for it.Next() {
					qut := it.GetQuantile()
					assert.True(t, compareFn(minV, qut) || minV == qut, fmt.Sprintf("min: \"%v\" \"%v\"", minV, qut))
					assert.True(t, !compareFn(maxV, qut) || maxV == qut, fmt.Sprintf("max: \"%v\" \"%v\"", maxV, qut))
					weight += it.GetWeight()
				}
				assert.Equal(t, weight, int64(n))
			}
		}
	})

	t.Run("Java KLL Double", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		serde := common.ItemSketchDoubleSerDe{}
		comparatorDouble := common.ItemSketchDoubleComparator(false)
		for _, n := range nArr {
			bytes, err := os.ReadFile(fmt.Sprintf("%s/kll_double_n%d_java.sk", internal.JavaPath, n))
			assert.NoError(t, err)
			sketch, err := NewKllItemsSketchFromSlice[float64](bytes, comparatorDouble, serde)
			if err != nil {
				return
			}

			assert.Equal(t, sketch.GetK(), uint16(200))
			if n == 0 {
				assert.True(t, sketch.IsEmpty())
			} else {
				assert.False(t, sketch.IsEmpty())
			}

			if n > 100 {
				assert.True(t, sketch.IsEstimationMode())
			} else {
				assert.False(t, sketch.IsEstimationMode())
			}

			if n > 0 {
				minV, err := sketch.GetMinItem()
				assert.NoError(t, err)
				assert.Equal(t, minV, float64(1))

				maxV, err := sketch.GetMaxItem()
				assert.NoError(t, err)
				assert.Equal(t, maxV, float64(n))

				weight := int64(0)
				it := sketch.GetIterator()
				for it.Next() {
					qut := it.GetQuantile()
					assert.True(t, comparatorDouble(minV, qut) || minV == qut, fmt.Sprintf("min: \"%v\" \"%v\"", minV, qut))
					assert.True(t, !comparatorDouble(maxV, qut) || maxV == qut, fmt.Sprintf("max: \"%v\" \"%v\"", maxV, qut))
					weight += it.GetWeight()
				}
				assert.Equal(t, weight, int64(n))
			}
		}
	})
}
