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
	"fmt"
	"github.com/apache/datasketches-go/common"
	"os"
	"strconv"
	"testing"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
)

func TestGenerateGoBinariesForCompatibilityTestingLongsSketch(t *testing.T) {
	if len(os.Getenv(internal.DSketchTestGenerateGo)) == 0 {
		t.Skipf("%s not set", internal.DSketchTestGenerateGo)
	}

	t.Run("Long Frequency", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			sk, err := NewLongsSketchWithMaxMapSize(64)
			assert.NoError(t, err)
			for i := 1; i <= n; i++ {
				err = sk.Update(int64(i))
				assert.NoError(t, err)
			}
			if n == 0 {
				assert.True(t, sk.IsEmpty())
			} else {
				assert.False(t, sk.IsEmpty())
			}
			if n > 10 {
				assert.True(t, sk.GetMaximumError() > 0)
			} else {
				assert.Equal(t, sk.GetMaximumError(), int64(0))
			}
			err = os.MkdirAll(internal.GoPath, os.ModePerm)
			assert.NoError(t, err)

			slc := sk.ToSlice()
			err = os.WriteFile(fmt.Sprintf("%s/frequent_long_n%d_go.sk", internal.GoPath, n), slc, 0644)
			if err != nil {
				t.Errorf("err != nil")
			}
		}
	})

	t.Run("String Frequency", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			sk, err := NewFrequencyItemsSketchWithMaxMapSize[string](64, common.ItemSketchStringHasher{}, common.ItemSketchStringSerDe{})
			assert.NoError(t, err)
			for i := 1; i <= n; i++ {
				err = sk.Update(strconv.Itoa(i))
				assert.NoError(t, err)
			}
			if n == 0 {
				assert.True(t, sk.IsEmpty())
			} else {
				assert.False(t, sk.IsEmpty())
			}
			if n > 10 {
				assert.True(t, sk.GetMaximumError() > 0)
			} else {
				assert.Equal(t, sk.GetMaximumError(), int64(0))
			}
			err = os.MkdirAll(internal.GoPath, os.ModePerm)
			assert.NoError(t, err)

			slc, err := sk.ToSlice()
			err = os.WriteFile(fmt.Sprintf("%s/frequent_string_n%d_go.sk", internal.GoPath, n), slc, 0644)
			if err != nil {
				t.Errorf("err != nil")
			}
		}
	})

	t.Run("String ut8", func(t *testing.T) {
		sk, err := NewFrequencyItemsSketchWithMaxMapSize[string](64, common.ItemSketchStringHasher{}, common.ItemSketchStringSerDe{})
		assert.NoError(t, err)

		assert.NoError(t, sk.UpdateMany("абвгд", 1))
		assert.NoError(t, sk.UpdateMany("еёжзи", 2))
		assert.NoError(t, sk.UpdateMany("йклмн", 3))
		assert.NoError(t, sk.UpdateMany("опрст", 4))
		assert.NoError(t, sk.UpdateMany("уфхцч", 5))
		assert.NoError(t, sk.UpdateMany("шщъыь", 6))
		assert.NoError(t, sk.UpdateMany("эюя", 7))

		err = os.MkdirAll(internal.GoPath, os.ModePerm)
		assert.NoError(t, err)

		slc, err := sk.ToSlice()
		err = os.WriteFile(fmt.Sprintf("%s/frequent_string_utf8_go.sk", internal.GoPath), slc, 0644)
		if err != nil {
			t.Errorf("err != nil")
		}
	})

	t.Run("String ascii", func(t *testing.T) {
		sk, err := NewFrequencyItemsSketchWithMaxMapSize[string](64, common.ItemSketchStringHasher{}, common.ItemSketchStringSerDe{})
		assert.NoError(t, err)

		assert.NoError(t, sk.UpdateMany("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1))
		assert.NoError(t, sk.UpdateMany("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 2))
		assert.NoError(t, sk.UpdateMany("ccccccccccccccccccccccccccccc", 3))
		assert.NoError(t, sk.UpdateMany("ddddddddddddddddddddddddddddd", 4))

		err = os.MkdirAll(internal.GoPath, os.ModePerm)
		assert.NoError(t, err)

		slc, err := sk.ToSlice()
		err = os.WriteFile(fmt.Sprintf("%s/frequent_string_ascii_go.sk", internal.GoPath), slc, 0644)
		if err != nil {
			t.Errorf("err != nil")
		}
	})
}

func TestJavaCompat(t *testing.T) {

	t.Run("Long ut8", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			bytes, err := os.ReadFile(fmt.Sprintf("%s/frequent_long_n%d_java.sk", internal.JavaPath, n))
			assert.NoError(t, err)
			sketch, err := NewLongsSketchFromSlice(bytes)
			if err != nil {
				return
			}

			if n == 0 {
				assert.True(t, sketch.IsEmpty())
			} else {
				assert.False(t, sketch.IsEmpty())
			}
			if n > 10 {
				assert.True(t, sketch.GetMaximumError() > 0)
			} else {
				assert.Equal(t, sketch.GetMaximumError(), int64(0))
			}
			assert.Equal(t, sketch.GetStreamLength(), int64(n))
		}
	})

	t.Run("String Frequency", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			bytes, err := os.ReadFile(fmt.Sprintf("%s/frequent_string_n%d_java.sk", internal.JavaPath, n))
			assert.NoError(t, err)
			sketch, err := NewFrequencyItemsSketchFromSlice[string](bytes, common.ItemSketchStringHasher{}, common.ItemSketchStringSerDe{})
			if err != nil {
				return
			}

			if n == 0 {
				assert.True(t, sketch.IsEmpty())
			} else {
				assert.False(t, sketch.IsEmpty())
			}
			if n > 10 {
				assert.True(t, sketch.GetMaximumError() > 0)
			} else {
				assert.Equal(t, sketch.GetMaximumError(), int64(0))
			}
			assert.Equal(t, sketch.GetStreamLength(), int64(n))
		}
	})

	t.Run("String utf8", func(t *testing.T) {
		bytes, err := os.ReadFile(fmt.Sprintf("%s/frequent_string_utf8_java.sk", internal.JavaPath))
		assert.NoError(t, err)
		sketch, err := NewFrequencyItemsSketchFromSlice[string](bytes, common.ItemSketchStringHasher{}, common.ItemSketchStringSerDe{})
		if err != nil {
			return
		}
		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, sketch.GetMaximumError(), int64(0))
		assert.Equal(t, sketch.GetStreamLength(), int64(28))
		est, err := sketch.GetEstimate("абвгд")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(1))
		est, err = sketch.GetEstimate("еёжзи")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(2))
		est, err = sketch.GetEstimate("йклмн")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(3))
		est, err = sketch.GetEstimate("опрст")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(4))
		est, err = sketch.GetEstimate("уфхцч")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(5))
		est, err = sketch.GetEstimate("шщъыь")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(6))
		est, err = sketch.GetEstimate("эюя")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(7))
	})

	t.Run("String ascii", func(t *testing.T) {
		bytes, err := os.ReadFile(fmt.Sprintf("%s/frequent_string_ascii_java.sk", internal.JavaPath))
		assert.NoError(t, err)
		sketch, err := NewFrequencyItemsSketchFromSlice[string](bytes, common.ItemSketchStringHasher{}, common.ItemSketchStringSerDe{})
		if err != nil {
			return
		}
		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, sketch.GetMaximumError(), int64(0))
		assert.Equal(t, sketch.GetStreamLength(), int64(10))
		est, err := sketch.GetEstimate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(1))
		est, err = sketch.GetEstimate("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(2))
		est, err = sketch.GetEstimate("ccccccccccccccccccccccccccccc")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(3))
		est, err = sketch.GetEstimate("ddddddddddddddddddddddddddddd")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(4))
	})
}

func TestCppCompat(t *testing.T) {
	t.Run("Long Frequency", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			bytes, err := os.ReadFile(fmt.Sprintf("%s/frequent_long_n%d_cpp.sk", internal.CppPath, n))
			assert.NoError(t, err)
			sketch, err := NewLongsSketchFromSlice(bytes)
			if err != nil {
				return
			}

			if n == 0 {
				assert.True(t, sketch.IsEmpty())
			} else {
				assert.False(t, sketch.IsEmpty())
			}
			if n > 10 {
				assert.True(t, sketch.GetMaximumError() > 0)
			} else {
				assert.Equal(t, sketch.GetMaximumError(), int64(0))
			}
			assert.Equal(t, sketch.GetStreamLength(), int64(n))
		}
	})

	t.Run("String Frequency", func(t *testing.T) {
		nArr := []int{0, 1, 10, 100, 1000, 10000, 100000, 1000000}
		for _, n := range nArr {
			bytes, err := os.ReadFile(fmt.Sprintf("%s/frequent_string_n%d_cpp.sk", internal.CppPath, n))
			assert.NoError(t, err)
			sketch, err := NewFrequencyItemsSketchFromSlice[string](bytes, common.ItemSketchStringHasher{}, common.ItemSketchStringSerDe{})
			if err != nil {
				return
			}

			if n == 0 {
				assert.True(t, sketch.IsEmpty())
			} else {
				assert.False(t, sketch.IsEmpty())
			}
			if n > 10 {
				assert.True(t, sketch.GetMaximumError() > 0)
			} else {
				assert.Equal(t, sketch.GetMaximumError(), int64(0))
			}
			assert.Equal(t, sketch.GetStreamLength(), int64(n))
		}
	})

	t.Run("String utf8", func(t *testing.T) {
		bytes, err := os.ReadFile(fmt.Sprintf("%s/frequent_string_utf8_cpp.sk", internal.CppPath))
		assert.NoError(t, err)
		sketch, err := NewFrequencyItemsSketchFromSlice[string](bytes, common.ItemSketchStringHasher{}, common.ItemSketchStringSerDe{})
		if err != nil {
			return
		}
		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, sketch.GetMaximumError(), int64(0))
		assert.Equal(t, sketch.GetStreamLength(), int64(28))
		est, err := sketch.GetEstimate("абвгд")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(1))
		est, err = sketch.GetEstimate("еёжзи")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(2))
		est, err = sketch.GetEstimate("йклмн")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(3))
		est, err = sketch.GetEstimate("опрст")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(4))
		est, err = sketch.GetEstimate("уфхцч")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(5))
		est, err = sketch.GetEstimate("шщъыь")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(6))
		est, err = sketch.GetEstimate("эюя")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(7))
	})

	t.Run("String ascii", func(t *testing.T) {
		bytes, err := os.ReadFile(fmt.Sprintf("%s/frequent_string_ascii_cpp.sk", internal.CppPath))
		assert.NoError(t, err)
		sketch, err := NewFrequencyItemsSketchFromSlice[string](bytes, common.ItemSketchStringHasher{}, common.ItemSketchStringSerDe{})
		if err != nil {
			return
		}
		assert.False(t, sketch.IsEmpty())
		assert.Equal(t, sketch.GetMaximumError(), int64(0))
		assert.Equal(t, sketch.GetStreamLength(), int64(10))
		est, err := sketch.GetEstimate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(1))
		est, err = sketch.GetEstimate("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(2))
		est, err = sketch.GetEstimate("ccccccccccccccccccccccccccccc")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(3))
		est, err = sketch.GetEstimate("ddddddddddddddddddddddddddddd")
		assert.NoError(t, err)
		assert.Equal(t, est, int64(4))
	})
}
