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

package hll

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMisc(t *testing.T) {
	hll, err := NewHllSketch(10, TgtHllTypeHll4)
	assert.NoError(t, err)
	err = hll.Reset()
	assert.NoError(t, err)
	assert.Equal(t, serVer, hll.GetSerializationVersion())
}

func TestUpdateTypes(t *testing.T) {
	checkUpdateType(t, TgtHllTypeHll4)
	checkUpdateType(t, TgtHllTypeHll6)
	checkUpdateType(t, TgtHllTypeHll8)
}

func checkUpdateType(t *testing.T, tgtHllType TgtHllType) {
	hll, err := NewHllSketch(11, tgtHllType)
	assert.NoError(t, err)

	assert.NoError(t, hll.UpdateSlice(nil))
	assert.NoError(t, hll.UpdateSlice(make([]byte, 0)))
	assert.NoError(t, hll.UpdateSlice([]byte{1, 2, 3}))
	assert.NoError(t, hll.UpdateString(""))
	assert.NoError(t, hll.UpdateString("abc"))

	assert.NoError(t, hll.UpdateInt64(0))
	assert.NoError(t, hll.UpdateInt64(1))
	assert.NoError(t, hll.UpdateInt64(-1))

	assert.NoError(t, hll.UpdateUInt64(0))
	assert.NoError(t, hll.UpdateUInt64(1))
}

func TestCopies(t *testing.T) {
	checkCopy(t, 14, TgtHllTypeHll4)
	checkCopy(t, 8, TgtHllTypeHll6)
	checkCopy(t, 8, TgtHllTypeHll8)
}

func checkCopy(t *testing.T, lgK int, tgtHllType TgtHllType) {
	sk, err := NewHllSketch(lgK, tgtHllType)
	assert.NoError(t, err)
	for i := 0; i < 7; i++ {
		err := sk.UpdateInt64(int64(i))
		assert.NoError(t, err)
	}
	assert.Equal(t, curModeList, sk.GetCurMode())

	skCopy, err := sk.Copy()
	assert.NoError(t, err)
	assert.Equal(t, curModeList, skCopy.GetCurMode())

	impl1 := sk.(*hllSketchState).sketch
	impl2 := skCopy.(*hllSketchState).sketch

	assert.Equal(t, impl1.(*couponListImpl).couponCount, impl2.(*couponListImpl).couponCount)

	est1, err := impl1.GetEstimate()
	assert.NoError(t, err)
	est2, err := impl2.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, est1, est2)
	assert.False(t, impl1 == impl2)

	for i := 7; i < 24; i++ {
		err := sk.UpdateInt64(int64(i))
		assert.NoError(t, err)
	}

	assert.Equal(t, curModeSet, sk.GetCurMode())
	skCopy, err = sk.Copy()
	assert.NoError(t, err)
	assert.Equal(t, curModeSet, skCopy.GetCurMode())

	impl1 = sk.(*hllSketchState).sketch
	impl2 = skCopy.(*hllSketchState).sketch

	assert.Equal(t, impl1.(*couponHashSetImpl).couponCount, impl2.(*couponHashSetImpl).couponCount)
	est1, err = impl1.GetEstimate()
	assert.NoError(t, err)
	est2, err = impl2.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, est1, est2)
	assert.False(t, impl1 == impl2)

	u := 25
	if tgtHllType == TgtHllTypeHll4 {
		u = 100000
	}
	for i := 24; i < u; i++ {
		err := sk.UpdateInt64(int64(i))
		assert.NoError(t, err)
	}

	assert.Equal(t, curModeHll, sk.GetCurMode())
	skCopy, err = sk.Copy()
	assert.NoError(t, err)
	assert.Equal(t, curModeHll, skCopy.GetCurMode())

	impl1 = sk.(*hllSketchState).sketch
	impl2 = skCopy.(*hllSketchState).sketch

	est1, err = impl1.GetEstimate()
	assert.NoError(t, err)
	est2, err = impl2.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, est1, est2)
	assert.False(t, impl1 == impl2)
}

func TestCopyAs(t *testing.T) {
	checkCopyAs(t, TgtHllTypeHll4, TgtHllTypeHll4)
	checkCopyAs(t, TgtHllTypeHll4, TgtHllTypeHll6)
	checkCopyAs(t, TgtHllTypeHll4, TgtHllTypeHll8)
	checkCopyAs(t, TgtHllTypeHll6, TgtHllTypeHll4)
	checkCopyAs(t, TgtHllTypeHll6, TgtHllTypeHll6)
	checkCopyAs(t, TgtHllTypeHll6, TgtHllTypeHll8)
	checkCopyAs(t, TgtHllTypeHll8, TgtHllTypeHll4)
	checkCopyAs(t, TgtHllTypeHll8, TgtHllTypeHll6)
	checkCopyAs(t, TgtHllTypeHll8, TgtHllTypeHll8)
}

func checkCopyAs(t *testing.T, srcType TgtHllType, dstType TgtHllType) {
	var (
		lgK  = 8
		n1   = 7
		n2   = 24
		n3   = 1000
		base = 0
	)

	src, err := NewHllSketch(lgK, srcType)
	assert.NoError(t, err)
	for i := 0; i < n1; i++ {
		err := src.UpdateInt64(int64(i + base))
		assert.NoError(t, err)
	}
	dst, err := src.CopyAs(dstType)
	assert.NoError(t, err)
	srcEst, err := src.GetEstimate()
	assert.NoError(t, err)
	dstEst, err := dst.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, srcEst, dstEst)

	for i := n1; i < n2; i++ {
		err := src.UpdateInt64(int64(i + base))
		assert.NoError(t, err)
	}
	dst, err = src.CopyAs(dstType)
	assert.NoError(t, err)
	srcEst, err = src.GetEstimate()
	assert.NoError(t, err)
	dstEst, err = dst.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, srcEst, dstEst)

	for i := n2; i < n3; i++ {
		err := src.UpdateInt64(int64(i + base))
		assert.NoError(t, err)
	}
	dst, err = src.CopyAs(dstType)
	assert.NoError(t, err)
	srcEst, err = src.GetEstimate()
	assert.NoError(t, err)
	dstEst, err = dst.GetEstimate()
	assert.NoError(t, err)
	assert.Equal(t, srcEst, dstEst)
}

func TestNewHLLDataSketchUint(t *testing.T) {
	tgts := []TgtHllType{TgtHllTypeHll4, TgtHllTypeHll6, TgtHllTypeHll8}
	ns := []int{1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, tgt := range tgts {
		hll, err := NewHllSketch(11, tgt)
		assert.NoError(t, err)
		for _, n := range ns {
			for i := 0; i < n; i++ {
				err := hll.UpdateUInt64(uint64(i))
				assert.NoError(t, err)
			}
			est, err := hll.GetEstimate()
			assert.NoError(t, err)
			assert.InDelta(t, n, est, float64(n)*0.03)
		}
	}
}

func TestNewHLLDataSketchString(t *testing.T) {
	tgts := []TgtHllType{TgtHllTypeHll4, TgtHllTypeHll6, TgtHllTypeHll8}
	ns := []int{1, 10, 100, 1000, 10000, 100000, 1000000}
	for _, tgt := range tgts {
		hll, err := NewHllSketch(11, tgt)
		assert.NoError(t, err)
		for _, n := range ns {
			for i := 0; i < n; i++ {
				err := hll.UpdateString(strconv.Itoa(i))
				assert.NoError(t, err)
			}
			est, err := hll.GetEstimate()
			assert.NoError(t, err)
			assert.InDelta(t, n, est, float64(n)*0.03)
		}
	}
}

func TestHLLDataSketchT(b *testing.T) {
	hll, err := NewHllSketch(21, TgtHllTypeHll4)
	assert.NoError(b, err)
	for i := 0; i < 1000000; i++ {
		_ = hll.UpdateUInt64(uint64(i))
	}
	est, err := hll.GetEstimate()
	assert.NoError(b, err)
	assert.InDelta(b, 1000000, est, float64(1000000)*0.03)

}

func BenchmarkHLLDataSketch(b *testing.B) {
	const iter = 2_000_000

	// HLL uint64 BenchMark
	b.Run("lgK4 HLL4 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(4, TgtHllTypeHll4)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				_ = hll.UpdateUInt64(uint64(j))
			}
		}
	})
	b.Run("lgK16 HLL4 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(16, TgtHllTypeHll4)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				_ = hll.UpdateUInt64(uint64(j))
			}
		}
	})
	b.Run("lgK21 HLL4 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(21, TgtHllTypeHll4)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				_ = hll.UpdateUInt64(uint64(j))
			}
		}
	})

	b.Run("lgK4 HLL6 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(11, TgtHllTypeHll6)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				_ = hll.UpdateUInt64(uint64(j))
			}
		}
	})
	b.Run("lgK16 HLL6 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(16, TgtHllTypeHll6)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				_ = hll.UpdateUInt64(uint64(j))
			}
		}
	})
	b.Run("lgK21 HLL6 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(21, TgtHllTypeHll6)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				_ = hll.UpdateUInt64(uint64(j))
			}
		}
	})

	b.Run("lgK4 HLL8 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(11, TgtHllTypeHll8)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				_ = hll.UpdateUInt64(uint64(j))
			}
		}
	})
	b.Run("lgK16 HLL8 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(16, TgtHllTypeHll8)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				_ = hll.UpdateUInt64(uint64(j))
			}
		}
	})
	b.Run("lgK21 HLL8 uint", func(b *testing.B) {
		hll, _ := NewHllSketch(21, TgtHllTypeHll8)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				_ = hll.UpdateUInt64(uint64(j))
			}
		}
	})

	// HLL Slice BenchMark
	bs := make([]byte, 8)
	b.Run("lgK4 HLL4 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(11, TgtHllTypeHll4)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				binary.LittleEndian.PutUint64(bs, uint64(j))
				_ = hll.UpdateSlice(bs)
			}
		}
	})
	b.Run("lgK16 HLL4 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(16, TgtHllTypeHll4)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				binary.LittleEndian.PutUint64(bs, uint64(j))
				_ = hll.UpdateSlice(bs)
			}
		}
	})
	b.Run("lgK21 HLL4 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(21, TgtHllTypeHll4)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				binary.LittleEndian.PutUint64(bs, uint64(j))
				_ = hll.UpdateSlice(bs)
			}
		}
	})

	b.Run("lgK4 HLL6 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(11, TgtHllTypeHll6)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				binary.LittleEndian.PutUint64(bs, uint64(j))
				_ = hll.UpdateSlice(bs)
			}
		}
	})
	b.Run("lgK16 HLL6 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(16, TgtHllTypeHll6)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				binary.LittleEndian.PutUint64(bs, uint64(j))
				_ = hll.UpdateSlice(bs)
			}
		}
	})
	b.Run("lgK21 HLL6 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(21, TgtHllTypeHll6)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				binary.LittleEndian.PutUint64(bs, uint64(j))
				_ = hll.UpdateSlice(bs)
			}
		}
	})

	b.Run("lgK4 HLL8 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(11, TgtHllTypeHll8)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				binary.LittleEndian.PutUint64(bs, uint64(j))
				_ = hll.UpdateSlice(bs)
			}
		}
	})
	b.Run("lgK16 HLL8 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(16, TgtHllTypeHll8)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				binary.LittleEndian.PutUint64(bs, uint64(j))
				_ = hll.UpdateSlice(bs)
			}
		}
	})
	b.Run("lgK21 HLL8 slice", func(b *testing.B) {
		hll, _ := NewHllSketch(21, TgtHllTypeHll8)
		for i := 0; i < b.N; i++ {
			for j := 0; j < iter; j++ {
				binary.LittleEndian.PutUint64(bs, uint64(j))
				_ = hll.UpdateSlice(bs)
			}
		}
	})

	// Union benchmark
	b.Run("lgK4 HLL8 union", func(b *testing.B) {
		hll, _ := NewHllSketch(4, TgtHllTypeHll8)
		union, _ := NewUnion(4)
		for i := 0; i < b.N; i++ {
			_ = hll.UpdateSlice(bs)
			_ = union.UpdateSketch(hll)
		}
	})
	b.Run("lgK16 HLL8 union", func(b *testing.B) {
		hll, _ := NewHllSketch(16, TgtHllTypeHll8)
		union, _ := NewUnion(16)
		for i := 0; i < b.N; i++ {
			_ = hll.UpdateSlice(bs)
			_ = union.UpdateSketch(hll)
		}
	})
	b.Run("lgK21 HLL8 union", func(b *testing.B) {
		hll, _ := NewHllSketch(21, TgtHllTypeHll8)
		union, _ := NewUnion(21)
		for i := 0; i < b.N; i++ {
			_ = hll.UpdateSlice(bs)
			_ = union.UpdateSketch(hll)
		}
	})

}

func BenchmarkHLLDataSketchWithEstimate(b *testing.B) {
	hll, err := NewHllSketch(11, TgtHllTypeHll8)
	assert.NoError(b, err)
	for i := 0; i < b.N; i++ {
		_ = hll.UpdateString(strconv.Itoa(i))
	}
	est, err := hll.GetEstimate()
	assert.NoError(b, err)
	estimate := int64(est)
	fmt.Printf("Estimated cardinality: %d (true: %d) (error: %f)\n ", estimate, b.N, float64(int64(b.N)-estimate)*100/float64(b.N))
}

// Test the hard case for (shiftedNewValue >= AUX_TOKEN) && (rawStoredOldNibble = AUX_TOKEN)
func TestHLL4RawStoredOldNibbleAndShiftedNewValueAuxToken(t *testing.T) {
	hll, _ := NewHllSketch(21, TgtHllTypeHll4)
	for i := uint64(0); i < 29197004; i++ {
		err := hll.UpdateUInt64(i)
		assert.NoError(t, err)
	}
	err := hll.UpdateUInt64(29197004)
	assert.NoError(t, err)
}

func BenchmarkHLLMerge(b *testing.B) {
	hll1, err := NewHllSketch(11, TgtHllTypeHll8)
	for i := uint64(0); i < 29197004; i++ {
		err = hll1.UpdateUInt64(i)
		assert.NoError(b, err)
	}
	u, _ := NewUnion(11)

	b.Run("merge", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			u.UpdateSketch(hll1)
		}
	})
}
