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
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/datasketches-go/internal"
	"github.com/stretchr/testify/assert"
)

func TestFrequentItemsStringSerial(t *testing.T) {
	sketch, err := NewLongsSketchWithMaxMapSize(8)
	assert.NoError(t, err)
	sketch2, err := NewLongsSketchWithMaxMapSize(128)
	assert.NoError(t, err)
	sketch.UpdateMany(10, 100)
	sketch.UpdateMany(10, 100)
	sketch.UpdateMany(15, 3443)
	sketch.UpdateMany(1000001, 1010230)
	sketch.UpdateMany(1000002, 1010230)

	ser, err := sketch.ToString()
	assert.NoError(t, err)
	newSk0, err := NewLongsSketchFromString(ser)
	assert.NoError(t, err)
	newSer0, err := newSk0.ToString()
	assert.NoError(t, err)
	assert.Equal(t, ser, newSer0)
	assert.Equal(t, sketch.GetMaximumMapCapacity(), newSk0.GetMaximumMapCapacity())
	assert.Equal(t, sketch.GetCurrentMapCapacity(), newSk0.GetCurrentMapCapacity())

	sketch2.UpdateMany(190, 12902390)
	sketch2.UpdateMany(191, 12902390)
	sketch2.UpdateMany(192, 12902390)
	sketch2.UpdateMany(193, 12902390)
	sketch2.UpdateMany(194, 12902390)
	sketch2.UpdateMany(195, 12902390)
	sketch2.UpdateMany(196, 12902390)
	sketch2.UpdateMany(197, 12902390)
	sketch2.UpdateMany(198, 12902390)
	sketch2.UpdateMany(199, 12902390)
	sketch2.UpdateMany(200, 12902390)
	sketch2.UpdateMany(201, 12902390)
	sketch2.UpdateMany(202, 12902390)
	sketch2.UpdateMany(203, 12902390)
	sketch2.UpdateMany(204, 12902390)
	sketch2.UpdateMany(205, 12902390)
	sketch2.UpdateMany(206, 12902390)
	sketch2.UpdateMany(207, 12902390)
	sketch2.UpdateMany(208, 12902390)

	s2, err := sketch2.ToString()
	assert.NoError(t, err)
	newSk2, err := NewLongsSketchFromString(s2)
	assert.NoError(t, err)
	newS2, err := newSk2.ToString()
	assert.NoError(t, err)
	assert.Equal(t, s2, newS2)
	assert.Equal(t, sketch2.GetMaximumMapCapacity(), newSk2.GetMaximumMapCapacity())
	assert.Equal(t, sketch2.GetCurrentMapCapacity(), newSk2.GetCurrentMapCapacity())
	assert.Equal(t, sketch2.GetStreamLength(), newSk2.GetStreamLength())

	mergedSketch, err := sketch.Merge(sketch2)
	assert.NoError(t, err)
	ms, err := mergedSketch.ToString()
	assert.NoError(t, err)
	newMs, err := NewLongsSketchFromString(ms)
	assert.NoError(t, err)
	newSMs, err := newMs.ToString()
	assert.NoError(t, err)
	assert.Equal(t, ms, newSMs)
	assert.Equal(t, mergedSketch.GetMaximumMapCapacity(), newMs.GetMaximumMapCapacity())
	assert.Equal(t, mergedSketch.GetCurrentMapCapacity(), newMs.GetCurrentMapCapacity())
	assert.Equal(t, mergedSketch.GetStreamLength(), newMs.GetStreamLength())
}

func TestFrequentItemsByteSerial(t *testing.T) {
	sketch, err := NewLongsSketchWithMaxMapSize(16)
	assert.NoError(t, err)
	byteArray0 := sketch.ToSlice()
	newSk0, err := NewLongsSketchFromSlice(byteArray0)
	assert.NoError(t, err)
	str0, err := sketch.ToString()
	assert.NoError(t, err)
	newStr0, err := newSk0.ToString()
	assert.NoError(t, err)
	assert.Equal(t, str0, newStr0)

	sketch2, err := NewLongsSketchWithMaxMapSize(128)
	assert.NoError(t, err)
	sketch.UpdateMany(10, 100)
	sketch.UpdateMany(10, 100)
	sketch.UpdateMany(15, 3443)
	sketch.UpdateMany(1000001, 1010230)
	sketch.UpdateMany(1000002, 1010230)

	byteArray1 := sketch.ToSlice()
	newSk1, err := NewLongsSketchFromSlice(byteArray1)
	assert.NoError(t, err)
	str1, err := sketch.ToString()
	newStr1, err := newSk1.ToString()
	assert.NoError(t, err)
	assert.Equal(t, str1, newStr1)
	assert.Equal(t, sketch.GetMaximumMapCapacity(), newSk1.GetMaximumMapCapacity())
	assert.Equal(t, sketch.GetCurrentMapCapacity(), newSk1.GetCurrentMapCapacity())

	sketch2.UpdateMany(190, 12902390)
	sketch2.UpdateMany(191, 12902390)
	sketch2.UpdateMany(192, 12902390)
	sketch2.UpdateMany(193, 12902390)
	sketch2.UpdateMany(194, 12902390)
	sketch2.UpdateMany(195, 12902390)
	sketch2.UpdateMany(196, 12902390)
	sketch2.UpdateMany(197, 12902390)
	sketch2.UpdateMany(198, 12902390)
	sketch2.UpdateMany(199, 12902390)
	sketch2.UpdateMany(200, 12902390)
	sketch2.UpdateMany(201, 12902390)
	sketch2.UpdateMany(202, 12902390)
	sketch2.UpdateMany(203, 12902390)
	sketch2.UpdateMany(204, 12902390)
	sketch2.UpdateMany(205, 12902390)
	sketch2.UpdateMany(206, 12902390)
	sketch2.UpdateMany(207, 12902390)
	sketch2.UpdateMany(208, 12902390)

	byteArray2 := sketch2.ToSlice()
	newSk2, err := NewLongsSketchFromSlice(byteArray2)
	assert.NoError(t, err)
	str2, err := sketch2.ToString()
	assert.NoError(t, err)
	newStr2, err := newSk2.ToString()
	assert.NoError(t, err)
	assert.Equal(t, str2, newStr2)
	assert.Equal(t, sketch2.GetMaximumMapCapacity(), newSk2.GetMaximumMapCapacity())
	assert.Equal(t, sketch2.GetCurrentMapCapacity(), newSk2.GetCurrentMapCapacity())
	assert.Equal(t, sketch2.GetStreamLength(), newSk2.GetStreamLength())

	mergedSketch, err := sketch.Merge(sketch2)
	assert.NoError(t, err)
	byteArray3 := mergedSketch.ToSlice()
	newSk3, err := NewLongsSketchFromSlice(byteArray3)
	assert.NoError(t, err)
	str3, err := mergedSketch.ToString()
	assert.NoError(t, err)
	newStr3, err := newSk3.ToString()
	assert.NoError(t, err)
	assert.Equal(t, str3, newStr3)
	assert.Equal(t, mergedSketch.GetMaximumMapCapacity(), newSk3.GetMaximumMapCapacity())
	assert.Equal(t, mergedSketch.GetCurrentMapCapacity(), newSk3.GetCurrentMapCapacity())
	assert.Equal(t, mergedSketch.GetStreamLength(), newSk3.GetStreamLength())
}

func TestFrequentItemsByteResetAndEmptySerial(t *testing.T) {
	sketch, err := NewLongsSketchWithMaxMapSize(16)
	assert.NoError(t, err)
	sketch.UpdateMany(10, 100)
	sketch.UpdateMany(10, 100)
	sketch.UpdateMany(15, 3443)
	sketch.UpdateMany(1000001, 1010230)
	sketch.UpdateMany(1000002, 1010230)
	sketch.Reset()

	byteArray0 := sketch.ToSlice()
	newSk0, err := NewLongsSketchFromSlice(byteArray0)
	assert.NoError(t, err)
	str0, err := sketch.ToString()
	assert.NoError(t, err)
	newStr0, err := newSk0.ToString()
	assert.NoError(t, err)
	assert.Equal(t, str0, newStr0)
	assert.Equal(t, sketch.GetMaximumMapCapacity(), newSk0.GetMaximumMapCapacity())
	assert.Equal(t, sketch.GetCurrentMapCapacity(), newSk0.GetCurrentMapCapacity())
}

func TestFreqLongSliceSerDe(t *testing.T) {
	minSize := 1 << _LG_MIN_MAP_SIZE
	sk1, err := NewLongsSketchWithMaxMapSize(minSize)
	assert.NoError(t, err)
	sk1.UpdateMany(10, 100)
	sk1.UpdateMany(10, 100)
	sk1.UpdateMany(15, 3443)
	sk1.UpdateMany(1000001, 1010230)
	sk1.UpdateMany(1000002, 1010230)

	byteArray0 := sk1.ToSlice()
	sk2, err := NewLongsSketchFromSlice(byteArray0)
	assert.NoError(t, err)

	checkEquality(t, sk1, sk2)
}

func TestFreqLongStringSerDe(t *testing.T) {
	minSize := 1 << _LG_MIN_MAP_SIZE
	sk1, err := NewLongsSketchWithMaxMapSize(minSize)
	assert.NoError(t, err)
	sk1.UpdateMany(10, 100)
	sk1.UpdateMany(10, 100)
	sk1.UpdateMany(15, 3443)
	sk1.UpdateMany(1000001, 1010230)
	sk1.UpdateMany(1000002, 1010230)

	str1, err := sk1.ToString()
	assert.NoError(t, err)
	sk2, err := NewLongsSketchFromString(str1)
	assert.NoError(t, err)

	checkEquality(t, sk1, sk2)
}

func checkEquality(t *testing.T, sk1, sk2 *LongsSketch) {
	assert.Equal(t, sk1.GetNumActiveItems(), sk2.GetNumActiveItems())
	assert.Equal(t, sk1.GetCurrentMapCapacity(), sk2.GetCurrentMapCapacity())
	assert.Equal(t, sk1.GetMaximumError(), sk2.GetMaximumError())
	assert.Equal(t, sk1.GetMaximumMapCapacity(), sk2.GetMaximumMapCapacity())
	assert.Equal(t, sk1.GetStorageBytes(), sk2.GetStorageBytes())
	assert.Equal(t, sk1.GetStreamLength(), sk2.GetStreamLength())
	assert.Equal(t, sk1.IsEmpty(), sk2.IsEmpty())

	NFN := ErrorTypeEnum.NoFalseNegatives
	NFP := ErrorTypeEnum.NoFalsePositives

	rowArr1, err := sk1.GetFrequentItems(NFN)
	assert.NoError(t, err)
	rowArr2, err := sk2.GetFrequentItems(NFN)
	assert.NoError(t, err)
	assert.Equal(t, len(rowArr1), len(rowArr2))
	for i := 0; i < len(rowArr1); i++ {
		s1 := rowArr1[i].String()
		s2 := rowArr2[i].String()
		assert.Equal(t, s1, s2)
	}

	rowArr1, err = sk1.GetFrequentItems(NFP)
	assert.NoError(t, err)
	rowArr2, err = sk2.GetFrequentItems(NFP)
	assert.NoError(t, err)
	assert.Equal(t, len(rowArr1), len(rowArr2))
	for i := 0; i < len(rowArr1); i++ {
		s1 := rowArr1[i].String()
		s2 := rowArr2[i].String()
		assert.Equal(t, s1, s2)
	}
}

func TestFreqLongSliceSerDeError(t *testing.T) {
	minSize := 1 << _LG_MIN_MAP_SIZE
	sk1, err := NewLongsSketchWithMaxMapSize(minSize)
	assert.NoError(t, err)
	sk1.Update(1)

	byteArray0 := sk1.ToSlice()
	pre0 := binary.LittleEndian.Uint64(byteArray0)

	tryBadMem(t, byteArray0, _PREAMBLE_LONGS_BYTE, 2) //Corrupt
	binary.LittleEndian.PutUint64(byteArray0, pre0)

	tryBadMem(t, byteArray0, _SER_VER_BYTE, 2) //Corrupt
	binary.LittleEndian.PutUint64(byteArray0, pre0)

	tryBadMem(t, byteArray0, _FAMILY_BYTE, 2) //Corrupt
	binary.LittleEndian.PutUint64(byteArray0, pre0)

	tryBadMem(t, byteArray0, _FLAGS_BYTE, 4) //Corrupt
	binary.LittleEndian.PutUint64(byteArray0, pre0)

}

func tryBadMem(t *testing.T, mem []byte, byteOffset, byteValue int) {
	binary.LittleEndian.PutUint64(mem[byteOffset:], uint64(byteValue))
	_, err := NewLongsSketchFromSlice(mem)
	assert.Error(t, err)
}

func TestFreqLongStringSerDeError(t *testing.T) {
	// sk1, err := NewLongsSketchWithMaxMapSize(8)
	// str1 := sk1.ToString()
	// correct   = "1,10,2,4,0,0,0,4,";

	tryBadString(t, "2,10,2,4,0,0,0,4,")   //bad SerVer of 2
	tryBadString(t, "1,10,2,0,0,0,0,4,")   //bad empty of 0
	tryBadString(t, "1,10,2,4,0,0,0,4,0,") //one extra
}

func tryBadString(t *testing.T, badString string) {
	_, err := NewLongsSketchFromString(badString)
	assert.Error(t, err)
}

func TestFreqLongs(t *testing.T) {
	numSketches := 1
	n := 2222
	errorTolerance := 1.0 / 100

	sketches := make([]*LongsSketch, numSketches)
	for h := 0; h < numSketches; h++ {
		sketches[h], _ = newFrequencySketch(errorTolerance)
	}

	prob := .001
	for i := 0; i < n; i++ {
		item := randomGeometricDist(prob) + 1
		for h := 0; h < numSketches; h++ {
			sketches[h].Update(item)
		}
	}

	for h := 0; h < numSketches; h++ {
		threshold := sketches[h].GetMaximumError()
		rows, err := sketches[h].GetFrequentItems(ErrorTypeEnum.NoFalseNegatives)
		assert.NoError(t, err)
		for i := 0; i < len(rows); i++ {
			assert.True(t, rows[i].GetUpperBound() > threshold)
		}

		rows, err = sketches[h].GetFrequentItems(ErrorTypeEnum.NoFalsePositives)
		assert.NoError(t, err)
		assert.Equal(t, len(rows), 0)
		for i := 0; i < len(rows); i++ {
			assert.True(t, rows[i].GetLowerBound() > threshold)
		}

		rows, err = sketches[h].GetFrequentItems(ErrorTypeEnum.NoFalseNegatives)
	}
}

func newFrequencySketch(eps float64) (*LongsSketch, error) {
	maxMapSize := internal.CeilPowerOf2(int(1.0 / (eps * reversePurgeLongHashMapLoadFactor)))
	return NewLongsSketchWithMaxMapSize(maxMapSize)
}

func TestUpdateOneTime(t *testing.T) {
	size := 100
	errorTolerance := 1.0 / float64(size)
	//delta := .01
	numSketches := 1
	for h := 0; h < numSketches; h++ {
		sketch, _ := newFrequencySketch(errorTolerance)
		ub, err := sketch.GetUpperBound(13)
		assert.NoError(t, err)
		assert.Equal(t, ub, int64(0))
		lb, err := sketch.GetLowerBound(13)
		assert.NoError(t, err)
		assert.Equal(t, lb, int64(0))
		assert.Equal(t, sketch.GetMaximumError(), int64(0))
		est, err := sketch.GetEstimate(13)
		assert.NoError(t, err)
		assert.Equal(t, est, int64(0))
		sketch.Update(13)
		// assert.Equal(t, sketch.GetEstimate(13), 1)
	}
}

func TestGetInstanceSlice(t *testing.T) {
	sl := make([]byte, 4)
	_, err := NewLongsSketchFromSlice(sl)
	assert.Error(t, err)
}

func TestGetInstanceString(t *testing.T) {
	_, err := NewLongsSketchFromString("")
	assert.Error(t, err)
}

func TestUpdateNegative(t *testing.T) {
	minSize := 1 << _LG_MIN_MAP_SIZE
	fls, err := NewLongsSketchWithMaxMapSize(minSize)
	assert.NoError(t, err)
	err = fls.UpdateMany(1, 0)
	assert.NoError(t, err)
	err = fls.UpdateMany(1, -1)
	assert.Error(t, err)
}

func TestGetFrequentItems1(t *testing.T) {
	minSize := 1 << _LG_MIN_MAP_SIZE
	fls, err := NewLongsSketchWithMaxMapSize(minSize)
	assert.NoError(t, err)
	fls.Update(1)
	rowArr, err := fls.GetFrequentItems(ErrorTypeEnum.NoFalsePositives)
	assert.NoError(t, err)
	assert.NotEmpty(t, rowArr)
	row := rowArr[0]
	assert.Equal(t, row.est, int64(1))
	assert.Equal(t, row.item, int64(1))
	assert.Equal(t, row.lb, int64(1))
	assert.Equal(t, row.ub, int64(1))
	nRow := newRow(row.item, row.est+1, row.ub, row.lb)
	assert.NotEqual(t, row, nRow)
	nRow = newRow(row.item, row.est, row.ub, row.lb)
	assert.Equal(t, row, nRow)

}

func TestGetStorageByes(t *testing.T) {
	minSize := 1 << _LG_MIN_MAP_SIZE
	fls, err := NewLongsSketchWithMaxMapSize(minSize)
	assert.NoError(t, err)
	sl := fls.ToSlice()
	assert.Equal(t, len(sl), fls.GetStorageBytes())
	err = fls.Update(1)
	assert.NoError(t, err)
	sl = fls.ToSlice()
	assert.Equal(t, len(sl), fls.GetStorageBytes())
}

func TestDeSerFromString(t *testing.T) {
	minSize := 1 << _LG_MIN_MAP_SIZE
	fls, err := NewLongsSketchWithMaxMapSize(minSize)
	assert.NoError(t, err)
	str, err := fls.ToString()
	fmt.Println(str)
	assert.NoError(t, err)
	fls.Update(1)
	str, err = fls.ToString()
	assert.NoError(t, err)
	fmt.Println(str)
}

func TestMerge(t *testing.T) {
	minSize := 1 << _LG_MIN_MAP_SIZE
	fls1, err := NewLongsSketchWithMaxMapSize(minSize)
	assert.NoError(t, err)
	var fls2 *LongsSketch
	fls2 = nil
	fle, err := fls1.Merge(fls2)
	assert.NoError(t, err)
	assert.True(t, fle.IsEmpty())

	fls2, err = NewLongsSketchWithMaxMapSize(minSize)
	assert.NoError(t, err)
	fle, err = fls1.Merge(fls2)
	assert.NoError(t, err)
}

func TestSortItems(t *testing.T) {
	numSketches := 1
	n := 2222
	errorTolerance := 1.0 / 100
	sketchSize := internal.CeilPowerOf2(int(1.0 / (errorTolerance * reversePurgeLongHashMapLoadFactor)))
	fmt.Printf("sketchSize: %d\n", sketchSize)

	sketches := make([]*LongsSketch, numSketches)
	for h := 0; h < numSketches; h++ {
		sketches[h], _ = newFrequencySketch(float64(sketchSize))
	}

	prob := .001
	for i := 0; i < n; i++ {
		item := randomGeometricDist(prob) + 1
		for h := 0; h < numSketches; h++ {
			err := sketches[h].Update(item)
			assert.NoError(t, err)
		}
	}

	for h := 0; h < numSketches; h++ {
		threshold := sketches[h].GetMaximumError()
		rows, err := sketches[h].GetFrequentItems(ErrorTypeEnum.NoFalseNegatives)
		assert.NoError(t, err)
		for i := 0; i < len(rows); i++ {
			assert.True(t, rows[i].GetUpperBound() > threshold)
		}
		first := rows[0]
		anItem := first.item
		anEst := first.est
		aLB := first.lb
		s := first.String()
		fmt.Println(s)
		assert.True(t, anEst >= 0)
		assert.True(t, aLB >= 0)
		assert.Equal(t, anItem, anItem) //dummy test

	}
}

func TestGetAndCheckPreLongs(t *testing.T) {
	byteArr := make([]byte, 8)
	byteArr[0] = 2
	_, err := checkPreambleSize(byteArr)
	assert.Error(t, err)
}

func TestToString1(t *testing.T) {
	size := 1 << _LG_MIN_MAP_SIZE
	printSketch(t, size, []int64{1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5})
	printSketch(t, size, []int64{5, 4, 3, 2, 1, 1, 1, 1, 1, 1, 1})
}

func printSketch(t *testing.T, size int, items []int64) {
	var sb strings.Builder
	fls, err := NewLongsSketchWithMaxMapSize(size)
	assert.NoError(t, err)
	for i := 0; i < len(items); i++ {
		fls.UpdateMany(int64(i+1), items[i])
	}
	sb.WriteString(fmt.Sprintf("Sketch Size: %d\n", size))
	sb.WriteString(fls.String())
	fmt.Println(sb.String())
	printRows(t, fls, ErrorTypeEnum.NoFalseNegatives)
	fmt.Println("")
	printRows(t, fls, ErrorTypeEnum.NoFalsePositives)
	fmt.Println("")
}

func printRows(t *testing.T, fls *LongsSketch, errorType errorType) {
	rows, err := fls.GetFrequentItems(errorType)
	assert.NoError(t, err)
	fmt.Println(errorType.Name)
	fmt.Printf("  %20s%20s%20s %s", "Est", "UB", "LB", "Item")
	fmt.Print("\n")
	for i := 0; i < len(rows); i++ {
		row := rows[i]
		s2 := row.String()
		fmt.Println(s2)
	}
	if len(rows) > 0 { //check equals null case
		var nullRow *Row
		assert.NotEqual(t, rows[0], nullRow)
	}
}

func TestStringDeserEmptyNotCorrupt(t *testing.T) {
	size := 1 << _LG_MIN_MAP_SIZE
	thresh := (size * 3) / 4
	format := "%6d%10s%s"
	fls, err := NewLongsSketchWithMaxMapSize(size)
	assert.NoError(t, err)
	fmt.Printf("Sketch Size: %d\n", size)
	for i := 0; i <= thresh; i++ {
		err := fls.UpdateMany(int64(i+1), 1)
		assert.NoError(t, err)
		s, err := fls.ToString()
		assert.NoError(t, err)
		fmt.Printf("SER   "+format+"\n", (i + 1), fmt.Sprintf("%t : ", fls.IsEmpty()), s)
		fls2, err := NewLongsSketchFromString(s)
		assert.NoError(t, err)
		fmt.Printf("DESER "+format+"\n", (i + 1), fmt.Sprintf("%t : ", fls2.IsEmpty()), s)
	}
}

func TestStringDeserEmptyCorrupt(t *testing.T) {
	var s strings.Builder
	s.WriteString("1,")  //serVer
	s.WriteString("10,") //FamID
	s.WriteString("3,")  //lgMaxMapSz
	s.WriteString("0,")  //Empty Flag = false ... corrupted, should be true
	s.WriteString("7,")  //stream Len so far
	s.WriteString("1,")  //error offset
	s.WriteString("0,")  //numActive ...conflict with empty
	s.WriteString("8,")  //curMapLen
	_, err := NewLongsSketchFromString(s.String())
	assert.Error(t, err)
}

func TestGetEpsilon(t *testing.T) {
	eps, err := GetEpsilonLongsSketch(1024)
	assert.NoError(t, err)
	assert.Equal(t, eps, 3.5/1024)

	_, err = GetEpsilonLongsSketch(1000)
	assert.Error(t, err)
}

func TestGetAprioriError(t *testing.T) {
	eps := 3.5 / 1024
	apr, err := GetAprioriErrorLongsSketch(1024, 10_000)
	assert.NoError(t, err)
	assert.Equal(t, apr, eps*10_000)
}

func TestLongsSketch_frequencies(t *testing.T) {
	sketch, err := NewLongsSketchWithMaxMapSize(1 << _LG_MIN_MAP_SIZE)
	assert.NoError(t, err)

	// Add some items with different frequencies
	err = sketch.UpdateMany(100, 5)
	assert.NoError(t, err)
	err = sketch.UpdateMany(200, 3)
	assert.NoError(t, err)
	err = sketch.Update(300)
	assert.NoError(t, err)
	err = sketch.UpdateMany(400, 2)
	assert.NoError(t, err)

	// Test existing items
	testItems := []int64{100, 200, 300, 400}
	for _, item := range testItems {
		expectedEst, err := sketch.GetEstimate(item)
		assert.NoError(t, err)
		expectedLower, err := sketch.GetLowerBound(item)
		assert.NoError(t, err)
		expectedUpper, err := sketch.GetUpperBound(item)
		assert.NoError(t, err)

		est, lower, upper, err := sketch.frequencies(item)
		assert.NoError(t, err)

		assert.Equal(t, expectedEst, est, "Estimate mismatch for item %d", item)
		assert.Equal(t, expectedLower, lower, "Lower bound mismatch for item %d", item)
		assert.Equal(t, expectedUpper, upper, "Upper bound mismatch for item %d", item)
	}

	// Test non-existing item
	nonExistingItem := int64(999)
	expectedEst, err := sketch.GetEstimate(nonExistingItem)
	assert.NoError(t, err)
	expectedLower, err := sketch.GetLowerBound(nonExistingItem)
	assert.NoError(t, err)
	expectedUpper, err := sketch.GetUpperBound(nonExistingItem)
	assert.NoError(t, err)

	est, lower, upper, err := sketch.frequencies(nonExistingItem)
	assert.NoError(t, err)

	assert.Equal(t, expectedEst, est, "Estimate mismatch for non-existing item")
	assert.Equal(t, expectedLower, lower, "Lower bound mismatch for non-existing item")
	assert.Equal(t, expectedUpper, upper, "Upper bound mismatch for non-existing item")
}

func BenchmarkLongSketch(b *testing.B) {
	sketch, err := NewLongsSketch(128, 8)
	assert.NoError(b, err)
	for i := 0; i < b.N; i++ {
		sketch.Update(int64(i))
	}
}
