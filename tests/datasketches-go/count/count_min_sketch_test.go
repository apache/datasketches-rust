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

package count

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_CountMinSketch(t *testing.T) {
	seed := int64(1234567)
	t.Run("CM init - throws", func(t *testing.T) {
		cms, err := NewCountMinSketch(5, 1, seed)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "using fewer than 3 buckets incurs relative error greater than 1.0")
		assert.Nil(t, cms)

		cms, err = NewCountMinSketch(4, 268435456, seed)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "these parameters generate a sketch that exceeds 2^30 elements")
		assert.Nil(t, cms)
	})

	t.Run("CM Init", func(t *testing.T) {
		numHashes := int8(3)
		numBuckets := int32(5)
		cms, err := NewCountMinSketch(numHashes, numBuckets, seed)
		assert.NoError(t, err)

		assert.Equal(t, numHashes, cms.GetNumHashes())
		assert.Equal(t, numBuckets, cms.GetNumBuckets())
		assert.Equal(t, seed, cms.GetSeed())
		assert.True(t, cms.isEmpty())
	})

	t.Run("CM parameter suggestion", func(t *testing.T) {
		var (
			numBuckets int32
			numHashes  int8
			err        error
		)

		// Bucket suggestions
		numBuckets, err = SuggestNumBuckets(-1.0)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "relative error must be greater than 0.0")
		assert.Equal(t, int32(0), numBuckets)

		numBuckets, err = SuggestNumBuckets(0.2)
		assert.NoError(t, err)
		assert.Equal(t, int32(14), numBuckets)

		numBuckets, err = SuggestNumBuckets(0.1)
		assert.NoError(t, err)
		assert.Equal(t, int32(28), numBuckets)

		numBuckets, err = SuggestNumBuckets(0.05)
		assert.NoError(t, err)
		assert.Equal(t, int32(55), numBuckets)

		numBuckets, err = SuggestNumBuckets(0.01)
		assert.NoError(t, err)
		assert.Equal(t, int32(272), numBuckets)

		// Check that the sketch get_epsilon acts inversely to suggest_num_buckets
		numHashes = int8(3)
		cms, err := NewCountMinSketch(numHashes, int32(14), seed)
		assert.NoError(t, err)
		assert.Less(t, cms.GetRelativeError(), 0.2)

		cms, err = NewCountMinSketch(numHashes, int32(28), seed)
		assert.NoError(t, err)
		assert.Less(t, cms.GetRelativeError(), 0.1)

		cms, err = NewCountMinSketch(numHashes, int32(55), seed)
		assert.NoError(t, err)
		assert.Less(t, cms.GetRelativeError(), 0.05)

		cms, err = NewCountMinSketch(numHashes, int32(272), seed)
		assert.NoError(t, err)
		assert.Less(t, cms.GetRelativeError(), 0.01)

		// Hash suggestion
		numHashes, err = SuggestNumHashes(-1.0)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "confidence must be between 0 and 1.0")
		assert.Equal(t, int8(0), numHashes)

		numHashes, err = SuggestNumHashes(10.0)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "confidence must be between 0 and 1.0")
		assert.Equal(t, int8(0), numHashes)

		numHashes, err = SuggestNumHashes(0.682689492)
		assert.NoError(t, err)
		assert.Equal(t, int8(2), numHashes)

		numHashes, err = SuggestNumHashes(0.954499736)
		assert.NoError(t, err)
		assert.Equal(t, int8(4), numHashes)

		numHashes, err = SuggestNumHashes(0.997300204)
		assert.NoError(t, err)
		assert.Equal(t, int8(6), numHashes)
	})

	t.Run("CM one update: uint64_t", func(t *testing.T) {
		numHashes := int8(3)
		numBuckets := int32(5)
		insertedWeights := int64(0)
		cms, err := NewCountMinSketch(numHashes, numBuckets, seed)
		assert.NoError(t, err)
		x := "x"

		assert.True(t, cms.isEmpty())
		estimate := cms.GetEstimateString(x)
		assert.Equal(t, int64(0), estimate) // no items in sketch so estimate should be zero

		err = cms.UpdateString(x, int64(1))
		assert.NoError(t, err)
		assert.False(t, cms.isEmpty())
		insertedWeights += 1
		estimate = cms.GetEstimateString(x)
		assert.Equal(t, insertedWeights, estimate)

		weight := int64(9)
		insertedWeights += 9
		err = cms.UpdateString(x, weight)
		assert.NoError(t, err)

		estimate = cms.GetEstimateString(x)
		assert.Equal(t, insertedWeights, estimate)
	})

	t.Run("CM frequency cancellation", func(t *testing.T) {
		cms, err := NewCountMinSketch(int8(1), int32(5), seed)
		assert.NoError(t, err)
		err = cms.UpdateString("x", 1)
		assert.NoError(t, err)
		err = cms.UpdateString("y", -1)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), cms.GetTotalWeight())
		assert.Equal(t, int64(1), cms.GetEstimateString("x"))
		assert.Equal(t, int64(-1), cms.GetEstimateString("y"))
	})

	t.Run("CM frequency estimates", func(t *testing.T) {
		numItems := 10
		data := make([]uint64, numItems)
		frequencies := make([]int64, numItems)

		// Populate data slices
		for i := range numItems {
			data[i] = uint64(i)
			frequencies[i] = int64(uint64(1) << (uint64(numItems) - uint64(i)))
		}

		relativeError := 0.1
		confidence := 0.99
		numBuckets, err := SuggestNumBuckets(relativeError)
		assert.NoError(t, err)
		numHashes, err := SuggestNumHashes(confidence)
		assert.NoError(t, err)

		cms, err := NewCountMinSketch(numHashes, numBuckets, seed)
		assert.NoError(t, err)
		for i := range numItems {
			value := data[i]
			freq := frequencies[i]
			err = cms.UpdateUint64(value, freq)
			assert.NoError(t, err)
		}

		for _, d := range data {
			est := cms.GetEstimateUint64(d)
			upp := cms.GetUpperBoundUint64(d)
			low := cms.GetLowerBoundUint64(d)
			assert.LessOrEqual(t, est, upp)
			assert.GreaterOrEqual(t, est, low)
		}
	})

	t.Run("CM merge - reject", func(t *testing.T) {
		relativeError := 0.25
		confidence := 0.9
		numBuckets, err := SuggestNumBuckets(relativeError)
		assert.NoError(t, err)
		numHashes, err := SuggestNumHashes(confidence)
		assert.NoError(t, err)

		cms, err := NewCountMinSketch(numHashes, numBuckets, seed)
		assert.NoError(t, err)
		err = cms.Merge(cms)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "cannot merge sketch with itself")

		s1, err := NewCountMinSketch(numHashes+1, numBuckets, seed)
		assert.NoError(t, err)
		err = cms.Merge(s1)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "sketches are incompatible")

		s2, err := NewCountMinSketch(numHashes, numBuckets+1, seed)
		assert.NoError(t, err)
		err = cms.Merge(s2)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "sketches are incompatible")

		s3, err := NewCountMinSketch(numHashes, numBuckets, 1)
		assert.NoError(t, err)
		err = cms.Merge(s3)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "sketches are incompatible")
	})

	t.Run("CM merge - pass", func(t *testing.T) {
		relativeError := 0.25
		confidence := 0.9
		numBuckets, err := SuggestNumBuckets(relativeError)
		assert.NoError(t, err)
		numHashes, err := SuggestNumHashes(confidence)
		assert.NoError(t, err)

		cms, err := NewCountMinSketch(numHashes, numBuckets, seed)
		assert.NoError(t, err)

		otherCms, err := NewCountMinSketch(cms.GetNumHashes(), cms.GetNumBuckets(), seed)
		assert.NoError(t, err)

		err = cms.Merge(otherCms)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), cms.GetTotalWeight())

		data := []uint64{2, 3, 5, 7}
		for _, d := range data {
			err = cms.UpdateUint64(d, int64(1))
			assert.NoError(t, err)
			err = otherCms.UpdateUint64(d, int64(1))
			assert.NoError(t, err)
		}
		err = cms.Merge(otherCms)
		assert.NoError(t, err)
		assert.Equal(t, cms.GetTotalWeight(), 2*otherCms.GetTotalWeight())

		for _, d := range data {
			assert.LessOrEqual(t, cms.GetEstimateUint64(d), cms.GetUpperBoundUint64(d))
			assert.LessOrEqual(t, cms.GetEstimateUint64(d), int64(2))
		}
	})

	t.Run("CM serialize-deserialize", func(t *testing.T) {
		numHashes := int8(3)
		numBuckets := int32(5)
		c, err := NewCountMinSketch(numHashes, numBuckets, seed)
		assert.NoError(t, err)
		var buf []byte
		b := bytes.NewBuffer(buf)
		err = c.Serialize(b)
		assert.NoError(t, err)

		d, err := c.Deserialize(b.Bytes(), seed)
		assert.NoError(t, err)
		assert.Equal(t, c, d)
		assert.NotEqual(t, &c, d)

		data := []uint64{2, 3, 5, 7}
		for _, d := range data {
			err = c.UpdateUint64(d, int64(1))
			assert.NoError(t, err)
		}

		b.Reset()
		err = c.Serialize(b)
		assert.NoError(t, err)

		d, err = c.Deserialize(b.Bytes(), seed)
		assert.NoError(t, err)
		assert.Equal(t, c, d)
		assert.NotEqual(t, &c, d)
	})
}
