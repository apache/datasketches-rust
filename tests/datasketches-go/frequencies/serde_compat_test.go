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
	"github.com/apache/datasketches-go/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestItemsToLongs(t *testing.T) {
	sketch1, err := NewFrequencyItemsSketchWithMaxMapSize[int64](8, common.ItemSketchLongHasher{}, common.ItemSketchLongSerDe{})
	assert.NoError(t, err)
	sketch1.Update(1)
	sketch1.Update(2)
	sketch1.Update(3)
	sketch1.Update(4)

	bytes, err := sketch1.ToSlice()
	assert.NoError(t, err)
	sketch2, err := NewLongsSketchFromSlice(bytes)
	assert.NoError(t, err)
	sketch2.Update(2)
	sketch2.Update(3)
	sketch2.Update(2)

	assert.False(t, sketch2.IsEmpty())
	assert.Equal(t, sketch2.GetNumActiveItems(), 4)
	assert.Equal(t, sketch2.GetStreamLength(), int64(7))
	est, err := sketch2.GetEstimate(1)
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))
	est, err = sketch2.GetEstimate(2)
	assert.NoError(t, err)
	assert.Equal(t, est, int64(3))
	est, err = sketch2.GetEstimate(3)
	assert.NoError(t, err)
	assert.Equal(t, est, int64(2))
	est, err = sketch2.GetEstimate(4)
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))
}

func TestLongToItems(t *testing.T) {
	sketch1, err := NewLongsSketchWithMaxMapSize(8)
	assert.NoError(t, err)
	sketch1.Update(1)
	sketch1.Update(2)
	sketch1.Update(3)
	sketch1.Update(4)

	bytes := sketch1.ToSlice()
	sketch2, err := NewFrequencyItemsSketchFromSlice[int64](bytes, common.ItemSketchLongHasher{}, common.ItemSketchLongSerDe{})
	assert.NoError(t, err)
	sketch2.Update(2)
	sketch2.Update(3)
	sketch2.Update(2)

	assert.False(t, sketch2.IsEmpty())
	assert.Equal(t, sketch2.GetNumActiveItems(), 4)
	assert.Equal(t, sketch2.GetStreamLength(), int64(7))
	est, err := sketch2.GetEstimate(1)
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))
	est, err = sketch2.GetEstimate(2)
	assert.NoError(t, err)
	assert.Equal(t, est, int64(3))
	est, err = sketch2.GetEstimate(3)
	assert.NoError(t, err)
	assert.Equal(t, est, int64(2))
	est, err = sketch2.GetEstimate(4)
	assert.NoError(t, err)
	assert.Equal(t, est, int64(1))
}
