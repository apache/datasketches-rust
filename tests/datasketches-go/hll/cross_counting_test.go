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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCrossCounting(t *testing.T) {
	crossCountingCheck(t, 4, 100)
	crossCountingCheck(t, 4, 10000)
	crossCountingCheck(t, 12, 7)
	crossCountingCheck(t, 12, 384)
	crossCountingCheck(t, 12, 10000)
}

func crossCountingCheck(t *testing.T, lgK int, n int) {
	sk4, err := buildSketch(lgK, n, TgtHllTypeHll4)
	assert.NoError(t, err)
	s4csum := computeCheckSum(t, sk4)

	sk6, err := buildSketch(lgK, n, TgtHllTypeHll6)
	assert.NoError(t, err)
	s6csum := computeCheckSum(t, sk6)

	assert.Equal(t, s6csum, s4csum)

	sk8, err := buildSketch(lgK, n, TgtHllTypeHll8)
	assert.NoError(t, err)
	s8csum := computeCheckSum(t, sk8)
	assert.Equal(t, s8csum, s4csum)

	// Conversions
	sk6to4, err := sk6.CopyAs(TgtHllTypeHll4)
	assert.NoError(t, err)
	sk6to4csum := computeCheckSum(t, sk6to4)
	assert.Equal(t, sk6to4csum, s4csum)

	sk8to4, err := sk8.CopyAs(TgtHllTypeHll4)
	assert.NoError(t, err)
	sk8to4csum := computeCheckSum(t, sk8to4)
	assert.Equal(t, sk8to4csum, s4csum)

	sk4to6, err := sk4.CopyAs(TgtHllTypeHll6)
	assert.NoError(t, err)
	sk4to6csum := computeCheckSum(t, sk4to6)
	assert.Equal(t, sk4to6csum, s4csum)

	sk8to6, err := sk8.CopyAs(TgtHllTypeHll6)
	assert.NoError(t, err)
	sk8to6csum := computeCheckSum(t, sk8to6)
	assert.Equal(t, sk8to6csum, s4csum)

	sk4to8, err := sk4.CopyAs(TgtHllTypeHll8)
	assert.NoError(t, err)
	sk4to8csum := computeCheckSum(t, sk4to8)
	assert.Equal(t, sk4to8csum, s4csum)

	sk6to8, err := sk6.CopyAs(TgtHllTypeHll8)
	assert.NoError(t, err)
	sk6to8csum := computeCheckSum(t, sk6to8)
	assert.Equal(t, sk6to8csum, s4csum)

}

func buildSketch(lgK int, n int, tgtHllType TgtHllType) (HllSketch, error) {
	sketch, err := NewHllSketch(lgK, tgtHllType)
	if err != nil {
		return nil, err
	}
	for i := 0; i < n; i++ {
		err = sketch.UpdateInt64(int64(i))
		if err != nil {
			return nil, err
		}
	}
	return sketch, nil
}

func computeCheckSum(t *testing.T, sketch HllSketch) int {
	itr := sketch.iterator()
	checksum := 0
	for itr.nextAll() {
		p, err := itr.getPair()
		assert.NoError(t, err)
		checksum += p
		_ = itr.getKey()
	}
	return checksum
}
