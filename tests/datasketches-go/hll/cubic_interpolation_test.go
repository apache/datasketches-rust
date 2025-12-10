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

func TestInterpolationExceptions(t *testing.T) {
	_, err := usingXAndYTables(couponMappingXArr, couponMappingYArr, -1)
	assert.Error(t, err, "X value out of range: -1.000000")

	_, err = usingXAndYTables(couponMappingXArr, couponMappingYArr, 11000000.0)
	assert.Error(t, err, "X value out of range: 11000000.000000")
}

func TestCornerCases(t *testing.T) {
	leng := len(couponMappingXArr)
	x := couponMappingXArr[leng-1]
	y, err := usingXAndYTables(couponMappingXArr, couponMappingYArr, x)
	assert.NoError(t, err)
	yExp := couponMappingYArr[leng-1]
	assert.Equal(t, y, yExp)
}
