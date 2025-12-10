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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHashMapSerial(t *testing.T) {
	mp, err := newReversePurgeLongHashMap(8)
	assert.NoError(t, err)
	mp.adjustOrPutValue(10, 15)
	mp.adjustOrPutValue(10, 5)
	mp.adjustOrPutValue(1, 1)
	mp.adjustOrPutValue(2, 3)
	strMp := mp.serializeToString()

	newMp, err := deserializeReversePurgeLongHashMapFromString(strMp)
	assert.NoError(t, err)
	newStrMp := newMp.serializeToString()
	assert.Equal(t, strMp, newStrMp)

}
