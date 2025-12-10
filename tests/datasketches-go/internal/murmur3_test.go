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

package internal

import (
	"github.com/twmb/murmur3"
	"testing"
)

func TestByteArrRemainderGT8(t *testing.T) {
	key := []byte("The quick brown fox jumps over the lazy dog")
	resultLo, resultHi := HashByteArrMurmur3(key, 0, len(key), 0)
	h1 := uint64(0xe34bbc7bbc071b6c)
	h2 := uint64(0x7a433ca9c49a9347)
	if resultLo != h1 {
		t.Errorf("expected %v, got %v", h1, resultLo)
	}
	if resultHi != h2 {
		t.Errorf("expected %v, got %v", h2, resultHi)
	}
}

func BenchmarkHashCharSliceMurmur3(b *testing.B) {
	b.Run("custom murmur3", func(b *testing.B) {
		key := []byte("The quick brown fox jumps over the lazy dog")
		for i := 0; i < b.N; i++ {
			HashCharSliceMurmur3(key, 0, len(key), 0)
		}
	})

	b.Run("stdlib murmur3", func(b *testing.B) {
		key := []byte("The quick brown fox jumps over the lazy dog")
		for i := 0; i < b.N; i++ {
			murmur3.SeedSum128(DEFAULT_UPDATE_SEED, DEFAULT_UPDATE_SEED, key)
		}
	})

}
