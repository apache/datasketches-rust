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

package theta

import (
	"fmt"
	"iter"
	"strings"

	"github.com/apache/datasketches-go/internal/binomialbounds"
)

// WrappedCompactSketch wraps a serialized compact sketch buffer for read-only access
type WrappedCompactSketch struct {
	data *compactSketchData
}

// WrapCompactSketch wraps a serialized compact sketch as an array of bytes
func WrapCompactSketch(bytes []byte, seed uint64) (*WrappedCompactSketch, error) {
	data, err := decodeCompactSketch(bytes, seed)
	if err != nil {
		return nil, err
	}

	return &WrappedCompactSketch{
		data: &data,
	}, nil
}

// IsEmpty returns true if this sketch represents an empty set
func (s *WrappedCompactSketch) IsEmpty() bool {
	return s.data.isEmpty
}

// IsOrdered returns true if retained entries are ordered
func (s *WrappedCompactSketch) IsOrdered() bool {
	return s.data.isOrdered
}

// Theta64 returns theta as a positive integer
func (s *WrappedCompactSketch) Theta64() uint64 {
	return s.data.theta
}

// NumRetained returns the number of retained entries
func (s *WrappedCompactSketch) NumRetained() uint32 {
	return s.data.numEntries
}

// SeedHash returns hash of the seed
func (s *WrappedCompactSketch) SeedHash() (uint16, error) {
	return s.data.seedHash, nil
}

// Theta returns theta as a fraction from 0 to 1
func (s *WrappedCompactSketch) Theta() float64 {
	return float64(s.Theta64()) / float64(MaxTheta)
}

// IsEstimationMode returns true if the sketch is in estimation mode
func (s *WrappedCompactSketch) IsEstimationMode() bool {
	return s.Theta64() < MaxTheta && !s.data.isEmpty
}

// Estimate returns the estimate of distinct count
func (s *WrappedCompactSketch) Estimate() float64 {
	return float64(s.NumRetained()) / s.Theta()
}

// LowerBound returns the approximate lower error bound
func (s *WrappedCompactSketch) LowerBound(numStdDevs uint8) (float64, error) {
	if !s.IsEstimationMode() {
		return float64(s.NumRetained()), nil
	}
	return binomialbounds.LowerBound(
		uint64(s.NumRetained()),
		s.Theta(),
		uint(numStdDevs),
	)
}

// UpperBound returns the approximate upper error bound
func (s *WrappedCompactSketch) UpperBound(numStdDevs uint8) (float64, error) {
	if !s.IsEstimationMode() {
		return float64(s.NumRetained()), nil
	}
	return binomialbounds.UpperBound(
		uint64(s.NumRetained()),
		s.Theta(),
		uint(numStdDevs),
	)
}

// All returns a lazy iterator over hash values.
func (s *WrappedCompactSketch) All() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		if s.data.entryBits == 64 { // no compression
			for i := uint32(0); i < s.data.numEntries; i++ {
				offset := s.data.entriesStartIdx + int(i)*8
				entry := uint64(s.data.bytes[offset]) |
					uint64(s.data.bytes[offset+1])<<8 |
					uint64(s.data.bytes[offset+2])<<16 |
					uint64(s.data.bytes[offset+3])<<24 |
					uint64(s.data.bytes[offset+4])<<32 |
					uint64(s.data.bytes[offset+5])<<40 |
					uint64(s.data.bytes[offset+6])<<48 |
					uint64(s.data.bytes[offset+7])<<56
				if !yield(entry) {
					return
				}
			}
			return
		}

		// For compressed format (entry_bits < 64), unpack deltas
		bytes := s.data.bytes[s.data.entriesStartIdx:]
		var previous uint64
		var buffer [8]uint64
		var offset uint8

		index := uint32(0)
		bytesIdx := 0

		// Process entries in blocks of 8
		for index+7 < s.data.numEntries {
			if err := unpackBitsBlock8(buffer[:], bytes[bytesIdx:], s.data.entryBits); err != nil {
				panic("unexpected error: " + err.Error())
			}
			bytesIdx += int(s.data.entryBits)

			for i := 0; i < 8; i++ {
				buffer[i] += previous
				previous = buffer[i]
				if !yield(buffer[i]) {
					return
				}
			}
			index += 8
		}

		// Process remaining entries (< 8)
		for index < s.data.numEntries {
			var delta uint64
			delta, bytesIdx, offset = unpackBits(s.data.entryBits, bytes, bytesIdx, offset)
			value := delta + previous
			previous = value
			if !yield(value) {
				return
			}
			index++
		}
	}
}

func (s *WrappedCompactSketch) String(shouldPrintItems bool) string {
	var sb strings.Builder

	seedHash, _ := s.SeedHash()
	lb, _ := s.LowerBound(2)
	ub, _ := s.UpperBound(2)

	sb.WriteString("### Theta sketch summary:\n")
	sb.WriteString(fmt.Sprintf("   num retained entries : %d\n", s.NumRetained()))
	sb.WriteString(fmt.Sprintf("   seed hash            : %d\n", seedHash))
	sb.WriteString(fmt.Sprintf("   empty?               : %t\n", s.IsEmpty()))
	sb.WriteString(fmt.Sprintf("   ordered?             : %t\n", s.IsOrdered()))
	sb.WriteString(fmt.Sprintf("   estimation mode?     : %t\n", s.IsEstimationMode()))
	sb.WriteString(fmt.Sprintf("   theta (fraction)     : %g\n", s.Theta()))
	sb.WriteString(fmt.Sprintf("   theta (raw 64-bit)   : %d\n", s.Theta64()))
	sb.WriteString(fmt.Sprintf("   estimate             : %g\n", s.Estimate()))
	sb.WriteString(fmt.Sprintf("   lower bound 95%% conf : %g\n", lb))
	sb.WriteString(fmt.Sprintf("   upper bound 95%% conf : %g\n", ub))
	sb.WriteString("### End sketch summary\n")

	if shouldPrintItems {
		sb.WriteString("### Retained entries\n")
		for entry := range s.All() {
			sb.WriteString(fmt.Sprintf("%d\n", entry))
		}
		sb.WriteString("### End retained entries\n")
	}

	return sb.String()
}
