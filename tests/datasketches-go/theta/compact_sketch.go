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
	"bytes"
	"fmt"
	"iter"
	"math/bits"
	"slices"
	"strings"

	"github.com/apache/datasketches-go/internal/binomialbounds"
	"golang.org/x/exp/constraints"
)

const UncompressedSerialVersion = 3

const CompressedSerialVersion = 4

const CompactSketchType = 3

// Offsets in sizeof(type)
const (
	compactSketchPreLongsByte          = 0
	compactSketchSerialVersionByte     = 1
	compactSketchTypeByte              = 2
	compactSketchFlagsByte             = 5
	compactSketchSeedHashU16           = 3 // offset in uint16 units
	compactSketchSingleEntryU64        = 1 // offset in uint64 units (v3)
	compactSketchNumEntriesU32         = 2 // offset in uint32 units (v1-3)
	compactSketchEntriesExactU64       = 2 // offset in uint64 units (v1-3)
	compactSketchEntriesEstimationU64  = 3 // offset in uint64 units (v1-3)
	compactSketchThetaU64              = 2 // offset in uint64 units (v1-3)
	compactSketchV4EntryBitsByte       = 3
	compactSketchV4NumEntriesBytesByte = 4
	compactSketchV4ThetaU64            = 1 // offset in uint64 units
	compactSketchV4PackedDataExactByte = 8
	compactSketchV4PackedDataEstByte   = 16
)

// Serialization flags
const (
	serializationFlagIsBigEndian uint8 = iota
	serializationFlagIsReadOnly
	serializationFlagIsEmpty
	serializationFlagIsCompact
	serializationFlagIsOrdered
)

// CompactSketch is an immutable form of the Theta sketch, the form that can be serialized and deserialized
type CompactSketch struct {
	entries   []uint64
	theta     uint64
	seedHash  uint16
	isEmpty   bool
	isOrdered bool
}

// NewCompactSketch creates a compact sketch from another sketch
func NewCompactSketch(source Sketch, ordered bool) *CompactSketch {
	isEmpty := source.IsEmpty()
	sourceOrdered := source.IsOrdered()
	seedHash, _ := source.SeedHash()
	theta := source.Theta64()

	var entries []uint64
	if !isEmpty {
		for entry := range source.All() {
			entries = append(entries, entry)
		}

		if ordered && !sourceOrdered {
			slices.Sort(entries)
		}
	}

	return &CompactSketch{
		isEmpty:   isEmpty,
		isOrdered: sourceOrdered || ordered,
		seedHash:  seedHash,
		theta:     theta,
		entries:   entries,
	}
}

func newCompactSketchFromEntries(isEmpty, isOrdered bool, seedHash uint16, theta uint64, entries []uint64) *CompactSketch {
	if len(entries) <= 1 {
		isOrdered = true
	}

	return &CompactSketch{
		isEmpty:   isEmpty,
		isOrdered: isOrdered,
		seedHash:  seedHash,
		theta:     theta,
		entries:   entries,
	}
}

// IsEmpty returns true if this sketch represents an empty set
// (not the same as no retained entries!)
func (s *CompactSketch) IsEmpty() bool {
	return s.isEmpty
}

// IsOrdered returns true if retained entries are ordered
func (s *CompactSketch) IsOrdered() bool {
	return s.isOrdered
}

// Theta64 returns theta as a positive integer between 0 and math.MaxInt64
func (s *CompactSketch) Theta64() uint64 {
	return s.theta
}

// NumRetained returns the number of retained entries in the sketch
func (s *CompactSketch) NumRetained() uint32 {
	return uint32(len(s.entries))
}

// SeedHash returns hash of the seed that was used to hash the input
func (s *CompactSketch) SeedHash() (uint16, error) {
	return s.seedHash, nil
}

// Estimate returns estimate of the distinct count of the input stream
func (s *CompactSketch) Estimate() float64 {
	return float64(s.NumRetained()) / s.Theta()
}

// LowerBound returns the approximate lower error bound given a number of standard deviations.
// This parameter is similar to the number of standard deviations of the normal distribution
// and corresponds to approximately 67%, 95% and 99% confidence intervals.
// numStdDevs number of Standard Deviations (1, 2 or 3)
func (s *CompactSketch) LowerBound(numStdDevs uint8) (float64, error) {
	if !s.IsEstimationMode() {
		return float64(len(s.entries)), nil
	}
	return binomialbounds.LowerBound(
		uint64(s.NumRetained()),
		s.Theta(),
		uint(numStdDevs),
	)
}

// UpperBound returns the approximate upper error bound given a number of standard deviations.
// This parameter is similar to the number of standard deviations of the normal distribution
// and corresponds to approximately 67%, 95% and 99% confidence intervals.
// numStdDevs number of Standard Deviations (1, 2 or 3)
func (s *CompactSketch) UpperBound(numStdDevs uint8) (float64, error) {
	if !s.IsEstimationMode() {
		return float64(len(s.entries)), nil
	}
	return binomialbounds.UpperBound(
		uint64(s.NumRetained()),
		s.Theta(),
		uint(numStdDevs),
	)
}

// IsEstimationMode returns true if the sketch is in estimation mode
// (as opposed to exact mode)
func (s *CompactSketch) IsEstimationMode() bool {
	return s.Theta64() < MaxTheta && !s.isEmpty
}

// Theta returns theta as a fraction from 0 to 1 (effective sampling rate)
func (s *CompactSketch) Theta() float64 {
	return float64(s.Theta64()) / float64(MaxTheta)
}

// String returns a human-readable summary of this sketch as a string
// If shouldPrintItems is true, include the list of items retained by the sketch
func (s *CompactSketch) String(shouldPrintItems bool) string {
	seedHash, _ := s.SeedHash()
	lb, _ := s.LowerBound(2)
	ub, _ := s.UpperBound(2)

	var result strings.Builder
	result.WriteString("### Theta sketch summary:")
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   num retained entries : %d", s.NumRetained()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   seed hash            : %d", seedHash))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   empty?               : %t", s.IsEmpty()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   ordered?             : %t", s.IsOrdered()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   estimation mode?     : %t", s.IsEstimationMode()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   theta (fraction)     : %f", s.Theta()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   theta (raw 64-bit)   : %d", s.Theta64()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   estimate             : %f", s.Estimate()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   lower bound 95%% conf : %f", lb))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   upper bound 95%% conf : %f", ub))
	result.WriteString("\n")
	result.WriteString("### End sketch summary")
	result.WriteString("\n")

	if shouldPrintItems {
		result.WriteString("### Retained entries")
		result.WriteString("\n")

		for entry := range s.All() {
			result.WriteString(fmt.Sprintf("%d", entry))
			result.WriteString("\n")
		}

		result.WriteString("### End retained entries")
		result.WriteString("\n")
	}

	return result.String()
}

// All returns hash values in the sketch.
func (s *CompactSketch) All() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		for _, entry := range s.entries {
			if !yield(entry) {
				return // Allow early termination
			}
		}
	}
}

// MarshalBinary implements encoding.BinaryMarshaler (uncompressed)
func (s *CompactSketch) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	encoder := NewEncoder(&buf, false)
	if err := encoder.Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *CompactSketch) preambleLongs(compressed bool) uint8 {
	if compressed {
		if s.IsEstimationMode() {
			return 2
		}
		return 1
	}

	if s.IsEstimationMode() {
		return 3
	}
	if s.isEmpty || len(s.entries) == 1 {
		return 1
	}
	return 2
}

// MaxSerializedSizeBytes computes maximum serialized size in bytes
// lgK is the nominal number of entries in the sketch
func (s *CompactSketch) MaxSerializedSizeBytes(lgK uint8) uint8 {
	capacity := computeCapacity(lgK+1, lgK)
	return uint8(8 * (3 + int(capacity)))
}

// SerializedSizeBytes computes the size in bytes required to serialize the current state of the sketch.
// Computing compressed size is expensive. It takes iterating over all retained hashes,
// and the actual serialization will have to look at them again.
// compressed if true compressed size is returned (if applicable)
func (s *CompactSketch) SerializedSizeBytes(compressed bool) int {
	if compressed && s.isSuitableForCompression() {
		entryBits := s.computeEntryBits()
		numEntriesBytes := s.numEntriesBytes()
		return s.compressedSerializedSizeBytes(entryBits, numEntriesBytes)
	}
	return int(s.preambleLongs(false))*8 + len(s.entries)*8
}

func (s *CompactSketch) isSuitableForCompression() bool {
	if !s.isOrdered ||
		len(s.entries) == 0 ||
		(len(s.entries) == 1 && !s.IsEstimationMode()) {
		return false
	}
	return true
}

func (s *CompactSketch) computeEntryBits() uint8 {
	// Compression based on leading zeros in deltas
	var previous uint64
	var ored uint64

	for _, entry := range s.entries {
		delta := entry - previous
		ored |= delta
		previous = entry
	}

	return uint8(64 - bits.LeadingZeros64(ored))
}

func (s *CompactSketch) numEntriesBytes() uint8 {
	numEntries := uint32(len(s.entries))
	if numEntries == 0 {
		return 1
	}
	leadingZeros := bits.LeadingZeros32(uint32(len(s.entries)))
	return uint8(wholeBytesToHoldBits(32 - leadingZeros))
}

func (s *CompactSketch) compressedSerializedSizeBytes(entryBits, numEntriesBytes uint8) int {
	compressedBits := int(entryBits) * len(s.entries)
	return int(s.preambleLongs(true))*8 + int(numEntriesBytes) + wholeBytesToHoldBits(compressedBits)
}

func wholeBytesToHoldBits[T constraints.Integer](bits T) T {
	var remainder T = 0
	if (bits & 7) > 0 {
		remainder = 1
	}
	return (bits >> 3) + remainder
}
