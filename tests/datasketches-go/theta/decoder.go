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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/datasketches-go/internal"
)

// Decoder decodes a compact sketch from the given reader.
type Decoder struct {
	seed uint64
}

// NewDecoder creates a new decoder.
func NewDecoder(seed uint64) Decoder {
	return Decoder{
		seed: seed,
	}
}

// Decode decodes a compact sketch from the given reader.
func (dec Decoder) Decode(r io.Reader) (*CompactSketch, error) {
	bytes, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return Decode(bytes, dec.seed)
}

// Decode decodes a compact sketch from the given bytes.
func Decode(bytes []byte, seed uint64) (*CompactSketch, error) {
	data, err := decodeCompactSketch(bytes, seed)
	if err != nil {
		return nil, err
	}

	// For versions 1-3 (entry_bits == 64)
	if data.entryBits == 64 {
		entries := make([]uint64, data.numEntries)
		for i := uint32(0); i < data.numEntries; i++ {
			offset := data.entriesStartIdx + int(i)*8
			entries[i] = binary.LittleEndian.Uint64(data.bytes[offset:])
		}

		return newCompactSketchFromEntries(
			data.isEmpty,
			data.isOrdered,
			data.seedHash,
			data.theta,
			entries,
		), nil
	}

	// For version 4 (entry_bits < 64): entries are compressed
	entries := make([]uint64, data.numEntries)
	ptr := data.bytes[data.entriesStartIdx:]

	// Unpack blocks of 8 deltas
	i := uint32(0)
	for i+7 < data.numEntries {
		if err := unpackBitsBlock8(entries[i:i+8], ptr, data.entryBits); err != nil {
			return nil, err
		}
		ptr = ptr[data.entryBits:]
		i += 8
	}

	// Unpack remaining deltas (< 8)
	ptrIdx := 0
	bitOffset := uint8(0)
	for i < data.numEntries {
		entries[i], ptrIdx, bitOffset = unpackBits(data.entryBits, ptr, ptrIdx, bitOffset)
		i++
	}

	// Undo deltas (accumulate to get actual hash values)
	previous := uint64(0)
	for i := uint32(0); i < data.numEntries; i++ {
		entries[i] += previous
		previous = entries[i]
	}

	return newCompactSketchFromEntries(
		data.isEmpty,
		data.isOrdered,
		data.seedHash,
		data.theta,
		entries,
	), nil
}

type compactSketchData struct {
	theta           uint64
	bytes           []byte
	entriesStartIdx int
	numEntries      uint32
	seedHash        uint16
	entryBits       uint8
	isEmpty         bool
	isOrdered       bool
}

func decodeCompactSketch(bytes []byte, seed uint64) (compactSketchData, error) {
	if err := validateMemorySize(bytes, 8); err != nil {
		return compactSketchData{}, err
	}

	if bytes[compactSketchTypeByte] != CompactSketchType {
		return compactSketchData{}, fmt.Errorf("invalid sketch type: expected %d, got %d", CompactSketchType, bytes[compactSketchTypeByte])
	}

	serialVersion := bytes[compactSketchSerialVersionByte]

	switch serialVersion {
	case 4:
		return decodeVersion4(bytes, seed)
	case 3:
		return decodeVersion3(bytes, seed)
	case 2:
		return decodeVersion2(bytes, seed)
	case 1:
		return decodeVersion1(bytes, seed)
	default:
		return compactSketchData{}, fmt.Errorf("unsupported serial version: %d", serialVersion)
	}
}

func decodeVersion4(bytes []byte, seed uint64) (compactSketchData, error) {
	// V4 sketches are always ordered and have entries (single item in exact mode is v3)
	seedHash := binary.LittleEndian.Uint16(bytes[compactSketchSeedHashU16*2:])
	expectedSeedHash, err := internal.ComputeSeedHash(int64(seed))
	if err != nil {
		return compactSketchData{}, err
	}
	if err := CheckSeedHashEqual(seedHash, uint16(expectedSeedHash)); err != nil {
		return compactSketchData{}, err
	}

	preambleLongs := bytes[compactSketchPreLongsByte]
	hasTheta := preambleLongs > 1
	theta := MaxTheta

	if hasTheta {
		if err := validateMemorySize(bytes, 16); err != nil {
			return compactSketchData{}, err
		}
		theta = binary.LittleEndian.Uint64(bytes[compactSketchV4ThetaU64*8:])
	}

	numEntriesBytes := bytes[compactSketchV4NumEntriesBytesByte]
	dataOffsetBytes := compactSketchV4PackedDataExactByte
	if hasTheta {
		dataOffsetBytes = compactSketchV4PackedDataEstByte
	}

	if err := validateMemorySize(bytes, dataOffsetBytes+int(numEntriesBytes)); err != nil {
		return compactSketchData{}, err
	}

	// Read variable-length num_entries
	var numEntries uint32
	for i := uint8(0); i < numEntriesBytes; i++ {
		numEntries |= uint32(bytes[dataOffsetBytes+int(i)]) << (i << 3)
	}
	dataOffsetBytes += int(numEntriesBytes)

	entryBits := bytes[compactSketchV4EntryBitsByte]
	expectedBits := uint64(entryBits) * uint64(numEntries)
	expectedSize := dataOffsetBytes + int(wholeBytesToHoldBits(expectedBits))

	if err := validateMemorySize(bytes, expectedSize); err != nil {
		return compactSketchData{}, err
	}

	return compactSketchData{
		isEmpty:         false,
		isOrdered:       true,
		seedHash:        seedHash,
		numEntries:      numEntries,
		theta:           theta,
		entriesStartIdx: dataOffsetBytes,
		entryBits:       entryBits,
		bytes:           bytes,
	}, nil
}

func decodeVersion3(bytes []byte, seed uint64) (compactSketchData, error) {
	theta := MaxTheta
	seedHash := binary.LittleEndian.Uint16(bytes[compactSketchSeedHashU16*2:])

	if bytes[compactSketchFlagsByte]&(1<<serializationFlagIsEmpty) != 0 {
		return compactSketchData{
			isEmpty:    true,
			isOrdered:  true,
			seedHash:   seedHash,
			numEntries: 0,
			theta:      theta,
			entryBits:  64,
			bytes:      bytes,
		}, nil
	}

	expectedSeedHash, err := internal.ComputeSeedHash(int64(seed))
	if err != nil {
		return compactSketchData{}, err
	}
	if err := CheckSeedHashEqual(seedHash, uint16(expectedSeedHash)); err != nil {
		return compactSketchData{}, err
	}

	preambleLongs := bytes[compactSketchPreLongsByte]
	hasTheta := preambleLongs > 2
	if hasTheta {
		if err := validateMemorySize(bytes, (compactSketchThetaU64+1)*8); err != nil {
			return compactSketchData{}, err
		}
		theta = binary.LittleEndian.Uint64(bytes[compactSketchThetaU64*8:])
	}

	// Single entry case
	if preambleLongs == 1 {
		if err := validateMemorySize(bytes, 16); err != nil {
			return compactSketchData{}, err
		}
		return compactSketchData{
			isEmpty:         false,
			isOrdered:       true,
			seedHash:        seedHash,
			numEntries:      1,
			theta:           theta,
			entriesStartIdx: compactSketchSingleEntryU64 * 8,
			entryBits:       64,
			bytes:           bytes,
		}, nil
	}

	numEntries := binary.LittleEndian.Uint32(bytes[compactSketchNumEntriesU32*4:])
	entriesStartU64 := compactSketchEntriesExactU64
	if hasTheta {
		entriesStartU64 = compactSketchEntriesEstimationU64
	}

	expectedSize := (entriesStartU64 + int(numEntries)) * 8
	if err := validateMemorySize(bytes, expectedSize); err != nil {
		return compactSketchData{}, err
	}

	isOrdered := bytes[compactSketchFlagsByte]&(1<<serializationFlagIsOrdered) != 0

	return compactSketchData{
		isEmpty:         false,
		isOrdered:       isOrdered,
		seedHash:        seedHash,
		numEntries:      numEntries,
		theta:           theta,
		entriesStartIdx: entriesStartU64 * 8,
		entryBits:       64,
		bytes:           bytes,
	}, nil
}

func decodeVersion2(bytes []byte, seed uint64) (compactSketchData, error) {
	preambleSize := bytes[compactSketchPreLongsByte]
	seedHash := binary.LittleEndian.Uint16(bytes[compactSketchSeedHashU16*2:])

	expectedSeedHash, err := internal.ComputeSeedHash(int64(seed))
	if err != nil {
		return compactSketchData{}, err
	}
	if err := CheckSeedHashEqual(seedHash, uint16(expectedSeedHash)); err != nil {
		return compactSketchData{}, err
	}

	switch preambleSize {
	case 1:
		return compactSketchData{
			isEmpty:    true,
			isOrdered:  true,
			seedHash:   seedHash,
			numEntries: 0,
			theta:      MaxTheta,
			entryBits:  64,
			bytes:      bytes,
		}, nil
	case 2:
		numEntries := binary.LittleEndian.Uint32(bytes[compactSketchNumEntriesU32*4:])
		if numEntries == 0 {
			return compactSketchData{
				isEmpty:    true,
				isOrdered:  true,
				seedHash:   seedHash,
				numEntries: 0,
				theta:      MaxTheta,
				entryBits:  64,
				bytes:      bytes,
			}, nil
		}

		expectedSize := (int(preambleSize) + int(numEntries)) << 3
		if err := validateMemorySize(bytes, expectedSize); err != nil {
			return compactSketchData{}, err
		}

		return compactSketchData{
			isEmpty:         false,
			isOrdered:       true,
			seedHash:        seedHash,
			numEntries:      numEntries,
			theta:           MaxTheta,
			entriesStartIdx: compactSketchEntriesExactU64 * 8,
			entryBits:       64,
			bytes:           bytes,
		}, nil
	case 3:
		numEntries := binary.LittleEndian.Uint32(bytes[compactSketchNumEntriesU32*4:])
		theta := binary.LittleEndian.Uint64(bytes[compactSketchThetaU64*8:])

		isEmpty := numEntries == 0 && theta == MaxTheta
		if isEmpty {
			return compactSketchData{
				isEmpty:    true,
				isOrdered:  true,
				seedHash:   seedHash,
				numEntries: 0,
				theta:      theta,
				entryBits:  64,
				bytes:      bytes,
			}, nil
		}

		expectedSize := (compactSketchEntriesEstimationU64 + int(numEntries)) * 8
		if err := validateMemorySize(bytes, expectedSize); err != nil {
			return compactSketchData{}, err
		}

		return compactSketchData{
			isEmpty:         false,
			isOrdered:       true,
			seedHash:        seedHash,
			numEntries:      numEntries,
			theta:           theta,
			entriesStartIdx: compactSketchEntriesEstimationU64 * 8,
			entryBits:       64,
			bytes:           bytes,
		}, nil
	default:
		return compactSketchData{}, fmt.Errorf("invalid preamble size: %d (expected 1, 2, or 3)", preambleSize)
	}
}

func decodeVersion1(bytes []byte, seed uint64) (compactSketchData, error) {
	seedHash, err := internal.ComputeSeedHash(int64(seed))
	if err != nil {
		return compactSketchData{}, err
	}

	numEntries := binary.LittleEndian.Uint32(bytes[compactSketchNumEntriesU32*4:])
	theta := binary.LittleEndian.Uint64(bytes[compactSketchThetaU64*8:])

	isEmpty := numEntries == 0 && theta == MaxTheta
	if isEmpty {
		return compactSketchData{
			isEmpty:    true,
			isOrdered:  true,
			seedHash:   uint16(seedHash),
			numEntries: 0,
			theta:      theta,
			entryBits:  64,
			bytes:      bytes,
		}, nil
	}

	expectedSize := (compactSketchEntriesEstimationU64 + int(numEntries)) * 8
	if err := validateMemorySize(bytes, expectedSize); err != nil {
		return compactSketchData{}, err
	}

	return compactSketchData{
		isEmpty:         false,
		isOrdered:       true,
		seedHash:        uint16(seedHash),
		numEntries:      numEntries,
		theta:           theta,
		entriesStartIdx: compactSketchEntriesEstimationU64 * 8,
		entryBits:       64,
		bytes:           bytes,
	}, nil
}

func validateMemorySize(bytes []byte, expectedBytes int) error {
	actualBytes := len(bytes)
	if actualBytes < expectedBytes {
		return fmt.Errorf("at least %d bytes expected, actual %d", expectedBytes, actualBytes)
	}
	return nil
}
