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
	"io"
)

// Encoder encodes a compact theta sketch to bytes.
type Encoder struct {
	w          io.Writer
	compressed bool
}

// NewEncoder creates a new encoder.
func NewEncoder(w io.Writer, compressed bool) Encoder {
	return Encoder{w: w, compressed: compressed}
}

// Encode encodes a compact theta sketch to bytes.
func (enc Encoder) Encode(sketch *CompactSketch) error {
	if enc.compressed {
		return enc.encodeWithCompression(sketch)
	}
	return enc.encodeWithoutCompression(sketch)
}

func (enc Encoder) encodeWithCompression(sketch *CompactSketch) error {
	if sketch.isSuitableForCompression() {
		entryBits := sketch.computeEntryBits()
		numEntriesBytes := sketch.numEntriesBytes()

		size := sketch.compressedSerializedSizeBytes(entryBits, numEntriesBytes)
		bytes := make([]byte, size)

		preambleLongs := sketch.preambleLongs(true)
		err := enc.encodeVersion4(sketch, bytes, 0, entryBits, numEntriesBytes, preambleLongs)
		if err != nil {
			return err
		}

		n, err := enc.w.Write(bytes)
		if err != nil {
			return err
		}
		if n != len(bytes) {
			return io.ErrShortWrite
		}
		return nil
	}
	return enc.encodeWithoutCompression(sketch)
}

func (enc Encoder) encodeVersion4(sketch *CompactSketch, bytes []byte, offset int, entryBits, numEntriesBytes, preambleLongs uint8) error {
	// Preamble
	bytes[offset] = preambleLongs
	offset++
	bytes[offset] = CompressedSerialVersion
	offset++
	bytes[offset] = CompactSketchType
	offset++
	bytes[offset] = entryBits
	offset++
	bytes[offset] = numEntriesBytes
	offset++

	// Flags
	flags := byte(0)
	flags |= 1 << serializationFlagIsCompact
	flags |= 1 << serializationFlagIsReadOnly
	flags |= 1 << serializationFlagIsOrdered
	bytes[offset] = flags
	offset++

	// Seed hash
	bytes[offset] = byte(sketch.seedHash)
	bytes[offset+1] = byte(sketch.seedHash >> 8)
	offset += 2

	// Theta (if estimation mode)
	if sketch.IsEstimationMode() {
		for i := 0; i < 8; i++ {
			bytes[offset+i] = byte(sketch.theta >> (i * 8))
		}
		offset += 8
	}

	// Write num_entries as variable-length integer
	numEntries := uint32(len(sketch.entries))
	for i := uint8(0); i < numEntriesBytes; i++ {
		bytes[offset] = byte(numEntries >> (i << 3))
		offset++
	}

	previous := uint64(0)
	deltas := make([]uint64, 8)

	// Pack blocks of 8 deltas
	i := 0
	for i+7 < len(sketch.entries) {
		for j := 0; j < 8; j++ {
			deltas[j] = sketch.entries[i+j] - previous
			previous = sketch.entries[i+j]
		}
		if err := packBitsBlock8(deltas, bytes[offset:], entryBits); err != nil {
			return err
		}

		offset += int(entryBits)
		i += 8
	}

	// Pack remaining deltas (< 8) using packBits
	bytesIdx := 0
	bitOffset := uint8(0)
	for i < len(sketch.entries) {
		delta := sketch.entries[i] - previous
		previous = sketch.entries[i]
		bytesIdx, bitOffset = packBits(delta, entryBits, bytes[offset:], bytesIdx, bitOffset)
		i++
	}

	return nil
}

func (enc Encoder) encodeWithoutCompression(sketch *CompactSketch) error {
	preambleLongs := sketch.preambleLongs(false)

	bytesSize := sketch.SerializedSizeBytes(false)
	bytes := make([]byte, bytesSize)

	enc.encodeSketch(sketch, bytes, 0, preambleLongs)

	n, err := enc.w.Write(bytes)
	if err != nil {
		return err
	}
	if n != len(bytes) {
		return io.ErrShortWrite
	}
	return nil
}

func (enc Encoder) encodeSketch(sketch *CompactSketch, bytes []byte, offset int64, preambleLongs uint8) {
	// Preamble
	bytes[offset] = preambleLongs
	offset++
	bytes[offset] = UncompressedSerialVersion
	offset++
	bytes[offset] = CompactSketchType
	offset++

	// 2 bytes unused
	offset += 2

	// Flags
	flags := byte(0)
	flags |= 1 << serializationFlagIsCompact
	flags |= 1 << serializationFlagIsReadOnly
	if sketch.IsEmpty() {
		flags |= 1 << serializationFlagIsEmpty
	}
	if sketch.IsOrdered() {
		flags |= 1 << serializationFlagIsOrdered
	}
	bytes[offset] = flags
	offset++

	// Seed hash
	seedHash, _ := sketch.SeedHash()
	binary.LittleEndian.PutUint16(bytes[offset:offset+2], seedHash)
	offset += 2

	if preambleLongs > 1 {
		numEntries := uint32(len(sketch.entries))
		binary.LittleEndian.PutUint32(bytes[offset:offset+4], numEntries)
		offset += 4
		// 4 bytes unused
		offset += 4
	}

	if sketch.IsEstimationMode() {
		binary.LittleEndian.PutUint64(bytes[offset:offset+8], sketch.theta)
		offset += 8
	}

	for _, entry := range sketch.entries {
		binary.LittleEndian.PutUint64(bytes[offset:offset+8], entry)
		offset += 8
	}
}
