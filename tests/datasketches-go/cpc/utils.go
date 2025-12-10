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

package cpc

import (
	"encoding/binary"
	"fmt"
	"github.com/apache/datasketches-go/internal"
	"math"
	"math/bits"
	"strings"
)

type CpcFormat int
type CpcFlavor int

const (
	CpcFormatEmptyMerged             CpcFormat = 0
	CpcFormatEmptyHip                CpcFormat = 1
	CpcFormatSparseHybridMerged      CpcFormat = 2
	CpcFormatSparseHybridHip         CpcFormat = 3
	CpcFormatPinnedSlidingMergedNosv CpcFormat = 4
	CpcFormatPinnedSlidingHipNosv    CpcFormat = 5
	CpcFormatPinnedSlidingMerged     CpcFormat = 6
	CpcFormatPinnedSlidingHip        CpcFormat = 7
)

const (
	CpcFlavorEmpty   CpcFlavor = 0 //    0  == C <    1
	CpcFlavorSparse  CpcFlavor = 1 //    1  <= C <   3K/32
	CpcFlavorHybrid  CpcFlavor = 2 // 3K/32 <= C <   K/2
	CpcFlavorPinned  CpcFlavor = 3 //   K/2 <= C < 27K/8  [NB: 27/8 = 3 + 3/8]
	CpcFlavorSliding CpcFlavor = 4 // 27K/8 <= C
)

const (
	loFieldPreInts = iota
	loFieldSerVer
	loFieldFamily
	loFieldLgK
	loFieldFiCol
	loFieldFlags
	loFieldSeedHash
)

const (
	// Preamble hi field definitions
	// This defines the eight additional preamble fields located after the <i>LoField</i>.
	// Do not change the order.
	//
	// Note: NUM_SV has dual meanings: In sparse and hybrid flavors it is equivalent to
	// numCoupons so it isn't stored separately. In pinned and sliding flavors is the
	// numSV of the PairTable, which stores only surprising values.

	hiFieldNumCoupons = iota
	hiFieldNumSV
	hiFieldKXP
	hiFieldHipAccum
	hiFieldSVLengthInts
	hiFieldWLengthInts
	hiFieldSVStream
	hiFieldWStream
)

var (
	// This defines the byte offset for each of the 8 <i>HiFields</i>
	// given the Format ordinal (1st dimension) and the HiField ordinal (2nd dimension).
	hiFieldOffset = [8][8]byte{
		{0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 0, 0, 0, 0, 0},
		{8, 0, 0, 0, 12, 0, 16, 0},
		{8, 0, 16, 24, 12, 0, 32, 0},
		{8, 0, 0, 0, 0, 12, 0, 16},
		{8, 0, 16, 24, 0, 12, 0, 32},
		{8, 12, 0, 0, 16, 20, 24, 24},   //the 2nd 24 is not used.
		{8, 12, 16, 24, 32, 36, 40, 40}, //the 2nd 40 is not used.
	}
)

const (
	serVer = 1

	// Flags bit masks, Byte 5
	bigEndianFlagMask  = 1  // Reserved.
	compressedFlagMask = 2  // Compressed Flag
	hipFlagMask        = 4  // HIP Flag
	supValFlagMask     = 8  // num Surprising Values > 0
	windowFlagMask     = 16 // window length > 0

	defaultLgK = 11
)

var (
	kxpByteLookup = makeKxpByteLookup()
)

var (
	// This defines the preamble space required by each of the formats in units of 4-byte integers.
	preIntsDefs = []byte{2, 2, 4, 8, 4, 8, 6, 10}
)

// dataFmt is the format string used to print each integer (index and hex value).
const dataFmt = "%10d %10x"

func getDefinedPreInts(format CpcFormat) int {
	return int(preIntsDefs[format])
}

func getPreInts(mem []byte) int {
	return int(mem[loFieldPreInts] & 0xFF)
}

func getSerVer(mem []byte) int {
	return int(mem[loFieldSerVer] & 0xFF)
}

func hasHip(mem []byte) bool {
	return (getFlags(mem) & hipFlagMask) > 0
}

func checkLgK(lgK int) error {
	if lgK < minLgK || lgK > maxLgK {
		return fmt.Errorf("LgK must be >= %d and <= %d: %d", minLgK, maxLgK, lgK)
	}
	return nil
}

func checkLgSizeInts(lgSizeInts int) error {
	if lgSizeInts < 2 || lgSizeInts > 26 {
		return fmt.Errorf("illegal LgSizeInts: %d", lgSizeInts)
	}
	return nil
}

func checkSeeds(seedA uint64, seedB uint64) error {
	if seedA != seedB {
		return fmt.Errorf("incompatible seeds: %d %d", seedA, seedB)
	}
	return nil
}

func (f CpcFormat) String() string {
	switch f {
	case CpcFormatEmptyMerged:
		return "EMPTY_MERGED"
	case CpcFormatEmptyHip:
		return "EMPTY_HIP"
	case CpcFormatSparseHybridMerged:
		return "SPARSE_HYBRID_MERGED"
	case CpcFormatSparseHybridHip:
		return "SPARSE_HYBRID_HIP"
	case CpcFormatPinnedSlidingMergedNosv:
		return "PINNED_SLIDING_MERGED_NOSV"
	case CpcFormatPinnedSlidingHipNosv:
		return "PINNED_SLIDING_HIP_NOSV"
	case CpcFormatPinnedSlidingMerged:
		return "PINNED_SLIDING_MERGED"
	case CpcFormatPinnedSlidingHip:
		return "PINNED_SLIDING_HIP"
	default:
		return fmt.Sprintf("UnknownFormat(%d)", f)
	}
}

func determineFlavor(lgK int, numCoupons uint64) CpcFlavor {
	c := numCoupons
	k := uint64(1) << lgK
	c2 := c << 1
	c8 := c << 3
	c32 := c << 5
	if c == 0 {
		return CpcFlavorEmpty //    0  == C <    1
	}
	if c32 < (3 * k) {
		return CpcFlavorSparse //    1  <= C <   3K/32
	}
	if c2 < k {
		return CpcFlavorHybrid // 3K/32 <= C <   K/2
	}
	if c8 < (27 * k) {
		return CpcFlavorPinned //   K/2 <= C < 27K/8
	}
	return CpcFlavorSliding // 27K/8 <= C
}

func (f CpcFlavor) String() string {
	switch f {
	case CpcFlavorEmpty:
		return "EMPTY"
	case CpcFlavorSparse:
		return "SPARSE"
	case CpcFlavorHybrid:
		return "HYBRID"
	case CpcFlavorPinned:
		return "PINNED"
	case CpcFlavorSliding:
		return "SLIDING"
	default:
		return fmt.Sprintf("UnknownFlavor(%d)", f)
	}
}

func orMatrixIntoMatrix(destMatrix []uint64, destLgK int, srcMatrix []uint64, srcLgK int) {
	destMask := (1 << destLgK) - 1
	srcK := 1 << srcLgK
	for srcRow := 0; srcRow < srcK; srcRow++ {
		destMatrix[srcRow&destMask] |= srcMatrix[srcRow]
	}
}

func countBitsSetInMatrix(matrix []uint64) uint64 {
	count := uint64(0)
	for _, v := range matrix {
		count += uint64(bits.OnesCount64(v))
	}
	return count
}

func walkTableUpdatingSketch(dest *CpcSketch, table *pairTable) error {
	slots := table.slotsArr
	numSlots := 1 << table.lgSizeInts
	destMask := ((1<<dest.lgK)-1)<<6 | 63 // downsamples when dest.lgK < srcLgK

	stride := int(internal.InverseGolden * float64(numSlots))
	if stride == (stride >> 1 << 1) {
		stride++
	}

	for i, j := 0, 0; i < numSlots; i, j = i+1, j+stride {
		j &= numSlots - 1
		rowCol := slots[j]
		if rowCol != -1 {
			if err := dest.rowColUpdate(rowCol & destMask); err != nil {
				return err
			}
		}

	}

	return nil
}

func makeKxpByteLookup() []float64 {
	lookup := make([]float64, 256)
	for b := 0; b < 256; b++ {
		sum := 0.0
		for col := 0; col < 8; col++ {
			bit := (b >> col) & 1
			if bit == 0 {
				sumI, _ := internal.InvPow2(col + 1)
				sum += sumI
			}
		}
		lookup[b] = sum
	}
	return lookup
}

func checkLoPreamble(bytes []byte) error {
	if err := checkBounds(0, 8, len(bytes)); err != nil {
		return err
	}
	if bytes[loFieldSerVer] != (serVer & 0xFF) {
		return fmt.Errorf("SerVer: %d, bytes[loFieldSerVer]: %d", serVer&0xFF, bytes[loFieldSerVer])
	}
	fmat := getFormat(bytes)
	preIntsDef := preIntsDefs[fmat] & 0xFF
	if bytes[loFieldPreInts] != preIntsDef {
		return fmt.Errorf("preIntsDef: %d, bytes[loFieldPreInts]: %d", preIntsDef, bytes[loFieldPreInts])
	}
	fam := getFamilyId(bytes)
	if fam != internal.FamilyEnum.CPC.Id {
		return fmt.Errorf("family: %d, bytes[loFieldFamily]: %d", internal.FamilyEnum.CPC.Id, fam)
	}
	lgK := getLgK(bytes)
	if lgK < 4 || lgK > 26 {
		return fmt.Errorf("lgK: %d", lgK)
	}
	fiCol := bytes[loFieldFiCol] & 0xFF
	if fiCol > 63 {
		return fmt.Errorf("fiCol: %d", fiCol)
	}
	return nil
}

func checkBounds(reqOff, reqLen, allocSize int) error {
	if reqOff < 0 || reqLen < 0 || reqOff+reqLen < 0 || allocSize-(reqOff+reqLen) < 0 {
		return fmt.Errorf("bounds Violation: reqOffset: %d, reqLength: %d, (reqOff + reqLen): %d, allocSize: %d", reqOff, reqLen, reqOff+reqLen, allocSize)
	}
	return nil
}

func checkCapacity(memCap, expectedCap int) error {
	if memCap < expectedCap {
		return fmt.Errorf("insufficient Image Bytes = %d, Expected = %d", memCap, expectedCap)
	}
	return nil
}

func isCompressed(bytes []byte) bool {
	return (getFlags(bytes) & compressedFlagMask) > 0
}

func getFamilyId(bytes []byte) int {
	return int(bytes[loFieldFamily] & 0xFF)
}

func getLgK(bytes []byte) int {
	return int(bytes[loFieldLgK] & 0xFF)
}

func getKxP(mem []byte) float64 {
	offset, _ := getHiFieldOffset(getFormat(mem), hiFieldKXP)
	return math.Float64frombits(binary.LittleEndian.Uint64(mem[offset:]))
}

func getHipAccum(bytes []byte) float64 {
	offset, _ := getHiFieldOffset(getFormat(bytes), hiFieldHipAccum)
	return math.Float64frombits(binary.LittleEndian.Uint64(bytes[offset : offset+8]))
}

func getSeedHash(mem []byte) int16 {
	return int16(binary.LittleEndian.Uint16(mem[loFieldSeedHash:]))
}

func getFormat(bytes []byte) CpcFormat {
	ord := getFormatOrdinal(bytes)
	return CpcFormat(ord)
}

func getFormatOrdinal(bytes []byte) int {
	flags := getFlags(bytes)
	return (flags >> 2) & 0x7
}

func getFlags(bytes []byte) int {
	return int(bytes[loFieldFlags] & 0xFF)
}

func getNumCoupons(bytes []byte) uint64 {
	offset, _ := getHiFieldOffset(getFormat(bytes), hiFieldNumCoupons)
	return uint64(binary.LittleEndian.Uint32(bytes[offset : offset+4]))
}

func getNumSV(bytes []byte) uint64 {
	offset, _ := getHiFieldOffset(getFormat(bytes), hiFieldNumSV)
	return uint64(binary.LittleEndian.Uint32(bytes[offset : offset+4]))
}

func getSvLengthInts(bytes []byte) int {
	offset, _ := getHiFieldOffset(getFormat(bytes), hiFieldSVLengthInts)
	return int(binary.LittleEndian.Uint32(bytes[offset : offset+4]))
}

func getWStream(bytes []byte) []int {
	offset, _ := getWStreamOffset(bytes)
	wLength := getWLengthInts(bytes)
	wStream := make([]int, wLength)
	for i := 0; i < wLength; i++ {
		wStream[i] = int(binary.LittleEndian.Uint32(bytes[offset : offset+4]))
		offset += 4
	}
	return wStream
}

func getWStreamOffset(mem []byte) (int, error) {
	// Get the format from the memory.
	format := getFormat(mem)
	// Check that a window is present.
	if getFlags(mem)&windowFlagMask <= 0 {
		return 0, fmt.Errorf("window not available for format %s", format.String())
	}
	offset, err := getHiFieldOffset(format, hiFieldWLengthInts)
	if err != nil {
		return 0, err
	}
	// Ensure there are enough bytes.
	if len(mem) < offset+4 {
		return 0, fmt.Errorf("insufficient memory length to read wLengthInts")
	}
	wLengthInts := int(binary.LittleEndian.Uint32(mem[offset : offset+4]))
	if wLengthInts == 0 {
		return 0, fmt.Errorf("wLengthInts cannot be zero")
	}
	preInts := getPreInts(mem)
	return preInts << 2, nil
}

func getSvStream(bytes []byte) []int {
	offset, _ := getSvStreamOffset(bytes)
	svLengthInts := getSvLengthInts(bytes)
	svStream := make([]int, svLengthInts)
	for i := 0; i < svLengthInts; i++ {
		svStream[i] = int(binary.LittleEndian.Uint32(bytes[offset : offset+4]))
		offset += 4
	}
	return svStream
}

func getSvStreamOffset(mem []byte) (int, error) {
	// Get the format from the byte slice.
	format := getFormat(mem)

	// If the memory does not have an SV stream, return an error.
	if getFlags(mem)&supValFlagMask <= 0 {
		return 0, fieldError(format, hiFieldSVLengthInts)
	}

	// Retrieve svLengthInts from the appropriate hi-field.
	offset, err := getHiFieldOffset(format, hiFieldSVLengthInts)
	if err != nil {
		return 0, err
	}
	svLengthInts := int(binary.LittleEndian.Uint32(mem[offset : offset+4]))
	if svLengthInts == 0 {
		return 0, fmt.Errorf("svLengthInts cannot be zero")
	}

	// Retrieve wLengthInts if a window is present.
	var wLengthInts int
	if getFlags(mem)&windowFlagMask > 0 {
		offset, err = getHiFieldOffset(format, hiFieldWLengthInts)
		if err != nil {
			return 0, err
		}
		wLengthInts = int(binary.LittleEndian.Uint32(mem[offset : offset+4]))
		if wLengthInts == 0 {
			return 0, fmt.Errorf("wLengthInts cannot be zero")
		}
	}

	// The offset for the SV stream is computed as: (preInts + wLengthInts) * 4.
	preInts := getPreInts(mem)
	return (preInts + wLengthInts) << 2, nil
}

func getWLengthInts(bytes []byte) int {
	offset, _ := getHiFieldOffset(getFormat(bytes), hiFieldWLengthInts)
	return int(binary.LittleEndian.Uint32(bytes[offset : offset+4]))
}

func getFiCol(bytes []byte) int {
	return int(bytes[loFieldFiCol] & 0xFF)
}

// getHiFieldOffset returns the defined byte offset from the start of the preamble given the Format and the HiField.
func getHiFieldOffset(format CpcFormat, hiField int) (int, error) {
	offset := int(hiFieldOffset[format][hiField]) & 0xFF
	if offset == 0 {
		return 0, fmt.Errorf("illegal operation: Format = %s, HiField = %d", format.String(), hiField)
	}
	return offset, nil
}

func determineCorrectOffset(lgK int, numCoupons uint64) int {
	c := int(numCoupons)
	k := int(1) << lgK
	tmp := (c << 3) - (19 * k) // 8C - 19K
	if tmp < 0 {
		return 0
	}
	return int(tmp >> (lgK + 3)) // tmp / 8K
}

// putEmptyMerged writes the empty merged preamble into the provided raw byte slice.
// mem is the output byte slice.
// lgK is the sketch parameter, and seedHash is the computed seed hash.
// Returns an error if the capacity is insufficient or if writing fails.
func putEmptyMerged(mem []byte, lgK int, seedHash int16) error {
	format := CpcFormatEmptyMerged
	preInts := byte(getDefinedPreInts(format))
	fiCol := byte(0)
	flags := byte((int(format) << 2) | compressedFlagMask)

	// Check that mem has at least 8 bytes.
	if err := checkCapacity(len(mem), 8); err != nil {
		return err
	}
	return putFirst8(mem, preInts, byte(lgK), fiCol, flags, seedHash)
}

func putEmptyHip(mem []byte, lgK int, seedHash int16) error {
	format := CpcFormatEmptyHip
	preInts := byte(getDefinedPreInts(format))
	fiCol := byte(0)
	flags := byte((int(format) << 2) | compressedFlagMask)
	if err := checkCapacity(len(mem), 8); err != nil {
		return err
	}
	return putFirst8(mem, preInts, byte(lgK), fiCol, flags, seedHash)
}

func putSparseHybridMerged(mem []byte, lgK int, numCoupons int, svLengthInts int, seedHash int16, svStream []int) error {
	// Set the format to SPARSE_HYBRID_MERGED.
	format := CpcFormatSparseHybridMerged
	preInts := byte(getDefinedPreInts(format))
	fiCol := byte(0)
	flags := byte((int(format) << 2) | compressedFlagMask)

	// Check that the memory slice has enough capacity.
	if err := checkCapacity(len(mem), 4*(int(preInts)+svLengthInts)); err != nil {
		return err
	}
	// Write the first 8 bytes (low preamble fields).
	if err := putFirst8(mem, preInts, byte(lgK), fiCol, flags, seedHash); err != nil {
		return err
	}

	// Write the high preamble fields.
	offset, err := getHiFieldOffset(format, hiFieldNumCoupons)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(numCoupons))
	offset, err = getHiFieldOffset(format, hiFieldSVLengthInts)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(svLengthInts))
	offset, err = getSvStreamOffset(mem)
	if err != nil {
		return err
	}
	for i := 0; i < svLengthInts; i++ {
		binary.LittleEndian.PutUint32(mem[offset+4*i:], uint32(svStream[i]))
	}
	return nil
}

func putSparseHybridHip(mem []byte, lgK int, numCoupons, svLengthInts int, kxp, hipAccum float64, seedHash int16, svStream []int) error {
	// Set the format.
	format := CpcFormatSparseHybridHip
	preInts := byte(getDefinedPreInts(format))
	fiCol := byte(0)
	flags := byte((int(format) << 2) | compressedFlagMask)

	// Check that mem has enough capacity: 4 bytes per preInt + 4 bytes per sv integer.
	if err := checkCapacity(len(mem), 4*(int(preInts)+svLengthInts)); err != nil {
		return err
	}

	// Write the low preamble fields.
	if err := putFirst8(mem, preInts, byte(lgK), fiCol, flags, seedHash); err != nil {
		return err
	}

	// Write the high preamble fields.
	offset, err := getHiFieldOffset(format, hiFieldNumCoupons)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(numCoupons))

	offset, err = getHiFieldOffset(format, hiFieldSVLengthInts)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(svLengthInts))

	offset, err = getHiFieldOffset(format, hiFieldKXP)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(mem[offset:], math.Float64bits(kxp))

	offset, err = getHiFieldOffset(format, hiFieldHipAccum)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(mem[offset:], math.Float64bits(hipAccum))

	// Write the SV stream into memory.
	offset, err = getSvStreamOffset(mem)
	if err != nil {
		return err
	}
	for i := 0; i < svLengthInts; i++ {
		binary.LittleEndian.PutUint32(mem[offset+4*i:], uint32(svStream[i]))
	}

	return nil
}

func putPinnedSlidingMergedNoSv(mem []byte, lgK int, fiCol int, numCoupons int, wLengthInts int, seedHash int16, wStream []int) error {
	// Set the format to PINNED_SLIDING_MERGED_NOSV.
	format := CpcFormatPinnedSlidingMergedNosv
	preInts := byte(getDefinedPreInts(format))
	flags := byte((int(format) << 2) | compressedFlagMask)

	// Check that the memory slice has enough capacity.
	if err := checkCapacity(len(mem), 4*(int(preInts)+wLengthInts)); err != nil {
		return err
	}

	// Write the low preamble fields.
	if err := putFirst8(mem, preInts, byte(lgK), byte(fiCol), flags, seedHash); err != nil {
		return err
	}

	// Write the high preamble fields.
	offset, err := getHiFieldOffset(format, hiFieldNumCoupons)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(numCoupons))

	offset, err = getHiFieldOffset(format, hiFieldWLengthInts)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(wLengthInts))

	// Write the window stream array.
	offset, err = getWStreamOffset(mem)
	if err != nil {
		return err
	}
	for i := 0; i < wLengthInts; i++ {
		binary.LittleEndian.PutUint32(mem[offset+4*i:], uint32(wStream[i]))
	}
	return nil
}

func putPinnedSlidingHipNoSv(mem []byte, lgK int, fiCol int, numCoupons int, wLengthInts int, kxp float64, hipAccum float64, seedHash int16, wStream []int) error {
	// Set the format to PINNED_SLIDING_HIP_NOSV.
	format := CpcFormatPinnedSlidingHipNosv
	preInts := byte(getDefinedPreInts(format))
	flags := byte((int(format) << 2) | compressedFlagMask)

	// Check that the memory slice has enough capacity.
	if err := checkCapacity(len(mem), 4*(int(preInts)+wLengthInts)); err != nil {
		return err
	}

	// Write the low preamble fields.
	if err := putFirst8(mem, preInts, byte(lgK), byte(fiCol), flags, seedHash); err != nil {
		return err
	}

	// Write the high preamble fields.
	offset, err := getHiFieldOffset(format, hiFieldNumCoupons)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(numCoupons))

	offset, err = getHiFieldOffset(format, hiFieldWLengthInts)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(wLengthInts))

	offset, err = getHiFieldOffset(format, hiFieldKXP)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(mem[offset:], math.Float64bits(kxp))

	offset, err = getHiFieldOffset(format, hiFieldHipAccum)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(mem[offset:], math.Float64bits(hipAccum))

	// Write the window stream array.
	offset, err = getWStreamOffset(mem)
	if err != nil {
		return err
	}
	for i := 0; i < wLengthInts; i++ {
		binary.LittleEndian.PutUint32(mem[offset+4*i:], uint32(wStream[i]))
	}
	return nil
}

func putPinnedSlidingMerged(mem []byte, lgK int, fiCol int, numCoupons int, numSv int, svLengthInts int, wLengthInts int, seedHash int16, svStream []int, wStream []int) error {
	// Set the format.
	format := CpcFormatPinnedSlidingMerged
	preInts := byte(getDefinedPreInts(format))
	flags := byte((int(format) << 2) | compressedFlagMask)

	// Check that the memory slice has enough capacity.
	if err := checkCapacity(len(mem), 4*(int(preInts)+svLengthInts+wLengthInts)); err != nil {
		return err
	}
	// Write the low preamble fields.
	if err := putFirst8(mem, preInts, byte(lgK), byte(fiCol), flags, seedHash); err != nil {
		return err
	}

	// Write high preamble fields.
	offset, err := getHiFieldOffset(format, hiFieldNumCoupons)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(numCoupons))

	offset, err = getHiFieldOffset(format, hiFieldNumSV)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(numSv))

	offset, err = getHiFieldOffset(format, hiFieldSVLengthInts)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(svLengthInts))

	offset, err = getHiFieldOffset(format, hiFieldWLengthInts)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(wLengthInts))

	// Write the SV stream array.
	offset, err = getSvStreamOffset(mem)
	if err != nil {
		return err
	}
	for i := 0; i < svLengthInts; i++ {
		binary.LittleEndian.PutUint32(mem[offset+4*i:], uint32(svStream[i]))
	}

	// Write the W stream array.
	offset, err = getWStreamOffset(mem)
	if err != nil {
		return err
	}
	for i := 0; i < wLengthInts; i++ {
		binary.LittleEndian.PutUint32(mem[offset+4*i:], uint32(wStream[i]))
	}
	return nil
}

func putPinnedSlidingHip(mem []byte, lgK int, fiCol int, numCoupons int, numSv int, kxp float64, hipAccum float64, svLengthInts int, wLengthInts int, seedHash int16, svStream []int, wStream []int) error {
	// Set the format.
	format := CpcFormatPinnedSlidingHip
	preInts := byte(getDefinedPreInts(format))
	flags := byte((int(format) << 2) | compressedFlagMask)

	// Check that the memory slice has enough capacity.
	if err := checkCapacity(len(mem), 4*(int(preInts)+svLengthInts+wLengthInts)); err != nil {
		return err
	}
	// Write the low preamble fields.
	if err := putFirst8(mem, preInts, byte(lgK), byte(fiCol), flags, seedHash); err != nil {
		return err
	}

	// Write high preamble fields.
	offset, err := getHiFieldOffset(format, hiFieldNumCoupons)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(numCoupons))

	offset, err = getHiFieldOffset(format, hiFieldNumSV)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(numSv))

	offset, err = getHiFieldOffset(format, hiFieldKXP)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(mem[offset:], math.Float64bits(kxp))

	offset, err = getHiFieldOffset(format, hiFieldHipAccum)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(mem[offset:], math.Float64bits(hipAccum))

	offset, err = getHiFieldOffset(format, hiFieldSVLengthInts)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(svLengthInts))

	offset, err = getHiFieldOffset(format, hiFieldWLengthInts)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(mem[offset:], uint32(wLengthInts))

	// Write the SV stream array.
	offset, err = getSvStreamOffset(mem)
	if err != nil {
		return err
	}
	for i := 0; i < svLengthInts; i++ {
		binary.LittleEndian.PutUint32(mem[offset+4*i:], uint32(svStream[i]))
	}

	// Write the W stream array.
	offset, err = getWStreamOffset(mem)
	if err != nil {
		return err
	}
	for i := 0; i < wLengthInts; i++ {
		binary.LittleEndian.PutUint32(mem[offset+4*i:], uint32(wStream[i]))
	}
	return nil
}

func putFirst8(mem []byte, preInts, lgK, fiCol, flags byte, seedHash int16) error {
	// Compute the total number of bytes to clear (4 bytes per preInt)
	requiredBytes := int(preInts) * 4
	if err := checkCapacity(len(mem), requiredBytes); err != nil {
		return err
	}
	// Clear the entire preamble region
	for i := 0; i < requiredBytes; i++ {
		mem[i] = 0
	}

	// Write the low preamble fields at their fixed offsets.
	mem[loFieldPreInts] = preInts
	mem[loFieldSerVer] = serVer
	mem[loFieldFamily] = byte(internal.FamilyEnum.CPC.Id)
	mem[loFieldLgK] = lgK
	mem[loFieldFiCol] = fiCol
	mem[loFieldFlags] = flags

	// Write the seed hash (2 bytes) at its offset (bytes 6-7).
	binary.LittleEndian.PutUint16(mem[loFieldSeedHash:], uint16(seedHash))
	return nil
}

func CpcSketchToString(mem []byte, detail bool) (string, error) {
	LS := "\n"
	capBytes := len(mem)

	// Low preamble fields (first 8 bytes)
	preInts := int(mem[loFieldPreInts]) & 0xFF
	serVerVal := int(mem[loFieldSerVer]) & 0xFF
	family := getFamilyId(mem)
	lgK := int(mem[loFieldLgK]) & 0xFF
	fiCol := int(mem[loFieldFiCol]) & 0xFF
	flags := int(mem[loFieldFlags]) & 0xFF
	seedHash := int(binary.LittleEndian.Uint16(mem[loFieldSeedHash:]))
	seedHashStr := fmt.Sprintf("%x", seedHash)

	flagsStr := zeroPad(fmt.Sprintf("%b", flags), 8) + ", " + fmt.Sprintf("%d", flags)
	bigEndian := (flags & bigEndianFlagMask) > 0
	compressed := (flags & compressedFlagMask) > 0
	hasHipVal := (flags & hipFlagMask) > 0
	hasSVVal := (flags & supValFlagMask) > 0
	hasWindowVal := (flags & windowFlagMask) > 0

	formatOrdinal := (flags >> 2) & 0x7
	format := CpcFormat(formatOrdinal)
	nativeOrderStr := "LittleEndian"

	var numCoupons, numSv, winOffset, svLengthInts, wLengthInts int64
	var kxp, hipAccum float64
	var svStreamStart, wStreamStart int64
	var reqBytes int64

	sb := &strings.Builder{}
	sb.WriteString(LS)
	sb.WriteString("### CPC SKETCH IMAGE - PREAMBLE:" + LS)
	sb.WriteString(fmt.Sprintf("Format                          : %s%s", format.String(), LS))
	sb.WriteString(fmt.Sprintf("Byte 0: Preamble Ints           : %d%s", preInts, LS))
	sb.WriteString(fmt.Sprintf("Byte 1: SerVer                  : %d%s", serVerVal, LS))
	sb.WriteString(fmt.Sprintf("Byte 2: Family                  : %d%s", family, LS))
	sb.WriteString(fmt.Sprintf("Byte 3: lgK                     : %d%s", lgK, LS))
	sb.WriteString(fmt.Sprintf("Byte 4: First Interesting Col   : %d%s", fiCol, LS))
	sb.WriteString(fmt.Sprintf("Byte 5: Flags                   : %s%s", flagsStr, LS))
	sb.WriteString(fmt.Sprintf("  BIG_ENDIAN_STORAGE            : %t%s", bigEndian, LS))
	sb.WriteString(fmt.Sprintf("  (Native Byte Order)           : %s%s", nativeOrderStr, LS))
	sb.WriteString(fmt.Sprintf("  Compressed                    : %t%s", compressed, LS))
	sb.WriteString(fmt.Sprintf("  Has HIP                       : %t%s", hasHipVal, LS))
	sb.WriteString(fmt.Sprintf("  Has Surprising Values         : %t%s", hasSVVal, LS))
	sb.WriteString(fmt.Sprintf("  Has Window Values             : %t%s", hasWindowVal, LS))
	sb.WriteString(fmt.Sprintf("Byte 6, 7: Seed Hash            : %s%s", seedHashStr, LS))

	var flavor string
	switch format {
	case CpcFormatEmptyMerged, CpcFormatEmptyHip:
		flavor = determineFlavor(lgK, uint64(numCoupons)).String()
		sb.WriteString(fmt.Sprintf("Flavor                          : %s%s", flavor, LS))
	case CpcFormatSparseHybridMerged:
		// NUM_COUPONS
		offset, err := getHiFieldOffset(format, hiFieldNumCoupons)
		if err != nil {
			return "", err
		}
		numCoupons = int64(binary.LittleEndian.Uint32(mem[offset:]))
		numSv = numCoupons

		// SV_LENGTH_INTS
		offset, err = getHiFieldOffset(format, hiFieldSVLengthInts)
		if err != nil {
			return "", err
		}
		svLengthInts = int64(binary.LittleEndian.Uint32(mem[offset:]))

		// SV Stream offset
		offset, err = getSvStreamOffset(mem)
		if err != nil {
			return "", err
		}
		svStreamStart = int64(offset)
		reqBytes = svStreamStart + (svLengthInts << 2)
		flavor = determineFlavor(lgK, uint64(numCoupons)).String()
		sb.WriteString(fmt.Sprintf("Flavor                          : %s%s", flavor, LS))
		sb.WriteString(fmt.Sprintf("Num Coupons                     : %d%s", numCoupons, LS))
		sb.WriteString(fmt.Sprintf("Num SV                          : %d%s", numSv, LS))
		sb.WriteString(fmt.Sprintf("SV Length Ints                  : %d%s", svLengthInts, LS))
		sb.WriteString(fmt.Sprintf("SV Stream Start                 : %d%s", svStreamStart, LS))
	case CpcFormatSparseHybridHip:
		offset, err := getHiFieldOffset(format, hiFieldNumCoupons)
		if err != nil {
			return "", err
		}
		numCoupons = int64(binary.LittleEndian.Uint32(mem[offset:]))
		numSv = numCoupons

		offset, err = getHiFieldOffset(format, hiFieldSVLengthInts)
		if err != nil {
			return "", err
		}
		svLengthInts = int64(binary.LittleEndian.Uint32(mem[offset:]))

		offset, err = getSvStreamOffset(mem)
		if err != nil {
			return "", err
		}
		svStreamStart = int64(offset)

		offset, err = getHiFieldOffset(format, hiFieldKXP)
		if err != nil {
			return "", err
		}
		kxp = math.Float64frombits(binary.LittleEndian.Uint64(mem[offset:]))

		offset, err = getHiFieldOffset(format, hiFieldHipAccum)
		if err != nil {
			return "", err
		}
		hipAccum = math.Float64frombits(binary.LittleEndian.Uint64(mem[offset:]))

		reqBytes = svStreamStart + (svLengthInts << 2)
		flavor = determineFlavor(lgK, uint64(numCoupons)).String()
		sb.WriteString(fmt.Sprintf("Flavor                          : %s%s", flavor, LS))
		sb.WriteString(fmt.Sprintf("Num Coupons                     : %d%s", numCoupons, LS))
		sb.WriteString(fmt.Sprintf("Num SV                          : %d%s", numSv, LS))
		sb.WriteString(fmt.Sprintf("SV Length Ints                  : %d%s", svLengthInts, LS))
		sb.WriteString(fmt.Sprintf("SV Stream Start                 : %d%s", svStreamStart, LS))
		sb.WriteString(fmt.Sprintf("KxP                             : %f%s", kxp, LS))
		sb.WriteString(fmt.Sprintf("HipAccum                        : %f%s", hipAccum, LS))
	case CpcFormatPinnedSlidingMergedNosv:
		offset, err := getHiFieldOffset(format, hiFieldNumCoupons)
		if err != nil {
			return "", err
		}
		numCoupons = int64(binary.LittleEndian.Uint32(mem[offset:]))
		winOffset = int64(determineCorrectOffset(lgK, uint64(numCoupons)))

		offset, err = getHiFieldOffset(format, hiFieldWLengthInts)
		if err != nil {
			return "", err
		}
		wLengthInts = int64(binary.LittleEndian.Uint32(mem[offset:]))

		offset, err = getWStreamOffset(mem)
		if err != nil {
			return "", err
		}
		wStreamStart = int64(offset)

		reqBytes = wStreamStart + (wLengthInts << 2)
		flavor = determineFlavor(lgK, uint64(numCoupons)).String()
		sb.WriteString(fmt.Sprintf("Flavor                          : %s%s", flavor, LS))
		sb.WriteString(fmt.Sprintf("Num Coupons                     : %d%s", numCoupons, LS))
		sb.WriteString(fmt.Sprintf("Window Offset                   : %d%s", winOffset, LS))
		sb.WriteString(fmt.Sprintf("Window Length Ints              : %d%s", wLengthInts, LS))
		sb.WriteString(fmt.Sprintf("Window Stream Start             : %d%s", wStreamStart, LS))
	case CpcFormatPinnedSlidingHipNosv:
		offset, err := getHiFieldOffset(format, hiFieldNumCoupons)
		if err != nil {
			return "", err
		}
		numCoupons = int64(binary.LittleEndian.Uint32(mem[offset:]))
		winOffset = int64(determineCorrectOffset(lgK, uint64(numCoupons)))

		offset, err = getHiFieldOffset(format, hiFieldWLengthInts)
		if err != nil {
			return "", err
		}
		wLengthInts = int64(binary.LittleEndian.Uint32(mem[offset:]))

		offset, err = getWStreamOffset(mem)
		if err != nil {
			return "", err
		}
		wStreamStart = int64(offset)

		offset, err = getHiFieldOffset(format, hiFieldKXP)
		if err != nil {
			return "", err
		}
		kxp = math.Float64frombits(binary.LittleEndian.Uint64(mem[offset:]))

		offset, err = getHiFieldOffset(format, hiFieldHipAccum)
		if err != nil {
			return "", err
		}
		hipAccum = math.Float64frombits(binary.LittleEndian.Uint64(mem[offset:]))

		reqBytes = wStreamStart + (wLengthInts << 2)
		flavor = determineFlavor(lgK, uint64(numCoupons)).String()
		sb.WriteString(fmt.Sprintf("Flavor                          : %s%s", flavor, LS))
		sb.WriteString(fmt.Sprintf("Num Coupons                     : %d%s", numCoupons, LS))
		sb.WriteString(fmt.Sprintf("Window Offset                   : %d%s", winOffset, LS))
		sb.WriteString(fmt.Sprintf("Window Length Ints              : %d%s", wLengthInts, LS))
		sb.WriteString(fmt.Sprintf("Window Stream Start             : %d%s", wStreamStart, LS))
		sb.WriteString(fmt.Sprintf("KxP                             : %f%s", kxp, LS))
		sb.WriteString(fmt.Sprintf("HipAccum                        : %f%s", hipAccum, LS))
	case CpcFormatPinnedSlidingMerged:
		offset, err := getHiFieldOffset(format, hiFieldNumCoupons)
		if err != nil {
			return "", err
		}
		numCoupons = int64(binary.LittleEndian.Uint32(mem[offset:]))
		winOffset = int64(determineCorrectOffset(lgK, uint64(numCoupons)))

		offset, err = getHiFieldOffset(format, hiFieldWLengthInts)
		if err != nil {
			return "", err
		}
		wLengthInts = int64(binary.LittleEndian.Uint32(mem[offset:]))

		offset, err = getHiFieldOffset(format, hiFieldNumSV)
		if err != nil {
			return "", err
		}
		numSv = int64(binary.LittleEndian.Uint32(mem[offset:]))

		offset, err = getHiFieldOffset(format, hiFieldSVLengthInts)
		if err != nil {
			return "", err
		}
		svLengthInts = int64(binary.LittleEndian.Uint32(mem[offset:]))

		offset, err = getWStreamOffset(mem)
		if err != nil {
			return "", err
		}
		wStreamStart = int64(offset)

		offset, err = getSvStreamOffset(mem)
		if err != nil {
			return "", err
		}
		svStreamStart = int64(offset)

		reqBytes = svStreamStart + (svLengthInts << 2)
		flavor = determineFlavor(lgK, uint64(numCoupons)).String()
		sb.WriteString(fmt.Sprintf("Flavor                          : %s%s", flavor, LS))
		sb.WriteString(fmt.Sprintf("Num Coupons                     : %d%s", numCoupons, LS))
		sb.WriteString(fmt.Sprintf("Num SV                          : %d%s", numSv, LS))
		sb.WriteString(fmt.Sprintf("SV Length Ints                  : %d%s", svLengthInts, LS))
		sb.WriteString(fmt.Sprintf("SV Stream Start                 : %d%s", svStreamStart, LS))
		sb.WriteString(fmt.Sprintf("Window Offset                   : %d%s", winOffset, LS))
		sb.WriteString(fmt.Sprintf("Window Length Ints              : %d%s", wLengthInts, LS))
		sb.WriteString(fmt.Sprintf("Window Stream Start             : %d%s", wStreamStart, LS))
	case CpcFormatPinnedSlidingHip:
		offset, err := getHiFieldOffset(format, hiFieldNumCoupons)
		if err != nil {
			return "", err
		}
		numCoupons = int64(binary.LittleEndian.Uint32(mem[offset:]))
		winOffset = int64(determineCorrectOffset(lgK, uint64(numCoupons)))

		offset, err = getHiFieldOffset(format, hiFieldWLengthInts)
		if err != nil {
			return "", err
		}
		wLengthInts = int64(binary.LittleEndian.Uint32(mem[offset:]))

		offset, err = getHiFieldOffset(format, hiFieldNumSV)
		if err != nil {
			return "", err
		}
		numSv = int64(binary.LittleEndian.Uint32(mem[offset:]))

		offset, err = getHiFieldOffset(format, hiFieldSVLengthInts)
		if err != nil {
			return "", err
		}
		svLengthInts = int64(binary.LittleEndian.Uint32(mem[offset:]))

		offset, err = getWStreamOffset(mem)
		if err != nil {
			return "", err
		}
		wStreamStart = int64(offset)

		offset, err = getSvStreamOffset(mem)
		if err != nil {
			return "", err
		}
		svStreamStart = int64(offset)

		offset, err = getHiFieldOffset(format, hiFieldKXP)
		if err != nil {
			return "", err
		}
		kxp = math.Float64frombits(binary.LittleEndian.Uint64(mem[offset:]))

		offset, err = getHiFieldOffset(format, hiFieldHipAccum)
		if err != nil {
			return "", err
		}
		hipAccum = math.Float64frombits(binary.LittleEndian.Uint64(mem[offset:]))
		reqBytes = svStreamStart + (svLengthInts << 2)
		flavor = determineFlavor(lgK, uint64(numCoupons)).String()
		sb.WriteString(fmt.Sprintf("Flavor                          : %s%s", flavor, LS))
		sb.WriteString(fmt.Sprintf("Num Coupons                     : %d%s", numCoupons, LS))
		sb.WriteString(fmt.Sprintf("Num SV                          : %d%s", numSv, LS))
		sb.WriteString(fmt.Sprintf("SV Length Ints                  : %d%s", svLengthInts, LS))
		sb.WriteString(fmt.Sprintf("SV Stream Start                 : %d%s", svStreamStart, LS))
		sb.WriteString(fmt.Sprintf("Window Offset                   : %d%s", winOffset, LS))
		sb.WriteString(fmt.Sprintf("Window Length Ints              : %d%s", wLengthInts, LS))
		sb.WriteString(fmt.Sprintf("Window Stream Start             : %d%s", wStreamStart, LS))
		sb.WriteString(fmt.Sprintf("KxP                             : %f%s", kxp, LS))
		sb.WriteString(fmt.Sprintf("HipAccum                        : %f%s", hipAccum, LS))
	}

	sb.WriteString(fmt.Sprintf("Actual Bytes                    : %d%s", capBytes, LS))
	sb.WriteString(fmt.Sprintf("Required Bytes                  : %d%s", reqBytes, LS))

	if detail {
		sb.WriteString(LS + "### CPC SKETCH IMAGE - DATA" + LS)
		if wLengthInts > 0 {
			sb.WriteString(LS + "Window Stream:" + LS)
			listData(mem, int(wStreamStart), int(wLengthInts), sb)
		}
		if svLengthInts > 0 {
			sb.WriteString(LS + "SV Stream:" + LS)
			listData(mem, int(svStreamStart), int(svLengthInts), sb)
		}
	}
	sb.WriteString("### END CPC SKETCH IMAGE" + LS)
	return sb.String(), nil
}

// zeroPad returns the given string s prepended with zeros so that the total length is at least fieldLength.
// If s is already fieldLength or longer, it returns s unchanged.
func zeroPad(s string, fieldLength int) string {
	return characterPad(s, fieldLength, '0', false)
}

// characterPad returns s padded with the given padChar to reach fieldLength characters.
// If append is false, the padding is added to the beginning of s; if true, to the end.
// If s is already at least fieldLength characters long, s is returned unchanged.
func characterPad(s string, fieldLength int, padChar rune, append bool) string {
	if len(s) >= fieldLength {
		return s
	}
	padCount := fieldLength - len(s)
	pad := strings.Repeat(string(padChar), padCount)
	if append {
		return s + pad
	}
	return pad + s
}

// listData reads lengthInts integers from mem starting at offsetBytes,
// and appends each formatted value (using dataFmt) to the provided strings.Builder.
func listData(mem []byte, offsetBytes, lengthInts int, sb *strings.Builder) {
	memCap := len(mem)
	expectedCap := offsetBytes + 4*lengthInts
	if err := checkCapacity(memCap, expectedCap); err != nil {
		panic(err)
	}
	for i := 0; i < lengthInts; i++ {
		start := offsetBytes + 4*i
		// Read 4 bytes as an uint32 (assuming little-endian).
		value := int(binary.LittleEndian.Uint32(mem[start : start+4]))
		sb.WriteString(fmt.Sprintf(dataFmt, i, value))
		sb.WriteString("\n")
	}
}
