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

// Package hll is dedicated to streaming algorithms that enable estimation of the
// cardinality of a stream of items.
//
// HllSketch and Union are the public facing classes of this high performance implementation of Phillipe Flajolet's
// HyperLogLog algorithm but with significantly improved error behavior and important features that can be
// essential for large production systems that must handle massive data.
package hll

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"unsafe"

	"github.com/apache/datasketches-go/internal"
	"github.com/twmb/murmur3"
)

type HllSketch interface {
	// Copy returns a clone of this sketch.
	Copy() (HllSketch, error)

	// CopyAs returns a clone of this sketch with the specified TgtHllType.
	//
	//   - tgtHllType, the TgtHllType enum
	CopyAs(tgtHllType TgtHllType) (HllSketch, error)

	// GetCompositeEstimate is less accurate than GetEstimate method and is automatically used
	// when the sketch has gone through union operations where the more accurate HIP estimator
	// cannot be used
	// This is made public only for error characterization  software that exists in separate package and is not
	// intended for normal use.
	GetCompositeEstimate() (float64, error)

	// GetEstimate returns the cardinality estimate
	GetEstimate() (float64, error)

	// UpdateUInt64 present the given unsigned 64-bit integer as a potential unique item.
	UpdateUInt64(datum uint64) error

	// UpdateInt64 present the given signed 64-bit integer as a potential unique item.
	UpdateInt64(datum int64) error

	// UpdateSlice present the given byte slice as a potential unique item.
	UpdateSlice(datum []byte) error

	// UpdateString present the given string as a potential unique item.
	UpdateString(datum string) error

	// Reset resets the sketch to empty, but does not change the configured values of lgConfigK and tgtHllType.
	Reset() error

	// GetLowerBound gets the approximate lower error bound given the specifified numbers of standard deviations.
	//
	//   - numStdDev, this must be an integer between 1 and 3, inclusive.
	GetLowerBound(numStdDev int) (float64, error)

	// GetUpperBound gets the approximate upper error bound given the specified number of standard deviations.
	//
	//   - numStdDev, this must be an integer between 1 and 3, inclusive.
	GetUpperBound(numStdDev int) (float64, error)

	// IsEmpty returns true if the sketch is empty.
	IsEmpty() bool

	// GetLgConfigK returns the lgConfigK of the sketch.
	GetLgConfigK() int

	// GetTgtHllType returns the TgtHllType of the sketch.
	GetTgtHllType() TgtHllType

	// GetCurMode returns the current mode of the sketch: LIST, SET, HLL.
	GetCurMode() curMode

	// GetUpdatableSerializationBytes gets the size in bytes of the current sketch when serialized using
	// ToUpdatableSlice.
	GetUpdatableSerializationBytes() int

	// ToCompactSlice serializes the sketch to a slice, compacting data structures
	// where feasible to eliminate unused storage in the serialized image.
	ToCompactSlice() ([]byte, error)

	// ToUpdatableSlice serializes the sketch as a byte slice in an updatable form.
	// The updatable form is larger than the compact form.
	ToUpdatableSlice() ([]byte, error)

	GetSerializationVersion() int

	couponUpdate(coupon int) (hllSketchStateI, error)
	iterator() pairIterator
}

type hllSketchStateI interface {
	GetCompositeEstimate() (float64, error)
	GetEstimate() (float64, error)
	GetHipEstimate() (float64, error)
	GetLowerBound(numStdDev int) (float64, error)
	GetUpperBound(numStdDev int) (float64, error)
	IsEmpty() bool

	GetLgConfigK() int
	GetTgtHllType() TgtHllType
	GetCurMode() curMode

	GetUpdatableSerializationBytes() int
	ToCompactSlice() ([]byte, error)
	ToUpdatableSlice() ([]byte, error)

	getMemDataStart() int
	getPreInts() int
	isOutOfOrder() bool
	isRebuildCurMinNumKxQFlag() bool

	putOutOfOrder(oooFlag bool)
	putRebuildCurMinNumKxQFlag(rebuildCurMinNumKxQFlag bool)
	copyAs(tgtHllType TgtHllType) (hllSketchStateI, error)
	copy() (hllSketchStateI, error)
	mergeTo(dest HllSketch) error

	couponUpdate(coupon int) (hllSketchStateI, error)
	iterator() pairIterator
}

type hllSketchState struct { // extends BaseHllSketch
	sketch  hllSketchStateI
	scratch [8]byte
}

func newHllSketchState(coupon hllSketchStateI) HllSketch {
	return &hllSketchState{
		sketch:  coupon,
		scratch: [8]byte{},
	}
}

// NewHllSketch constructs a new sketch with the type of HLL sketch to configure
//
//   - lgConfigK, the Log2 of K for the target HLL sketch. This value must be
//
// between 4 and 21 inclusively.
//
//   - tgtHllType. the desired HLL type.
func NewHllSketch(lgConfigK int, tgtHllType TgtHllType) (HllSketch, error) {
	lgK := lgConfigK
	lgK, err := checkLgK(lgK)
	if err != nil {
		return nil, err
	}
	couponList, err := newCouponList(lgK, tgtHllType, curModeList)
	if err != nil {
		return nil, err
	}
	return newHllSketchState(&couponList), nil
}

// NewHllSketchWithDefault constructs a new on-heap sketch with the default lgK and tgtHllType.
func NewHllSketchWithDefault() (HllSketch, error) {
	return NewHllSketch(defaultLgK, TgtHllTypeDefault)
}

// NewHllSketchFromSlice deserialize a given byte slice, which must be a valid HllSketch image and may have data.
//
//   - bytes, the given byte slice, this slice is not modified and is not retained by the sketch
func NewHllSketchFromSlice(bytes []byte, checkRebuild bool) (HllSketch, error) {
	if len(bytes) < 8 {
		return nil, fmt.Errorf("input array too small: %d", len(bytes))
	}
	curMode, err := checkPreamble(bytes)
	if err != nil {
		return nil, err
	}
	if curMode == curModeHll {
		tgtHllType := extractTgtHllType(bytes)
		if tgtHllType == TgtHllTypeHll4 {
			sk, err := deserializeHll4(bytes)
			if err != nil {
				return nil, err
			}
			return newHllSketchState(sk), nil
		} else if tgtHllType == TgtHllTypeHll6 {
			return newHllSketchState(deserializeHll6(bytes)), nil
		} else {
			a := newHllSketchState(deserializeHll8(bytes))
			if checkRebuild {
				err := checkRebuildCurMinNumKxQ(a)
				if err != nil {
					return nil, err
				}
			}
			return a, nil
		}
	} else if curMode == curModeList {
		cp, err := deserializeCouponList(bytes)
		if err != nil {
			return nil, err
		}
		return newHllSketchState(cp), nil
	} else {
		chs, err := deserializeCouponHashSet(bytes)
		if err != nil {
			return nil, err
		}
		return newHllSketchState(chs), nil
	}
}

func (h *hllSketchState) Copy() (HllSketch, error) {
	sketch, err := h.sketch.copy()
	if err != nil {
		return nil, err
	}
	return newHllSketchState(sketch), nil
}

func (h *hllSketchState) CopyAs(tgtHllType TgtHllType) (HllSketch, error) {
	sketch, err := h.sketch.copyAs(tgtHllType)
	if err != nil {
		return nil, err
	}
	return newHllSketchState(sketch), nil
}

func (h *hllSketchState) GetCompositeEstimate() (float64, error) {
	return h.sketch.GetCompositeEstimate()
}

func (h *hllSketchState) GetEstimate() (float64, error) {
	return h.sketch.GetEstimate()
}

func (h *hllSketchState) GetHipEstimate() (float64, error) {
	return h.sketch.GetHipEstimate()
}

func (h *hllSketchState) GetUpperBound(numStdDev int) (float64, error) {
	return h.sketch.GetUpperBound(numStdDev)
}

func (h *hllSketchState) GetLowerBound(numStdDev int) (float64, error) {
	return h.sketch.GetLowerBound(numStdDev)
}

func (h *hllSketchState) GetUpdatableSerializationBytes() int {
	return h.sketch.GetUpdatableSerializationBytes()
}

func (h *hllSketchState) UpdateUInt64(datum uint64) error {
	binary.LittleEndian.PutUint64(h.scratch[:], datum)
	_, err := h.couponUpdate(coupon(h.hash(h.scratch[:])))
	return err
}

func (h *hllSketchState) UpdateInt64(datum int64) error {
	return h.UpdateUInt64(uint64(datum))
}

func (h *hllSketchState) UpdateSlice(datum []byte) error {
	if len(datum) == 0 {
		return nil
	}
	_, err := h.couponUpdate(coupon(h.hash(datum)))
	return err
}

func (h *hllSketchState) UpdateString(datum string) error {
	// get a slice to the string data (avoiding a copy to heap)
	return h.UpdateSlice(unsafe.Slice(unsafe.StringData(datum), len(datum)))
}

func (h *hllSketchState) IsEmpty() bool {
	return h.sketch.IsEmpty()
}

func (h *hllSketchState) ToCompactSlice() ([]byte, error) {
	return h.sketch.ToCompactSlice()
}

func (h *hllSketchState) ToUpdatableSlice() ([]byte, error) {
	return h.sketch.ToUpdatableSlice()
}

func (h *hllSketchState) GetLgConfigK() int {
	return h.sketch.GetLgConfigK()
}

func (h *hllSketchState) GetTgtHllType() TgtHllType {
	return h.sketch.GetTgtHllType()
}

func (h *hllSketchState) GetCurMode() curMode {
	return h.sketch.GetCurMode()
}

func (h *hllSketchState) Reset() error {
	lgK, err := checkLgK(h.sketch.GetLgConfigK())
	if err != nil {
		return err
	}
	couponList, err := newCouponList(lgK, h.sketch.GetTgtHllType(), curModeList)
	if err != nil {
		return err
	}
	h.sketch = &couponList
	return nil
}

func (h *hllSketchState) iterator() pairIterator {
	return h.sketch.iterator()
}

func coupon(hashLo uint64, hashHi uint64) int {
	addr26 := hashLo & keyMask26
	lz := uint64(bits.LeadingZeros64(hashHi))
	value := min(lz, 62) + 1
	return int((value << keyBits26) | addr26)
}

func (h *hllSketchState) couponUpdate(coupon int) (hllSketchStateI, error) {
	if (coupon >> keyBits26) == empty {
		return h.sketch, nil
	}
	sk, err := h.sketch.couponUpdate(coupon)
	h.sketch = sk
	return h.sketch, err
}

func (h *hllSketchState) putRebuildCurMinNumKxQFlag(rebuildCurMinNumKxQFlag bool) {
	h.sketch.putRebuildCurMinNumKxQFlag(rebuildCurMinNumKxQFlag)
}

func (h *hllSketchState) mergeTo(dest HllSketch) error {
	return h.sketch.mergeTo(dest)
}

// GetSerializationVersion returns the serialization version used by this sketch.
func (h *hllSketchState) GetSerializationVersion() int {
	return serVer
}

func (h *hllSketchState) hash(bs []byte) (uint64, uint64) {
	return murmur3.SeedSum128(internal.DEFAULT_UPDATE_SEED, internal.DEFAULT_UPDATE_SEED, bs)
}
