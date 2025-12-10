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
	"errors"
	"fmt"
	"iter"
	"math"
	"strings"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/internal/binomialbounds"
)

var (
	ErrUpdateEmptyString = errors.New("cannot update empty string")
	ErrDuplicateKey      = errors.New("duplicate key")
)

// QuickSelectUpdateSketch is an Update Theta sketch based on the QuickSelect algorithm.
// The purpose of this class is to build a Theta sketch from input data via the update() methods.
type QuickSelectUpdateSketch struct {
	table *Hashtable
}

type updateSketchOptions struct {
	theta     uint64
	seed      uint64
	p         float32
	lgCurSize uint8
	lgK       uint8
	rf        ResizeFactor
}

type UpdateSketchOptionFunc func(*updateSketchOptions)

// WithUpdateSketchLgK sets log2(k), where k is a nominal number of entries in the sketch
func WithUpdateSketchLgK(lgK uint8) UpdateSketchOptionFunc {
	return func(opts *updateSketchOptions) {
		opts.lgK = lgK
	}
}

// WithUpdateSketchResizeFactor sets a resize factor for the internal hash table (defaults to 8)
func WithUpdateSketchResizeFactor(rf ResizeFactor) UpdateSketchOptionFunc {
	return func(opts *updateSketchOptions) {
		opts.rf = rf
	}
}

// WithUpdateSketchP sets sampling probability (initial theta). The default is 1, so the sketch retains
// all entries until it reaches the limit, at which point it goes into the estimation mode
// and reduces the effective sampling probability (theta) as necessary
func WithUpdateSketchP(p float32) UpdateSketchOptionFunc {
	return func(opts *updateSketchOptions) {
		opts.p = p
	}
}

// WithUpdateSketchSeed sets the seed for the hash function. Should be used carefully if needed.
// Sketches produced with different seed are not compatible
// and cannot be mixed in set operations.
func WithUpdateSketchSeed(seed uint64) UpdateSketchOptionFunc {
	return func(opts *updateSketchOptions) {
		opts.seed = seed
	}
}

// NewQuickSelectUpdateSketch creates a new quickselect update sketch with the given options
func NewQuickSelectUpdateSketch(opts ...UpdateSketchOptionFunc) (*QuickSelectUpdateSketch, error) {
	options := &updateSketchOptions{
		lgK:  DefaultLgK,
		rf:   DefaultResizeFactor,
		p:    1.0,
		seed: DefaultSeed,
	}
	for _, opt := range opts {
		opt(options)
	}

	if options.lgK < MinLgK {
		return nil, fmt.Errorf("lg_k must not be less than %d: %d", MinLgK, options.lgK)
	}
	if options.lgK > MaxLgK {
		return nil, fmt.Errorf("lg_k must not be greater than %d: %d", MaxLgK, options.lgK)
	}
	if options.p <= 0 || options.p > 1 {
		return nil, errors.New("sampling probability must be between 0 and 1")
	}

	options.lgCurSize = startingSubMultiple(options.lgK+1, MinLgK, uint8(options.rf))
	options.theta = startingThetaFromP(options.p)

	return &QuickSelectUpdateSketch{
		table: NewHashtable(
			options.lgCurSize, options.lgK, options.rf, options.p, options.theta, options.seed, true,
		),
	}, nil
}

// IsEmpty returns true if this sketch represents an empty set
// (not the same as no retained entries!)
func (s *QuickSelectUpdateSketch) IsEmpty() bool {
	return s.table.isEmpty
}

// IsOrdered returns true if retained entries are ordered
func (s *QuickSelectUpdateSketch) IsOrdered() bool {
	return s.table.numEntries <= 1
}

// Theta64 returns theta as a positive integer between 0 and math.MaxInt64
func (s *QuickSelectUpdateSketch) Theta64() uint64 {
	if s.IsEmpty() {
		return MaxTheta
	}
	return s.table.theta
}

// NumRetained returns the number of retained entries in the sketch
func (s *QuickSelectUpdateSketch) NumRetained() uint32 {
	return s.table.numEntries
}

// SeedHash returns hash of the seed that was used to hash the input
func (s *QuickSelectUpdateSketch) SeedHash() (uint16, error) {
	seedHash, err := internal.ComputeSeedHash(int64(s.table.seed))
	if err != nil {
		return 0, err
	}
	return uint16(seedHash), nil
}

// Estimate returns estimate of the distinct count of the input stream
func (s *QuickSelectUpdateSketch) Estimate() float64 {
	return float64(s.NumRetained()) / s.Theta()
}

// LowerBound returns the approximate lower error bound given a number of standard deviations.
// This parameter is similar to the number of standard deviations of the normal distribution
// and corresponds to approximately 67%, 95% and 99% confidence intervals.
// numStdDevs number of Standard Deviations (1, 2 or 3)
func (s *QuickSelectUpdateSketch) LowerBound(numStdDevs uint8) (float64, error) {
	if !s.IsEstimationMode() {
		return float64(s.NumRetained()), nil
	}
	return binomialbounds.LowerBound(uint64(s.NumRetained()), s.Theta(), uint(numStdDevs))
}

// UpperBound returns the approximate upper error bound given a number of standard deviations.
// This parameter is similar to the number of standard deviations of the normal distribution
// and corresponds to approximately 67%, 95% and 99% confidence intervals.
// numStdDevs number of Standard Deviations (1, 2 or 3)
func (s *QuickSelectUpdateSketch) UpperBound(numStdDevs uint8) (float64, error) {
	if !s.IsEstimationMode() {
		return float64(s.NumRetained()), nil
	}
	return binomialbounds.UpperBound(uint64(s.NumRetained()), s.Theta(), uint(numStdDevs))
}

// IsEstimationMode returns true if the sketch is in estimation mode
// (as opposed to exact mode)
func (s *QuickSelectUpdateSketch) IsEstimationMode() bool {
	return s.Theta64() < MaxTheta && !s.IsEmpty()
}

// Theta returns theta as a fraction from 0 to 1 (effective sampling rate)
func (s *QuickSelectUpdateSketch) Theta() float64 {
	return float64(s.Theta64()) / float64(MaxTheta)
}

// String returns a human-readable summary of this sketch as a string
// If shouldPrintItems is true, include the list of items retained by the sketch
func (s *QuickSelectUpdateSketch) String(shouldPrintItems bool) string {
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
	result.WriteString(fmt.Sprintf("   lg nominal size      : %d", s.LgK()))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   lg current size      : %d", s.table.lgCurSize))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   resize factor        : %d", 1<<s.ResizeFactor()))
	result.WriteString("\n")
	result.WriteString("### End sketch summary")
	result.WriteString("\n")

	if shouldPrintItems {
		result.WriteString("### Retained entries")
		result.WriteString("\n")

		for hash := range s.All() {
			result.WriteString(fmt.Sprintf("%d", hash))
			result.WriteString("\n")
		}

		result.WriteString("### End retained entries")
		result.WriteString("\n")
	}

	return result.String()
}

// LgK returns configured nominal number of entries in the sketch
func (s *QuickSelectUpdateSketch) LgK() uint8 {
	return s.table.lgNomSize
}

// ResizeFactor returns a configured resize factor of the sketch
func (s *QuickSelectUpdateSketch) ResizeFactor() ResizeFactor {
	return s.table.rf
}

// UpdateUint64 updates this sketch with a given unsigned 64-bit integer
// Only update when the value is not existing
func (s *QuickSelectUpdateSketch) UpdateUint64(value uint64) error {
	return s.UpdateInt64(int64(value))
}

// UpdateInt64 updates this sketch with a given signed 64-bit integer
// Only update when the value is not existing
func (s *QuickSelectUpdateSketch) UpdateInt64(value int64) error {
	hash, err := s.table.HashInt64AndScreen(value)
	if err != nil {
		return err
	}

	index, err := s.table.Find(hash)
	if err != nil {
		if err == ErrKeyNotFound {
			s.table.Insert(index, hash)
			return nil
		}
		return err
	}

	return ErrDuplicateKey
}

// UpdateUint32 updates this sketch with a given unsigned 32-bit integer
// Only update when the value is not existing
func (s *QuickSelectUpdateSketch) UpdateUint32(value uint32) error {
	return s.UpdateInt64(int64(value))
}

// UpdateInt32 updates this sketch with a given signed 32-bit integer
// Only update when the value is not existing
func (s *QuickSelectUpdateSketch) UpdateInt32(value int32) error {
	hash, err := s.table.HashInt32AndScreen(value)
	if err != nil {
		return err
	}

	index, err := s.table.Find(hash)
	if err != nil {
		if err == ErrKeyNotFound {
			s.table.Insert(index, hash)
			return nil
		}
		return err
	}

	return ErrDuplicateKey
}

// UpdateUint16 updates this sketch with a given unsigned 16-bit integer
// Only update when the value is not existing
func (s *QuickSelectUpdateSketch) UpdateUint16(value uint16) error {
	return s.UpdateInt32(int32(value))
}

// UpdateInt16 updates this sketch with a given signed 16-bit integer
// Only update when the value is not existing
func (s *QuickSelectUpdateSketch) UpdateInt16(value int16) error {
	return s.UpdateInt32(int32(value))
}

// UpdateUint8 updates this sketch with a given unsigned 8-bit integer
// Only update when the value is not existing
func (s *QuickSelectUpdateSketch) UpdateUint8(value uint8) error {
	return s.UpdateInt32(int32(value))
}

// UpdateInt8 updates this sketch with a given signed 8-bit integer
// Only update when the value is not existing
func (s *QuickSelectUpdateSketch) UpdateInt8(value int8) error {
	return s.UpdateInt32(int32(value))
}

// UpdateFloat64 updates this sketch with a given double-precision floating point value
// Only update when the value is not existing
func (s *QuickSelectUpdateSketch) UpdateFloat64(value float64) error {
	return s.UpdateInt64(canonicalDouble(value))
}

// canonicalDouble canonicalizes double values for Java compatibility
func canonicalDouble(value float64) int64 {
	if value == 0.0 {
		value = 0.0 // canonicalize -0.0 to 0.0
	} else if math.IsNaN(value) {
		return 0x7ff8000000000000 // canonicalize NaN using value from Java's Double.doubleToLongBits()
	}
	return int64(math.Float64bits(value))
}

// UpdateFloat32 updates this sketch with a given floating point value
// Only update when the value is not existing
func (s *QuickSelectUpdateSketch) UpdateFloat32(value float32) error {
	return s.UpdateFloat64(float64(value))
}

// UpdateString updates this sketch with a given string
// Only update when the value is not existing
func (s *QuickSelectUpdateSketch) UpdateString(value string) error {
	if value == "" {
		return ErrUpdateEmptyString
	}

	hash, err := s.table.HashStringAndScreen(value)
	if err != nil {
		return err
	}

	index, err := s.table.Find(hash)
	if err != nil {
		if err == ErrKeyNotFound {
			s.table.Insert(index, hash)
			return nil
		}
		return err
	}

	return ErrDuplicateKey
}

// UpdateBytes updates this sketch with given data
// Only update when the value is not existing
func (s *QuickSelectUpdateSketch) UpdateBytes(data []byte) error {
	hash, err := s.table.HashBytesAndScreen(data)
	if err != nil {
		return err
	}

	index, err := s.table.Find(hash)
	if err != nil {
		if err == ErrKeyNotFound {
			s.table.Insert(index, hash)
			return nil
		}
		return err
	}

	return ErrDuplicateKey
}

// Trim removes retained entries in excess of the nominal size k (if any)
func (s *QuickSelectUpdateSketch) Trim() {
	s.table.Trim()
}

// Reset resets the sketch to the initial empty state
func (s *QuickSelectUpdateSketch) Reset() {
	s.table.Reset()
}

// All returns an iterator over hash values in this sketch
func (s *QuickSelectUpdateSketch) All() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		for _, entry := range s.table.entries {
			if entry != 0 {
				if !yield(entry) {
					return
				}
			}
		}
	}
}

func (s *QuickSelectUpdateSketch) Compact(ordered bool) *CompactSketch {
	return NewCompactSketch(s, ordered)
}

func (s *QuickSelectUpdateSketch) CompactOrdered() *CompactSketch {
	return s.Compact(true)
}
