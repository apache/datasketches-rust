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

package tuple

import (
	"errors"
	"fmt"
	"iter"
	"math"
	"strings"

	"github.com/apache/datasketches-go/internal"
	"github.com/apache/datasketches-go/internal/binomialbounds"
	"github.com/apache/datasketches-go/theta"
)

var (
	ErrUpdateEmptyString = errors.New("cannot update empty string")
)

// UpdatableSummary represents a summary that can be updated with values of type V.
type UpdatableSummary[V any] interface {
	Summary

	// Update incorporates a new value into the summary, modifying its internal state based on the given input value.
	Update(value V)
}

// UpdateSketch builds Tuple sketch from input data via update methods.
type UpdateSketch[S UpdatableSummary[V], V any] struct {
	newSummary func() S
	table      *hashtable[S]
}

type updateSketchOptions struct {
	theta     uint64
	seed      uint64
	p         float32
	lgCurSize uint8
	lgK       uint8
	rf        theta.ResizeFactor
}

type UpdateSketchOptionFunc func(*updateSketchOptions)

// WithUpdateSketchLgK sets log2(k), where k is a nominal number of entries in the sketch
func WithUpdateSketchLgK(lgK uint8) UpdateSketchOptionFunc {
	return func(opts *updateSketchOptions) {
		opts.lgK = lgK
	}
}

// WithUpdateSketchResizeFactor sets a resize factor for the internal hash table (defaults to 8)
func WithUpdateSketchResizeFactor(rf theta.ResizeFactor) UpdateSketchOptionFunc {
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

// NewUpdateSketch initializes and returns a new instance of UpdateSketch with the specified parameters.
func NewUpdateSketch[S UpdatableSummary[V], V any](
	newSummaryFunc func() S, opts ...UpdateSketchOptionFunc,
) (*UpdateSketch[S, V], error) {
	options := &updateSketchOptions{
		lgK:  theta.DefaultLgK,
		rf:   theta.DefaultResizeFactor,
		p:    1.0,
		seed: theta.DefaultSeed,
	}
	for _, opt := range opts {
		opt(options)
	}

	if options.lgK < theta.MinLgK {
		return nil, fmt.Errorf("lg_k must not be less than %d: %d", theta.MinLgK, options.lgK)
	}
	if options.lgK > theta.MaxLgK {
		return nil, fmt.Errorf("lg_k must not be greater than %d: %d", theta.MaxLgK, options.lgK)
	}
	if options.p <= 0 || options.p > 1 {
		return nil, errors.New("sampling probability must be between 0 and 1")
	}

	options.lgCurSize = startingSubMultiple(options.lgK+1, theta.MinLgK, uint8(options.rf))
	options.theta = startingThetaFromP(options.p)

	return &UpdateSketch[S, V]{
		newSummary: newSummaryFunc,
		table: newHashtable[S](
			options.lgCurSize,
			options.lgK,
			options.rf,
			options.p,
			options.theta,
			options.seed,
			true,
		),
	}, nil
}

// IsEstimationMode reports whether the sketch is in estimation mode,
// as opposed to exact mode.
func (s *UpdateSketch[S, V]) IsEstimationMode() bool {
	return s.Theta64() < theta.MaxTheta && !s.IsEmpty()
}

// Theta returns theta as a fraction from 0 to 1, representing the
// effective sampling rate.
func (s *UpdateSketch[S, V]) Theta() float64 {
	return float64(s.Theta64()) / float64(theta.MaxTheta)
}

// Estimate returns the estimated distinct count of the input stream.
func (s *UpdateSketch[S, V]) Estimate() float64 {
	return float64(s.NumRetained()) / s.Theta()
}

// LowerBoundFromSubset returns the approximate lower error bound for
// the given number of standard deviations over a subset of retained hashes.
// numStdDevs specifies the confidence level (1, 2, or 3) corresponding to
// approximately 67%, 95%, or 99% confidence intervals.
// numSubsetEntries specifies number of items from {0, 1, ..., get_num_retained()}
// over which to estimate the bound.
func (s *UpdateSketch[S, V]) LowerBoundFromSubset(numStdDevs uint8, numSubsetEntries uint32) (float64, error) {
	numSubsetEntries = min(numSubsetEntries, s.NumRetained())
	if !s.IsEstimationMode() {
		return float64(numSubsetEntries), nil
	}
	return binomialbounds.LowerBound(uint64(numSubsetEntries), s.Theta(), uint(numStdDevs))
}

// LowerBound returns the approximate lower error bound for the given
// number of standard deviations. numStdDevs should be 1, 2, or 3 for
// approximately 67%, 95%, or 99% confidence intervals.
func (s *UpdateSketch[S, V]) LowerBound(numStdDevs uint8) (float64, error) {
	return s.LowerBoundFromSubset(numStdDevs, s.NumRetained())
}

// UpperBoundFromSubset returns the approximate upper error bound for
// the given number of standard deviations over a subset of retained hashes.
// numStdDevs specifies the confidence level (1, 2, or 3) corresponding to
// approximately 67%, 95%, or 99% confidence intervals.
// numSubsetEntries specifies number of items from {0, 1, ..., get_num_retained()}
// over which to estimate the bound.
func (s *UpdateSketch[S, V]) UpperBoundFromSubset(numStdDevs uint8, numSubsetEntries uint32) (float64, error) {
	numSubsetEntries = min(numSubsetEntries, s.NumRetained())
	if !s.IsEstimationMode() {
		return float64(numSubsetEntries), nil
	}
	return binomialbounds.UpperBound(uint64(numSubsetEntries), s.Theta(), uint(numStdDevs))
}

// UpperBound returns the approximate upper error bound for the given
// number of standard deviations. numStdDevs should be 1, 2, or 3 for
// approximately 67%, 95%, or 99% confidence intervals.
func (s *UpdateSketch[S, V]) UpperBound(numStdDevs uint8) (float64, error) {
	return s.UpperBoundFromSubset(numStdDevs, s.NumRetained())
}

// String returns a human-readable summary of this sketch.
// If printItems is true, the output includes all retained hashes.
func (s *UpdateSketch[S, V]) String(shouldPrintItems bool) string {
	seedHash, _ := s.SeedHash()
	lb, _ := s.LowerBound(2)
	ub, _ := s.UpperBound(2)

	var result strings.Builder
	result.WriteString("### Tuple sketch summary:")
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   num retained hashes : %d", s.NumRetained()))
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
	result.WriteString(fmt.Sprintf("   lg nominal size      : %d", s.table.lgNomSize))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   lg current size      : %d", s.table.lgCurSize))
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf("   resize factor        : %d", 1<<s.table.rf))
	result.WriteString("\n")
	result.WriteString("### End sketch summary")
	result.WriteString("\n")

	if shouldPrintItems {
		result.WriteString("### Retained entries")
		result.WriteString("\n")

		for hash, summary := range s.All() {
			result.WriteString(fmt.Sprintf("%d: %v", hash, summary))
			result.WriteString("\n")
		}

		result.WriteString("### End retained entries")
		result.WriteString("\n")
	}

	return result.String()
}

// IsEmpty reports whether this sketch represents an empty set.
func (s *UpdateSketch[S, V]) IsEmpty() bool {
	return s.table.isEmpty
}

// IsOrdered reports whether retained hashes are sorted by hash value.
func (s *UpdateSketch[S, V]) IsOrdered() bool {
	return s.table.numEntries <= 1
}

// Theta64 returns theta as a positive integer between 0 and math.MaxUint64.
func (s *UpdateSketch[S, V]) Theta64() uint64 {
	if s.IsEmpty() {
		return theta.MaxTheta
	}
	return s.table.theta
}

// NumRetained returns the number of hashes retained in the sketch.
func (s *UpdateSketch[S, V]) NumRetained() uint32 {
	return s.table.numEntries
}

// SeedHash returns the hash of the seed used to hash the input.
func (s *UpdateSketch[S, V]) SeedHash() (uint16, error) {
	seedHash, err := internal.ComputeSeedHash(int64(s.table.seed))
	if err != nil {
		return 0, err
	}
	return uint16(seedHash), nil
}

// All returns an iterator over all hash-summary pairs in the sketch.
func (s *UpdateSketch[S, V]) All() iter.Seq2[uint64, S] {
	return func(yield func(uint64, S) bool) {
		for _, e := range s.table.entries {
			if e.Hash != 0 {
				if !yield(e.Hash, e.Summary) {
					return
				}
			}
		}
	}
}

// LgK returns a configured nominal number of entries in the sketch
func (s *UpdateSketch[S, V]) LgK() uint8 {
	return s.table.lgNomSize
}

// ResizeFactor returns a configured resize factor of the sketch
func (s *UpdateSketch[S, V]) ResizeFactor() theta.ResizeFactor {
	return s.table.rf
}

// UpdateUint64 updates this sketch with a given unsigned 64-bit integer
func (s *UpdateSketch[S, V]) UpdateUint64(key uint64, value V) error {
	return s.UpdateInt64(int64(key), value)
}

// UpdateInt64 updates this sketch with a given signed 64-bit integer
func (s *UpdateSketch[S, V]) UpdateInt64(key int64, value V) error {
	hash, err := s.table.HashInt64AndScreen(key)
	if err != nil {
		return err
	}

	index, err := s.table.Find(hash)
	if err != nil {
		if err == ErrKeyNotFound {
			summary := s.newSummary()
			summary.Update(value)

			s.table.Insert(index, entry[S]{Hash: hash, Summary: summary})
			return nil
		}
		return err
	}

	s.table.entries[index].Summary.Update(value)
	return nil
}

// UpdateUint32 updates this sketch with a given unsigned 32-bit integer
func (s *UpdateSketch[S, V]) UpdateUint32(key uint32, value V) error {
	return s.UpdateInt64(int64(key), value)
}

// UpdateInt32 updates this sketch with a given signed 32-bit integer
func (s *UpdateSketch[S, V]) UpdateInt32(key int32, value V) error {
	hash, err := s.table.HashInt32AndScreen(key)
	if err != nil {
		return err
	}

	index, err := s.table.Find(hash)
	if err != nil {
		if err == ErrKeyNotFound {
			summary := s.newSummary()
			summary.Update(value)

			s.table.Insert(index, entry[S]{Hash: hash, Summary: summary})
			return nil
		}
		return err
	}

	s.table.entries[index].Summary.Update(value)
	return nil
}

// UpdateUint16 updates this sketch with a given unsigned 16-bit integer
func (s *UpdateSketch[S, V]) UpdateUint16(key uint16, value V) error {
	return s.UpdateInt32(int32(key), value)
}

// UpdateInt16 updates this sketch with a given signed 16-bit integer
func (s *UpdateSketch[S, V]) UpdateInt16(key int16, value V) error {
	return s.UpdateInt32(int32(key), value)
}

// UpdateUint8 updates this sketch with a given unsigned 8-bit integer
func (s *UpdateSketch[S, V]) UpdateUint8(key uint8, value V) error {
	return s.UpdateInt32(int32(key), value)
}

// UpdateInt8 updates this sketch with a given signed 8-bit integer
func (s *UpdateSketch[S, V]) UpdateInt8(key int8, value V) error {
	return s.UpdateInt32(int32(key), value)
}

// UpdateFloat64 updates this sketch with a given double-precision floating point value
func (s *UpdateSketch[S, V]) UpdateFloat64(key float64, value V) error {
	return s.UpdateInt64(canonicalDouble(key), value)
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
func (s *UpdateSketch[S, V]) UpdateFloat32(key float32, value V) error {
	return s.UpdateFloat64(float64(key), value)
}

// UpdateString updates this sketch with a given string
func (s *UpdateSketch[S, V]) UpdateString(key string, value V) error {
	if key == "" {
		return ErrUpdateEmptyString
	}

	hash, err := s.table.HashStringAndScreen(key)
	if err != nil {
		return err
	}

	index, err := s.table.Find(hash)
	if err != nil {
		if err == ErrKeyNotFound {
			summary := s.newSummary()
			summary.Update(value)

			s.table.Insert(index, entry[S]{Hash: hash, Summary: summary})
			return nil
		}
		return err
	}

	s.table.entries[index].Summary.Update(value)
	return nil
}

// UpdateBytes updates this sketch with given data
func (s *UpdateSketch[S, V]) UpdateBytes(data []byte, value V) error {
	hash, err := s.table.HashBytesAndScreen(data)
	if err != nil {
		return err
	}

	index, err := s.table.Find(hash)
	if err != nil {
		if err == ErrKeyNotFound {
			summary := s.newSummary()
			summary.Update(value)

			s.table.Insert(index, entry[S]{Hash: hash, Summary: summary})
			return nil
		}
		return err
	}

	s.table.entries[index].Summary.Update(value)
	return nil
}

// Trim removes retained entries in excess of the nominal size k (if any)
func (s *UpdateSketch[S, V]) Trim() {
	s.table.Trim()
}

// Reset resets the sketch to the initial empty state
func (s *UpdateSketch[S, V]) Reset() {
	s.table.Reset()
}
