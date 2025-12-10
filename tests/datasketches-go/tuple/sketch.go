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
	"iter"
)

// Summary is the base interface for all summary types used in tuple sketches.
// A summary holds aggregate data associated with each retained hash key.
type Summary interface {
	// Reset clears the content of the summary, restoring it to its initial state.
	Reset()
	// Clone creates and returns a deep copy of the current Summary instance.
	Clone() Summary
}

// Sketch is the base interface for tuple sketches.
// It extends Theta sketch to associate arbitrary summaries with each retained key.
type Sketch[S Summary] interface {
	// IsEmpty reports whether this sketch represents an empty set.
	// Note: this is not the same as having no retained hashes.
	IsEmpty() bool

	// Estimate returns the estimated distinct count of the input stream.
	Estimate() float64

	// LowerBoundFromSubset returns the approximate lower error bound for
	// the given number of standard deviations over a subset of retained hashes.
	// numStdDevs specifies the confidence level (1, 2, or 3) corresponding to
	// approximately 67%, 95%, or 99% confidence intervals.
	// numSubsetEntries specifies number of items from {0, 1, ..., get_num_retained()}
	// over which to estimate the bound.
	LowerBoundFromSubset(numStdDevs uint8, numSubsetEntries uint32) (float64, error)

	// LowerBound returns the approximate lower error bound for the given
	// number of standard deviations. numStdDevs should be 1, 2, or 3 for
	// approximately 67%, 95%, or 99% confidence intervals.
	LowerBound(numStdDevs uint8) (float64, error)

	// UpperBoundFromSubset returns the approximate upper error bound for
	// the given number of standard deviations over a subset of retained hashes.
	// numStdDevs specifies the confidence level (1, 2, or 3) corresponding to
	// approximately 67%, 95%, or 99% confidence intervals.
	// numSubsetEntries specifies number of items from {0, 1, ..., get_num_retained()}
	// over which to estimate the bound.
	UpperBoundFromSubset(numStdDevs uint8, numSubsetEntries uint32) (float64, error)

	// UpperBound returns the approximate upper error bound for the given
	// number of standard deviations. numStdDevs should be 1, 2, or 3 for
	// approximately 67%, 95%, or 99% confidence intervals.
	UpperBound(numStdDevs uint8) (float64, error)

	// IsEstimationMode reports whether the sketch is in estimation mode,
	// as opposed to exact mode.
	IsEstimationMode() bool

	// Theta returns theta as a fraction from 0 to 1, representing the
	// effective sampling rate.
	Theta() float64

	// Theta64 returns theta as a positive integer between 0 and math.MaxUint64.
	Theta64() uint64

	// NumRetained returns the number of hashes retained in the sketch.
	NumRetained() uint32

	// SeedHash returns the hash of the seed used to hash the input.
	SeedHash() (uint16, error)

	// IsOrdered reports whether retained hashes are sorted by hash value.
	IsOrdered() bool

	// String returns a human-readable summary of this sketch.
	// If printItems is true, the output includes all retained hashes.
	String(shouldPrintItems bool) string

	// All returns an iterator over all hash-summary pairs in the sketch.
	All() iter.Seq2[uint64, S]
}
