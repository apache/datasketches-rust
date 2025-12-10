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

import "iter"

// Sketch is a generalization of the Kth Minimum Value (KMV) sketch.
type Sketch interface {
	// IsEmpty returns true if this sketch represents an empty set
	// (not the same as no retained entries!)
	IsEmpty() bool

	// Estimate returns estimate of the distinct count of the input stream
	Estimate() float64

	// LowerBound returns the approximate lower error bound given a number of standard deviations.
	// This parameter is similar to the number of standard deviations of the normal distribution
	// and corresponds to approximately 67%, 95% and 99% confidence intervals.
	// numStdDevs number of Standard Deviations (1, 2 or 3)
	LowerBound(numStdDevs uint8) (float64, error)

	// UpperBound returns the approximate upper error bound given a number of standard deviations.
	// This parameter is similar to the number of standard deviations of the normal distribution
	// and corresponds to approximately 67%, 95% and 99% confidence intervals.
	// numStdDevs number of Standard Deviations (1, 2 or 3)
	UpperBound(numStdDevs uint8) (float64, error)

	// IsEstimationMode returns true if the sketch is in estimation mode
	// (as opposed to exact mode)
	IsEstimationMode() bool

	// Theta returns theta as a fraction from 0 to 1 (effective sampling rate)
	Theta() float64

	// Theta64 returns theta as a positive integer between 0 and math.MaxInt64
	Theta64() uint64

	// NumRetained returns the number of retained entries in the sketch
	NumRetained() uint32

	// SeedHash returns hash of the seed that was used to hash the input
	SeedHash() (uint16, error)

	// IsOrdered returns true if retained entries are ordered
	IsOrdered() bool

	// String returns a human-readable summary of this sketch as a string
	// If shouldPrintItems is true, include the list of items retained by the sketch
	String(shouldPrintItems bool) string

	// All returns hash values in the sketch.
	All() iter.Seq[uint64]
}
