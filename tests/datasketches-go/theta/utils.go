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
)

func checkEqual[T comparable](actual, expected T, description string) error {
	if actual != expected {
		return fmt.Errorf("%s mismatch: expected %v, actual %v", description, expected, actual)
	}
	return nil
}

// CheckSerialVersionEqual checks serial version
func CheckSerialVersionEqual(actual, expected uint8) error {
	return checkEqual(actual, expected, "serial version")
}

// CheckSketchFamilyEqual checks sketch family
func CheckSketchFamilyEqual(actual, expected uint8) error {
	return checkEqual(actual, expected, "sketch family")
}

// CheckSketchTypeEqual checks sketch type
func CheckSketchTypeEqual(actual, expected uint8) error {
	return checkEqual(actual, expected, "sketch type")
}

// CheckSeedHashEqual checks seed hash
func CheckSeedHashEqual(actual, expected uint16) error {
	return checkEqual(actual, expected, "seed hash")
}

// startingThetaFromP returns the starting theta value from probability p
// Consistent way of initializing theta from p
// Avoids multiplication if p == 1 since it might not yield MAX_THETA exactly
func startingThetaFromP(p float32) uint64 {
	if p < 1 {
		return uint64(float64(MaxTheta) * float64(p))
	}
	return MaxTheta
}

// startingSubMultiple calculates the starting sub-multiple
func startingSubMultiple(lgTgt, lgMin, lgRf uint8) uint8 {
	if lgTgt <= lgMin {
		return lgMin
	}
	if lgRf == 0 {
		return lgTgt
	}
	return ((lgTgt - lgMin) % lgRf) + lgMin
}
