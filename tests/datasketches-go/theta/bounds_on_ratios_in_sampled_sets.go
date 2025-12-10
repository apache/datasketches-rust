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
	"math"

	"github.com/apache/datasketches-go/internal/binomialproportionsbounds"
)

const numStdDevs = 2.0

// lowerBoundForBOverA returns the approximate lower bound based on a 95% confidence interval.
// The parameter a (|S_A|) is the observed size of a sample of A that was obtained by
// Bernoulli sampling with a known inclusion probability f. The parameter b (|S_A ∩ B|)
// is the observed size of a subset of S_A. The parameter f is the inclusion probability
// used to produce the set with size a and should generally be less than 0.5. Above this
// value, the results may not be reliable. When f is 1.0 this returns the estimate.
func lowerBoundForBOverA(a, b uint64, f float64) (float64, error) {
	if err := validateInputs(a, b, f); err != nil {
		return 0.0, err
	}
	if a == 0 {
		return 0.0, nil
	}
	if f == 1.0 {
		return float64(b) / float64(a), nil
	}
	result, err := binomialproportionsbounds.ApproximateLowerBoundOnP(a, b, numStdDevs*hackyAdjuster(f))
	if err != nil {
		return 0.0, err
	}
	return result, nil
}

// upperBoundForBOverA returns the approximate upper bound based on a 95% confidence interval.
// The parameter a (|S_A|) is the observed size of a sample of A that was obtained by
// Bernoulli sampling with a known inclusion probability f. The parameter b (|S_A ∩ B|)
// is the observed size of a subset of S_A. The parameter f is the inclusion probability
// used to produce the set with size a.
func upperBoundForBOverA(a, b uint64, f float64) (float64, error) {
	if err := validateInputs(a, b, f); err != nil {
		return 0.0, err
	}
	if a == 0 {
		return 1.0, nil
	}
	if f == 1.0 {
		return float64(b) / float64(a), nil
	}
	result, err := binomialproportionsbounds.ApproximateUpperBoundOnP(a, b, numStdDevs*hackyAdjuster(f))
	if err != nil {
		return 0.0, err
	}
	return result, nil
}

// hackyAdjuster is tightly coupled with the width of the confidence interval normally
// specified with number of standard deviations. To simplify this interface the number of
// standard deviations has been fixed to 2.0, which corresponds to a confidence interval
// of 95%. The parameter f is the inclusion probability used to produce the set with size a.
func hackyAdjuster(f float64) float64 {
	tmp := math.Sqrt(1.0 - f)
	if f <= 0.5 {
		return tmp
	}
	return tmp + (0.01 * (f - 0.5))
}

func validateInputs(a, b uint64, f float64) error {
	if a < b {
		return fmt.Errorf("a must be >= b: a = %d, b = %d", a, b)
	}
	if f > 1.0 || f <= 0.0 {
		return fmt.Errorf("Required: ((f <= 1.0) && (f > 0.0)): %f", f)
	}
	return nil
}
