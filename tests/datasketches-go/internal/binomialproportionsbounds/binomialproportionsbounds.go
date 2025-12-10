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

// Package binomialproportionsbounds computes an approximation to the Clopper-Pearson confidence interval
// for a binomial proportion. Exact Clopper-Pearson intervals are strictly
// conservative, but these approximations are not.
//
// The main inputs are numbers n and k, which are not the same as other things
// that are called n and k in our sketching library. There is also a third
// parameter, numStdDev, that specifies the desired confidence level.
//
//   - n is the number of independent randomized trials. It is given and therefore known.
//   - p is the probability of a trial being a success. It is unknown.
//   - k is the number of trials (out of n) that turn out to be successes. It is
//     a random variable governed by a binomial distribution. After any given
//     batch of n independent trials, the random variable k has a specific
//     value which is observed and is therefore known.
//   - pHat = k / n is an unbiased estimate of the unknown success probability p.
//
// Alternatively, consider a coin with unknown heads probability p. Where
// n is the number of independent flips of that coin, and k is the number
// of times that the coin comes up heads during a given batch of n flips.
// This computes a frequentist confidence interval [lowerBoundOnP, upperBoundOnP] for the
// unknown p.
package binomialproportionsbounds

import (
	"fmt"
	"math"
)

// ApproximateLowerBoundOnP computes the lower bound of an approximate Clopper-Pearson
// confidence interval for a binomial proportion. The parameter n is the number of trials
// and must be non-negative. The parameter k is the number of successes, must be non-negative,
// and cannot exceed n. The parameter numStdDevs is the number of standard deviations defining
// the confidence interval.
//
// Implementation Notes:
// The ApproximateLowerBoundOnP is defined with respect to the right tail of the binomial
// distribution.
//   - We want to solve for the p for which sum_{j,k,n}bino(j;n,p) = delta.
//   - We now restate that in terms of the left tail.
//   - We want to solve for the p for which sum_{j,0,(k-1)}bino(j;n,p) = 1 - delta.
//   - Define x = 1-p.
//   - We want to solve for the x for which I_x(n-k+1,k) = 1 - delta.
//   - We specify 1-delta via numStdDevs through the right tail of the standard normal distribution.
//   - Smaller values of numStdDevs correspond to bigger values of 1-delta and hence to smaller
//     values of delta. In fact, usefully small values of delta correspond to negative values of
//     numStdDevs.
//   - return p = 1-x.
func ApproximateLowerBoundOnP(n, k uint64, numStdDevs float64) (float64, error) {
	if err := validateInputs(n, k); err != nil {
		return 0, err
	}
	if n == 0 {
		return 0.0, nil // the coin was never flipped, so we know nothing
	} else if k == 0 {
		return 0.0, nil
	} else if k == 1 {
		return exactLowerBoundOnPKEq1(n, deltaOfNumStdevs(numStdDevs)), nil
	} else if k == n {
		return exactLowerBoundOnPKEqN(n, deltaOfNumStdevs(numStdDevs)), nil
	} else {
		x := abramowitzStegunFormula26p5p22(float64((n-k)+1), float64(k), -1.0*numStdDevs)
		return 1.0 - x, nil // which is p
	}
}

// ApproximateUpperBoundOnP computes the upper bound of an approximate Clopper-Pearson
// confidence interval for a binomial proportion. The parameter n is the number of trials
// and must be non-negative. The parameter k is the number of successes, must be non-negative,
// and cannot exceed n. The parameter numStdDevs is the number of standard deviations defining
// the confidence interval.
//
// Implementation Notes:
// The ApproximateUpperBoundOnP is defined with respect to the left tail of the binomial
// distribution.
//   - We want to solve for the p for which sum_{j,0,k}bino(j;n,p) = delta.
//   - Define x = 1-p.
//   - We want to solve for the x for which I_x(n-k,k+1) = delta.
//   - We specify delta via numStdDevs through the right tail of the standard normal distribution.
//   - Bigger values of numStdDevs correspond to smaller values of delta.
//   - return p = 1-x.
func ApproximateUpperBoundOnP(n, k uint64, numStdDevs float64) (float64, error) {
	if err := validateInputs(n, k); err != nil {
		return 0, err
	}
	if n == 0 {
		return 1.0, nil // the coin was never flipped, so we know nothing
	} else if k == n {
		return 1.0, nil
	} else if k == n-1 {
		return exactUpperBoundOnPKEqMinusOne(n, deltaOfNumStdevs(numStdDevs)), nil
	} else if k == 0 {
		return exactUpperBoundOnPKEqZero(n, deltaOfNumStdevs(numStdDevs)), nil
	} else {
		x := abramowitzStegunFormula26p5p22(float64(n-k), float64(k+1), numStdDevs)
		return 1.0 - x, nil // which is p
	}
}

// Erf computes an approximation to the erf() function for the input x.
// The result is accurate to roughly 7 decimal digits.
func Erf(x float64) float64 {
	if x < 0.0 {
		return -1.0 * erfOfNonneg(-1.0*x)
	}
	return erfOfNonneg(x)
}

// NormalCDF computes an approximation to normal_cdf(x) for the input x.
func NormalCDF(x float64) float64 {
	return 0.5 * (1.0 + Erf(x/math.Sqrt(2.0)))
}

// validateInputs validates that k does not exceed n.
func validateInputs(n, k uint64) error {
	if k > n {
		return fmt.Errorf("K cannot exceed N: n=%d, k=%d", n, k)
	}
	return nil
}

// erfOfNonneg implements Abramowitz and Stegun formula 7.1.28, p. 88.
// It claims accuracy of about 7 decimal digits for the input x.
func erfOfNonneg(x float64) float64 {
	// The constants that appear below, formatted for easy checking against the book.
	//    a1 = 0.07052 30784
	//    a3 = 0.00927 05272
	//    a5 = 0.00027 65672
	//    a2 = 0.04228 20123
	//    a4 = 0.00015 20143
	//    a6 = 0.00004 30638
	const a1 = 0.0705230784
	const a3 = 0.0092705272
	const a5 = 0.0002765672
	const a2 = 0.0422820123
	const a4 = 0.0001520143
	const a6 = 0.0000430638

	x2 := x * x // x squared, x cubed, etc.
	x3 := x2 * x
	x4 := x2 * x2
	x5 := x2 * x3
	x6 := x3 * x3

	sum := 1.0 +
		(a1 * x) +
		(a2 * x2) +
		(a3 * x3) +
		(a4 * x4) +
		(a5 * x5) +
		(a6 * x6)

	sum2 := sum * sum // raise the sum to the 16th power
	sum4 := sum2 * sum2
	sum8 := sum4 * sum4
	sum16 := sum8 * sum8

	return 1.0 - (1.0 / sum16)
}

func deltaOfNumStdevs(kappa float64) float64 {
	return NormalCDF(-1.0 * kappa)
}

// abramowitzStegunFormula26p5p22 is Formula 26.5.22 on page 945 of Abramowitz & Stegun,
// which is an approximation of the inverse of the incomplete beta function I_x(a,b) = delta
// viewed as a scalar function of x.
//
// In other words, we specify delta, and it gives us x (with a and b held constant).
// However, delta is specified in an indirect way through yp which
// is the number of stdDevs that leaves delta probability in the right
// tail of a standard gaussian distribution.
//
// We point out that the variable names correspond to those in the book,
// and it is worth keeping it that way so that it will always be easy to verify
// that the formula was typed in correctly.
func abramowitzStegunFormula26p5p22(a, b, yp float64) float64 {
	b2m1 := (2.0 * b) - 1.0
	a2m1 := (2.0 * a) - 1.0
	lambda := ((yp * yp) - 3.0) / 6.0
	htmp := (1.0 / a2m1) + (1.0 / b2m1)
	h := 2.0 / htmp
	term1 := (yp * math.Sqrt(h+lambda)) / h
	term2 := (1.0 / b2m1) - (1.0 / a2m1)
	term3 := (lambda + (5.0 / 6.0)) - (2.0 / (3.0 * h))
	w := term1 - (term2 * term3)
	xp := a / (a + (b * math.Exp(2.0*w)))
	return xp
}

// Formulas for some special cases.

func exactUpperBoundOnPKEqZero(n uint64, delta float64) float64 {
	return 1.0 - math.Pow(delta, 1.0/float64(n))
}

func exactLowerBoundOnPKEqN(n uint64, delta float64) float64 {
	return math.Pow(delta, 1.0/float64(n))
}

func exactLowerBoundOnPKEq1(n uint64, delta float64) float64 {
	return 1.0 - math.Pow(1.0-delta, 1.0/float64(n))
}

func exactUpperBoundOnPKEqMinusOne(n uint64, delta float64) float64 {
	return math.Pow(1.0-delta, 1.0/float64(n))
}
