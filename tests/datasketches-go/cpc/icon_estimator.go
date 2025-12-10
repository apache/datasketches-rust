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

import "math"

func iconEstimate(lgK int, c uint64) float64 {
	if c == 0 {
		return 0.0
	}
	if c < 2 {
		return 1.0
	}
	k := 1 << lgK
	doubleK := float64(k)
	doubleC := float64(c)
	// Differing thresholds ensure that the approximated estimator is monotonically increasing.
	var thresholdFactor float64
	if lgK < 14 {
		thresholdFactor = 5.7
	} else {
		thresholdFactor = 5.6
	}
	if doubleC > (thresholdFactor * doubleK) {
		return iconExponentialApproximation(doubleK, doubleC)
	}
	factor := evaluatePolynomial(
		iconPolynomialCoefficents,
		iconPolynomialNumCoefficients*(lgK-minLgK),
		iconPolynomialNumCoefficients,
		// The constant 2.0 is baked into the table iconPolynomialCoefficients[].
		// This factor, although somewhat arbitrary, is based on extensive characterization studies
		// and is considered a safe conservative factor.
		doubleC/(2.0*doubleK))
	ratio := doubleC / doubleK
	// The constant 66.774757 is baked into the table iconPolynomialCoefficients[].
	// This factor, although somewhat arbitrary, is based on extensive characterization studies
	// and is considered a safe conservative factor.
	term := 1.0 + ((ratio * ratio * ratio) / 66.774757)
	result := doubleC * factor * term
	if result >= doubleC {
		return result
	}
	return doubleC
}

func iconExponentialApproximation(k, c float64) float64 {
	return 0.7940236163830469 * k * math.Pow(2.0, c/k)
}

func evaluatePolynomial(coefficients []float64, start, num int, x float64) float64 {
	end := start + num - 1
	total := coefficients[end]
	for j := end - 1; j >= start; j-- {
		total *= x
		total += coefficients[j]
	}
	return total
}
