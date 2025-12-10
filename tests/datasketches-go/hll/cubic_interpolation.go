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

package hll

import "fmt"

// UsingXAndYTables returns the cubic interpolation using the X and Y tables.
func usingXAndYTables(xArr []float64, yArr []float64, x float64) (float64, error) {
	if len(xArr) < 4 || len(xArr) != len(yArr) {
		return 0, fmt.Errorf("X value out of range: %f", x)
	}

	if x == xArr[len(xArr)-1] {
		return yArr[len(yArr)-1], nil // corer case
	}

	offset, err := findStraddle(xArr, x) //uses recursion
	if err != nil {
		return 0, err
	}
	if (offset < 0) || (offset > (len(xArr) - 2)) {
		return 0, fmt.Errorf("offset out of range: %d", offset)
	}
	if offset == 0 {
		return interpolateUsingXAndYTables(xArr, yArr, offset, x), nil // corner case
	}

	if offset == len(xArr)-2 {
		return interpolateUsingXAndYTables(xArr, yArr, offset-2, x), nil // corner case
	}

	return interpolateUsingXAndYTables(xArr, yArr, offset-1, x), nil
}

func interpolateUsingXAndYTables(xArr []float64, yArr []float64, offset int, x float64) float64 {
	return cubicInterpolate(
		xArr[offset], yArr[offset],
		xArr[offset+1], yArr[offset+1],
		xArr[offset+2], yArr[offset+2],
		xArr[offset+3], yArr[offset+3],
		x)
}

func usingXArrAndYStride(xArr []float64, yStride float64, x float64) (float64, error) {
	xArrLen := len(xArr)
	xArrLenM1 := xArrLen - 1

	if xArrLen < 4 || x < xArr[0] || x > xArr[xArrLenM1] {
		return 0, fmt.Errorf("X value out of range: %f", x)
	}
	if x == xArr[xArrLenM1] {
		return yStride * float64(xArrLenM1), nil // corner case
	}
	offset, err := findStraddle(xArr, x) //uses recursion
	if err != nil {
		return 0, err
	}
	xArrLenM2 := xArrLen - 2
	if (offset < 0) || (offset > xArrLenM2) {
		return 0, fmt.Errorf("offset out of range: %d", offset)
	}
	if offset == 0 {
		return interpolateUsingXArrAndYStride(xArr, yStride, offset, x), nil // corner case
	}
	if offset == xArrLenM2 {
		return interpolateUsingXArrAndYStride(xArr, yStride, offset-2, x), nil // corner case
	}
	return interpolateUsingXArrAndYStride(xArr, yStride, offset-1, x), nil
}

// interpolateUsingXArrAndYStride interpolates using the X array and the Y stride.
func interpolateUsingXArrAndYStride(xArr []float64, yStride float64, offset int, x float64) float64 {
	return cubicInterpolate(xArr[offset+0], yStride*float64(offset+0),
		xArr[offset+1], yStride*float64(offset+1),
		xArr[offset+2], yStride*float64(offset+2),
		xArr[offset+3], yStride*float64(offset+3), x)
}

// cubicInterpolate interpolates using the cubic curve that passes through the four given points, using the
// Lagrange interpolation formula.
func cubicInterpolate(x0 float64, y0 float64, x1 float64, y1 float64, x2 float64, y2 float64, x3 float64, y3 float64, x float64) float64 {
	l0Numer := (x - x1) * (x - x2) * (x - x3)
	l1Numer := (x - x0) * (x - x2) * (x - x3)
	l2Numer := (x - x0) * (x - x1) * (x - x3)
	l3Numer := (x - x0) * (x - x1) * (x - x2)

	l0Denom := (x0 - x1) * (x0 - x2) * (x0 - x3)
	l1Denom := (x1 - x0) * (x1 - x2) * (x1 - x3)
	l2Denom := (x2 - x0) * (x2 - x1) * (x2 - x3)
	l3Denom := (x3 - x0) * (x3 - x1) * (x3 - x2)

	term0 := (y0 * l0Numer) / l0Denom
	term1 := (y1 * l1Numer) / l1Denom
	term2 := (y2 * l2Numer) / l2Denom
	term3 := (y3 * l3Numer) / l3Denom

	return term0 + term1 + term2 + term3
}

// findStraddle returns the index of the largest value in the array that is less than or equal to the given value.
func findStraddle(xArr []float64, x float64) (int, error) {
	if len(xArr) < 2 || x < xArr[0] || x > xArr[len(xArr)-1] {
		return 0, fmt.Errorf("X value out of range: %f", x)
	}
	return recursiveFindStraddle(xArr, 0, len(xArr)-1, x)
}

// recursiveFindStraddle returns the index of the largest value in the array that is less than or equal to the given value.
func recursiveFindStraddle(xArr []float64, left int, right int, x float64) (int, error) {
	if left >= right {
		return 0, fmt.Errorf("left >= right: %d >= %d", left, right)
	}

	if xArr[left] > x || x >= xArr[right] {
		return 0, fmt.Errorf("X value out of range: %f", x)
	}

	if left+1 == right {
		return left, nil
	}

	middle := left + ((right - left) / 2)

	if xArr[middle] <= x {
		return recursiveFindStraddle(xArr, middle, right, x)
	} else {
		return recursiveFindStraddle(xArr, left, middle, x)
	}
}
