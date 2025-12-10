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

import (
	"math"
)

// hllCompositeEstimate is the (non-HIP) estimator.
// It is called "composite" because multiple estimators are pasted together.
func hllCompositeEstimate(hllArray *hllArrayImpl) (float64, error) {
	lgConfigK := hllArray.lgConfigK
	rawEst := getHllRawEstimate(lgConfigK, hllArray.kxq0+hllArray.kxq1)

	xArr := compositeInterpolationXarrs[lgConfigK-minLogK]
	yStride := compositeInterpolationYstrides[lgConfigK-minLogK]
	xArrLen := len(xArr)

	if rawEst < xArr[0] {
		return 0, nil
	}

	xArrLenM1 := xArrLen - 1

	if rawEst > xArr[xArrLenM1] {
		finalY := yStride * float64(xArrLenM1)
		factor := finalY / xArr[xArrLenM1]
		return rawEst * factor, nil
	}
	adjEst, err := usingXArrAndYStride(xArr, yStride, rawEst)
	if err != nil {
		return 0, err
	}
	// We need to completely avoid the linear_counting estimator if it might have a crazy value.
	// Empirical evidence suggests that the threshold 3*k will keep us safe if 2^4 <= k <= 2^21.
	if adjEst > float64(uint64(3<<lgConfigK)) {
		return adjEst, nil
	}

	linEst := getHllBitMapEstimate(lgConfigK, hllArray.curMin, hllArray.numAtCurMin)

	// Bias is created when the value of an estimator is compared with a threshold to decide whether
	// to use that estimator or a different one.
	// We conjecture that less bias is created when the average of the two estimators
	// is compared with the threshold. Empirical measurements support this conjecture.
	avgEst := (adjEst + linEst) / 2.0

	// The following constants comes from empirical measurements of the crossover point
	// between the average error of the linear estimator and the adjusted HLL estimator
	crossOver := 0.64
	if lgConfigK == 4 {
		crossOver = 0.718
	} else if lgConfigK == 5 {
		crossOver = 0.672
	}

	if avgEst > (crossOver * float64(uint64(1<<lgConfigK))) {
		return adjEst, nil
	} else {
		return linEst, nil
	}
}

// getHllBitMapEstimate is the estimator when N is small, roughly less than k log(k).
// Refer to Wikipedia: Coupon Collector Problem
func getHllBitMapEstimate(lgConfigK int, curMin int, numAtCurMin int) float64 {
	configK := 1 << lgConfigK
	numUnhitBuckets := 0
	if curMin == 0 {
		numUnhitBuckets = numAtCurMin
	}

	//This will eventually go away.
	if numUnhitBuckets == 0 {
		return float64(configK) * math.Log(float64(configK)/0.5)
	}

	numHitBuckets := configK - numUnhitBuckets
	return getBitMapEstimate(configK, numHitBuckets)
}

// getHllRawEstimate is the algorithm from Flajolet's, et al, 2007 HLL paper, Fig 3.
func getHllRawEstimate(lgConfigK int, kxqSum float64) float64 {
	configK := 1 << lgConfigK
	correctionFactor := 0.0

	if lgConfigK == 4 {
		correctionFactor = 0.673
	} else if lgConfigK == 5 {
		correctionFactor = 0.697
	} else if lgConfigK == 6 {
		correctionFactor = 0.709
	} else {
		correctionFactor = 0.7213 / (1.0 + (1.079 / float64(configK)))
	}

	return (correctionFactor * float64(configK) * float64(configK)) / kxqSum
}

func hllUpperBound(hllArray *hllArrayImpl, numStdDev int) (float64, error) {
	lgConfigK := hllArray.lgConfigK
	estimate, err := hllArray.GetEstimate()
	if err != nil {
		return 0, err
	}
	oooFlag := hllArray.isOutOfOrder()
	relErr, err := getRelErrAllK(true, oooFlag, lgConfigK, numStdDev)
	if err != nil {
		return 0, err
	}
	return estimate / (1.0 - relErr), nil
}

func hllLowerBound(hllArray *hllArrayImpl, numStdDev int) (float64, error) {
	lgConfigK := hllArray.lgConfigK
	configK := 1 << lgConfigK
	numNonZeros := float64(configK)
	if hllArray.curMin == 0 {
		numNonZeros -= float64(hllArray.numAtCurMin)
	}
	estimate, err := hllArray.GetEstimate()
	if err != nil {
		return 0, err
	}
	oooFlag := hllArray.isOutOfOrder()
	relErr, err := getRelErrAllK(false, oooFlag, lgConfigK, numStdDev)
	if err != nil {
		return 0, err
	}
	return math.Max(estimate/(1.0+relErr), numNonZeros), nil
}

func getRelErrAllK(upperBound bool, oooFlag bool, lgConfigK int, numStdDev int) (float64, error) {
	lgK, err := checkLgK(lgConfigK)
	if err != nil {
		return 0, err
	}
	if lgK > 12 {
		rseFactor := hllHipRSEFActor
		if oooFlag {
			rseFactor = hllNonHipRSEFactor
		}
		configK := 1 << lgK
		return (float64(numStdDev) * rseFactor) / math.Sqrt(float64(configK)), nil
	}
	return math.Abs(getRelErrKLT12(upperBound, oooFlag, lgK, numStdDev)), nil
}
