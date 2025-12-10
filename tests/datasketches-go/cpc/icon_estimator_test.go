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

import (
	"math"
	"testing"
)

const iconInversionTolerance = 1.0e-15

// qnj is a helper function for exactCofN. It returns the quantity qnj(kf, nf, col).
func qnj(kf, nf float64, col int) float64 {
	tmp1 := -1.0 / (kf * math.Pow(2.0, float64(col)))
	tmp2 := math.Log1p(tmp1)
	return -1.0 * math.Expm1(nf*tmp2)
}

// exactCofN is the "true" C(coupon count) as a function of N for a given k (kf).
func exactCofN(kf, nf float64) float64 {
	total := 0.0
	for col := 128; col >= 1; col-- {
		total += qnj(kf, nf, col)
	}
	return kf * total
}

// exactIconEstimatorBinarySearch does a binary search for N that yields coupon count c=targetC.
func exactIconEstimatorBinarySearch(kf, targetC, nLo, nHi float64) float64 {
	depth := 0
	for {
		if depth > 100 {
			panic("Excessive recursion in binary search")
		}
		if !(nHi > nLo) {
			panic("nHi <= nLo in binary search")
		}
		nMid := nLo + 0.5*(nHi-nLo)
		if !(nMid > nLo && nMid < nHi) {
			panic("nMid is not strictly between nLo and nHi in binary search")
		}
		if (nHi-nLo)/nMid < iconInversionTolerance {
			return nMid
		}
		midC := exactCofN(kf, nMid)
		if midC == targetC {
			return nMid
		}
		if midC < targetC {
			nLo = nMid
			depth++
			continue
		} else { // midC > targetC
			nHi = nMid
			depth++
			continue
		}
	}
}

// exactIconEstimatorBracketHi doubles N until it finds a bracket N where c(N) > targetC.
func exactIconEstimatorBracketHi(kf, targetC, nLo float64) float64 {
	depth := 0
	curN := 2.0 * nLo
	curC := exactCofN(kf, curN)
	for curC <= targetC {
		if depth > 100 {
			panic("Excessive looping in exactIconEstimatorBracketHi")
		}
		depth++
		curN *= 2.0
		curC = exactCofN(kf, curN)
	}
	return curN
}

// exactIconEstimator is the "exact" CPC ICON estimator.
// It uses bracket + binary search to invert the function c(N).
func exactIconEstimator(lgK int, c uint64) float64 {
	targetC := float64(c)
	if c == 0 || c == 1 {
		// If c==0 or c==1, the result is just c.
		return targetC
	}
	kf := float64(int(1) << lgK)
	nLo := targetC
	cofNLo := exactCofN(kf, nLo)
	if cofNLo >= targetC {
		panic("exactCofN(kf, nLo) >= targetC; unexpected bracket")
	}
	nHi := exactIconEstimatorBracketHi(kf, targetC, nLo)
	return exactIconEstimatorBinarySearch(kf, targetC, nLo, nHi)
}

// ---------------------
// Tests
// ---------------------

// It checks the approximate iconEstimate vs. the exact reference for various c values.
func TestQuickIconEstimator(t *testing.T) {
	for lgK := 4; lgK <= 26; lgK++ {
		k := uint64(1) << lgK
		cArr := []uint64{2, 5 * k, 6 * k, 60 * k}

		// Check c=0 => 0.0
		if got := iconEstimate(lgK, 0); got != 0.0 {
			t.Errorf("iconEstimate(%d,0)=%g, want 0.0", lgK, got)
		}
		// Check c=1 => 1.0
		if got := iconEstimate(lgK, 1); got != 1.0 {
			t.Errorf("iconEstimate(%d,1)=%g, want 1.0", lgK, got)
		}

		for _, c := range cArr {
			exact := exactIconEstimator(lgK, c)
			approx := iconEstimate(lgK, c)
			relDiff := math.Abs((approx - exact) / exact)
			threshold := math.Max(2e-6, 1.0/(80.0*float64(k)))
			if relDiff >= threshold {
				t.Errorf("lgK=%d, c=%d => exact=%g, approx=%g, relDiff=%g >= threshold=%g",
					lgK, c, exact, approx, relDiff, threshold)
			}
		}
	}
}

func TestIconEstimatorPrintlnTest(t *testing.T) {
	t.Logf("PRINTING: cpc.IconEstimatorTest (Go version). Class: %s", "cpc.icon_estimator_test")
}

func TestIconEstimatorCharacterization(t *testing.T) {
	lgK := 12
	k := 1 << lgK
	c := uint64(1)
	for float64(c) < float64(k)*64.0 {
		exact := exactIconEstimator(lgK, c)
		approx := iconEstimate(lgK, c)
		relDiff := (approx - exact) / exact
		t.Logf("%d\t%.19g\t%.19g\t%.19g", c, relDiff, exact, approx)

		a := c + 1
		b := (1001 * c) / 1000
		if a > b {
			c = a
		} else {
			c = b
		}
	}
}
