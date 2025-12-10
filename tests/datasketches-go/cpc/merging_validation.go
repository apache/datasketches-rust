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
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/apache/datasketches-go/common"
)

type MergingValidation struct {
	hfmt, dfmt string
	hStrArr    []string
	vIn        uint64 // increments each update

	// Inputs
	lgMinK, lgMaxK int
	lgMulK         int
	uPPO           int
	incLgK         int

	printStream io.Writer
	printWriter io.Writer
}

func NewMergingValidation(
	lgMinK, lgMaxK, lgMulK, uPPO, incLgK int,
	pS, pW io.Writer,
) *MergingValidation {

	if uPPO < 1 {
		uPPO = 1
	}
	if incLgK < 1 {
		incLgK = 1
	}

	mv := &MergingValidation{
		lgMinK:      lgMinK,
		lgMaxK:      lgMaxK,
		lgMulK:      lgMulK,
		uPPO:        uPPO,
		incLgK:      incLgK,
		printStream: pS,
		printWriter: pW,
	}
	mv.assembleFormats()
	return mv
}

// Start prints the header, then calls doRangeOfLgK.
func (mv *MergingValidation) Start() error {
	mv.printf(mv.hfmt, mv.toInterfaceSlice(mv.hStrArr)...)
	return mv.doRangeOfLgK()
}

// doRangeOfLgK calls multiTestMerging for various (lgK, lgK±1) combinations.
func (mv *MergingValidation) doRangeOfLgK() error {
	for lgK := mv.lgMinK; lgK <= mv.lgMaxK; lgK += mv.incLgK {
		if err := mv.multiTestMerging(lgK, lgK-1, lgK-1); err != nil {
			return err
		}
		if err := mv.multiTestMerging(lgK, lgK-1, lgK); err != nil {
			return err
		}
		if err := mv.multiTestMerging(lgK, lgK-1, lgK+1); err != nil {
			return err
		}

		if err := mv.multiTestMerging(lgK, lgK, lgK-1); err != nil {
			return err
		}
		if err := mv.multiTestMerging(lgK, lgK, lgK); err != nil {
			return err
		}
		if err := mv.multiTestMerging(lgK, lgK, lgK+1); err != nil {
			return err
		}

		if err := mv.multiTestMerging(lgK, lgK+1, lgK-1); err != nil {
			return err
		}
		if err := mv.multiTestMerging(lgK, lgK+1, lgK); err != nil {
			return err
		}
		if err := mv.multiTestMerging(lgK, lgK+1, lgK+1); err != nil {
			return err
		}
	}
	return nil
}

// multiTestMerging loops over nA and nB up to 2^(lgKa+lgMulK) and 2^(lgKb+lgMulK), respectively.
func (mv *MergingValidation) multiTestMerging(lgKm, lgKa, lgKb int) error {
	limA := int64(1 << uint(lgKa+mv.lgMulK))
	limB := int64(1 << uint(lgKb+mv.lgMulK))

	var nA int64 = 0
	for nA <= limA {
		var nB int64 = 0
		for nB <= limB {
			if err := mv.testMerging(lgKm, lgKa, lgKb, nA, nB); err != nil {
				return err
			}
			nB = int64(math.Round(common.PowerSeriesNextDouble(mv.uPPO, float64(nB), true, 2.0)))
		}
		nA = int64(math.Round(common.PowerSeriesNextDouble(mv.uPPO, float64(nA), true, 2.0)))
	}
	return nil
}

// testMerging does the actual test for one combination of (lgKm, lgKa, lgKb, nA, nB).
// It merges two sketches A and B into a union ugM, compares it with a direct combined sketch D,
// and returns an error if any discrepancy is found.
func (mv *MergingValidation) testMerging(lgKm, lgKa, lgKb int, nA, nB int64) error {
	// Create the union with the minimum lgK among lgKm, lgKa, and lgKb.
	minLg := lgKm
	if lgKa < minLg {
		minLg = lgKa
	}
	if lgKb < minLg {
		minLg = lgKb
	}
	ugM, err := NewCpcUnionSketchWithDefault(lgKm)
	if err != nil {
		return fmt.Errorf("failed to create CpcUnion: %v", err)
	}

	// Determine the direct sketch's lgK: the minimum among non-empty sketches.
	lgKd := lgKm
	if lgKa < lgKd && nA != 0 {
		lgKd = lgKa
	}
	if lgKb < lgKd && nB != 0 {
		lgKd = lgKb
	}

	skD, err := NewCpcSketchWithDefault(lgKd)
	if err != nil {
		return fmt.Errorf("failed to create CpcSketch: %v", err)
	}

	skA, err := NewCpcSketchWithDefault(lgKa)
	if err != nil {
		return fmt.Errorf("failed to create CpcSketch: %v", err)
	}
	skB, err := NewCpcSketchWithDefault(lgKb)
	if err != nil {
		return fmt.Errorf("failed to create CpcSketch: %v", err)
	}

	for i := int64(0); i < nA; i++ {
		mv.vIn += common.InverseGoldenU64
		in := mv.vIn
		if err = skA.UpdateUint64(in); err != nil {
			return fmt.Errorf("skA.UpdateUint64 error: %v", err)
		}
		if err = skD.UpdateUint64(in); err != nil {
			return fmt.Errorf("skD.UpdateUint64 error: %v", err)
		}
	}
	for i := int64(0); i < nB; i++ {
		mv.vIn += common.InverseGoldenU64
		in := mv.vIn
		if err = skB.UpdateUint64(in); err != nil {
			return fmt.Errorf("skB.UpdateUint64 error: %v", err)
		}
		if err = skD.UpdateUint64(in); err != nil {
			return fmt.Errorf("skD.UpdateUint64 error: %v", err)
		}
	}

	if err := ugM.Update(skA); err != nil {
		return fmt.Errorf("union update skA error: %v", err)
	}
	if err := ugM.Update(skB); err != nil {
		return fmt.Errorf("union update skB error: %v", err)
	}

	finalLgKm := ugM.lgK
	matrixM, err := ugM.GetBitMatrix()
	if err != nil {
		return fmt.Errorf("ugM.GetBitMatrix error: %v", err)
	}

	cM := ugM.getNumCoupons()
	cD := skD.numCoupons

	flavorD := skD.getFlavor()
	flavorA := skA.getFlavor()
	flavorB := skB.getFlavor()

	dOff := skD.windowOffset
	aOff := skA.windowOffset
	bOff := skB.windowOffset
	flavorDoff := fmt.Sprintf("%s%2d", flavorD.String(), dOff)
	flavorAoff := fmt.Sprintf("%s%2d", flavorA.String(), aOff)
	flavorBoff := fmt.Sprintf("%s%2d", flavorB.String(), bOff)

	iconEstD := iconEstimate(lgKd, cD)

	if finalLgKm > lgKm {
		return fmt.Errorf("finalLgKm > lgKm")
	}
	if cM > (skA.numCoupons + skB.numCoupons) {
		return fmt.Errorf("union coupon count too large")
	}
	if cM != cD {
		return fmt.Errorf("mismatch coupon counts union=%d direct=%d", cM, cD)
	}
	if finalLgKm != lgKd {
		return fmt.Errorf("union lgK mismatch: got %d, expected %d", finalLgKm, lgKd)
	}

	// Compare union bit matrix with direct sketch bit matrix.
	matrixD, err := skD.bitMatrixOfSketch()
	if err != nil {
		return fmt.Errorf("bitMatrixOfSketch error: %v", err)
	}
	if len(matrixM) != len(matrixD) {
		return fmt.Errorf("matrix length mismatch union vs direct")
	}
	for i := range matrixM {
		if matrixM[i] != matrixD[i] {
			return fmt.Errorf("matrix bits mismatch union vs direct")
		}
	}

	// Compare union's result with direct.
	skR, err := ugM.GetResult()
	if err != nil {
		return err
	}
	iconEstR := iconEstimate(skR.lgK, skR.numCoupons)
	if math.Abs(iconEstD-iconEstR) > 1e-9 {
		return fmt.Errorf("ICON mismatch direct=%.9g union=%.9g", iconEstD, iconEstR)
	}

	if !specialEquals(skD, skR, false, true) {
		return fmt.Errorf("skD != skR")
	}

	// Print final line
	mv.printf(mv.dfmt,
		lgKm, lgKa, lgKb, lgKd,
		nA, nB, nA+nB,
		flavorAoff, flavorBoff, flavorDoff,
		skA.numCoupons, skB.numCoupons, cD, iconEstR,
	)
	return nil
}

// assembleFormats sets up columns for printing the final results.
func (mv *MergingValidation) assembleFormats() {
	assy := [][]string{
		{"lgKm", "%4s", "%4d"},
		{"lgKa", "%4s", "%4d"},
		{"lgKb", "%4s", "%4d"},
		{"lgKfd", "%6s", "%6d"},
		{"nA", "%12s", "%12d"},
		{"nB", "%12s", "%12d"},
		{"nA+nB", "%12s", "%12d"},
		{"Flavor_a", "%11s", "%11s"},
		{"Flavor_b", "%11s", "%11s"},
		{"Flavor_fd", "%11s", "%11s"},
		{"Coupons_a", "%9s", "%9d"},
		{"Coupons_b", "%9s", "%9d"},
		{"Coupons_fd", "%9s", "%9d"},
		{"IconEst_dr", "%12s", "%,12.0f"},
	}

	cols := len(assy)
	mv.hStrArr = make([]string, cols)
	var headerFmt strings.Builder
	var dataFmt strings.Builder

	headerFmt.WriteString("\nMerging Validation\n")
	for i := 0; i < cols; i++ {
		mv.hStrArr[i] = assy[i][0]
		headerFmt.WriteString(assy[i][1])
		if i < cols-1 {
			headerFmt.WriteString("\t")
		} else {
			headerFmt.WriteString("\n")
		}
		dataFmt.WriteString(assy[i][2])
		if i < cols-1 {
			dataFmt.WriteString("\t")
		} else {
			dataFmt.WriteString("\n")
		}
	}
	mv.hfmt = headerFmt.String()
	mv.dfmt = dataFmt.String()
}

// printf writes to both printStream and printWriter if they are not nil.
func (mv *MergingValidation) printf(format string, args ...interface{}) {
	if mv.printStream != nil {
		fmt.Fprintf(mv.printStream, format, args...)
	}
	if mv.printWriter != nil {
		fmt.Fprintf(mv.printWriter, format, args...)
	}
}

// toInterfaceSlice helps pass a slice of strings to fmt.Fprintf for the header.
func (mv *MergingValidation) toInterfaceSlice(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i := range ss {
		out[i] = ss[i]
	}
	return out
}
