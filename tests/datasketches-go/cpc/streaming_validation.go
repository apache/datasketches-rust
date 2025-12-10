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

	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
)

// StreamingValidation is a test/characterization harness that repeatedly
// updates a CPC sketch and a BitMatrix, checks their correctness, and logs results.
type StreamingValidation struct {
	// Config inputs
	lgMinK      int
	lgMaxK      int
	trials      int
	ppoN        int
	printStream io.Writer
	printWriter io.Writer

	// Internal formatting for table columns
	hfmt    string
	dfmt    string
	hStrArr []string

	// Internal state
	vIn    uint64 // increments each update
	sketch *CpcSketch
	matrix *BitMatrix
}

func NewStreamingValidation(
	lgMinK, lgMaxK, trials, ppoN int,
	pS, pW io.Writer,
) *StreamingValidation {
	sv := &StreamingValidation{
		lgMinK:      lgMinK,
		lgMaxK:      lgMaxK,
		trials:      trials,
		ppoN:        ppoN,
		printStream: pS,
		printWriter: pW,
	}
	sv.assembleStrings()
	return sv
}

// Start begins the streaming validation process, printing column headers and running the test loops.
func (sv *StreamingValidation) Start() {
	sv.printf(sv.hfmt, sv.stringArrayToInterface(sv.hStrArr)...)
	sv.doRangeOfLgK()
}

// doRangeOfLgK loops from lgMinK to lgMaxK inclusive.
func (sv *StreamingValidation) doRangeOfLgK() {
	for lgK := sv.lgMinK; lgK <= sv.lgMaxK; lgK++ {
		sv.doRangeOfNAtLgK(lgK)
	}
}

// doRangeOfNAtLgK loops over n from 1 up to 64 * (1 << lgK),
// stepping in a power-series style (ppoN increments).
func (sv *StreamingValidation) doRangeOfNAtLgK(lgK int) {
	var n int64 = 1
	maxN := int64(64) * (1 << lgK)
	for n < maxN {
		sv.doTrialsAtLgKAtN(lgK, n)
		// Use powerSeriesNextDouble to pick the next n.
		n = int64(math.Round(common.PowerSeriesNextDouble(sv.ppoN, float64(n), true, 2.0)))
	}
}

// doTrialsAtLgKAtN performs the configured number of trials at a given lgK and n.
func (sv *StreamingValidation) doTrialsAtLgKAtN(lgK int, n int64) {
	var sumC, sumIconEst, sumHipEst float64

	// We'll create sketches once outside the loop, but we reset them each trial.
	// Also create a BitMatrix to compare the set bits.
	sketch, _ := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
	matrix := NewBitMatrixWithSeed(lgK, internal.DEFAULT_UPDATE_SEED)

	for t := 0; t < sv.trials; t++ {
		sketch.reset()
		matrix.Reset()

		for i := int64(0); i < n; i++ {
			sv.vIn += common.InverseGoldenU64
			in := sv.vIn
			// Update the CPC sketch
			_ = sketch.UpdateUint64(in)
			// Update the BitMatrix
			matrix.Update(int64(in))
		}
		// Accumulate sums
		sumC += float64(sketch.numCoupons)
		sumIconEst += iconEstimate(lgK, sketch.numCoupons)
		sumHipEst += sketch.hipEstAccum

		// Check that the number of coupons matches the matrix
		if matrix.GetNumCoupons() != sketch.numCoupons {
			panic(fmt.Sprintf("Mismatch in numCoupons: bitMatrix=%d, cpcSketch=%d",
				matrix.GetNumCoupons(), sketch.numCoupons))
		}
		// Check that the actual bit matrix matches
		bitMat, err := sketch.bitMatrixOfSketch()
		if err != nil {
			panic(fmt.Sprintf("bitMatrixOfSketch error: %v", err))
		}
		mat2 := matrix.GetMatrix()
		// Compare row by row
		if len(bitMat) != len(mat2) {
			panic(fmt.Sprintf("Mismatch: bitMatrixOfSketch len=%d, matrix.GetMatrix len=%d",
				len(bitMat), len(mat2)))
		}
		for i := range bitMat {
			if bitMat[i] != mat2[i] {
				panic(fmt.Sprintf("Mismatch at row %d: bitMat=%x, mat2=%x", i, bitMat[i], mat2[i]))
			}
		}
	}

	// final state from the last trial
	finC := sketch.numCoupons
	finFlavor := sketch.getFlavor()
	finOff := sketch.windowOffset
	avgC := sumC / float64(sv.trials)
	avgIconEst := sumIconEst / float64(sv.trials)
	avgHipEst := sumHipEst / float64(sv.trials)

	// Print the row
	sv.printf(
		sv.dfmt,
		lgK,
		sv.trials,
		n,
		finC,
		finFlavor.String(),
		finOff,
		avgC,
		avgIconEst,
		avgHipEst,
	)
}

func (sv *StreamingValidation) assembleStrings() {
	// columns: name, headerFormat, dataFormat
	columns := []struct {
		name      string
		headerFmt string
		dataFmt   string
	}{
		{"lgK", "%3s", "%3d"},
		{"Trials", "%7s", "%7d"},
		{"n", "%8s", "%8d"},
		{"FinC", "%8s", "%8d"},
		{"FinFlavor", "%10s", "%10s"},
		{"FinOff", "%7s", "%7d"},
		{"AvgC", "%12s", "%12.3f"},
		{"AvgICON", "%12s", "%12.3f"},
		{"AvgHIP", "%12s", "%12.3f"},
	}
	sv.hStrArr = make([]string, len(columns))

	// Build a single line for the header format, and one for the data format.
	headerLine := "\nStreaming Validation\n"
	dataLine := ""
	for i, col := range columns {
		sv.hStrArr[i] = col.name
		// Add a tab or line break
		sep := "\t"
		if i == len(columns)-1 {
			sep = "\n"
		}
		headerLine += fmt.Sprintf(col.headerFmt, col.name) + sep
		dataLine += col.dataFmt
		dataLine += sep
	}
	sv.hfmt = headerLine
	sv.dfmt = dataLine
}

// printf writes to both printStream and printWriter if non-nil.
func (sv *StreamingValidation) printf(format string, args ...interface{}) {
	if sv.printStream != nil {
		fmt.Fprintf(sv.printStream, format, args...)
	}
	if sv.printWriter != nil {
		fmt.Fprintf(sv.printWriter, format, args...)
	}
}

// stringArrayToInterface helps pass a []string to a varargs function (like Fprintf).
func (sv *StreamingValidation) stringArrayToInterface(ss []string) []interface{} {
	ii := make([]interface{}, len(ss))
	for i := range ss {
		ii[i] = ss[i]
	}
	return ii
}
