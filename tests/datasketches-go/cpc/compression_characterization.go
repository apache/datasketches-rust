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
	"math/bits"
	"time"

	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/internal"
)

// CompressionCharacterization is a harness that tests compression performance
// for various log(K) and N, measuring the time spent in creation, updates, compression,
// serialization, deserialization, uncompression, and equality checks.
type CompressionCharacterization struct {
	// Inputs
	lgMinK   int
	lgMaxK   int
	lgMinT   int
	lgMaxT   int
	lgMulK   int
	uPPO     int
	incLgK   int
	printStr io.Writer
	printWtr io.Writer

	// Internal formatting for the table
	hfmt    string
	dfmt    string
	hStrArr []string

	// A running counter for updates
	vIn uint64
}

func NewCompressionCharacterization(
	lgMinK, lgMaxK, lgMinT, lgMaxT, lgMulK, uPPO, incLgK int,
	pS, pW io.Writer,
) *CompressionCharacterization {
	// Ensure some parameters are at least 1
	if uPPO < 1 {
		uPPO = 1
	}
	if incLgK < 1 {
		incLgK = 1
	}

	cc := &CompressionCharacterization{
		lgMinK:   lgMinK,
		lgMaxK:   lgMaxK,
		lgMinT:   lgMinT,
		lgMaxT:   lgMaxT,
		lgMulK:   lgMulK,
		uPPO:     uPPO,
		incLgK:   incLgK,
		printStr: pS,
		printWtr: pW,
	}
	cc.assembleFormats()
	return cc
}

// Start runs the entire characterization, printing the header first.
func (cc *CompressionCharacterization) Start() error {
	cc.printf(cc.hfmt, cc.toInterfaceSlice(cc.hStrArr)...)
	return cc.doRangeOfLgK()
}

// doRangeOfLgK loops from lgMinK to lgMaxK in steps of incLgK
func (cc *CompressionCharacterization) doRangeOfLgK() error {
	for lgK := cc.lgMinK; lgK <= cc.lgMaxK; lgK += cc.incLgK {
		err := cc.doRangeOfNAtLgK(lgK)
		if err != nil {
			return err
		}
	}
	return nil
}

// doRangeOfNAtLgK iterates over n up to 2^(lgK + lgMulK)
func (cc *CompressionCharacterization) doRangeOfNAtLgK(lgK int) error {
	var n int64 = 1
	lgMaxN := lgK + cc.lgMulK
	maxN := int64(1 << lgMaxN)

	// The slope for computing total trials:
	// slope = -(lgMaxT - lgMinT) / (lgMaxN)
	slope := float64(-(cc.lgMaxT - cc.lgMinT)) / float64(lgMaxN)

	for n <= maxN {
		// totalTrials = 2^( (slope * log2(n)) + lgMaxT ), clamped to at least 2^lgMinT
		// we do a partial linear interpolation in log space
		lgT := (slope * math.Log2(float64(n))) + float64(cc.lgMaxT)
		// Round up to the next power of two, but also at least 2^lgMinT
		totTrials := common.CeilingPowerOf2(int(math.Round(math.Pow(2.0, lgT))))
		minTrials := 1 << cc.lgMinT
		if totTrials < minTrials {
			totTrials = minTrials
		}
		err := cc.doTrialsAtLgKAtN(lgK, n, totTrials)
		if err != nil {
			return err
		}

		// step n using powerSeriesNextDouble with base 2.0
		newN := common.PowerSeriesNextDouble(cc.uPPO, float64(n), true, 2.0)
		n = int64(math.Round(newN))
	}
	return nil
}

// doTrialsAtLgKAtN runs the wave-based test for a given (lgK, n, totalTrials)
func (cc *CompressionCharacterization) doTrialsAtLgKAtN(lgK int, n int64, totalTrials int) error {
	k := 1 << lgK
	minNK := k
	if int64(k) > n {
		minNK = int(n)
	}
	nOverK := float64(n) / float64(k)
	lgTotTrials := bits.TrailingZeros32(uint32(totalTrials))
	// We'll define waves = 2^(lgWaves). Each wave has trialsPerWave = 2^(lgTotTrials - lgWaves).
	lgWaves := lgTotTrials - 10
	if lgWaves < 0 {
		lgWaves = 0
	}
	trialsPerWave := 1 << (lgTotTrials - lgWaves)
	wavesCount := 1 << lgWaves

	streamSketches := make([]*CpcSketch, trialsPerWave)
	compressedStates1 := make([]*CpcCompressedState, trialsPerWave)
	memoryArr := make([][]byte, trialsPerWave)
	compressedStates2 := make([]*CpcCompressedState, trialsPerWave)
	unCompressedSketches := make([]*CpcSketch, trialsPerWave)

	var totalC, totalW int64
	var sumCtorNS, sumUpdNS, sumComNS, sumSerNS, sumDesNS, sumUncNS, sumEquNS int64

	startTime := time.Now()

	// wave loop
	for w := 0; w < wavesCount; w++ {
		// Construct sketches
		nanoStart := time.Now().UnixNano()
		for t := 0; t < trialsPerWave; t++ {
			sketch, err := NewCpcSketch(lgK, internal.DEFAULT_UPDATE_SEED)
			if err != nil {
				return err
			}
			streamSketches[t] = sketch
		}
		nanoEnd := time.Now().UnixNano()
		sumCtorNS += nanoEnd - nanoStart
		nanoStart = nanoEnd

		// Update each sketch
		for t := 0; t < trialsPerWave; t++ {
			sketch := streamSketches[t]
			for i := int64(0); i < n; i++ {
				cc.vIn += common.InverseGoldenU64
				_ = sketch.UpdateUint64(cc.vIn)
			}
		}
		nanoEnd = time.Now().UnixNano()
		sumUpdNS += nanoEnd - nanoStart
		nanoStart = nanoEnd

		// Compress each sketch
		for t := 0; t < trialsPerWave; t++ {
			sketch := streamSketches[t]
			state, err := NewCpcCompressedStateFromSketch(sketch)
			if err != nil {
				panic(fmt.Sprintf("Compression error: %v", err))
			}
			compressedStates1[t] = state
			totalC += int64(sketch.numCoupons)
			// approximate measure of total words in CSV + CW
			totalW += int64(state.CsvLengthInts + state.CwLengthInts)
		}
		nanoEnd = time.Now().UnixNano()
		sumComNS += nanoEnd - nanoStart
		nanoStart = nanoEnd

		// Convert each CompressedState to a byte slice
		for t := 0; t < trialsPerWave; t++ {
			state := compressedStates1[t]
			mem, err := state.exportToMemory()
			if err != nil {
				panic(fmt.Sprintf("exportToMemory error: %v", err))
			}
			memoryArr[t] = mem
		}
		nanoEnd = time.Now().UnixNano()
		sumSerNS += nanoEnd - nanoStart
		nanoStart = nanoEnd

		// Import from memory to new CompressedState
		for t := 0; t < trialsPerWave; t++ {
			mem := memoryArr[t]
			state, err := importFromMemory(mem)
			if err != nil {
				panic(fmt.Sprintf("importFromMemory error: %v", err))
			}
			compressedStates2[t] = state
		}
		nanoEnd = time.Now().UnixNano()
		sumDesNS += nanoEnd - nanoStart
		nanoStart = nanoEnd

		// Uncompress into a new CpcSketch
		for t := 0; t < trialsPerWave; t++ {
			state := compressedStates2[t]
			uncSk, err := uncompressSketch(state, internal.DEFAULT_UPDATE_SEED)
			if err != nil {
				return err
			}
			unCompressedSketches[t] = uncSk
		}
		nanoEnd = time.Now().UnixNano()
		sumUncNS += nanoEnd - nanoStart
		nanoStart = nanoEnd

		// Equality check
		for t := 0; t < trialsPerWave; t++ {
			s1 := streamSketches[t]
			s2 := unCompressedSketches[t]
			if !specialEquals(s1, s2, false, false) {
				return fmt.Errorf("uncompressed sketch not equal to original")
			}
		}
		nanoEnd = time.Now().UnixNano()
		sumEquNS += nanoEnd - nanoStart
		nanoStart = nanoEnd
	}

	totalSeconds := time.Since(startTime).Seconds()
	avgC := float64(totalC) / float64(totalTrials)
	avgCoK := avgC / float64(k)
	avgWords := float64(totalW) / float64(totalTrials)
	avgBytes := 4.0 * avgWords // 4 bytes per int

	// compute average times
	// Each sum is total for all waves, so we divide by totalTrials
	avgCtor := float64(sumCtorNS) / float64(totalTrials)
	avgUpd := float64(sumUpdNS) / float64(totalTrials)
	avgCom := float64(sumComNS) / float64(totalTrials)
	avgSer := float64(sumSerNS) / float64(totalTrials)
	avgDes := float64(sumDesNS) / float64(totalTrials)
	avgUnc := float64(sumUncNS) / float64(totalTrials)
	avgEqu := float64(sumEquNS) / float64(totalTrials)

	avgUpdPerN := avgUpd / float64(n)
	avgComPer2C := avgCom / (2.0 * avgC)
	avgComPerK := avgCom / float64(k)
	avgSerPerW := avgSer / avgWords
	avgDesPerW := avgDes / avgWords
	avgUncPer2C := avgUnc / (2.0 * avgC)
	avgUncPerK := avgUnc / float64(k)
	avgEquPerMinNK := avgEqu / float64(minNK)

	// final flavor/offset from last wave
	lastSketch := unCompressedSketches[len(unCompressedSketches)-1]
	finFlavor := lastSketch.getFlavor()
	finOff := lastSketch.windowOffset
	flavorOff := fmt.Sprintf("%s%2d", finFlavor.String(), finOff)

	// Print final line
	cc.printf(
		cc.dfmt,
		lgK,
		totalTrials,
		n,
		minNK,
		avgCoK,
		flavorOff,
		nOverK,
		avgBytes,
		avgCtor,
		avgUpd,
		avgCom,
		avgSer,
		avgDes,
		avgUnc,
		avgEqu,
		avgUpdPerN,
		avgComPer2C,
		avgComPerK,
		avgSerPerW,
		avgDesPerW,
		avgUncPer2C,
		avgUncPerK,
		avgEquPerMinNK,
		totalSeconds,
	)
	return nil
}

// assembleFormats sets up the column headers & format strings for the final output.
func (cc *CompressionCharacterization) assembleFormats() {
	columns := []struct {
		name      string
		headerFmt string
		dataFmt   string
	}{
		{"lgK", "%3s", "%3d"},
		{"Trials", "%9s", "%9d"},
		{"n", "%12s", "%12d"},
		{"MinKN", "%9s", "%9d"},
		{"AvgC/K", "%9s", "%9.4g"},
		{"FinFlavor", "%11s", "%11s"},
		{"N/K", "%9s", "%9.4g"},
		{"AvgBytes", "%9s", "%9.0f"},
		{"AvgCtor_nS", "%11s", "%11.0f"},
		{"AvgUpd_nS", "%10s", "%10.4e"},
		{"AvgCom_nS", "%10s", "%10.0f"},
		{"AvgSer_nS", "%10s", "%10.2f"},
		{"AvgDes_nS", "%10s", "%10.2f"},
		{"AvgUnc_nS", "%10s", "%10.0f"},
		{"AvgEqu_nS", "%10s", "%10.0f"},
		{"AvgUpd_nSperN", "%14s", "%14.2f"},
		{"AvgCom_nSper2C", "%15s", "%15.4g"},
		{"AvgCom_nSperK", "%14s", "%14.4g"},
		{"AvgSer_nSperW", "%14s", "%14.2f"},
		{"AvgDes_nSperW", "%14s", "%14.2f"},
		{"AvgUnc_nSper2C", "%15s", "%15.4g"},
		{"AvgUnc_nSperK", "%14s", "%14.4g"},
		{"AvgEqu_nSperMinNK", "%18s", "%18.4g"},
		{"Total_S", "%8s", "%8.3f"},
	}

	cc.hStrArr = make([]string, len(columns))
	headerLine := "\nCompression Characterization\n"
	dataLine := ""

	for i, col := range columns {
		cc.hStrArr[i] = col.name
		sep := "\t"
		if i == len(columns)-1 {
			sep = "\n"
		}
		headerLine += fmt.Sprintf(col.headerFmt, col.name) + sep
		dataLine += col.dataFmt + sep
	}
	cc.hfmt = headerLine
	cc.dfmt = dataLine
}

// printf writes to both outputs if they exist
func (cc *CompressionCharacterization) printf(format string, args ...interface{}) {
	if cc.printStr != nil {
		fmt.Fprintf(cc.printStr, format, args...)
	}
	if cc.printWtr != nil {
		fmt.Fprintf(cc.printWtr, format, args...)
	}
}

// toInterfaceSlice helps pass a slice of strings to fmt.Fprintf for the header.
func (cc *CompressionCharacterization) toInterfaceSlice(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i := range ss {
		out[i] = ss[i]
	}
	return out
}
