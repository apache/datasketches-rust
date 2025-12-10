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
	"time"

	"github.com/apache/datasketches-go/common"
)

type QuickMergingValidation struct {
	hfmt, dfmt string
	hStrArr    []string

	vIn uint64

	lgMinK, lgMaxK, incLgK int

	printStream io.Writer
	printWriter io.Writer
}

func NewQuickMergingValidation(
	lgMinK, lgMaxK, incLgK int,
	ps, pw io.Writer,
) *QuickMergingValidation {
	qmv := &QuickMergingValidation{
		lgMinK:      lgMinK,
		lgMaxK:      lgMaxK,
		incLgK:      incLgK,
		printStream: ps,
		printWriter: pw,
	}
	qmv.assembleFormats()
	return qmv
}

func (qmv *QuickMergingValidation) toInterfaceSlice(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

func (qmv *QuickMergingValidation) Start() error {
	qmv.printf(qmv.hfmt, qmv.toInterfaceSlice(qmv.hStrArr)...)
	return qmv.doRangeOfLgK()
}

func (qmv *QuickMergingValidation) doRangeOfLgK() error {
	for lgK := qmv.lgMinK; lgK <= qmv.lgMaxK; lgK += qmv.incLgK {
		err := qmv.multiQuickTest(lgK)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qmv *QuickMergingValidation) multiQuickTest(lgK int) error {
	k := 1 << lgK
	targetC := []int64{
		0,
		1,
		int64((3*k)/32) - 1,
		int64(k / 3),
		int64(k),
		int64((7 * k) / 2),
	}
	for i := 0; i < len(targetC); i++ {
		for j := 0; j < len(targetC); j++ {
			err := qmv.quickTest(lgK, targetC[i], targetC[j])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (qmv *QuickMergingValidation) quickTest(lgK int, cA, cB int64) error {
	skA, _ := NewCpcSketchWithDefault(lgK)
	skB, _ := NewCpcSketchWithDefault(lgK)
	skD, _ := NewCpcSketchWithDefault(lgK) // direct combined

	t0 := time.Now().UnixNano()
	for skA.numCoupons < uint64(cA) {
		qmv.vIn += common.InverseGoldenU64
		in := qmv.vIn
		_ = skA.UpdateUint64(in)
		_ = skD.UpdateUint64(in)
	}
	t1 := time.Now().UnixNano()
	for skB.numCoupons < uint64(cB) {
		qmv.vIn += common.InverseGoldenU64
		in := qmv.vIn
		_ = skB.UpdateUint64(in)
		_ = skD.UpdateUint64(in)
	}
	t2 := time.Now().UnixNano()

	ugM, _ := NewCpcUnionSketchWithDefault(lgK)
	_ = ugM.Update(skA)
	t3 := time.Now().UnixNano()
	_ = ugM.Update(skB)
	t4 := time.Now().UnixNano()
	skR, _ := ugM.GetResult()
	t5 := time.Now().UnixNano()

	if !specialEquals(skD, skR, false, true) {
		return fmt.Errorf("merged result differs from direct combined sketch")
	}

	flavorA := fmt.Sprintf("%s%2d", skA.getFlavor(), skA.windowOffset)
	flavorB := fmt.Sprintf("%s%2d", skB.getFlavor(), skB.windowOffset)
	flavorM := fmt.Sprintf("%s%2d", skR.getFlavor(), skR.windowOffset)

	updAms := float64(t1-t0) / 2e6
	updBms := float64(t2-t1) / 2e6
	mrgAms := float64(t3-t2) / 1e6
	mrgBms := float64(t4-t3) / 1e6
	rsltms := float64(t5-t4) / 1e6

	qmv.printf(
		qmv.dfmt,
		lgK,
		cA,
		cB,
		flavorA,
		flavorB,
		flavorM,
		updAms,
		updBms,
		mrgAms,
		mrgBms,
		rsltms,
	)
	return nil
}

func (qmv *QuickMergingValidation) printf(format string, args ...interface{}) {
	if qmv.printStream != nil {
		fmt.Fprintf(qmv.printStream, format, args...)
	}
	if qmv.printWriter != nil {
		fmt.Fprintf(qmv.printWriter, format, args...)
	}
}

func (qmv *QuickMergingValidation) assembleFormats() {
	columns := []struct {
		name      string
		headerFmt string
		dataFmt   string
	}{
		{"lgK", "%3s", "%3d"},
		{"Ca", "%10s", "%10d"},
		{"Cb", "%10s", "%10d"},
		{"Flavor_a", "%10s", "%10s"},
		{"Flavor_b", "%10s", "%10s"},
		{"Flavor_m", "%10s", "%10s"},
		{"updA_mS", "%9s", "%9.3f"},
		{"updB_mS", "%9s", "%9.3f"},
		{"mrgA_mS", "%9s", "%9.3f"},
		{"mrgB_mS", "%9s", "%9.3f"},
		{"rslt_mS", "%9s", "%9.3f"},
	}
	qmv.hStrArr = make([]string, len(columns))

	headerLine := "\nQuick Merging Validation\n"
	dataLine := ""
	for i, col := range columns {
		qmv.hStrArr[i] = col.name
		sep := "\t"
		if i == len(columns)-1 {
			sep = "\n"
		}
		headerLine += fmt.Sprintf(col.headerFmt, col.name) + sep
		dataLine += col.dataFmt + sep
	}
	qmv.hfmt = headerLine
	qmv.dfmt = dataLine
}
