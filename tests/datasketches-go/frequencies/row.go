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

package frequencies

import (
	"fmt"
)

type Row struct {
	item int64
	est  int64
	ub   int64
	lb   int64
}

type RowItem[C comparable] struct {
	item C
	est  int64
	ub   int64
	lb   int64
}

func newRow(item int64, estimate int64, ub int64, lb int64) *Row {
	return &Row{
		item: item,
		est:  estimate,
		ub:   ub,
		lb:   lb,
	}
}

func newRowItem[C comparable](item C, estimate int64, ub int64, lb int64) *RowItem[C] {
	return &RowItem[C]{
		item: item,
		est:  estimate,
		ub:   ub,
		lb:   lb,
	}
}

func (r *Row) String() string {
	return fmt.Sprintf("  %20d%20d%20d %d", r.est, r.ub, r.lb, r.item)
}

func (r *Row) GetItem() int64 {
	return r.item
}

func (r *Row) GetEstimate() int64 {
	return r.est
}

func (r *Row) GetUpperBound() int64 {
	return r.ub
}

func (r *Row) GetLowerBound() int64 {
	return r.lb
}

func (r *RowItem[C]) String() string {
	return fmt.Sprintf("  %20d%20d%20d %v", r.est, r.ub, r.lb, r.item)
}

func (r *RowItem[C]) GetItem() C {
	return r.item
}

func (r *RowItem[C]) GetEstimate() int64 {
	return r.est
}

func (r *RowItem[C]) GetUpperBound() int64 {
	return r.ub
}

func (r *RowItem[C]) GetLowerBound() int64 {
	return r.lb
}
