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

type hllSketchConfig struct { // extends hllSketchConfig
	lgConfigK  int
	tgtHllType TgtHllType
	curMode    curMode

	slotNoMask int // mask from lgConfigK to extract slotNo
}

func newHllSketchConfig(lgConfigK int, tgtHllType TgtHllType, curMode curMode) hllSketchConfig {
	return hllSketchConfig{
		lgConfigK:  lgConfigK,
		tgtHllType: tgtHllType,
		curMode:    curMode,
		slotNoMask: (1 << lgConfigK) - 1,
	}
}

func (c *hllSketchConfig) GetLgConfigK() int {
	return c.lgConfigK
}

func (c *hllSketchConfig) GetTgtHllType() TgtHllType {
	return c.tgtHllType
}

func (c *hllSketchConfig) GetCurMode() curMode {
	return c.curMode
}
