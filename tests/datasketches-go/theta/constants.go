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

package theta

import "math"

// ResizeFactor represents the hash table resize factor
type ResizeFactor uint8

const (
	// ResizeX1 - resize by factor of 1 (no resize)
	ResizeX1 ResizeFactor = iota
	// ResizeX2 - resize by factor of 2
	ResizeX2
	// ResizeX4 - resize by factor of 4
	ResizeX4
	// ResizeX8 - resize by factor of 8
	ResizeX8
)

// DefaultResizeFactor is the default resize factor
const DefaultResizeFactor = ResizeX8

// MaxTheta is the max theta - signed max for compatibility with Java
const MaxTheta uint64 = math.MaxInt64

// MinLgK is the min log2 of K
const MinLgK uint8 = 5

// MaxLgK is the max log2 of K
const MaxLgK uint8 = 26

// DefaultLgK is the default log2 of K
const DefaultLgK uint8 = 12

// DefaultSeed is the default seed for hashing
const DefaultSeed uint64 = 9001
