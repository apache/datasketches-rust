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

import "math"

var (
	iconErrorConstant = math.Log(2.0)                  //0.693147180559945286
	hipErrorConstant  = math.Sqrt(math.Log(2.0) / 2.0) //0.588705011257737332

	iconHighSideData = []int{
		//1,    2,    3,   kappa
		//                 lgK numTrials
		8031, 8559, 9309, // 4 1000000
		7084, 7959, 8660, // 5 1000000
		7141, 7514, 7876, // 6 1000000
		7458, 7430, 7572, // 7 1000000
		6892, 7141, 7497, // 8 1000000
		6889, 7132, 7290, // 9 1000000
		7075, 7118, 7185, // 10 1000000
		7040, 7047, 7085, // 11 1000000
		6993, 7019, 7053, // 12 1046369
		6953, 7001, 6983, // 13 1043411
		6944, 6966, 7004, // 14 1000297
	}

	hipHighSideData = []int{
		//1,    2,    3,   kappa
		//                 lgK numTrials
		5855, 6688, 7391, // 4 1000000
		5886, 6444, 6923, // 5 1000000
		5885, 6254, 6594, // 6 1000000
		5889, 6134, 6326, // 7 1000000
		5900, 6072, 6203, // 8 1000000
		5875, 6005, 6089, // 9 1000000
		5871, 5980, 6040, // 10 1000000
		5889, 5941, 6015, // 11 1000000
		5871, 5926, 5973, // 12 1046369
		5866, 5901, 5915, // 13 1043411
		5880, 5914, 5953, // 14 1000297
	}
)

func iconConfidenceLB(lgK int, numCoupons uint64, kappa int) float64 {
	if numCoupons == 0 {
		return 0.0
	}
	x := iconErrorConstant
	if lgK <= 14 {
		x = float64(iconHighSideData[(3*(lgK-4))+(kappa-1)]) / 10000.0
	}
	rel := x / math.Sqrt(float64(uint64(1)<<lgK))
	eps := float64(kappa) * rel
	est := iconEstimate(lgK, numCoupons)
	result := est / (1.0 + eps)
	if result < float64(numCoupons) {
		result = float64(numCoupons)
	}
	return result
}

func iconConfidenceUB(lgK int, numCoupons uint64, kappa int) float64 {
	if numCoupons == 0 {
		return 0.0
	}
	x := iconErrorConstant
	if lgK <= 14 {
		x = float64(iconHighSideData[(3*(lgK-4))+(kappa-1)]) / 10000.0
	}
	rel := x / math.Sqrt(float64(uint64(1)<<lgK))
	eps := float64(kappa) * rel
	est := iconEstimate(lgK, numCoupons)
	result := est / (1.0 - eps)
	return math.Ceil(result)
}

func hipConfidenceLB(lgK int, numCoupons uint64, hipEstAccum float64, kappa int) float64 {
	if numCoupons == 0 {
		return 0.0
	}
	x := hipErrorConstant
	if lgK <= 14 {
		x = float64(hipHighSideData[(3*(lgK-4))+(kappa-1)]) / 10000.0
	}
	rel := x / math.Sqrt(float64(uint64(1)<<lgK))
	eps := float64(kappa) * rel
	est := hipEstAccum
	result := est / (1.0 + eps)
	if result < float64(numCoupons) {
		result = float64(numCoupons)
	}
	return result
}

func hipConfidenceUB(lgK int, numCoupons uint64, hipEstAccum float64, kappa int) float64 {
	if numCoupons == 0 {
		return 0.0
	}
	x := hipErrorConstant
	if lgK <= 14 {
		x = float64(hipHighSideData[(3*(lgK-4))+(kappa-1)]) / 10000.0
	}
	rel := x / math.Sqrt(float64(uint64(1)<<lgK))
	eps := float64(kappa) * rel
	est := hipEstAccum
	result := est / (1.0 - eps)
	return math.Ceil(result)
}
