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

import "fmt"

func Example() {

	// Creating a first HLL sketch
	sketch, _ := NewHllSketch(10, TgtHllTypeHll4)

	// Add 100 distinct values
	for i := 0; i < 100; i++ {
		sketch.UpdateInt64(int64(i))
	}
	est, _ := sketch.GetEstimate()
	fmt.Printf("Cardinality estimation of first sketch(0-100): %d\n", int64(est))

	// Add another 100000 distinct values (repeating the 100 previously added)
	for i := 0; i < 100000; i++ {
		sketch.UpdateInt64(int64(i))
	}
	est, _ = sketch.GetEstimate()
	fmt.Printf("Cardinality estimation of first sketch(0-100000): %d\n", int64(est))

	// Get the upper bound (2nd std deviation) of the estimate
	ub, _ := sketch.GetUpperBound(2)
	fmt.Printf("Upper bound (2nd std deviation) of first sketch(0-100000): %d\n", int64(ub))

	// Get the lower bound (2nd std deviation) of the estimate
	lb, _ := sketch.GetLowerBound(2)
	fmt.Printf("Lower bound (2nd std deviation) of first sketch(0-100000): %d\n", int64(lb))
	fmt.Printf("\n")

	// Creating a second HLL sketch
	anotherSketch, _ := NewHllSketch(10, TgtHllTypeHll4)
	// Add another 100000 distinct values (starting at 50000
	for i := 50000; i < 150000; i++ {
		anotherSketch.UpdateInt64(int64(i))
	}
	est, _ = anotherSketch.GetEstimate()
	fmt.Printf("Cardinality estimation of second sketch(50000-150000): %d\n", int64(est))
	fmt.Printf("\n")

	// Creating a union sketch and merge the two sketches
	unionsketchBldr, _ := NewUnion(10)
	unionsketchBldr.UpdateSketch(sketch)
	unionsketchBldr.UpdateSketch(anotherSketch)
	unionSketch, _ := unionsketchBldr.GetResult(TgtHllTypeHll4)

	unionEst, _ := unionSketch.GetEstimate()
	fmt.Printf("Cardinality estimation of first and second union: %d\n", int64(unionEst))

	// Get the upper bound (2nd std deviation) of the estimate
	ub, _ = unionSketch.GetUpperBound(2)
	fmt.Printf("Upper bound (2nd std deviation) of first and second union: %d\n", int64(ub))

	// Get the lower bound (2nd std deviation) of the estimate
	lb, _ = unionSketch.GetLowerBound(2)
	fmt.Printf("Lower bound (2nd std deviation) of first and second union: %d\n", int64(lb))
	fmt.Printf("\n")

	// Serialize and deserialize the union sketch
	serializedSketch, _ := unionSketch.ToUpdatableSlice()
	reloadedSketch, _ := NewHllSketchFromSlice(serializedSketch, true)
	reloadedEst, _ := reloadedSketch.GetEstimate()
	fmt.Printf("Cardinality estimation of reloaded unioned sketch: %d\n", int64(reloadedEst))

	// Output:
	// Cardinality estimation of first sketch(0-100): 100
	// Cardinality estimation of first sketch(0-100000): 104403
	// Upper bound (2nd std deviation) of first sketch(0-100000): 109997
	// Lower bound (2nd std deviation) of first sketch(0-100000): 99134
	//
	// Cardinality estimation of second sketch(50000-150000): 96390
	//
	// Cardinality estimation of first and second union: 151359
	// Upper bound (2nd std deviation) of first and second union: 161518
	// Lower bound (2nd std deviation) of first and second union: 141853
	//
	// Cardinality estimation of reloaded unioned sketch: 151359
}
