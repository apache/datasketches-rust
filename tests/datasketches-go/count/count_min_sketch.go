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

package count

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/rand"

	"github.com/apache/datasketches-go/internal"
)

// Implementation of the CountMin sketch data structure of Cormode and Muthukrishnan.
// [1] - http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf
type CountMinSketch struct {
	numBuckets  int32 // counter array size for each of the hashing function
	numHashes   int8  // number of hashing functions
	sketchSlice []int64
	seed        int64
	totalWeight int64
	hashSeeds   []int64
}

// NewCountMinSketch creates an instance of the CounrMin sketch given parameters numHashes, numBuckets and hash seed.
// The items inserted into the sketch can be arbitrary type, so long as they are hashable via murmurhash.
// Only update and estimate methods are added for uint64 and string types.
func NewCountMinSketch(numHashes int8, numBuckets int32, seed int64) (*CountMinSketch, error) {
	if numBuckets < 3 {
		return nil, errors.New("using fewer than 3 buckets incurs relative error greater than 1.0")
	}

	if numBuckets*int32(numHashes) >= 1<<30 {
		return nil, errors.New("these parameters generate a sketch that exceeds 2^30 elements")
	}

	rng := rand.New(rand.NewSource(seed))
	hashSeeds := make([]int64, numHashes)
	for i := range int(numHashes) {
		hashSeeds[i] = int64(rng.Int()) + seed
	}

	sketchSize := int(numBuckets * int32(numHashes))
	sketchSlice := make([]int64, sketchSize)

	return &CountMinSketch{
		numBuckets:  numBuckets,
		numHashes:   numHashes,
		sketchSlice: sketchSlice,
		seed:        seed,
		hashSeeds:   hashSeeds,
	}, nil
}

func (c *CountMinSketch) GetNumBuckets() int32 {
	return c.numBuckets
}

func (c *CountMinSketch) GetNumHashes() int8 {
	return c.numHashes
}

func (c *CountMinSketch) GetTotalWeight() int64 {
	return c.totalWeight
}

func (c *CountMinSketch) GetSeed() int64 {
	return c.seed
}

func (c *CountMinSketch) GetRelativeError() float64 {
	return math.Exp(1.0) / float64(c.numBuckets)
}

func (c *CountMinSketch) isEmpty() bool {
	return c.totalWeight == 0
}

func (c *CountMinSketch) getHashes(item []byte) []int64 {
	sketchUpdateLocations := make([]int64, c.numHashes)

	for i, s := range c.hashSeeds {
		h1, _ := internal.HashByteArrMurmur3(item, 0, len(item), uint64(s))
		bucketIndex := h1 % uint64(c.numBuckets)
		sketchUpdateLocations[i] = int64(i)*int64(c.numBuckets) + int64(bucketIndex)
	}

	return sketchUpdateLocations
}

func (c *CountMinSketch) Update(item []byte, weight int64) error {
	if len(item) == 0 {
		return nil
	}

	if weight < 0 {
		c.totalWeight += -weight
	} else {
		c.totalWeight += weight
	}

	hashLocations := c.getHashes(item)
	for _, h := range hashLocations {
		c.sketchSlice[h] += weight
	}
	return nil
}

func (c *CountMinSketch) UpdateUint64(item uint64, weight int64) error {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, item)
	return c.Update(b, weight)
}

func (c *CountMinSketch) UpdateString(item string, weight int64) error {
	if len(item) == 0 {
		return nil
	}

	return c.Update([]byte(item), weight)
}

func (c *CountMinSketch) GetEstimate(item []byte) int64 {
	if len(item) == 0 {
		return 0
	}

	hashLocations := c.getHashes(item)
	estimate := int64(math.MaxInt64)
	for _, h := range hashLocations {
		estimate = Min(estimate, c.sketchSlice[h])
	}
	return estimate
}

func (c *CountMinSketch) GetEstimateUint64(item uint64) int64 {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, item)
	return c.GetEstimate(b)
}

func (c *CountMinSketch) GetEstimateString(item string) int64 {
	if len(item) == 0 {
		return 0
	}
	return c.GetEstimate([]byte(item))
}

func (c *CountMinSketch) GetUpperBound(item []byte) int64 {
	return c.GetEstimate(item) + int64(c.GetRelativeError()*float64(c.GetTotalWeight()))
}

func (c *CountMinSketch) GetUpperBoundUint64(item uint64) int64 {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, item)
	return c.GetUpperBound(b)
}

func (c *CountMinSketch) GetLowerBound(item []byte) int64 {
	return c.GetEstimate(item)
}

func (c *CountMinSketch) GetLowerBoundUint64(item uint64) int64 {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, item)
	return c.GetLowerBound(b)
}

func (c *CountMinSketch) Merge(otherSketch *CountMinSketch) error {
	if c == otherSketch {
		return errors.New("cannot merge sketch with itself")
	}

	canMerge := c.GetNumHashes() == otherSketch.GetNumHashes() &&
		c.GetNumBuckets() == otherSketch.GetNumBuckets() &&
		c.GetSeed() == otherSketch.GetSeed()

	if !canMerge {
		return errors.New("sketches are incompatible")
	}

	for i := range c.sketchSlice {
		c.sketchSlice[i] += otherSketch.sketchSlice[i]
	}
	c.totalWeight += otherSketch.totalWeight

	return nil
}

func (c *CountMinSketch) Serialize(w io.Writer) error {
	preambleLongs := byte(PreambleLongsShort)
	serVer := byte(SerialVersion1)
	familyID := byte(internal.FamilyEnum.CountMinSketch.Id)

	var flagsByte byte
	if c.isEmpty() {
		flagsByte |= 1 << IsEmpty
	}
	unused32 := uint32(Null32)

	if err := binary.Write(w, binary.LittleEndian, preambleLongs); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, serVer); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, familyID); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, flagsByte); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, unused32); err != nil {
		return err
	}

	seedHash, err := internal.ComputeSeedHash(c.seed)
	if err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, c.numBuckets); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, c.numHashes); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, seedHash); err != nil {
		return err
	}
	unused8 := byte(Null8)
	if err := binary.Write(w, binary.LittleEndian, unused8); err != nil {
		return err
	}

	// Skip rest if sketch is empty
	if c.isEmpty() {
		return nil
	}

	if err := binary.Write(w, binary.LittleEndian, c.totalWeight); err != nil {
		return err
	}

	for _, h := range c.sketchSlice {
		err := binary.Write(w, binary.LittleEndian, h)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *CountMinSketch) Deserialize(b []byte, seed int64) (*CountMinSketch, error) {
	r := bytes.NewReader(b)
	var err error

	var preamble byte
	err = binary.Read(r, binary.LittleEndian, &preamble)
	if err != nil {
		return nil, err
	}

	var serVe byte
	err = binary.Read(r, binary.LittleEndian, &serVe)
	if err != nil {
		return nil, err
	}

	var familyID byte
	err = binary.Read(r, binary.LittleEndian, &familyID)
	if err != nil {
		return nil, err
	}

	var flag byte
	err = binary.Read(r, binary.LittleEndian, &flag)
	if err != nil {
		return nil, err
	}

	err = checkHeaderValidity(preamble, serVe, familyID, flag)
	if err != nil {
		return nil, err
	}

	unused32 := make([]byte, 4)
	_, err = r.Read(unused32)
	if err != nil {
		return nil, err
	}

	var numBuckets int32
	err = binary.Read(r, binary.LittleEndian, &numBuckets)
	if err != nil {
		return nil, err
	}

	var numHashes int8
	err = binary.Read(r, binary.LittleEndian, &numHashes)
	if err != nil {
		return nil, err
	}

	var seedHash int16
	err = binary.Read(r, binary.LittleEndian, &seedHash)
	if err != nil {
		return nil, err
	}

	var unused8 int8
	err = binary.Read(r, binary.LittleEndian, &unused8)
	if err != nil {
		return nil, err
	}

	cms, err := NewCountMinSketch(numHashes, numBuckets, seed)
	if err != nil {
		return nil, err
	}

	isEmpty := (flag & (1 << IsEmpty)) > 0
	if isEmpty {
		return cms, nil
	}

	var totalWeight int64
	err = binary.Read(r, binary.LittleEndian, &totalWeight)
	if err != nil {
		return nil, err
	}
	cms.totalWeight = totalWeight

	var w int64
	var i int
	for r.Len() > 0 {
		err = binary.Read(r, binary.LittleEndian, &w)
		if err != nil {
			return nil, err
		}
		cms.sketchSlice[i] = w
		i++
	}

	return cms, nil
}
