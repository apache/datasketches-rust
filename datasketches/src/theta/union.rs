// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::BTreeSet;

use crate::error::Error;
use crate::hash::compute_seed_hash;
use crate::theta::CompactThetaSketch;
use crate::theta::ThetaSketchView;

pub(super) struct ThetaUnion;

impl ThetaUnion {
    pub(super) fn compute<A: ThetaSketchView, B: ThetaSketchView>(
        sketch_a: &A,
        sketch_b: &B,
        seed: u64,
    ) -> Result<CompactThetaSketch, Error> {
        let seed_hash = compute_seed_hash(seed);
        validate_seed_hash(sketch_a, seed_hash, "sketch A")?;
        validate_seed_hash(sketch_b, seed_hash, "sketch B")?;

        let theta = sketch_a.theta64().min(sketch_b.theta64());
        let mut entries = BTreeSet::new();
        entries.extend(sketch_a.iter().filter(|&hash| hash < theta));
        entries.extend(sketch_b.iter().filter(|&hash| hash < theta));

        Ok(CompactThetaSketch::from_parts(
            entries.into_iter().collect(),
            theta,
            seed_hash,
            false,
            sketch_a.is_empty() && sketch_b.is_empty(),
        ))
    }
}

fn validate_seed_hash<S: ThetaSketchView>(
    sketch: &S,
    expected_seed_hash: u16,
    label: &str,
) -> Result<(), Error> {
    if !sketch.is_empty() && sketch.seed_hash() != expected_seed_hash {
        return Err(Error::invalid_argument(format!(
            "incompatible seed hash for {label}: expected {}, got {}",
            expected_seed_hash,
            sketch.seed_hash()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::hash::DEFAULT_UPDATE_SEED;
    use crate::theta::ThetaSketch;
    use crate::theta::union::ThetaUnion;

    fn sketch_with_range(start: u64, count: u64) -> ThetaSketch {
        let mut sketch = ThetaSketch::builder().build();
        for value in start..start + count {
            sketch.update(value);
        }
        sketch
    }

    #[test]
    fn exact_mode_half_overlap() {
        let sketch_a = sketch_with_range(0, 1000);
        let sketch_b = sketch_with_range(500, 1000);

        let union = ThetaUnion::compute(&sketch_a, &sketch_b, DEFAULT_UPDATE_SEED).unwrap();

        assert!(!union.is_empty());
        assert!(!union.is_estimation_mode());
        assert_eq!(union.estimate(), 1500.0);
    }

    #[test]
    fn empty_inputs_produce_empty_union() {
        let sketch_a = ThetaSketch::builder().build();
        let sketch_b = ThetaSketch::builder().build();

        let union = ThetaUnion::compute(&sketch_a, &sketch_b, DEFAULT_UPDATE_SEED).unwrap();

        assert!(union.is_empty());
        assert!(!union.is_estimation_mode());
        assert_eq!(union.num_retained(), 0);
    }

    #[test]
    fn seed_mismatch_on_non_empty_sketch_returns_error() {
        let mut sketch_a = ThetaSketch::builder().seed(123).build();
        sketch_a.update(1u64);
        let sketch_b = ThetaSketch::builder().build();

        assert!(ThetaUnion::compute(&sketch_a, &sketch_b, DEFAULT_UPDATE_SEED).is_err());
    }
}
