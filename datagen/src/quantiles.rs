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

use std::io;
use std::path::Path;

use datasketches::codec::SketchBytes;
use datasketches::codec::SketchSlice;
use datasketches::error::Error;
use datasketches::error::ErrorKind;
use datasketches::quantiles::QuantilesFloat;
use datasketches::quantiles::QuantilesSketch;
use datasketches::quantiles::QuantilesValue;

use crate::N_STD;
use crate::write_snapshot;

// Match the TCK's numeric order and unpadded decimal strings.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct NumericString(u64);

impl QuantilesValue for NumericString {
    const MIN_SERIALIZED_SIZE: usize = String::MIN_SERIALIZED_SIZE + 1;

    fn serialized_size(value: &Self) -> usize {
        String::serialized_size(&value.0.to_string())
    }

    fn serialize(value: &Self, bytes: &mut SketchBytes) {
        String::serialize(&value.0.to_string(), bytes);
    }

    fn deserialize(input: &mut SketchSlice<'_>) -> Result<Self, Error> {
        String::deserialize(input)?
            .parse()
            .map(Self)
            .map_err(|error| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid decimal string: {error}"),
                )
            })
    }
}

pub fn generate(output_dir: &Path) -> io::Result<()> {
    for n in N_STD {
        let mut doubles = QuantilesSketch::default();
        let mut strings = QuantilesSketch::default();
        for i in 1..=n {
            doubles.update(QuantilesFloat::<f64>::new(i as f64).unwrap());
            strings.update(NumericString(i));
        }

        write_snapshot(
            output_dir,
            &format!("quantiles_double_n{n}_rust.sk"),
            &doubles.serialize(),
        )?;
        write_snapshot(
            output_dir,
            &format!("quantiles_string_n{n}_rust.sk"),
            &strings.serialize(),
        )?;
    }
    Ok(())
}
