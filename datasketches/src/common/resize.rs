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

/// Controls internal cache growth for sketch families that support resizing.
///
/// For Theta sketches, the resize factor provides a dynamic trade-off between update speed and
/// memory use. Sketches configured with a resize factor greater than `X1` start with an internal
/// hash table size that is the smallest submultiple of the target nominal entries
/// and larger than the minimum required hash table size for that sketch.
///
/// When the sketch needs to grow, the resize factor is used as a multiplier for
/// the current sketch cache array size.
///
/// `X1` means no resizing is allowed and the sketch will be initialized at full size.
///
/// `X2` means the internal cache will start very small and double in size until the target size is
/// reached.
///
/// Similarly, `X4` is a factor of `4` and `X8` is a factor of `8`.
///
/// # Examples
///
/// ```
/// use datasketches::common::ResizeFactor;
///
/// let factor = ResizeFactor::X4;
/// assert_eq!(factor.value(), 4);
/// assert_eq!(factor.lg_value(), 2);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResizeFactor {
    /// Does not resize; configures the sketch at full size.
    X1,
    /// Resizes by a factor of `2`.
    X2,
    /// Resizes by a factor of `4`.
    X4,
    /// Resizes by a factor of `8`.
    X8,
}

impl ResizeFactor {
    /// Returns the base-2 logarithm of the resize factor.
    pub fn lg_value(self) -> u8 {
        match self {
            ResizeFactor::X1 => 0,
            ResizeFactor::X2 => 1,
            ResizeFactor::X4 => 2,
            ResizeFactor::X8 => 3,
        }
    }

    /// Returns the Resize Factor.
    pub fn value(self) -> usize {
        // 1 << lg_value
        match self {
            ResizeFactor::X1 => 1,
            ResizeFactor::X2 => 2,
            ResizeFactor::X4 => 4,
            ResizeFactor::X8 => 8,
        }
    }
}
