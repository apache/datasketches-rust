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

use crate::error::Error;

/// Computes an approximate lower bound for an unknown binomial proportion.
pub(crate) fn approximate_lower_bound_on_p(
    n: u64,
    k: u64,
    num_std_devs: f64,
) -> Result<f64, Error> {
    check_inputs(n, k)?;
    if n == 0 || k == 0 {
        Ok(0.0)
    } else if k == 1 {
        Ok(exact_lower_bound_on_p_k_eq_1(
            n,
            delta_of_num_stdevs(num_std_devs),
        ))
    } else if k == n {
        Ok(exact_lower_bound_on_p_k_eq_n(
            n,
            delta_of_num_stdevs(num_std_devs),
        ))
    } else {
        let x = abramowitz_stegun_formula_26p5p22((n - k) as f64 + 1.0, k as f64, -num_std_devs);
        Ok(1.0 - x)
    }
}

/// Computes an approximate upper bound for an unknown binomial proportion.
pub(crate) fn approximate_upper_bound_on_p(
    n: u64,
    k: u64,
    num_std_devs: f64,
) -> Result<f64, Error> {
    check_inputs(n, k)?;
    if n == 0 || k == n {
        Ok(1.0)
    } else if k == n - 1 {
        Ok(exact_upper_bound_on_p_k_eq_minusone(
            n,
            delta_of_num_stdevs(num_std_devs),
        ))
    } else if k == 0 {
        Ok(exact_upper_bound_on_p_k_eq_zero(
            n,
            delta_of_num_stdevs(num_std_devs),
        ))
    } else {
        let x = abramowitz_stegun_formula_26p5p22((n - k) as f64, k as f64 + 1.0, num_std_devs);
        Ok(1.0 - x)
    }
}

fn check_inputs(n: u64, k: u64) -> Result<(), Error> {
    if k > n {
        return Err(Error::invalid_argument(format!(
            "k cannot exceed n: k={k}, n={n}"
        )));
    }
    Ok(())
}

fn delta_of_num_stdevs(kappa: f64) -> f64 {
    normal_cdf(-kappa)
}

fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + erf(x / 2.0_f64.sqrt()))
}

fn erf(x: f64) -> f64 {
    if x < 0.0 {
        -erf_of_nonneg(-x)
    } else {
        erf_of_nonneg(x)
    }
}

fn erf_of_nonneg(x: f64) -> f64 {
    let a1 = 0.0705230784;
    let a2 = 0.0422820123;
    let a3 = 0.0092705272;
    let a4 = 0.0001520143;
    let a5 = 0.0002765672;
    let a6 = 0.0000430638;
    let x2 = x * x;
    let x3 = x2 * x;
    let x4 = x2 * x2;
    let x5 = x2 * x3;
    let x6 = x3 * x3;
    let sum = 1.0 + (a1 * x) + (a2 * x2) + (a3 * x3) + (a4 * x4) + (a5 * x5) + (a6 * x6);
    let sum2 = sum * sum;
    let sum4 = sum2 * sum2;
    let sum8 = sum4 * sum4;
    let sum16 = sum8 * sum8;
    1.0 - (1.0 / sum16)
}

fn abramowitz_stegun_formula_26p5p22(a: f64, b: f64, yp: f64) -> f64 {
    let b2m1 = (2.0 * b) - 1.0;
    let a2m1 = (2.0 * a) - 1.0;
    let lambda = ((yp * yp) - 3.0) / 6.0;
    let htmp = (1.0 / a2m1) + (1.0 / b2m1);
    let h = 2.0 / htmp;
    let term1 = (yp * (h + lambda).sqrt()) / h;
    let term2 = (1.0 / b2m1) - (1.0 / a2m1);
    let term3 = (lambda + (5.0 / 6.0)) - (2.0 / (3.0 * h));
    let w = term1 - (term2 * term3);
    a / (a + (b * (2.0 * w).exp()))
}

fn exact_upper_bound_on_p_k_eq_zero(n: u64, delta: f64) -> f64 {
    1.0 - delta.powf(1.0 / n as f64)
}

fn exact_lower_bound_on_p_k_eq_n(n: u64, delta: f64) -> f64 {
    delta.powf(1.0 / n as f64)
}

fn exact_lower_bound_on_p_k_eq_1(n: u64, delta: f64) -> f64 {
    1.0 - (1.0 - delta).powf(1.0 / n as f64)
}

fn exact_upper_bound_on_p_k_eq_minusone(n: u64, delta: f64) -> f64 {
    (1.0 - delta).powf(1.0 / n as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_counts() {
        assert!(approximate_lower_bound_on_p(1, 2, 2.0).is_err());
        assert!(approximate_upper_bound_on_p(1, 2, 2.0).is_err());
    }

    #[test]
    fn computes_exact_edge_cases() {
        assert_eq!(approximate_lower_bound_on_p(0, 0, 2.0).unwrap(), 0.0);
        assert_eq!(approximate_upper_bound_on_p(0, 0, 2.0).unwrap(), 1.0);
        assert_eq!(approximate_lower_bound_on_p(10, 0, 2.0).unwrap(), 0.0);
        assert_eq!(approximate_upper_bound_on_p(10, 10, 2.0).unwrap(), 1.0);
    }
}
