use crate::tdigest::{Centroid, TDigest};

impl IntoIterator for TDigest {
    type Item = (f64, u64);
    type IntoIter = TDigestIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        TDigestIntoIter {
            centroids: self.centroids,
            index: 0,
        }
    }
}

/// Iterator over the centroids of a TDigest.
pub struct TDigestIntoIter {
    centroids: Vec<Centroid>,
    index: usize,
}

impl Iterator for TDigestIntoIter {
    type Item = (f64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.centroids.len() {
            let centroid = self.centroids[self.index];
            self.index += 1;
            Some((centroid.mean, centroid.weight))
        } else {
            None
        }
    }
}

impl FromIterator<(f64, u64)> for TDigest {
    fn from_iter<I: IntoIterator<Item = (f64, u64)>>(iter: I) -> Self {
        let iter = iter.into_iter();

        let mut tmp = Vec::with_capacity(iter.size_hint().0);
        let mut total_weight = 0;
        for (mean, weight) in iter {
            tmp.push(Centroid { mean, weight });
            total_weight += weight;
        }

        let mut tdigest = TDigest::default();
        tdigest.do_merge(tmp, total_weight);
        tdigest
    }
}
