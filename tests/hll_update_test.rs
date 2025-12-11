use datasketches::hll::{HllSketch, HllType};

#[test]
fn test_basic_update() {
    let mut sketch = HllSketch::new(12, HllType::Hll8);

    // Initially empty
    assert_eq!(sketch.estimate(), 0.0);

    // Update with some values
    for i in 0..100 {
        sketch.update(&i);
    }

    let estimate = sketch.estimate();
    assert!(estimate > 0.0, "Estimate should be positive after updates");
    assert!(
        (estimate - 100.0).abs() < 20.0,
        "Estimate should be reasonably close to 100, got {}",
        estimate
    );
}

#[test]
fn test_list_to_set_promotion() {
    // Use lg_k=12, which has promotion threshold ~512 for List→Set
    let mut sketch = HllSketch::new(12, HllType::Hll8);

    // Add enough unique values to trigger promotion
    for i in 0..600 {
        sketch.update(&i);
    }

    let estimate = sketch.estimate();
    assert!(
        (estimate - 600.0).abs() < 100.0,
        "Estimate should be close to 600 after promotion, got {}",
        estimate
    );
}

#[test]
fn test_set_to_hll_promotion() {
    // Use lg_k=10 (K=1024), set promotes at 75% = 768
    let mut sketch = HllSketch::new(10, HllType::Hll8);

    // Add enough values to trigger List→Set→HLL promotions
    for i in 0..1000 {
        sketch.update(&i);
    }

    let estimate = sketch.estimate();
    assert!(
        (estimate - 1000.0).abs() < 150.0,
        "Estimate should be close to 1000 after full promotion, got {}",
        estimate
    );
}

#[test]
fn test_duplicate_handling() {
    let mut sketch = HllSketch::new(12, HllType::Hll8);

    // Add same values multiple times
    for _ in 0..10 {
        for i in 0..100 {
            sketch.update(&i);
        }
    }

    // Estimate should reflect ~100 unique values, not 1000
    let estimate = sketch.estimate();
    assert!(
        (estimate - 100.0).abs() < 20.0,
        "Duplicates should not inflate estimate, got {}",
        estimate
    );
}

#[test]
fn test_different_types() {
    let mut sketch = HllSketch::new(10, HllType::Hll8);

    // Mix different types
    sketch.update(&42i32);
    sketch.update(&"hello");
    sketch.update(&100u64);
    sketch.update(&true);
    sketch.update(&vec![1, 2, 3]);

    let estimate = sketch.estimate();
    assert!(estimate >= 5.0, "Should have at least 5 distinct values");
}

#[test]
fn test_hll4_type() {
    let mut sketch = HllSketch::new(12, HllType::Hll4);

    for i in 0..1000 {
        sketch.update(&i);
    }

    let estimate = sketch.estimate();
    assert!(
        (estimate - 1000.0).abs() < 200.0,
        "HLL4 estimate should be reasonable, got {}",
        estimate
    );
}

#[test]
fn test_hll6_type() {
    let mut sketch = HllSketch::new(12, HllType::Hll6);

    for i in 0..1000 {
        sketch.update(&i);
    }

    let estimate = sketch.estimate();
    assert!(
        (estimate - 1000.0).abs() < 200.0,
        "HLL6 estimate should be reasonable, got {}",
        estimate
    );
}

#[test]
fn test_serialization_roundtrip_after_updates() {
    let mut sketch1 = HllSketch::new(12, HllType::Hll8);

    // Add values and promote through all modes
    for i in 0..2000 {
        sketch1.update(&i);
    }

    let estimate1 = sketch1.estimate();

    // Serialize and deserialize
    let bytes = sketch1.serialize().unwrap();
    let sketch2 = HllSketch::deserialize(&bytes).unwrap();

    let estimate2 = sketch2.estimate();

    // Estimates should match after round-trip (allow some numerical error)
    let relative_error = (estimate1 - estimate2).abs() / estimate1;
    assert!(
        relative_error < 0.05,
        "Estimates should match after serialization (< 5% error), got {} vs {} ({:.2}% error)",
        estimate1,
        estimate2,
        relative_error * 100.0
    );
}

#[test]
fn test_large_cardinality() {
    let mut sketch = HllSketch::new(14, HllType::Hll8);

    // Add 100K unique values
    for i in 0..100_000 {
        sketch.update(&i);
    }

    let estimate = sketch.estimate();
    let relative_error = (estimate - 100_000.0).abs() / 100_000.0;

    // For lg_k=14, relative error should be ~1.04%
    assert!(
        relative_error < 0.05,
        "Relative error should be < 5% for large cardinality, got {:.2}%",
        relative_error * 100.0
    );
}

#[test]
fn test_equals_method() {
    let mut sketch1 = HllSketch::new(10, HllType::Hll8);
    let mut sketch2 = HllSketch::new(10, HllType::Hll8);

    // Both start equal (empty)
    assert!(sketch1.eq(&sketch2));

    // Add same values to both
    for i in 0..100 {
        sketch1.update(&i);
        sketch2.update(&i);
    }

    // Should still be equal
    assert!(sketch1.eq(&sketch2));

    // Add different value to sketch2
    sketch2.update(&999);

    // Now they're different
    assert!(!sketch1.eq(&sketch2));
}

#[test]
#[should_panic(expected = "lg_config_k must be in [4, 21]")]
fn test_invalid_lg_k_low() {
    HllSketch::new(3, HllType::Hll8);
}

#[test]
#[should_panic(expected = "lg_config_k must be in [4, 21]")]
fn test_invalid_lg_k_high() {
    HllSketch::new(22, HllType::Hll8);
}
