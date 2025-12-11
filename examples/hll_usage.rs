use datasketches::hll::{HllSketch, HllType};

fn main() {
    // Create a new HLL sketch
    // lg_k=12 means 4096 buckets, ~1.6% relative error
    let mut sketch = HllSketch::new(12, HllType::Hll8);

    println!("Created HLL sketch with lg_k=12 (K=4096)");
    println!("Initial estimate: {}", sketch.estimate());

    // Add some values
    println!("\nAdding 10,000 unique integers...");
    for i in 0..10_000 {
        sketch.update(i);
    }

    let estimate = sketch.estimate();
    let actual = 10_000;
    let error = ((estimate - actual as f64) / actual as f64 * 100.0).abs();

    println!("Actual unique values: {}", actual);
    println!("Estimated unique values: {:.2}", estimate);
    println!("Relative error: {:.2}%", error);

    // Test duplicate handling
    println!("\nAdding the same 10,000 values again...");
    for i in 0..10_000 {
        sketch.update(i);
    }

    let estimate2 = sketch.estimate();
    println!("Estimate after duplicates: {:.2}", estimate2);
    println!("(Should remain ~10,000, got {:.2})", estimate2);

    // Serialize and deserialize
    println!("\nSerializing sketch...");
    let bytes = sketch.serialize().unwrap();
    println!("Serialized size: {} bytes", bytes.len());

    let sketch2 = HllSketch::deserialize(&bytes).unwrap();
    let estimate3 = sketch2.estimate();
    println!("Estimate after deserialization: {:.2}", estimate3);

    println!("\nDebug: {:?}", sketch2);

    // Different types
    println!("\nHLL works with any hashable type:");
    let mut multi_sketch = HllSketch::new(10, HllType::Hll6);
    multi_sketch.update("hello");
    multi_sketch.update("world");
    multi_sketch.update(42);
    multi_sketch.update(vec![1, 2, 3]);
    println!("Estimate with mixed types: {:.2}", multi_sketch.estimate());

    println!("\nDebug: {:?}", multi_sketch);
}
