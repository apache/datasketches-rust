# Compact Theta Sketch (Implementation Notes)

This document describes how the Rust implementation should represent and interoperate with the
Apache DataSketches **Compact Theta Sketch** formats used by the Java and C++ libraries.

The intent is cross-language compatibility:

- **On-heap representation**: a minimal immutable form of a Theta sketch.
- **Binary format**: compatible serialization/deserialization (uncompressed `serVer = 3`), matching
  the preamble layout and flags used by `datasketches-java` and `datasketches-cpp`.

---

## compact theta sketch

A compact theta sketch is the immutable, serialized-friendly form of a theta sketch:

- It stores a **compact array** of retained hash values (no interstitial zeros like the update
  sketch’s hash table).
- It stores the **theta** threshold (`thetaLong`) and a **seed hash** (16-bit).
- It can be **ordered** (sorted ascending by hash) or **unordered**.
- It is **read-only** (cannot be updated), but is intended to participate in set operations.

### Hash invariants (cross-language)

Java/C++ (and this Rust crate) treat retained hashes as:

- 63-bit, non-negative values derived from MurmurHash3 (128-bit), taking `h1 >> 1`.
- `0` is reserved for empty slots and must not appear as a retained entry.
- Every retained entry must satisfy: `0 < hash < thetaLong`.
- `thetaLong` uses the signed max (`Long.MAX_VALUE` / `i64::MAX`) as “1.0” (no sampling).

In Rust, `MAX_THETA` is `i64::MAX as u64`, matching Java/C++.

### Compact-state truth table (Java/C++ behavior)

When producing a compact sketch (or serializing), Java defines a truth table over `(empty, curCount,
thetaLong)` and applies corrections in specific cases (see `CompactOperations.correctThetaOnCompact`
and related helpers):

- Normal empty: `empty = true`, `curCount = 0`, `thetaLong = MAX_THETA` → encoded as an 8-byte sketch.
- A sketch with `p < 1.0` but never updated may have `empty = true`, `curCount = 0`,
  `thetaLong < MAX_THETA` internally; Java corrects theta back to `MAX_THETA` during compaction/
  serialization so it becomes a normal empty compact sketch.
- A compact sketch can be **non-empty flag false** while still having `curCount = 0` and
  `thetaLong < MAX_THETA` as a possible result of set operations; this must serialize with
  `preLongs = 3` to preserve theta.

Rust should mirror these behaviors for cross-language parity.

---

## serailzation/deserialization

This section documents the uncompressed compact theta sketch binary format (`serVer = 3`), as used
by Java and C++.

### Endianness

Multi-byte integers are written in the platform’s native endianness in the Java/C++ implementations,
with a legacy “big-endian” bit in the flags byte (bit 0). In practice, modern platforms are little
endian and serialize with that bit cleared.

For Rust cross-platform robustness:

- **Serialize** using little-endian encodings and keep the big-endian flag bit clear.
- **Deserialize** by reading the big-endian flag bit and decoding multi-byte fields accordingly.

### Preamble (first 8 bytes)

All compact sketches start with a single 8-byte “preamble long” with fixed byte offsets:

| Byte offset | Field | Notes |
|---:|---|---|
| 0 | `preLongs` (low 6 bits) | Number of 8-byte longs in the preamble (1–3 for v3 compact). |
| 1 | `serVer` | Must be `3` for uncompressed compact sketches. |
| 2 | `family` | Must be `3` (`Family.COMPACT`). |
| 3 | `lgNomLongs` | **Unused for compact**; must be written as `0`. |
| 4 | `lgArrLongs` | **Unused for compact**; must be written as `0`. |
| 5 | `flags` | Bitfield, defined below. |
| 6–7 | `seedHash` (`u16`) | Must match `computeSeedHash(expectedSeed)` (Java/C++). |

### Flags byte (byte 5)

Bit positions follow Java/C++:

- Bit 0: big-endian legacy indicator (reserved in Java, still present in C++).
- Bit 1: read-only (must be set for compact sketches).
- Bit 2: empty.
- Bit 3: compact (must be set for compact sketches).
- Bit 4: ordered.
- Bit 5: single-item.
- Bits 6–7: reserved (must be zero).

### `preLongs` and payload layout (v3)

The total serialized size is `(preLongs + curCount) * 8` bytes, except the “empty compact” case
which is always exactly 8 bytes.

The format varies by `(empty, curCount, thetaLong)`:

#### 1) Empty compact sketch (8 bytes)

- `preLongs = 1`
- `flags.empty = 1`
- No `curCount`, no `thetaLong`, no entries.
- `thetaLong` is implicitly `MAX_THETA`.

#### 2) Single item (16 bytes)

- `preLongs = 1`
- `flags.singleItem = 1`, `flags.ordered = 1` (Java sets ordered for single-item).
- No `curCount`, no `thetaLong`.
- Payload: one 8-byte hash at offset `8`.

#### 3) Exact compact sketch (non-estimating)

- `thetaLong == MAX_THETA`
- `preLongs = 2` for `curCount > 1`; (for `curCount == 1`, Java uses the single-item form above).
- Long at offset `8` contains:
  - `curCount` as a 4-byte int at offsets `8..12`
  - `p` as a 4-byte float at offsets `12..16` (**not used**; Java writes `0.0` to match C++).
- Payload: `curCount` hashes starting at offset `preLongs * 8` (i.e. 16).

#### 4) Estimating compact sketch

- `thetaLong < MAX_THETA`
- `preLongs = 3`
- Long at offset `8` contains:
  - `curCount` as a 4-byte int at offsets `8..12`
  - `p` as a 4-byte float at offsets `12..16` (**not used**; Java writes `0.0`).
- Long at offset `16` contains:
  - `thetaLong` as an 8-byte long at offsets `16..24`.
- Payload: `curCount` hashes starting at offset `preLongs * 8` (i.e. 24).

### Serialization algorithm (v3, conceptual)

1. Determine `(empty, curCount, thetaLong, ordered)` from the compact sketch state.
2. Apply the “empty + never-updated sampled sketch” correction:
   if `empty && curCount == 0`, serialize as an empty compact with `thetaLong = MAX_THETA`.
3. Compute `preLongs`:
   - if `thetaLong < MAX_THETA` → `preLongs = 3`
   - else if `empty` → `preLongs = 1`
   - else if `curCount == 1` → `preLongs = 1` (single item)
   - else → `preLongs = 2`
4. Write preamble fields; ensure `lgNomLongs = 0`, `lgArrLongs = 0`, and set flags:
   `readOnly=1`, `compact=1`, plus `empty/ordered/singleItem` as applicable.
5. For `preLongs >= 2`, write `curCount` and `p = 0.0f`.
6. For `preLongs == 3`, write `thetaLong`.
7. Write the `curCount` retained hashes, ordered if requested.

### Deserialization requirements (v3)

When decoding bytes into a compact theta sketch:

- Validate `family == 3` and `serVer == 3`.
- Validate flags:
  - `compact` must be set
  - `readOnly` must be set
  - reserved bits must be zero (or tolerated for legacy inputs, depending on strictness)
- Validate `seedHash` matches the expected seed.
- If `empty` flag is set:
  - require `preLongs == 1` and total size == 8
  - return the empty compact sketch (`thetaLong = MAX_THETA`, `entries = []`)
- Else if `singleItem`:
  - require `preLongs == 1` and total size == 16
  - read one hash at offset 8
- Else:
  - read `curCount` (u32) at offset 8
  - if `preLongs == 3`, read `thetaLong` at offset 16; else `thetaLong = MAX_THETA`
  - read `curCount` hashes from offset `preLongs * 8`
  - optionally validate ordering if `ordered` flag is set

### Note on compressed format (`serVer = 4`)

Java/C++ also support a compressed, delta-encoded ordered compact sketch (`serVer = 4`), with a
different layout (variable-length retained entries count and packed deltas). This is not required
for basic cross-language interoperability, but can be added later for reduced serialized sizes.

