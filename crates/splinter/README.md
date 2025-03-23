<h1 align="center">Splinter</h1>
<p align="center">
  <a href="https://docs.rs/splinter"><img alt="docs.rs" src="https://img.shields.io/docsrs/splinter"></a>
  &nbsp;
  <a href="https://crates.io/crates/splinter"><img alt="crates.io" src="https://img.shields.io/crates/v/splinter.svg"></a>
</p>

Splinter is a compressed bitmap format similar to [Roaring], optimized specifically for small, sparse sets of 32-bit unsigned integers (`u32`).

## Key Features:

- **Tree-based Encoding**: Splinter encodes `u32` values into a 256-way tree structure by decomposing integers into big-endian component bytes. Leaf nodes efficiently transition from byte lists to compact bitmaps at up to 32 values.

- **Zero-copy Access**: Designed for efficient querying without deserialization, the `SplinterRef` type allows direct, zero-copy reads from any type implementing `AsRef<[u8]>`.

[Roaring]: https://roaringbitmap.org/

## Serialized Format

```
header (4 bytes)
    magic (2 bytes)
    unused (2 bytes)

footer (4 bytes)
    partitions (2 bytes)
    unused (2 bytes)

block (cardinality)
    cardinality == 256
        data: OMITTED
    cardinality < 32
        data: [u8; cardinality]
    else
        data: [u8; 32]

index (cardinality, offset_size: u16|u32)
    keys: block(cardinality)
    cardinalities: [u8; cardinality] // 1 based
    offsets: [offset_size; cardinality]

map (cardinality, off_type, val_type)
    values: [val_type(index->cardinalities[i]); cardinality]
    index (cardinality, off_type)

splinter
    header
    map (footer->partitions, u32,
      map (cardinality, u32,
        map (cardinality, u16, block)))
    footer

```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE] or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT] or https://opensource.org/licenses/MIT)

at your option.

[LICENSE-APACHE]: https://github.com/orbitinghail/graft/blob/main/LICENSE-APACHE
[LICENSE-MIT]: https://github.com/orbitinghail/graft/blob/main/LICENSE-MIT

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you shall be dual licensed as above, without any
additional terms or conditions.
