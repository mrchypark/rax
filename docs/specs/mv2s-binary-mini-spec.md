# MV2S Binary Mini-Spec (Frozen for RAX v0.1)

- Endianness: little-endian for fixed integers (`u8/u32/u64`).
- Length-prefixed fields: `bytes` and `string` are encoded as `u32(len) + payload`.
- String encoding: UTF-8 only.
- EOF behavior: decoder returns `UnexpectedEof` when not enough bytes remain.
- Overflow behavior: decoder returns `LengthOverflow` on index arithmetic overflow.
- UTF-8 behavior: decoder returns `Utf8` for invalid UTF-8 byte sequences.

Golden vector:
- Sequence: `u8(7), u32(42), u64(9999), string("mv2s"), bytes([1,2,3])`
- Bytes (hex): `07 2a000000 0f27000000000000 04000000 6d763273 03000000 010203`
