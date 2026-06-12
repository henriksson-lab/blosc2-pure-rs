//! Parsing and writing of the per-chunk Blosc/Blosc2 header.
//!
//! Every compressed chunk starts with either a 16-byte Blosc1 header or a
//! 32-byte extended Blosc2 header. The extended form is signalled by having
//! both [`BLOSC_DOSHUFFLE`] and [`BLOSC_DOBITSHUFFLE`] set in the flags byte
//! and carries the filter pipeline, user-defined codec slot, codec metadata,
//! and the two Blosc2 flag bytes.

use crate::constants::*;

/// Parsed chunk header.
///
/// Mirrors the on-disk layout: the first seven fields are the legacy 16-byte
/// header, the remainder lives in the extended 32-byte Blosc2 header.
#[derive(Debug, Clone, Default)]
pub struct ChunkHeader {
    /// Blosc chunk format version.
    pub version: u8,
    /// Format version of the internal codec stream (normally 1).
    pub versionlz: u8,
    /// Flags byte: filters applied, memcpy bit, split bit, and codec format code (bits 5-7).
    pub flags: u8,
    /// Size in bytes of the atomic type.
    pub typesize: u8,
    /// Uncompressed payload size, not including the header.
    pub nbytes: i32,
    /// Size in bytes of the internal blocks (or block count when [`BLOSC2_VL_BLOCKS`] is set).
    pub blocksize: i32,
    /// Total compressed chunk size including the header.
    pub cbytes: i32,
    /// Filter pipeline (one ID per slot, applied in increasing index order).
    pub filters: [u8; BLOSC2_MAX_FILTERS],
    /// Per-filter metadata, one byte per slot in [`filters`](Self::filters).
    pub filters_meta: [u8; BLOSC2_MAX_FILTERS],
    /// User-defined codec ID, valid when the codec format bits equal [`BLOSC_UDCODEC_FORMAT`].
    pub udcompcode: u8,
    /// Codec metadata byte (codec-specific).
    pub compcode_meta: u8,
    /// Primary Blosc2 flags byte (dict use, endianness, special values, instrumentation, ...).
    pub blosc2_flags: u8,
    /// Secondary Blosc2 flags byte (currently only the variable-length-blocks bit).
    pub blosc2_flags2: u8,
}

impl ChunkHeader {
    fn read_legacy_fields(data: &[u8]) -> Result<Self, &'static str> {
        if data.len() < BLOSC_MIN_HEADER_LENGTH {
            return Err("Buffer too small for header");
        }

        Ok(ChunkHeader {
            version: data[BLOSC2_CHUNK_VERSION],
            versionlz: data[BLOSC2_CHUNK_VERSIONLZ],
            flags: data[BLOSC2_CHUNK_FLAGS],
            typesize: data[BLOSC2_CHUNK_TYPESIZE],
            nbytes: i32::from_le_bytes(data[4..8].try_into().unwrap()),
            blocksize: i32::from_le_bytes(data[8..12].try_into().unwrap()),
            cbytes: i32::from_le_bytes(data[12..16].try_into().unwrap()),
            ..Default::default()
        })
    }

    fn validate_legacy_fields(&self) -> Result<(), &'static str> {
        if self.cbytes < BLOSC_MIN_HEADER_LENGTH as i32 {
            return Err("Invalid cbytes");
        }
        if self.blocksize <= 0 || self.blocksize as usize > BLOSC2_MAXBLOCKSIZE {
            return Err("Invalid blocksize");
        }
        if self.typesize == 0 {
            return Err("Invalid typesize");
        }
        Ok(())
    }

    fn flags_to_filters(&mut self) {
        if self.flags & BLOSC_DOBITSHUFFLE != 0 {
            self.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_BITSHUFFLE;
        } else if self.flags & BLOSC_DOSHUFFLE != 0 {
            self.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
        }
        if self.flags & BLOSC_DODELTA != 0 {
            self.filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_DELTA;
        }
    }

    fn normalize_regular_blocksize(&mut self) {
        if !self.vl_blocks() && self.nbytes > 0 && self.blocksize > self.nbytes {
            self.blocksize = self.nbytes;
        }
    }

    /// Parses only the 16-byte legacy prefix of a chunk header.
    ///
    /// This mirrors C-Blosc metadata queries, which inspect only the size and
    /// legacy scalar fields without requiring the full compressed payload.
    pub fn read_minimal(data: &[u8]) -> Result<Self, &'static str> {
        let mut h = Self::read_legacy_fields(data)?;
        h.validate_legacy_fields()?;
        h.flags_to_filters();
        h.normalize_regular_blocksize();
        Ok(h)
    }

    /// Returns `true` if this chunk uses the extended 32-byte Blosc2 header.
    ///
    /// Encoded as both the shuffle and bit-shuffle flags being set in the flags byte.
    pub fn is_extended(&self) -> bool {
        (self.flags & BLOSC_DOSHUFFLE != 0) && (self.flags & BLOSC_DOBITSHUFFLE != 0)
    }

    /// Returns the 3-bit codec format code embedded in flags bits 5-7.
    pub fn compformat(&self) -> u8 {
        (self.flags >> 5) & 0x07
    }

    /// Returns the effective codec ID.
    ///
    /// For user-defined codec format headers, the value from
    /// [`udcompcode`](Self::udcompcode) is returned; otherwise the 3-bit format
    /// code is mapped back to a codec ID via [`compformat_to_compcode`].
    pub fn compcode(&self) -> u8 {
        if self.compformat() == BLOSC_UDCODEC_FORMAT {
            self.udcompcode
        } else {
            compformat_to_compcode(self.compformat())
        }
    }

    /// Returns `true` if the payload is a raw memcpy of the source (no compression applied).
    pub fn memcpyed(&self) -> bool {
        self.flags & BLOSC_MEMCPYED != 0
    }

    /// Returns `true` if blocks must not be split into per-typesize streams.
    pub fn dont_split(&self) -> bool {
        self.flags & BLOSC_DONT_SPLIT != 0
    }

    /// Returns the 3-bit special-value type stored in bits 4-6 of `blosc2_flags`.
    ///
    /// See the `BLOSC2_SPECIAL_*` constants for the possible values.
    pub fn special_type(&self) -> u8 {
        (self.blosc2_flags >> 4) & BLOSC2_SPECIAL_MASK
    }

    /// Returns `true` if dictionary-based compression was used.
    pub fn use_dict(&self) -> bool {
        self.blosc2_flags & BLOSC2_USEDICT != 0
    }

    /// Returns `true` if the chunk uses variable-length blocks.
    pub fn vl_blocks(&self) -> bool {
        self.blosc2_flags2 & BLOSC2_VL_BLOCKS != 0
    }

    /// Returns the actual on-disk header length in bytes (16 or 32).
    pub fn header_len(&self) -> usize {
        if self.is_extended() {
            BLOSC_EXTENDED_HEADER_LENGTH
        } else {
            BLOSC_MIN_HEADER_LENGTH
        }
    }

    /// Returns the number of blocks that make up this chunk.
    ///
    /// Regular chunks compute this as `ceil(nbytes / blocksize)`. For
    /// variable-length-block chunks, the header `blocksize` field stores the
    /// encoded block count.
    pub fn nblocks(&self) -> usize {
        if self.vl_blocks() {
            return self.blocksize.max(0) as usize;
        }
        if self.nbytes <= 0 || self.blocksize <= 0 {
            return 0;
        }
        (self.nbytes as usize).div_ceil(self.blocksize as usize)
    }

    /// Returns the size of the last (possibly partial) block.
    ///
    /// When `nbytes` is an exact multiple of `blocksize` this returns 0,
    /// matching C-Blosc2's `leftover` field. Returns 0 for invalid signed sizes.
    pub fn leftover(&self) -> usize {
        if self.vl_blocks() {
            return 0;
        }
        if self.nbytes <= 0 || self.blocksize <= 0 {
            return 0;
        }
        self.nbytes as usize % self.blocksize as usize
    }

    /// Parses a chunk header from raw bytes.
    ///
    /// Requires at least [`BLOSC_MIN_HEADER_LENGTH`] bytes. Extended-header
    /// fields are required when both extended-header flag bits are set.
    pub fn read(data: &[u8]) -> Result<Self, &'static str> {
        let mut h = Self::read_legacy_fields(data)?;
        h.validate_legacy_fields()?;

        // Extended header (32 bytes)
        if h.is_extended() {
            if h.cbytes < BLOSC_EXTENDED_HEADER_LENGTH as i32 {
                return Err("Invalid cbytes");
            }
            if data.len() < BLOSC_EXTENDED_HEADER_LENGTH {
                return Err("Buffer too small for extended header");
            }
            h.filters
                .copy_from_slice(&data[BLOSC2_CHUNK_FILTER_CODES..BLOSC2_CHUNK_FILTER_CODES + 6]);
            h.udcompcode = data[BLOSC2_CHUNK_UDCOMPCODE];
            h.compcode_meta = data[BLOSC2_CHUNK_COMPCODE_META];
            h.filters_meta
                .copy_from_slice(&data[BLOSC2_CHUNK_FILTER_META..BLOSC2_CHUNK_FILTER_META + 6]);
            h.blosc2_flags2 = data[BLOSC2_CHUNK_BLOSC2_FLAGS2];
            h.blosc2_flags = data[BLOSC2_CHUNK_BLOSC2_FLAGS];
            if h.vl_blocks() && h.special_type() != BLOSC2_NO_SPECIAL {
                return Err("Invalid VL-block special chunk");
            }
            if h.special_type() != BLOSC2_NO_SPECIAL {
                if h.special_type() == BLOSC2_SPECIAL_VALUE {
                    let typesize = h.cbytes - BLOSC_EXTENDED_HEADER_LENGTH as i32;
                    if typesize <= 0
                        || typesize as usize > BLOSC2_MAXTYPESIZE
                        || typesize > h.nbytes
                        || h.nbytes % typesize != 0
                    {
                        return Err("Invalid special value typesize");
                    }
                } else if h.nbytes % h.typesize as i32 != 0 {
                    return Err("Invalid special chunk nbytes");
                }
            }
            if h.version == BLOSC2_VERSION_FORMAT_ALPHA {
                h.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_NOFILTER;
                h.filters_meta[BLOSC2_MAX_FILTERS - 1] = 0;
            }
        } else {
            h.flags_to_filters();
        }
        if h.version > BLOSC2_VERSION_FORMAT && (h.blosc2_flags2 & !BLOSC2_VL_BLOCKS) != 0 {
            return Err("Unsupported chunk version features");
        }
        h.normalize_regular_blocksize();

        Ok(h)
    }

    /// Writes a 32-byte extended header into `buf`, returning an error if it does not fit.
    ///
    /// All multi-byte integer fields are written in little-endian order, matching the on-disk format.
    pub fn try_write(&self, buf: &mut [u8]) -> Result<(), &'static str> {
        if buf.len() < BLOSC_EXTENDED_HEADER_LENGTH {
            return Err("Buffer too small for extended header");
        }

        buf[BLOSC2_CHUNK_VERSION] = self.version;
        buf[BLOSC2_CHUNK_VERSIONLZ] = self.versionlz;
        buf[BLOSC2_CHUNK_FLAGS] = self.flags;
        buf[BLOSC2_CHUNK_TYPESIZE] = self.typesize;
        buf[4..8].copy_from_slice(&self.nbytes.to_le_bytes());
        let mut blocksize = self.blocksize;
        if !self.vl_blocks() && self.nbytes > 0 && blocksize > self.nbytes {
            blocksize = self.nbytes;
        }
        buf[8..12].copy_from_slice(&blocksize.to_le_bytes());
        buf[12..16].copy_from_slice(&self.cbytes.to_le_bytes());
        buf[BLOSC2_CHUNK_FILTER_CODES..BLOSC2_CHUNK_FILTER_CODES + 6]
            .copy_from_slice(&self.filters);
        buf[BLOSC2_CHUNK_UDCOMPCODE] = self.udcompcode;
        buf[BLOSC2_CHUNK_COMPCODE_META] = self.compcode_meta;
        buf[BLOSC2_CHUNK_FILTER_META..BLOSC2_CHUNK_FILTER_META + 6]
            .copy_from_slice(&self.filters_meta);
        buf[BLOSC2_CHUNK_BLOSC2_FLAGS2] = self.blosc2_flags2;
        let blosc2_flags = self.blosc2_flags;
        #[cfg(target_endian = "big")]
        let blosc2_flags = blosc2_flags | BLOSC2_BIGENDIAN;
        buf[BLOSC2_CHUNK_BLOSC2_FLAGS] = blosc2_flags;
        Ok(())
    }

    /// Writes a 32-byte extended header into `buf`.
    ///
    /// Panics if `buf` is shorter than [`BLOSC_EXTENDED_HEADER_LENGTH`]. Use
    /// [`try_write`](Self::try_write) for a fallible variant.
    pub fn write(&self, buf: &mut [u8]) {
        self.try_write(buf)
            .expect("buffer must fit a Blosc2 extended header");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let h = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: 1,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE | (BLOSC_LZ4_FORMAT << 5),
            typesize: 4,
            nbytes: 40000,
            blocksize: 8192,
            cbytes: 5000,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            filters_meta: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };

        let mut buf = [0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        h.try_write(&mut buf).unwrap();
        let h2 = ChunkHeader::read(&buf).unwrap();

        assert_eq!(h.version, h2.version);
        assert_eq!(h.typesize, h2.typesize);
        assert_eq!(h.nbytes, h2.nbytes);
        assert_eq!(h.blocksize, h2.blocksize);
        assert_eq!(h.cbytes, h2.cbytes);
        assert_eq!(h.filters, h2.filters);
        assert!(h2.is_extended());
    }

    #[test]
    fn test_header_try_write_rejects_short_buffer() {
        let h = ChunkHeader::default();
        let mut buf = [0u8; BLOSC_EXTENDED_HEADER_LENGTH - 1];

        assert!(h.try_write(&mut buf).is_err());
    }

    #[test]
    fn test_try_write_normalizes_regular_blocksize_like_c() {
        let h = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: 1,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
            typesize: 1,
            nbytes: 100,
            blocksize: 200,
            cbytes: BLOSC_EXTENDED_HEADER_LENGTH as i32,
            ..Default::default()
        };

        let mut buf = [0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        h.try_write(&mut buf).unwrap();

        assert_eq!(
            i32::from_le_bytes(
                buf[BLOSC2_CHUNK_BLOCKSIZE..BLOSC2_CHUNK_BLOCKSIZE + 4]
                    .try_into()
                    .unwrap()
            ),
            100
        );
        assert_eq!(h.blocksize, 200);
    }

    #[test]
    fn test_try_write_preserves_vl_block_count_like_c() {
        let h = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_VL_BLOCKS,
            versionlz: 1,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
            typesize: 1,
            nbytes: 100,
            blocksize: 200,
            cbytes: BLOSC_EXTENDED_HEADER_LENGTH as i32,
            blosc2_flags2: BLOSC2_VL_BLOCKS,
            ..Default::default()
        };

        let mut buf = [0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        h.try_write(&mut buf).unwrap();

        assert_eq!(
            i32::from_le_bytes(
                buf[BLOSC2_CHUNK_BLOCKSIZE..BLOSC2_CHUNK_BLOCKSIZE + 4]
                    .try_into()
                    .unwrap()
            ),
            200
        );
    }

    #[test]
    fn test_user_codec_format_uses_udcompcode_like_c() {
        let parsed_legacy = ChunkHeader {
            flags: BLOSC_UDCODEC_FORMAT << 5,
            ..Default::default()
        };
        assert_eq!(parsed_legacy.compcode(), 0);

        let parsed_extended = ChunkHeader {
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE | (BLOSC_UDCODEC_FORMAT << 5),
            udcompcode: 201,
            ..Default::default()
        };
        assert_eq!(parsed_extended.compcode(), 201);
    }

    #[test]
    fn test_nblocks_calculation() {
        let h = ChunkHeader {
            nbytes: 10000,
            blocksize: 4096,
            ..Default::default()
        };
        assert_eq!(h.nblocks(), 3); // ceil(10000/4096)
        assert_eq!(h.leftover(), 10000 - 4096 * 2);
    }

    #[test]
    fn test_leftover_is_zero_for_exact_block_multiple_like_c() {
        let h = ChunkHeader {
            nbytes: 8192,
            blocksize: 4096,
            ..Default::default()
        };

        assert_eq!(h.nblocks(), 2);
        assert_eq!(h.leftover(), 0);
    }

    #[test]
    fn test_nblocks_rejects_invalid_signed_sizes() {
        for h in [
            ChunkHeader {
                nbytes: -1,
                blocksize: 4096,
                ..Default::default()
            },
            ChunkHeader {
                nbytes: 10000,
                blocksize: -1,
                ..Default::default()
            },
            ChunkHeader {
                nbytes: 10000,
                blocksize: 0,
                ..Default::default()
            },
        ] {
            assert_eq!(h.nblocks(), 0);
            assert_eq!(h.leftover(), 0);
        }
    }

    #[test]
    fn test_read_normalizes_oversized_regular_blocksize_like_c() {
        let mut buf = [0u8; BLOSC_MIN_HEADER_LENGTH];
        buf[BLOSC2_CHUNK_VERSION] = BLOSC2_VERSION_FORMAT_STABLE;
        buf[BLOSC2_CHUNK_VERSIONLZ] = 1;
        buf[BLOSC2_CHUNK_FLAGS] = BLOSC_LZ4_FORMAT << 5;
        buf[BLOSC2_CHUNK_TYPESIZE] = 4;
        buf[BLOSC2_CHUNK_NBYTES..BLOSC2_CHUNK_NBYTES + 4].copy_from_slice(&100i32.to_le_bytes());
        buf[BLOSC2_CHUNK_BLOCKSIZE..BLOSC2_CHUNK_BLOCKSIZE + 4]
            .copy_from_slice(&200i32.to_le_bytes());
        buf[BLOSC2_CHUNK_CBYTES..BLOSC2_CHUNK_CBYTES + 4].copy_from_slice(&16i32.to_le_bytes());

        let h = ChunkHeader::read_minimal(&buf).unwrap();

        assert_eq!(h.blocksize, 100);
        assert_eq!(h.nblocks(), 1);
    }

    #[test]
    fn test_legacy_flag_bits_populate_filter_slots_like_c() {
        let mut buf = [0u8; BLOSC_MIN_HEADER_LENGTH];
        buf[BLOSC2_CHUNK_VERSION] = BLOSC2_VERSION_FORMAT_STABLE;
        buf[BLOSC2_CHUNK_VERSIONLZ] = 1;
        buf[BLOSC2_CHUNK_FLAGS] = BLOSC_DOSHUFFLE | BLOSC_DODELTA | (BLOSC_LZ4_FORMAT << 5);
        buf[BLOSC2_CHUNK_TYPESIZE] = 4;
        buf[BLOSC2_CHUNK_NBYTES..BLOSC2_CHUNK_NBYTES + 4].copy_from_slice(&100i32.to_le_bytes());
        buf[BLOSC2_CHUNK_BLOCKSIZE..BLOSC2_CHUNK_BLOCKSIZE + 4]
            .copy_from_slice(&100i32.to_le_bytes());
        buf[BLOSC2_CHUNK_CBYTES..BLOSC2_CHUNK_CBYTES + 4].copy_from_slice(&16i32.to_le_bytes());

        let full = ChunkHeader::read(&buf).unwrap();
        let minimal = ChunkHeader::read_minimal(&buf).unwrap();

        assert_eq!(
            full.flags & (BLOSC_DOSHUFFLE | BLOSC_DODELTA),
            BLOSC_DOSHUFFLE | BLOSC_DODELTA
        );
        assert_eq!(
            minimal.flags & (BLOSC_DOSHUFFLE | BLOSC_DODELTA),
            BLOSC_DOSHUFFLE | BLOSC_DODELTA
        );
        assert_eq!(
            full.filters,
            [
                BLOSC_NOFILTER,
                BLOSC_NOFILTER,
                BLOSC_NOFILTER,
                BLOSC_NOFILTER,
                BLOSC_DELTA,
                BLOSC_SHUFFLE,
            ]
        );
        assert_eq!(minimal.filters, full.filters);
    }

    #[test]
    fn test_legacy_bitshuffle_takes_precedence_over_shuffle_like_c_minimal() {
        let mut buf = [0u8; BLOSC_MIN_HEADER_LENGTH];
        buf[BLOSC2_CHUNK_VERSION] = BLOSC2_VERSION_FORMAT_STABLE;
        buf[BLOSC2_CHUNK_VERSIONLZ] = 1;
        buf[BLOSC2_CHUNK_FLAGS] = BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE | (BLOSC_LZ4_FORMAT << 5);
        buf[BLOSC2_CHUNK_TYPESIZE] = 4;
        buf[BLOSC2_CHUNK_NBYTES..BLOSC2_CHUNK_NBYTES + 4].copy_from_slice(&100i32.to_le_bytes());
        buf[BLOSC2_CHUNK_BLOCKSIZE..BLOSC2_CHUNK_BLOCKSIZE + 4]
            .copy_from_slice(&100i32.to_le_bytes());
        buf[BLOSC2_CHUNK_CBYTES..BLOSC2_CHUNK_CBYTES + 4].copy_from_slice(&16i32.to_le_bytes());

        let minimal = ChunkHeader::read_minimal(&buf).unwrap();

        assert_eq!(minimal.filters[BLOSC2_MAX_FILTERS - 1], BLOSC_BITSHUFFLE);
    }

    #[test]
    fn test_read_rejects_c_invalid_legacy_fields() {
        let mut valid = [0u8; BLOSC_MIN_HEADER_LENGTH];
        valid[BLOSC2_CHUNK_VERSION] = BLOSC2_VERSION_FORMAT_STABLE;
        valid[BLOSC2_CHUNK_VERSIONLZ] = 1;
        valid[BLOSC2_CHUNK_TYPESIZE] = 1;
        valid[BLOSC2_CHUNK_NBYTES..BLOSC2_CHUNK_NBYTES + 4].copy_from_slice(&1i32.to_le_bytes());
        valid[BLOSC2_CHUNK_BLOCKSIZE..BLOSC2_CHUNK_BLOCKSIZE + 4]
            .copy_from_slice(&1i32.to_le_bytes());
        valid[BLOSC2_CHUNK_CBYTES..BLOSC2_CHUNK_CBYTES + 4].copy_from_slice(&16i32.to_le_bytes());

        let mut bad_cbytes = valid;
        bad_cbytes[BLOSC2_CHUNK_CBYTES..BLOSC2_CHUNK_CBYTES + 4]
            .copy_from_slice(&15i32.to_le_bytes());
        assert!(ChunkHeader::read_minimal(&bad_cbytes).is_err());

        let mut bad_blocksize = valid;
        bad_blocksize[BLOSC2_CHUNK_BLOCKSIZE..BLOSC2_CHUNK_BLOCKSIZE + 4]
            .copy_from_slice(&0i32.to_le_bytes());
        assert!(ChunkHeader::read_minimal(&bad_blocksize).is_err());

        let mut bad_typesize = valid;
        bad_typesize[BLOSC2_CHUNK_TYPESIZE] = 0;
        assert!(ChunkHeader::read_minimal(&bad_typesize).is_err());
    }

    #[test]
    fn test_read_preserves_vl_block_count_like_c() {
        let h = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_VL_BLOCKS,
            versionlz: 1,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE | (BLOSC_LZ4_FORMAT << 5),
            typesize: 4,
            nbytes: 100,
            blocksize: 200,
            cbytes: BLOSC_EXTENDED_HEADER_LENGTH as i32,
            blosc2_flags2: BLOSC2_VL_BLOCKS,
            ..Default::default()
        };

        let mut buf = [0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        h.try_write(&mut buf).unwrap();
        let h2 = ChunkHeader::read(&buf).unwrap();

        assert_eq!(h2.blocksize, 200);
        assert_eq!(h2.nblocks(), 200);
        assert_eq!(h2.leftover(), 0);
        assert!(h2.vl_blocks());
    }

    #[test]
    fn test_vl_nblocks_uses_encoded_block_count_like_c() {
        let h = ChunkHeader {
            nbytes: 10_000,
            blocksize: 3,
            blosc2_flags2: BLOSC2_VL_BLOCKS,
            ..Default::default()
        };

        assert_eq!(h.nblocks(), 3);
        assert_eq!(h.leftover(), 0);
    }

    #[test]
    fn test_read_rejects_vl_special_like_c() {
        let h = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_VL_BLOCKS,
            versionlz: 1,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
            typesize: 1,
            nbytes: 1,
            blocksize: 1,
            cbytes: BLOSC_EXTENDED_HEADER_LENGTH as i32,
            blosc2_flags2: BLOSC2_VL_BLOCKS,
            blosc2_flags: BLOSC2_SPECIAL_ZERO << 4,
            ..Default::default()
        };

        let mut buf = [0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        h.try_write(&mut buf).unwrap();

        assert!(ChunkHeader::read(&buf).is_err());
    }

    #[test]
    fn test_read_rejects_zero_nbytes_repeat_value_like_c() {
        let h = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: 1,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
            typesize: 1,
            nbytes: 0,
            blocksize: 1,
            cbytes: BLOSC_EXTENDED_HEADER_LENGTH as i32 + 1,
            blosc2_flags: BLOSC2_SPECIAL_VALUE << 4,
            ..Default::default()
        };

        let mut buf = [0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        h.try_write(&mut buf).unwrap();

        assert!(ChunkHeader::read(&buf).is_err());
    }

    #[test]
    fn test_read_preserves_unknown_special_type_like_c_header_parser() {
        let h = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: 1,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
            typesize: 1,
            nbytes: 1,
            blocksize: 1,
            cbytes: BLOSC_EXTENDED_HEADER_LENGTH as i32,
            blosc2_flags: (BLOSC2_SPECIAL_LASTID + 1) << 4,
            ..Default::default()
        };

        let mut buf = [0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        h.try_write(&mut buf).unwrap();

        assert_eq!(
            ChunkHeader::read(&buf).unwrap().special_type(),
            BLOSC2_SPECIAL_LASTID + 1
        );
    }

    #[test]
    fn test_read_rejects_future_version_unknown_flags2_like_c() {
        let h = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT + 1,
            versionlz: 1,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
            typesize: 1,
            nbytes: 1,
            blocksize: 1,
            cbytes: BLOSC_EXTENDED_HEADER_LENGTH as i32,
            blosc2_flags2: BLOSC2_VL_BLOCKS | 0x80,
            ..Default::default()
        };

        let mut buf = [0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        h.try_write(&mut buf).unwrap();

        let err = ChunkHeader::read(&buf).unwrap_err();
        assert!(err.contains("version") || err.contains("Version"));
    }

    #[test]
    fn test_alpha_extended_header_clears_last_filter_slot_like_c() {
        let h = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_ALPHA,
            versionlz: 1,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
            typesize: 1,
            nbytes: 1,
            blocksize: 1,
            cbytes: BLOSC_EXTENDED_HEADER_LENGTH as i32,
            filters: [1, 2, 3, 4, 5, 6],
            filters_meta: [6, 5, 4, 3, 2, 1],
            ..Default::default()
        };

        let mut buf = [0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        h.try_write(&mut buf).unwrap();

        let parsed = ChunkHeader::read(&buf).unwrap();
        assert_eq!(parsed.filters, [1, 2, 3, 4, 5, BLOSC_NOFILTER]);
        assert_eq!(parsed.filters_meta, [6, 5, 4, 3, 2, 0]);
    }
}
