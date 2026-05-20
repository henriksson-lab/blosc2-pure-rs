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

        let mut h = ChunkHeader {
            version: data[BLOSC2_CHUNK_VERSION],
            versionlz: data[BLOSC2_CHUNK_VERSIONLZ],
            flags: data[BLOSC2_CHUNK_FLAGS],
            typesize: data[BLOSC2_CHUNK_TYPESIZE],
            nbytes: i32::from_le_bytes(data[4..8].try_into().unwrap()),
            blocksize: i32::from_le_bytes(data[8..12].try_into().unwrap()),
            cbytes: i32::from_le_bytes(data[12..16].try_into().unwrap()),
            ..Default::default()
        };

        if h.flags & BLOSC_DOBITSHUFFLE != 0 {
            h.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_BITSHUFFLE;
        } else if h.flags & BLOSC_DOSHUFFLE != 0 {
            h.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
        }
        if h.flags & BLOSC_DODELTA != 0 {
            h.filters[BLOSC2_MAX_FILTERS - 2] = BLOSC_DELTA;
        }

        Ok(h)
    }

    /// Parses only the 16-byte legacy prefix of a chunk header.
    ///
    /// This mirrors C-Blosc metadata queries, which can inspect size and basic
    /// filter information without requiring the full compressed payload.
    pub fn read_minimal(data: &[u8]) -> Result<Self, &'static str> {
        Self::read_legacy_fields(data)
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
        if self.is_extended() && self.compformat() == BLOSC_UDCODEC_FORMAT {
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
    /// Computed as `ceil(nbytes / blocksize)`. Returns 0 for negative or zero
    /// sizes so that malformed headers do not panic.
    pub fn nblocks(&self) -> usize {
        if self.nbytes <= 0 || self.blocksize <= 0 {
            return 0;
        }
        (self.nbytes as usize).div_ceil(self.blocksize as usize)
    }

    /// Returns the size of the last (possibly partial) block.
    ///
    /// When `nbytes` is an exact multiple of `blocksize` this returns the full
    /// block size. Returns 0 for invalid signed sizes.
    pub fn leftover(&self) -> usize {
        if self.nbytes <= 0 || self.blocksize <= 0 {
            return 0;
        }
        let rem = self.nbytes as usize % self.blocksize as usize;
        if rem == 0 {
            self.blocksize as usize
        } else {
            rem
        }
    }

    /// Parses a chunk header from raw bytes.
    ///
    /// Requires at least [`BLOSC_MIN_HEADER_LENGTH`] bytes. Extended-header
    /// fields are required when both extended-header flag bits are set.
    pub fn read(data: &[u8]) -> Result<Self, &'static str> {
        let mut h = Self::read_legacy_fields(data)?;

        // Extended header (32 bytes)
        if h.is_extended() {
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
            if h.version == BLOSC2_VERSION_FORMAT_ALPHA {
                h.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_NOFILTER;
                h.filters_meta[BLOSC2_MAX_FILTERS - 1] = 0;
            }
        }

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
        buf[8..12].copy_from_slice(&self.blocksize.to_le_bytes());
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
}
