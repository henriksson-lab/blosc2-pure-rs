use crate::constants::*;

/// Parsed chunk header (32 bytes for extended header).
#[derive(Debug, Clone, Default)]
pub struct ChunkHeader {
    pub version: u8,
    pub versionlz: u8,
    pub flags: u8,
    pub typesize: u8,
    pub nbytes: i32,
    pub blocksize: i32,
    pub cbytes: i32,
    pub filters: [u8; BLOSC2_MAX_FILTERS],
    pub filters_meta: [u8; BLOSC2_MAX_FILTERS],
    pub udcompcode: u8,
    pub compcode_meta: u8,
    pub blosc2_flags: u8,
    pub blosc2_flags2: u8,
}

impl ChunkHeader {
    /// Whether this uses the extended 32-byte header.
    pub fn is_extended(&self) -> bool {
        (self.flags & BLOSC_DOSHUFFLE != 0) && (self.flags & BLOSC_DOBITSHUFFLE != 0)
    }

    /// Get the compression format code from the flags (bits 5-7).
    pub fn compformat(&self) -> u8 {
        (self.flags >> 5) & 0x07
    }

    /// Get the actual compressor code.
    pub fn compcode(&self) -> u8 {
        if self.is_extended() && self.udcompcode != 0 {
            self.udcompcode
        } else {
            compformat_to_compcode(self.compformat())
        }
    }

    /// Whether the data was memcpyed (not compressed).
    pub fn memcpyed(&self) -> bool {
        self.flags & BLOSC_MEMCPYED != 0
    }

    /// Whether blocks should not be split into streams.
    pub fn dont_split(&self) -> bool {
        self.flags & BLOSC_DONT_SPLIT != 0
    }

    /// Get the special value type (bits 4-6 of blosc2_flags).
    pub fn special_type(&self) -> u8 {
        (self.blosc2_flags >> 4) & BLOSC2_SPECIAL_MASK
    }

    /// Whether dictionary compression was used.
    pub fn use_dict(&self) -> bool {
        self.blosc2_flags & BLOSC2_USEDICT != 0
    }

    /// Whether variable-length blocks are used.
    pub fn vl_blocks(&self) -> bool {
        self.blosc2_flags2 & BLOSC2_VL_BLOCKS != 0
    }

    /// Header size in bytes.
    pub fn header_len(&self) -> usize {
        if self.is_extended() {
            BLOSC_EXTENDED_HEADER_LENGTH
        } else {
            BLOSC_MIN_HEADER_LENGTH
        }
    }

    /// Number of blocks in this chunk.
    pub fn nblocks(&self) -> usize {
        if self.blocksize == 0 {
            return 0;
        }
        (self.nbytes as usize + self.blocksize as usize - 1) / self.blocksize as usize
    }

    /// Size of the last (possibly partial) block.
    pub fn leftover(&self) -> usize {
        if self.blocksize == 0 {
            return 0;
        }
        let rem = self.nbytes as usize % self.blocksize as usize;
        if rem == 0 {
            self.blocksize as usize
        } else {
            rem
        }
    }

    /// Parse a chunk header from raw bytes.
    pub fn read(data: &[u8]) -> Result<Self, &'static str> {
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

        // Extended header (32 bytes)
        if h.is_extended() && data.len() >= BLOSC_EXTENDED_HEADER_LENGTH {
            h.filters.copy_from_slice(&data[BLOSC2_CHUNK_FILTER_CODES..BLOSC2_CHUNK_FILTER_CODES + 6]);
            h.udcompcode = data[BLOSC2_CHUNK_UDCOMPCODE];
            h.compcode_meta = data[BLOSC2_CHUNK_COMPCODE_META];
            h.filters_meta.copy_from_slice(&data[BLOSC2_CHUNK_FILTER_META..BLOSC2_CHUNK_FILTER_META + 6]);
            h.blosc2_flags2 = data[BLOSC2_CHUNK_BLOSC2_FLAGS2];
            h.blosc2_flags = data[BLOSC2_CHUNK_BLOSC2_FLAGS];
        }

        Ok(h)
    }

    /// Write a 32-byte extended header to a buffer.
    pub fn write(&self, buf: &mut [u8]) {
        assert!(buf.len() >= BLOSC_EXTENDED_HEADER_LENGTH);

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
        buf[BLOSC2_CHUNK_BLOSC2_FLAGS] = self.blosc2_flags;
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
        h.write(&mut buf);
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
    fn test_nblocks_calculation() {
        let h = ChunkHeader {
            nbytes: 10000,
            blocksize: 4096,
            ..Default::default()
        };
        assert_eq!(h.nblocks(), 3); // ceil(10000/4096)
        assert_eq!(h.leftover(), 10000 - 4096 * 2);
    }
}
