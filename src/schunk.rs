use crate::compress::{self, CParams, DParams};
use crate::constants::*;
use crate::header::ChunkHeader;

/// A super-chunk: a collection of compressed chunks with shared compression parameters.
pub struct Schunk {
    pub cparams: CParams,
    pub dparams: DParams,
    /// Compressed chunks stored in memory
    pub chunks: Vec<Vec<u8>>,
    /// Uncompressed size of each chunk's data
    pub chunksize: usize,
    /// Total uncompressed bytes across all chunks
    pub nbytes: i64,
    /// Total compressed bytes across all chunks
    pub cbytes: i64,
}

impl Schunk {
    /// Create a new empty super-chunk.
    pub fn new(cparams: CParams, dparams: DParams) -> Self {
        Schunk {
            cparams,
            dparams,
            chunks: Vec::new(),
            chunksize: 0,
            nbytes: 0,
            cbytes: 0,
        }
    }

    /// Number of chunks.
    pub fn nchunks(&self) -> i64 {
        self.chunks.len() as i64
    }

    /// Compress and append a data buffer as a new chunk.
    /// Returns the chunk index on success.
    pub fn append_buffer(&mut self, data: &[u8]) -> Result<i64, &'static str> {
        let chunk = compress::compress(data, &self.cparams)?;

        if self.chunks.is_empty() {
            self.chunksize = data.len();
        }

        self.nbytes += data.len() as i64;
        self.cbytes += chunk.len() as i64;
        self.chunks.push(chunk);

        Ok(self.chunks.len() as i64 - 1)
    }

    /// Decompress a chunk by index.
    /// Returns the decompressed data.
    pub fn decompress_chunk(&self, nchunk: i64) -> Result<Vec<u8>, &'static str> {
        let idx = nchunk as usize;
        if idx >= self.chunks.len() {
            return Err("Chunk index out of range");
        }
        compress::decompress_with_threads(&self.chunks[idx], self.dparams.nthreads)
    }

    /// Serialize to a contiguous frame in memory (b2frame format).
    pub fn to_frame(&self) -> Vec<u8> {
        frame::write_frame(self)
    }

    /// Deserialize from a contiguous frame buffer.
    pub fn from_frame(data: &[u8]) -> Result<Self, String> {
        frame::read_frame(data)
    }

    /// Write to a file in b2frame format.
    pub fn to_file(&self, path: &str) -> std::io::Result<()> {
        let frame_data = self.to_frame();
        std::fs::write(path, frame_data)
    }

    /// Open a b2frame file.
    pub fn open(path: &str) -> Result<Self, String> {
        let data = std::fs::read(path)
            .map_err(|e| format!("Failed to read file: {e}"))?;
        Self::from_frame(&data)
    }
}

/// Frame format implementation.
///
/// The frame format uses msgpack encoding for the header and stores
/// compressed chunks contiguously with an offset index.
pub mod frame {
    use super::*;

    // Msgpack format markers
    const MSGPACK_FIXARRAY_14: u8 = 0x9E; // fixarray with 14 elements
    const MSGPACK_STR8: u8 = 0xA8;         // fixstr of 8 bytes
    const MSGPACK_INT32: u8 = 0xD2;
    const MSGPACK_UINT64: u8 = 0xCF;
    const MSGPACK_INT64: u8 = 0xD3;
    const MSGPACK_INT16: u8 = 0xD1;
    const MSGPACK_STR4: u8 = 0xA4;         // fixstr of 4 bytes
    const MSGPACK_TRUE: u8 = 0xC3;
    const MSGPACK_FALSE: u8 = 0xC2;
    const MSGPACK_FIXEXT16: u8 = 0xD8;

    const FRAME_MAGIC: &[u8] = b"b2frame\0";
    const FRAME_HEADER_MIN_LEN: usize = 87;

    /// Write a frame from a schunk.
    pub fn write_frame(schunk: &Schunk) -> Vec<u8> {
        // Build header first to know its size
        let header = build_header(schunk);
        let header_size = header.len();

        // Calculate data sizes
        let data_cbytes: i64 = schunk.chunks.iter().map(|c| c.len() as i64).sum();

        // Build the offset index as a Blosc2 chunk with int64 offsets
        let offsets_data = build_offsets(schunk, header_size);
        let offsets_chunk = if !offsets_data.is_empty() {
            build_offsets_chunk(&offsets_data)
        } else {
            Vec::new()
        };

        // Build trailer
        let trailer = build_trailer();

        // Assemble the frame
        let frame_size = header_size + data_cbytes as usize + offsets_chunk.len() + trailer.len();
        let mut frame = Vec::with_capacity(frame_size);
        frame.extend_from_slice(&header);

        // Data chunks
        for chunk in &schunk.chunks {
            frame.extend_from_slice(chunk);
        }

        // Offset index
        frame.extend_from_slice(&offsets_chunk);

        // Trailer
        frame.extend_from_slice(&trailer);

        // Update frame_size in header (bytes 16-23, big-endian uint64)
        let actual_size = frame.len() as u64;
        frame[16..24].copy_from_slice(&actual_size.to_be_bytes());

        frame
    }

    /// Build the offsets array: uint64 offsets for each chunk relative to data section start.
    /// This matches the C convention where offset 0 = first chunk (at header_size position).
    fn build_offsets(schunk: &Schunk, _header_size: usize) -> Vec<u8> {
        let nchunks = schunk.chunks.len();
        if nchunks == 0 {
            return Vec::new();
        }

        let mut offsets = Vec::with_capacity(nchunks * 8);
        let mut coffset: u64 = 0;

        for chunk in &schunk.chunks {
            offsets.extend_from_slice(&coffset.to_le_bytes());
            coffset += chunk.len() as u64;
        }

        offsets
    }

    /// Build a simple memcpy Blosc2 chunk for the offsets (matching C behavior).
    /// Small data like offsets is stored as-is with the MEMCPYED flag.
    fn build_offsets_chunk(data: &[u8]) -> Vec<u8> {
        let nbytes = data.len() as i32;
        let typesize: u8 = 8;
        let cbytes = BLOSC_EXTENDED_HEADER_LENGTH as i32 + nbytes;

        let mut chunk = vec![0u8; cbytes as usize];

        // Write extended header (32 bytes)
        let header = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE, // version 5
            versionlz: 1,
            // flags: extended header (DOSHUFFLE|DOBITSHUFFLE) + MEMCPYED
            flags: BLOSC_DOSHUFFLE | BLOSC_MEMCPYED | BLOSC_DOBITSHUFFLE,
            typesize,
            nbytes,
            blocksize: nbytes,
            cbytes,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        header.write(&mut chunk[..BLOSC_EXTENDED_HEADER_LENGTH]);

        // Copy data after header
        chunk[BLOSC_EXTENDED_HEADER_LENGTH..].copy_from_slice(data);

        chunk
    }

    // Frame format constants matching C code (frame.h)
    const FRAME_UDCODEC: usize = 77;
    const FRAME_CODEC_META: usize = 78;
    const FRAME_OTHER_FLAGS2: usize = 85;

    /// Build frame header in msgpack format — exactly matching C's new_header_frame().
    fn build_header(schunk: &Schunk) -> Vec<u8> {
        // Start with 87-byte minimum header (zeroed)
        let mut h = vec![0u8; FRAME_HEADER_MIN_LEN];
        let mut pos = 0;

        // [0] fixarray(14)
        h[pos] = 0x9E;
        pos += 1;

        // [1-9] fixstr(8) + "b2frame\0"
        h[pos] = 0xA8;
        pos += 1;
        h[pos..pos + 8].copy_from_slice(FRAME_MAGIC);
        pos += 8;

        // [10-14] int32: header_size (placeholder, updated at end)
        h[pos] = MSGPACK_INT32;
        pos += 1;
        let header_size_pos = pos;
        pos += 4;

        // [15-23] uint64: frame_size (placeholder, updated after assembly)
        h[pos] = MSGPACK_UINT64;
        pos += 1;
        // frame_size at bytes 16-23 — filled later
        pos += 8;

        // [24-28] fixstr(4): flags
        h[pos] = MSGPACK_STR4;
        pos += 1;

        // [25] general_flags: version + 0x10 (64-bit offsets)
        h[pos] = 0x10 | 0x02; // 64-bit offsets flag + version 2 (RC1)
        pos += 1;

        // [26] frame_type: 0 = contiguous
        h[pos] = 0;
        pos += 1;

        // [27] codec_flags: codec in bits 0-3, clevel in bits 4-7
        h[pos] = (schunk.cparams.compcode & 0x0F)
            | ((schunk.cparams.clevel & 0x0F) << 4);
        pos += 1;

        // [28] other_flags: splitmode - 1 (C convention)
        h[pos] = (schunk.cparams.splitmode - 1) as u8;
        pos += 1;

        // [29-37] int64: uncompressed_size
        h[pos] = MSGPACK_INT64;
        pos += 1;
        h[pos..pos + 8].copy_from_slice(&schunk.nbytes.to_be_bytes());
        pos += 8;

        // [38-46] int64: compressed_size
        h[pos] = MSGPACK_INT64;
        pos += 1;
        h[pos..pos + 8].copy_from_slice(&schunk.cbytes.to_be_bytes());
        pos += 8;

        // [47-51] int32: typesize
        h[pos] = MSGPACK_INT32;
        pos += 1;
        h[pos..pos + 4].copy_from_slice(&schunk.cparams.typesize.to_be_bytes());
        pos += 4;

        // [52-56] int32: blocksize (0 = auto, matching C behavior)
        h[pos] = MSGPACK_INT32;
        pos += 1;
        h[pos..pos + 4].copy_from_slice(&0i32.to_be_bytes());
        pos += 4;

        // [57-61] int32: chunksize
        h[pos] = MSGPACK_INT32;
        pos += 1;
        h[pos..pos + 4].copy_from_slice(&(schunk.chunksize as i32).to_be_bytes());
        pos += 4;

        // [62-64] int16: nthreads_comp
        h[pos] = MSGPACK_INT16;
        pos += 1;
        h[pos..pos + 2].copy_from_slice(&schunk.cparams.nthreads.to_be_bytes());
        pos += 2;

        // [65-67] int16: nthreads_decomp
        h[pos] = MSGPACK_INT16;
        pos += 1;
        h[pos..pos + 2].copy_from_slice(&schunk.dparams.nthreads.to_be_bytes());
        pos += 2;

        // [68] bool: has_vlmetalayers
        h[pos] = MSGPACK_FALSE;
        pos += 1;

        // [69] fixext16 marker
        h[pos] = MSGPACK_FIXEXT16;
        pos += 1;

        // [70] nfilters
        h[pos] = BLOSC2_MAX_FILTERS as u8;
        let _ = pos; // pos tracking ends here; remaining fields use fixed offsets

        // [71-78] 8 bytes filter codes (6 filters + 2 padding)
        for i in 0..BLOSC2_MAX_FILTERS {
            h[71 + i] = schunk.cparams.filters[i];
        }
        // [79-86] 8 bytes filter meta
        for i in 0..BLOSC2_MAX_FILTERS {
            h[79 + i] = schunk.cparams.filters_meta[i];
        }

        // [77] udcodec (at fixed offset, overlaps with filter bytes — C stores it here)
        h[FRAME_UDCODEC] = schunk.cparams.compcode;
        // [78] codec_meta
        h[FRAME_CODEC_META] = 0;

        // [85] other_flags2: bit 0 = use_dict
        h[FRAME_OTHER_FLAGS2] = 0;

        assert_eq!(h.len(), FRAME_HEADER_MIN_LEN);

        // Metalayers section (empty, matching C format exactly)
        // [87] fixarray(3)
        h.push(0x93);
        // [88] uint16 marker for map size
        h.push(0xCD);
        // [89-90] uint16: map size = 7 bytes (the map + array section)
        h.push(0x00);
        h.push(0x07);
        // [91] map16 with 0 keys
        h.push(0xDE);
        h.push(0x00);
        h.push(0x00);
        // [94] array16 with 0 elements
        h.push(0xDC);
        h.push(0x00);
        h.push(0x00);

        // Update header_size
        let header_size = h.len() as i32;
        h[header_size_pos..header_size_pos + 4]
            .copy_from_slice(&header_size.to_be_bytes());

        h
    }

    /// Build trailer matching C's frame_update_trailer() format.
    fn build_trailer() -> Vec<u8> {
        let mut t = Vec::new();

        // Trailer layout (matching C exactly):
        // [0] fixarray(4) — vlmetalayers section
        t.push(0x94);
        // [1] trailer version
        t.push(0x01);
        // [2] fixarray(3) — vlmetalayers content
        t.push(0x93);
        // [3] uint16: vlmetalayers map size
        t.push(0xCD);
        t.push(0x00);
        t.push(0x06); // size = 6 bytes
        // [6] map16(0) — empty map
        t.push(0xDE);
        t.push(0x00);
        t.push(0x00);
        // [9] array16(0) — empty array
        t.push(0xDC);
        t.push(0x00);
        t.push(0x00);

        // [12] uint32: trailer_len
        t.push(0xCE);
        let trailer_len_pos = t.len();
        t.extend_from_slice(&0u32.to_be_bytes()); // placeholder

        // [17] fixext16: fingerprint
        t.push(MSGPACK_FIXEXT16);
        t.push(0x00); // fingerprint type = none
        t.extend_from_slice(&[0u8; 16]); // 16 zero bytes

        // Total trailer length = 35 bytes (matching C)
        let trailer_len = t.len() as u32;
        t[trailer_len_pos..trailer_len_pos + 4]
            .copy_from_slice(&trailer_len.to_be_bytes());

        t
    }

    /// Read a frame and return a Schunk.
    pub fn read_frame(data: &[u8]) -> Result<Schunk, String> {
        if data.len() < FRAME_HEADER_MIN_LEN {
            return Err("Frame too small".into());
        }

        // Parse header
        // [0] fixarray marker
        if data[0] != MSGPACK_FIXARRAY_14 {
            return Err(format!("Invalid frame marker: 0x{:02X}", data[0]));
        }

        // [1-9] magic
        if data[1] != MSGPACK_STR8 || &data[2..10] != FRAME_MAGIC {
            return Err("Invalid frame magic".into());
        }

        // [10-14] header_size
        if data[10] != MSGPACK_INT32 {
            return Err("Expected int32 for header_size".into());
        }
        let header_size = i32::from_be_bytes(data[11..15].try_into().unwrap()) as usize;

        // [15-23] frame_size
        if data[15] != MSGPACK_UINT64 {
            return Err("Expected uint64 for frame_size".into());
        }
        let _frame_size = u64::from_be_bytes(data[16..24].try_into().unwrap()) as usize;

        // [24-28] flags string
        if data[24] != MSGPACK_STR4 {
            return Err("Expected fixstr(4) for flags".into());
        }
        let _general_flags = data[25];
        let _frame_type = data[26];
        let codec_flags = data[27];
        let other_flags = data[28];

        let compcode = codec_flags & 0x0F;
        let clevel = (codec_flags >> 4) & 0x0F;
        let splitmode = match other_flags & 0x03 {
            1 => BLOSC_ALWAYS_SPLIT,
            2 => BLOSC_NEVER_SPLIT,
            3 => BLOSC_AUTO_SPLIT,
            _ => BLOSC_FORWARD_COMPAT_SPLIT,
        };

        // [29-37] uncompressed_size
        if data[29] != MSGPACK_INT64 {
            return Err("Expected int64 for nbytes".into());
        }
        let nbytes = i64::from_be_bytes(data[30..38].try_into().unwrap());

        // [38-46] compressed_size
        if data[38] != MSGPACK_INT64 {
            return Err("Expected int64 for cbytes".into());
        }
        let cbytes = i64::from_be_bytes(data[39..47].try_into().unwrap());

        // [47-51] typesize
        if data[47] != MSGPACK_INT32 {
            return Err("Expected int32 for typesize".into());
        }
        let typesize = i32::from_be_bytes(data[48..52].try_into().unwrap());

        // [52-56] blocksize
        if data[52] != MSGPACK_INT32 {
            return Err("Expected int32 for blocksize".into());
        }
        let blocksize = i32::from_be_bytes(data[53..57].try_into().unwrap());

        // [57-61] chunksize
        if data[57] != MSGPACK_INT32 {
            return Err("Expected int32 for chunksize".into());
        }
        let chunksize = i32::from_be_bytes(data[58..62].try_into().unwrap()) as usize;

        // [62-64] nthreads_comp
        if data[62] != MSGPACK_INT16 {
            return Err("Expected int16 for nthreads_comp".into());
        }
        let nthreads_comp = i16::from_be_bytes(data[63..65].try_into().unwrap());

        // [65-67] nthreads_decomp
        if data[65] != MSGPACK_INT16 {
            return Err("Expected int16 for nthreads_decomp".into());
        }
        let nthreads_decomp = i16::from_be_bytes(data[66..68].try_into().unwrap());

        // [68] has_vlmetalayers
        let _has_vlmeta = data[68] == MSGPACK_TRUE;

        // [69-86] filter info: fixext16
        if data[69] != MSGPACK_FIXEXT16 {
            return Err("Expected fixext16 for filters".into());
        }
        let _nfilters = data[70];
        let mut filters = [0u8; BLOSC2_MAX_FILTERS];
        let mut filters_meta = [0u8; BLOSC2_MAX_FILTERS];
        for i in 0..BLOSC2_MAX_FILTERS {
            filters[i] = data[71 + i];
        }
        for i in 0..BLOSC2_MAX_FILTERS {
            filters_meta[i] = data[79 + i];
        }

        // Now we need to find and read the chunks
        // The offset index is after the data chunks
        // First, find data start (= header_size) and read the offset index

        // Read chunks from the frame
        let data_start = header_size;

        // The offsets chunk is after all data chunks
        // We need to scan forward from data_start, reading chunk headers to find all chunks
        let mut chunks = Vec::new();
        let mut pos = data_start;

        // We know the total cbytes, so data region ends at data_start + cbytes
        if cbytes < 0 {
            return Err("Invalid frame: negative cbytes".into());
        }
        let data_end = data_start + cbytes as usize;

        while pos < data_end && pos + BLOSC_MIN_HEADER_LENGTH <= data.len() {
            // Read chunk header to get cbytes
            match ChunkHeader::read(&data[pos..]) {
                Ok(ch) => {
                    let chunk_cbytes = ch.cbytes as usize;
                    if chunk_cbytes == 0 || pos + chunk_cbytes > data.len() {
                        break;
                    }
                    chunks.push(data[pos..pos + chunk_cbytes].to_vec());
                    pos += chunk_cbytes;
                }
                Err(_) => break,
            }
        }

        let cparams = CParams {
            compcode,
            clevel,
            typesize,
            blocksize,
            splitmode,
            filters,
            filters_meta,
            nthreads: nthreads_comp,
        };

        let dparams = DParams {
            nthreads: nthreads_decomp,
        };

        Ok(Schunk {
            cparams,
            dparams,
            chunks,
            chunksize,
            nbytes,
            cbytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schunk_basic() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let dparams = DParams::default();

        let mut schunk = Schunk::new(cparams, dparams);

        let data1: Vec<u8> = (0..1000u32).flat_map(|i| i.to_le_bytes()).collect();
        let data2: Vec<u8> = (1000..2000u32).flat_map(|i| i.to_le_bytes()).collect();

        schunk.append_buffer(&data1).unwrap();
        schunk.append_buffer(&data2).unwrap();

        assert_eq!(schunk.nchunks(), 2);

        let d1 = schunk.decompress_chunk(0).unwrap();
        let d2 = schunk.decompress_chunk(1).unwrap();
        assert_eq!(data1, d1);
        assert_eq!(data2, d2);
    }

    #[test]
    fn test_schunk_frame_roundtrip() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let dparams = DParams::default();

        let mut schunk = Schunk::new(cparams, dparams);

        let data: Vec<u8> = (0..5000u32).flat_map(|i| i.to_le_bytes()).collect();
        schunk.append_buffer(&data).unwrap();

        // Serialize to frame
        let frame = schunk.to_frame();

        // Deserialize from frame
        let schunk2 = Schunk::from_frame(&frame).unwrap();

        assert_eq!(schunk2.nchunks(), 1);
        let decompressed = schunk2.decompress_chunk(0).unwrap();
        assert_eq!(data, decompressed);
    }
}
