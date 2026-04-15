use crate::compress::{self, CParams, DParams};
use crate::constants::*;
use crate::header::ChunkHeader;

/// Named fixed-size metadata stored in a super-chunk frame header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Metalayer {
    pub name: String,
    pub content: Vec<u8>,
}

/// A super-chunk: a collection of compressed chunks with shared compression parameters.
#[derive(Clone)]
pub struct Schunk {
    /// Compression parameters used when adding or replacing chunks.
    pub cparams: CParams,
    /// Decompression parameters used when reading chunks.
    pub dparams: DParams,
    /// Compressed chunks stored in memory
    pub chunks: Vec<Vec<u8>>,
    /// Uncompressed size of each chunk's data
    pub chunksize: usize,
    /// Total uncompressed bytes across all chunks
    pub nbytes: i64,
    /// Total compressed bytes across all chunks
    pub cbytes: i64,
    /// Fixed-size metadata layers stored in the frame header.
    pub metalayers: Vec<Metalayer>,
    /// Variable-length metadata layers stored in the frame trailer.
    pub vlmetalayers: Vec<Metalayer>,
    variable_chunks: bool,
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
            metalayers: Vec::new(),
            vlmetalayers: Vec::new(),
            variable_chunks: false,
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

        let new_chunksize = if self.chunks.is_empty() {
            data.len()
        } else {
            self.chunksize
        };
        let new_nbytes = self
            .nbytes
            .checked_add(data.len() as i64)
            .ok_or("Schunk nbytes overflow")?;
        let new_cbytes = self
            .cbytes
            .checked_add(chunk.len() as i64)
            .ok_or("Schunk cbytes overflow")?;
        self.chunksize = new_chunksize;
        self.nbytes = new_nbytes;
        self.cbytes = new_cbytes;
        self.chunks.push(chunk);
        self.refresh_chunk_shape()?;

        Ok(self.chunks.len() as i64 - 1)
    }

    /// Decompress a chunk by index.
    /// Returns the decompressed data.
    pub fn decompress_chunk(&self, nchunk: i64) -> Result<Vec<u8>, &'static str> {
        if nchunk < 0 {
            return Err("Chunk index out of range");
        }
        let idx = nchunk as usize;
        if idx >= self.chunks.len() {
            return Err("Chunk index out of range");
        }
        compress::decompress_with_threads(&self.chunks[idx], self.dparams.nthreads)
    }

    /// Compress and insert a data buffer before `nchunk`.
    /// Returns the inserted chunk index on success.
    pub fn insert_buffer(&mut self, nchunk: i64, data: &[u8]) -> Result<i64, &'static str> {
        if nchunk < 0 || nchunk as usize > self.chunks.len() {
            return Err("Chunk index out of range");
        }

        let chunk = compress::compress(data, &self.cparams)?;
        let new_nbytes = self
            .nbytes
            .checked_add(data.len() as i64)
            .ok_or("Schunk nbytes overflow")?;
        let new_cbytes = self
            .cbytes
            .checked_add(chunk.len() as i64)
            .ok_or("Schunk cbytes overflow")?;
        let new_chunksize = if self.chunks.is_empty() || nchunk == 0 {
            data.len()
        } else {
            self.chunksize
        };

        self.chunks.insert(nchunk as usize, chunk);
        self.chunksize = new_chunksize;
        self.nbytes = new_nbytes;
        self.cbytes = new_cbytes;
        self.refresh_chunk_shape()?;

        Ok(nchunk)
    }

    /// Delete a chunk and return its decompressed data.
    pub fn delete_chunk(&mut self, nchunk: i64) -> Result<Vec<u8>, &'static str> {
        if nchunk < 0 || nchunk as usize >= self.chunks.len() {
            return Err("Chunk index out of range");
        }
        let idx = nchunk as usize;
        let data = self.decompress_chunk(nchunk)?;

        self.chunks.remove(idx);
        self.recompute_metadata()?;

        Ok(data)
    }

    /// Replace a chunk with newly compressed data.
    pub fn update_chunk(&mut self, nchunk: i64, data: &[u8]) -> Result<(), &'static str> {
        if nchunk < 0 || nchunk as usize >= self.chunks.len() {
            return Err("Chunk index out of range");
        }

        let idx = nchunk as usize;
        let chunk = compress::compress(data, &self.cparams)?;
        self.chunks[idx] = chunk;
        self.recompute_metadata()
    }

    /// Deep-copy the super-chunk.
    pub fn copy_schunk(&self) -> Self {
        self.clone()
    }

    /// Add or replace a named fixed-size metalayer.
    pub fn add_metalayer(&mut self, name: &str, content: &[u8]) -> Result<(), &'static str> {
        validate_metalayer_name(name)?;
        validate_metalayers_encoded_size(
            self.metalayers
                .iter()
                .filter(|layer| layer.name != name)
                .map(|layer| (layer.name.as_str(), layer.content.as_slice()))
                .chain(std::iter::once((name, content))),
        )?;

        if let Some(layer) = self.metalayers.iter_mut().find(|layer| layer.name == name) {
            layer.content.clear();
            layer.content.extend_from_slice(content);
        } else {
            self.metalayers.push(Metalayer {
                name: name.to_string(),
                content: content.to_vec(),
            });
        }
        Ok(())
    }

    /// Return a metalayer payload by name.
    pub fn metalayer(&self, name: &str) -> Option<&[u8]> {
        self.metalayers
            .iter()
            .find(|layer| layer.name == name)
            .map(|layer| layer.content.as_slice())
    }

    /// Remove a metalayer by name and return its payload.
    pub fn remove_metalayer(&mut self, name: &str) -> Option<Vec<u8>> {
        let pos = self
            .metalayers
            .iter()
            .position(|layer| layer.name == name)?;
        Some(self.metalayers.remove(pos).content)
    }

    /// Add or replace a named variable-length metalayer.
    pub fn add_vlmetalayer(&mut self, name: &str, content: &[u8]) -> Result<(), &'static str> {
        validate_vlmetalayer_name(name)?;
        let compressed = compress::compress(content, &CParams::default())?;
        let mut compressed_layers = Vec::with_capacity(self.vlmetalayers.len() + 1);
        for layer in self.vlmetalayers.iter().filter(|layer| layer.name != name) {
            compressed_layers.push((
                layer.name.as_str(),
                compress::compress(&layer.content, &CParams::default())?,
            ));
        }
        compressed_layers.push((name, compressed));
        validate_vlmetalayers_encoded_size(
            compressed_layers
                .iter()
                .map(|(layer_name, compressed)| (*layer_name, compressed.as_slice())),
        )?;

        if let Some(layer) = self
            .vlmetalayers
            .iter_mut()
            .find(|layer| layer.name == name)
        {
            layer.content.clear();
            layer.content.extend_from_slice(content);
        } else {
            self.vlmetalayers.push(Metalayer {
                name: name.to_string(),
                content: content.to_vec(),
            });
        }
        Ok(())
    }

    /// Return a variable-length metalayer payload by name.
    pub fn vlmetalayer(&self, name: &str) -> Option<&[u8]> {
        self.vlmetalayers
            .iter()
            .find(|layer| layer.name == name)
            .map(|layer| layer.content.as_slice())
    }

    /// Remove a variable-length metalayer by name and return its payload.
    pub fn remove_vlmetalayer(&mut self, name: &str) -> Option<Vec<u8>> {
        let pos = self
            .vlmetalayers
            .iter()
            .position(|layer| layer.name == name)?;
        Some(self.vlmetalayers.remove(pos).content)
    }

    /// Return decompressed bytes spanning the whole super-chunk.
    pub fn decompress_all(&self) -> Result<Vec<u8>, &'static str> {
        let capacity = usize::try_from(self.nbytes).map_err(|_| "Invalid schunk nbytes")?;
        let mut out = Vec::with_capacity(capacity);
        for idx in 0..self.chunks.len() {
            out.extend(self.decompress_chunk(idx as i64)?);
        }
        Ok(out)
    }

    /// Read a byte slice spanning one or more chunks.
    pub fn get_slice(&self, start: usize, len: usize) -> Result<Vec<u8>, &'static str> {
        let end = checked_slice_end(start, len, self.nbytes)?;
        if len == 0 {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(len);
        let mut chunk_start = 0usize;
        for idx in 0..self.chunks.len() {
            let chunk = self.decompress_chunk(idx as i64)?;
            let chunk_end = chunk_start
                .checked_add(chunk.len())
                .ok_or("Slice offset overflow")?;
            if chunk_end > start && chunk_start < end {
                let local_start = start.saturating_sub(chunk_start);
                let local_end = end.min(chunk_end) - chunk_start;
                out.extend_from_slice(&chunk[local_start..local_end]);
            }
            if chunk_end >= end {
                break;
            }
            chunk_start = chunk_end;
        }

        Ok(out)
    }

    /// Overwrite a byte slice spanning one or more chunks.
    ///
    /// The replacement length defines the slice length; chunk boundaries and
    /// uncompressed chunk sizes are preserved.
    pub fn set_slice(&mut self, start: usize, data: &[u8]) -> Result<(), &'static str> {
        let end = checked_slice_end(start, data.len(), self.nbytes)?;
        if data.is_empty() {
            return Ok(());
        }

        let mut replacements = Vec::new();
        let mut replacement_pos = 0usize;
        let mut chunk_start = 0usize;

        for idx in 0..self.chunks.len() {
            let mut chunk_data = self.decompress_chunk(idx as i64)?;
            let chunk_end = chunk_start
                .checked_add(chunk_data.len())
                .ok_or("Slice offset overflow")?;
            if chunk_end > start && chunk_start < end {
                let local_start = start.saturating_sub(chunk_start);
                let local_end = end.min(chunk_end) - chunk_start;
                let copy_len = local_end - local_start;
                chunk_data[local_start..local_end]
                    .copy_from_slice(&data[replacement_pos..replacement_pos + copy_len]);
                replacement_pos += copy_len;
                replacements.push((idx, compress::compress(&chunk_data, &self.cparams)?));
            }
            if chunk_end >= end {
                break;
            }
            chunk_start = chunk_end;
        }

        if replacement_pos != data.len() {
            return Err("Slice range out of bounds");
        }

        for (idx, chunk) in replacements {
            self.chunks[idx] = chunk;
        }
        self.recompute_metadata()
    }

    /// Reorder chunks according to a permutation of current indices.
    pub fn reorder_chunks(&mut self, order: &[i64]) -> Result<(), &'static str> {
        if order.len() != self.chunks.len() {
            return Err("Invalid chunk permutation");
        }

        let mut seen = vec![false; self.chunks.len()];
        let mut reordered = Vec::with_capacity(self.chunks.len());
        for &idx in order {
            if idx < 0 || idx as usize >= self.chunks.len() {
                return Err("Invalid chunk permutation");
            }
            let idx = idx as usize;
            if seen[idx] {
                return Err("Invalid chunk permutation");
            }
            seen[idx] = true;
            reordered.push(self.chunks[idx].clone());
        }

        self.chunks = reordered;
        self.recompute_metadata()
    }

    /// Return compressed chunk offsets relative to the frame data section.
    pub fn chunk_offsets(&self) -> Vec<u64> {
        let mut offsets = Vec::with_capacity(self.chunks.len());
        let mut offset = 0u64;
        for chunk in &self.chunks {
            offsets.push(offset);
            offset = offset.saturating_add(chunk.len() as u64);
        }
        offsets
    }

    /// Return the chunk index range touched by a byte slice.
    pub fn chunk_range_for_byte_slice(
        &self,
        start: usize,
        len: usize,
    ) -> Result<std::ops::Range<usize>, &'static str> {
        let end = checked_slice_end(start, len, self.nbytes)?;
        if len == 0 {
            let mut offset = 0usize;
            for (idx, chunk) in self.chunks.iter().enumerate() {
                let (nbytes, _, _) = compress::cbuffer_sizes(chunk)?;
                if start <= offset {
                    return Ok(idx..idx);
                }
                offset = offset.checked_add(nbytes).ok_or("Slice offset overflow")?;
            }
            return Ok(self.chunks.len()..self.chunks.len());
        }

        let mut first = None;
        let mut last = None;
        let mut chunk_start = 0usize;
        for (idx, chunk) in self.chunks.iter().enumerate() {
            let (nbytes, _, _) = compress::cbuffer_sizes(chunk)?;
            let chunk_end = chunk_start
                .checked_add(nbytes)
                .ok_or("Slice offset overflow")?;
            if chunk_end > start && chunk_start < end {
                first.get_or_insert(idx);
                last = Some(idx + 1);
            }
            if chunk_end >= end {
                break;
            }
            chunk_start = chunk_end;
        }

        Ok(first.unwrap_or(self.chunks.len())..last.unwrap_or(self.chunks.len()))
    }

    fn recompute_metadata(&mut self) -> Result<(), &'static str> {
        let mut nbytes = 0i64;
        let mut cbytes = 0i64;
        let mut chunksize = 0usize;

        for (idx, chunk) in self.chunks.iter().enumerate() {
            let (chunk_nbytes, chunk_cbytes, _) = compress::cbuffer_sizes(chunk)?;
            if idx == 0 {
                chunksize = chunk_nbytes;
            }
            nbytes = nbytes
                .checked_add(chunk_nbytes as i64)
                .ok_or("Schunk nbytes overflow")?;
            cbytes = cbytes
                .checked_add(chunk_cbytes as i64)
                .ok_or("Schunk cbytes overflow")?;
        }

        self.chunksize = chunksize;
        self.nbytes = nbytes;
        self.cbytes = cbytes;
        self.refresh_chunk_shape()?;
        Ok(())
    }

    fn refresh_chunk_shape(&mut self) -> Result<(), &'static str> {
        let Some((first, rest)) = self.chunks.split_first() else {
            self.chunksize = 0;
            self.variable_chunks = false;
            return Ok(());
        };
        let (first_nbytes, _, _) = compress::cbuffer_sizes(first)?;
        let mut variable = false;
        for chunk in rest {
            let (chunk_nbytes, _, _) = compress::cbuffer_sizes(chunk)?;
            if chunk_nbytes != first_nbytes {
                variable = true;
                break;
            }
        }
        self.variable_chunks = variable;
        self.chunksize = if variable { 0 } else { first_nbytes };
        Ok(())
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
        let data = std::fs::read(path).map_err(|e| format!("Failed to read file: {e}"))?;
        Self::from_frame(&data)
    }
}

fn validate_metalayer_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Metalayer name cannot be empty");
    }
    if name.len() > u16::MAX as usize {
        return Err("Metalayer name too large");
    }
    Ok(())
}

fn validate_vlmetalayer_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("VL-metalayer name cannot be empty");
    }
    if name.len() > 31 {
        return Err("VL-metalayer name too large");
    }
    Ok(())
}

fn validate_vlmetalayers_encoded_size<'a>(
    layers: impl Iterator<Item = (&'a str, &'a [u8])>,
) -> Result<(), &'static str> {
    let mut index_len = 3usize;
    let mut values_len = 3usize;
    let mut count = 0usize;
    for (name, compressed_content) in layers {
        validate_vlmetalayer_name(name)?;
        count += 1;
        if count > i16::MAX as usize {
            return Err("Too many VL-metalayers");
        }
        index_len = index_len
            .checked_add(encoded_str_len(name))
            .and_then(|len| len.checked_add(5))
            .ok_or("VL-metalayers too large")?;
        values_len = values_len
            .checked_add(5)
            .and_then(|len| len.checked_add(compressed_content.len()))
            .ok_or("VL-metalayers too large")?;
    }
    if index_len > u16::MAX as usize {
        return Err("VL-metalayer index too large");
    }
    if index_len
        .checked_add(values_len)
        .and_then(|len| len.checked_add(23))
        .is_none_or(|len| len > i32::MAX as usize)
    {
        return Err("VL-metalayers too large");
    }
    Ok(())
}

fn encoded_str_len(name: &str) -> usize {
    if name.len() <= 31 {
        1 + name.len()
    } else if name.len() <= u8::MAX as usize {
        2 + name.len()
    } else {
        3 + name.len()
    }
}

fn encoded_bin_len(content: &[u8]) -> usize {
    if content.len() <= u8::MAX as usize {
        2 + content.len()
    } else if content.len() <= u16::MAX as usize {
        3 + content.len()
    } else {
        5 + content.len()
    }
}

fn validate_metalayers_encoded_size<'a>(
    layers: impl Iterator<Item = (&'a str, &'a [u8])>,
) -> Result<(), &'static str> {
    let mut body_len = 3usize + 3; // map16 + trailing array16(0)
    let mut count = 0usize;
    for (name, content) in layers {
        count += 1;
        if count > u16::MAX as usize {
            return Err("Too many metalayers");
        }
        body_len = body_len
            .checked_add(encoded_str_len(name))
            .and_then(|len| len.checked_add(encoded_bin_len(content)))
            .ok_or("Metalayers too large")?;
    }
    if body_len + 1 > u16::MAX as usize {
        return Err("Metalayers too large");
    }
    Ok(())
}

fn checked_slice_end(start: usize, len: usize, nbytes: i64) -> Result<usize, &'static str> {
    if nbytes < 0 {
        return Err("Invalid schunk nbytes");
    }
    let end = start.checked_add(len).ok_or("Slice range overflow")?;
    if end > nbytes as usize {
        return Err("Slice range out of bounds");
    }
    Ok(end)
}

/// Frame format implementation.
///
/// The frame format uses msgpack encoding for the header and stores
/// compressed chunks contiguously with an offset index.
pub mod frame {
    use super::*;

    // Msgpack format markers
    const MSGPACK_FIXARRAY_14: u8 = 0x9E; // fixarray with 14 elements
    const MSGPACK_STR8: u8 = 0xA8; // fixstr of 8 bytes
    const MSGPACK_INT32: u8 = 0xD2;
    const MSGPACK_UINT64: u8 = 0xCF;
    const MSGPACK_INT64: u8 = 0xD3;
    const MSGPACK_INT16: u8 = 0xD1;
    const MSGPACK_STR4: u8 = 0xA4; // fixstr of 4 bytes
    const MSGPACK_STR16: u8 = 0xDA;
    const MSGPACK_BIN8: u8 = 0xC4;
    const MSGPACK_BIN16: u8 = 0xC5;
    const MSGPACK_BIN32: u8 = 0xC6;
    const MSGPACK_UINT16: u8 = 0xCD;
    const MSGPACK_UINT32: u8 = 0xCE;
    const MSGPACK_MAP16: u8 = 0xDE;
    const MSGPACK_ARRAY16: u8 = 0xDC;
    const MSGPACK_TRUE: u8 = 0xC3;
    const MSGPACK_FALSE: u8 = 0xC2;
    const MSGPACK_FIXEXT16: u8 = 0xD8;

    const FRAME_MAGIC: &[u8] = b"b2frame\0";
    pub(super) const FRAME_HEADER_MIN_LEN: usize = 87;

    /// Write a frame from a schunk.
    pub fn write_frame(schunk: &Schunk) -> Vec<u8> {
        let nbytes: i64 = schunk
            .chunks
            .iter()
            .filter_map(|chunk| ChunkHeader::read(chunk).ok())
            .map(|header| i64::from(header.nbytes))
            .sum();
        let cbytes: i64 = schunk.chunks.iter().map(|chunk| chunk.len() as i64).sum();
        let chunksize = derive_frame_chunksize(schunk);

        // Build header first to know its size
        let header = build_header(schunk, nbytes, cbytes, chunksize);
        let header_size = header.len();

        // Build the offset index as a Blosc2 chunk with int64 offsets
        let offsets_data = build_offsets(schunk, header_size);
        let offsets_chunk = if !offsets_data.is_empty() {
            build_offsets_chunk(&offsets_data)
        } else {
            Vec::new()
        };

        // Build trailer
        let trailer = build_trailer(schunk);

        // Assemble the frame
        let frame_size = header_size + cbytes as usize + offsets_chunk.len() + trailer.len();
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

    fn derive_frame_chunksize(schunk: &Schunk) -> usize {
        let Some((first, rest)) = schunk.chunks.split_first() else {
            return 0;
        };
        let Ok(first_header) = ChunkHeader::read(first) else {
            return 0;
        };
        if first_header.nbytes < 0 {
            return 0;
        }
        let first_nbytes = first_header.nbytes as usize;
        for chunk in rest {
            let Ok(header) = ChunkHeader::read(chunk) else {
                return 0;
            };
            if header.nbytes < 0 || header.nbytes as usize != first_nbytes {
                return 0;
            }
        }
        first_nbytes
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
        header
            .try_write(&mut chunk[..BLOSC_EXTENDED_HEADER_LENGTH])
            .expect("offsets chunk header buffer is allocated to the extended header size");

        // Copy data after header
        chunk[BLOSC_EXTENDED_HEADER_LENGTH..].copy_from_slice(data);

        chunk
    }

    // Frame format constants matching C code (frame.h)
    const FRAME_UDCODEC: usize = 77;
    const FRAME_CODEC_META: usize = 78;
    const FRAME_OTHER_FLAGS2: usize = 85;

    /// Build frame header in msgpack format — exactly matching C's new_header_frame().
    fn build_header(schunk: &Schunk, nbytes: i64, cbytes: i64, chunksize: usize) -> Vec<u8> {
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
        h[pos] = 0x10
            | if chunksize == 0 && !schunk.chunks.is_empty() {
                BLOSC2_VERSION_FRAME_FORMAT
            } else {
                BLOSC2_VERSION_FRAME_FORMAT_RC1
            };
        if chunksize == 0 && !schunk.chunks.is_empty() {
            h[pos] |= FRAME_VARIABLE_CHUNKS;
        }
        pos += 1;

        // [26] frame_type: 0 = contiguous
        h[pos] = 0;
        pos += 1;

        // [27] codec_flags: codec in bits 0-3, clevel in bits 4-7
        h[pos] = (schunk.cparams.compcode & 0x0F) | ((schunk.cparams.clevel & 0x0F) << 4);
        pos += 1;

        // [28] other_flags: splitmode - 1 (C convention)
        h[pos] = (schunk.cparams.splitmode - 1) as u8;
        pos += 1;

        // [29-37] int64: uncompressed_size
        h[pos] = MSGPACK_INT64;
        pos += 1;
        h[pos..pos + 8].copy_from_slice(&nbytes.to_be_bytes());
        pos += 8;

        // [38-46] int64: compressed_size
        h[pos] = MSGPACK_INT64;
        pos += 1;
        h[pos..pos + 8].copy_from_slice(&cbytes.to_be_bytes());
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
        h[pos..pos + 4].copy_from_slice(&(chunksize as i32).to_be_bytes());
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
        h[pos] = if schunk.vlmetalayers.is_empty() {
            MSGPACK_FALSE
        } else {
            MSGPACK_TRUE
        };
        pos += 1;

        // [69] fixext16 marker
        h[pos] = MSGPACK_FIXEXT16;
        pos += 1;

        // [70] nfilters
        h[pos] = BLOSC2_MAX_FILTERS as u8;
        let _ = pos; // pos tracking ends here; remaining fields use fixed offsets

        // [71-78] 8 bytes filter codes (6 filters + 2 padding)
        h[71..71 + BLOSC2_MAX_FILTERS].copy_from_slice(&schunk.cparams.filters);
        // [79-86] 8 bytes filter meta
        h[79..79 + BLOSC2_MAX_FILTERS].copy_from_slice(&schunk.cparams.filters_meta);

        // [77] udcodec (at fixed offset, overlaps with filter bytes — C stores it here)
        h[FRAME_UDCODEC] = schunk.cparams.compcode;
        // [78] codec_meta
        h[FRAME_CODEC_META] = 0;

        // [85] other_flags2: bit 0 = use_dict
        h[FRAME_OTHER_FLAGS2] = 0;

        assert_eq!(h.len(), FRAME_HEADER_MIN_LEN);

        h.extend_from_slice(&encode_metalayers(&schunk.metalayers));

        // Update header_size
        let header_size = h.len() as i32;
        h[header_size_pos..header_size_pos + 4].copy_from_slice(&header_size.to_be_bytes());

        h
    }

    fn encode_metalayers(metalayers: &[Metalayer]) -> Vec<u8> {
        let mut body = Vec::new();
        body.push(MSGPACK_MAP16);
        body.extend_from_slice(&(metalayers.len() as u16).to_be_bytes());
        for layer in metalayers {
            encode_msgpack_str(&mut body, &layer.name);
            encode_msgpack_bin(&mut body, &layer.content);
        }
        body.push(MSGPACK_ARRAY16);
        body.extend_from_slice(&0u16.to_be_bytes());

        let map_size =
            u16::try_from(body.len() + 1).expect("metalayer size is validated before insertion");
        let mut section = vec![0x93, MSGPACK_UINT16];
        section.extend_from_slice(&map_size.to_be_bytes());
        section.extend_from_slice(&body);
        section
    }

    fn encode_msgpack_str(out: &mut Vec<u8>, value: &str) {
        let bytes = value.as_bytes();
        if bytes.len() <= 31 {
            out.push(0xA0 | bytes.len() as u8);
        } else if bytes.len() <= u8::MAX as usize {
            out.push(MSGPACK_STR8);
            out.push(bytes.len() as u8);
        } else {
            out.push(MSGPACK_STR16);
            out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        }
        out.extend_from_slice(bytes);
    }

    fn encode_msgpack_bin(out: &mut Vec<u8>, value: &[u8]) {
        if value.len() <= u8::MAX as usize {
            out.push(MSGPACK_BIN8);
            out.push(value.len() as u8);
        } else if value.len() <= u16::MAX as usize {
            out.push(MSGPACK_BIN16);
            out.extend_from_slice(&(value.len() as u16).to_be_bytes());
        } else {
            out.push(MSGPACK_BIN32);
            out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        }
        out.extend_from_slice(value);
    }

    /// Build trailer matching C's frame_update_trailer() format.
    fn build_trailer(schunk: &Schunk) -> Vec<u8> {
        let compressed_vlmetalayers: Vec<_> = schunk
            .vlmetalayers
            .iter()
            .map(|layer| {
                (
                    layer.name.as_str(),
                    compress::compress(&layer.content, &CParams::default())
                        .expect("VL-metalayer compression is validated when inserting"),
                )
            })
            .collect();

        let mut t = vec![0x94, 0x01, 0x93, MSGPACK_UINT16];
        let map_size_pos = t.len();
        t.extend_from_slice(&0u16.to_be_bytes());
        let index_start = map_size_pos - 1;

        t.push(MSGPACK_MAP16);
        t.extend_from_slice(&(compressed_vlmetalayers.len() as u16).to_be_bytes());

        let mut offset_positions = Vec::with_capacity(compressed_vlmetalayers.len());
        for (name, _) in &compressed_vlmetalayers {
            encode_vlmeta_name(&mut t, name);
            t.push(MSGPACK_INT32);
            offset_positions.push(t.len());
            t.extend_from_slice(&0i32.to_be_bytes());
        }

        let map_size = u16::try_from(t.len() - index_start)
            .expect("VL-metalayer index size is validated when inserting");
        t[map_size_pos..map_size_pos + 2].copy_from_slice(&map_size.to_be_bytes());

        t.push(MSGPACK_ARRAY16);
        t.extend_from_slice(&(compressed_vlmetalayers.len() as u16).to_be_bytes());
        for ((_, compressed), offset_pos) in compressed_vlmetalayers.iter().zip(offset_positions) {
            let offset = i32::try_from(t.len()).expect("VL-metalayer trailer offset fits i32");
            t[offset_pos..offset_pos + 4].copy_from_slice(&offset.to_be_bytes());
            t.push(MSGPACK_BIN32);
            t.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
            t.extend_from_slice(compressed);
        }

        let trailer_len_pos = t.len();
        t.push(MSGPACK_UINT32);
        t.extend_from_slice(&0u32.to_be_bytes());

        t.push(MSGPACK_FIXEXT16);
        t.push(0x00); // fingerprint type = none
        t.extend_from_slice(&[0u8; 16]);

        let trailer_len = t.len() as u32;
        t[trailer_len_pos + 1..trailer_len_pos + 5].copy_from_slice(&trailer_len.to_be_bytes());

        t
    }

    fn encode_vlmeta_name(out: &mut Vec<u8>, name: &str) {
        let bytes = name.as_bytes();
        debug_assert!(bytes.len() <= 31);
        out.push(0xA0 | bytes.len() as u8);
        out.extend_from_slice(bytes);
    }

    fn validate_embedded_chunk_header(
        ch: &ChunkHeader,
        compcode: u8,
        typesize: i32,
        filters: &[u8; BLOSC2_MAX_FILTERS],
        filters_meta: &[u8; BLOSC2_MAX_FILTERS],
    ) -> Result<(), String> {
        if ch.cbytes <= 0 {
            return Err("Invalid frame: invalid chunk compressed size".into());
        }
        if ch.nbytes < 0 {
            return Err("Invalid frame: invalid chunk uncompressed size".into());
        }
        if ch.cbytes < ch.header_len() as i32 {
            return Err("Invalid frame: chunk cbytes smaller than header".into());
        }
        if ch.nbytes > 0 {
            if ch.typesize == 0 || ch.typesize as usize > BLOSC_MAX_TYPESIZE {
                return Err("Invalid frame: invalid chunk typesize".into());
            }
            if ch.blocksize <= 0 {
                return Err("Invalid frame: invalid chunk blocksize".into());
            }
        }
        if ch.special_type() == BLOSC2_NO_SPECIAL
            && (ch.use_dict()
                || ch.blosc2_flags & (BLOSC2_INSTR_CODEC | BLOSC2_LAZY_CHUNK) != 0
                || ch.vl_blocks())
        {
            return Err("Invalid frame: unsupported chunk flags".into());
        }
        if !matches!(
            ch.compcode(),
            BLOSC_BLOSCLZ | BLOSC_LZ4 | BLOSC_LZ4HC | BLOSC_ZLIB | BLOSC_ZSTD
        ) {
            return Err("Invalid frame: unsupported chunk codec".into());
        }
        let codec_matches =
            ch.compcode() == compcode || (compcode == BLOSC_LZ4HC && ch.compcode() == BLOSC_LZ4);
        if !codec_matches {
            return Err("Invalid frame: chunk codec does not match frame".into());
        }
        if ch.nbytes > 0 && ch.typesize as i32 != typesize {
            return Err("Invalid frame: chunk typesize does not match frame".into());
        }
        if &ch.filters != filters || &ch.filters_meta != filters_meta {
            return Err("Invalid frame: chunk filters do not match frame".into());
        }
        for &filter in &ch.filters {
            if !matches!(
                filter,
                BLOSC_NOFILTER | BLOSC_SHUFFLE | BLOSC_BITSHUFFLE | BLOSC_DELTA | BLOSC_TRUNC_PREC
            ) {
                return Err("Invalid frame: unsupported chunk filter".into());
            }
        }

        Ok(())
    }

    fn parse_metalayers(header: &[u8]) -> Result<Vec<Metalayer>, String> {
        if header.len() == FRAME_HEADER_MIN_LEN {
            return Ok(Vec::new());
        }

        let mut pos = FRAME_HEADER_MIN_LEN;
        if header.get(pos) != Some(&0x93) {
            return Err("Invalid frame: expected metalayers array".into());
        }
        pos += 1;

        if header.get(pos) != Some(&MSGPACK_UINT16) {
            return Err("Invalid frame: expected metalayers map size".into());
        }
        pos += 1;
        if pos + 2 > header.len() {
            return Err("Invalid frame: truncated metalayers map size".into());
        }
        let map_size = u16::from_be_bytes(header[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        let body_start = pos;
        let body_len = map_size
            .checked_sub(1)
            .ok_or_else(|| "Invalid frame: invalid metalayers map size".to_string())?;
        let body_end = body_start
            .checked_add(body_len)
            .ok_or_else(|| "Invalid frame: metalayers size overflow".to_string())?;
        if body_end > header.len() {
            return Err("Invalid frame: truncated metalayers".into());
        }

        if header.get(pos) != Some(&MSGPACK_MAP16) {
            return Err("Invalid frame: expected metalayers map".into());
        }
        pos += 1;
        if pos + 2 > body_end {
            return Err("Invalid frame: truncated metalayers map".into());
        }
        let count = u16::from_be_bytes(header[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        let mut metalayers = Vec::with_capacity(count);
        for _ in 0..count {
            let name = decode_msgpack_str(header, &mut pos, body_end)?;
            let content = decode_msgpack_bin(header, &mut pos, body_end)?;
            metalayers.push(Metalayer { name, content });
        }

        if header.get(pos) != Some(&MSGPACK_ARRAY16) {
            return Err("Invalid frame: expected metalayers index array".into());
        }
        pos += 1;
        if pos + 2 > body_end {
            return Err("Invalid frame: truncated metalayers index array".into());
        }
        let index_count = u16::from_be_bytes(header[pos..pos + 2].try_into().unwrap());
        pos += 2;
        if index_count != 0 {
            return Err("Invalid frame: unsupported metalayers index array".into());
        }
        if pos != body_end {
            return Err("Invalid frame: trailing bytes in metalayers".into());
        }
        if body_end != header.len() {
            return Err("Invalid frame: unsupported header extension after metalayers".into());
        }

        Ok(metalayers)
    }

    fn decode_msgpack_str(data: &[u8], pos: &mut usize, limit: usize) -> Result<String, String> {
        let marker = *data
            .get(*pos)
            .ok_or_else(|| "Invalid frame: truncated metalayer name".to_string())?;
        *pos += 1;

        let len = if marker & 0xE0 == 0xA0 {
            (marker & 0x1F) as usize
        } else if marker == MSGPACK_STR8 {
            let len = *data
                .get(*pos)
                .ok_or_else(|| "Invalid frame: truncated metalayer name length".to_string())?
                as usize;
            *pos += 1;
            len
        } else if marker == MSGPACK_STR16 {
            if *pos + 2 > limit {
                return Err("Invalid frame: truncated metalayer name length".into());
            }
            let len = u16::from_be_bytes(data[*pos..*pos + 2].try_into().unwrap()) as usize;
            *pos += 2;
            len
        } else {
            return Err("Invalid frame: expected metalayer name string".into());
        };

        let end = (*pos)
            .checked_add(len)
            .ok_or_else(|| "Invalid frame: metalayer name size overflow".to_string())?;
        if end > limit {
            return Err("Invalid frame: truncated metalayer name".into());
        }
        let name = std::str::from_utf8(&data[*pos..end])
            .map_err(|_| "Invalid frame: metalayer name is not UTF-8".to_string())?
            .to_string();
        if name.is_empty() {
            return Err("Invalid frame: empty metalayer name".into());
        }
        *pos = end;
        Ok(name)
    }

    fn decode_msgpack_bin(data: &[u8], pos: &mut usize, limit: usize) -> Result<Vec<u8>, String> {
        let marker = *data
            .get(*pos)
            .ok_or_else(|| "Invalid frame: truncated metalayer content".to_string())?;
        *pos += 1;

        let len = match marker {
            MSGPACK_BIN8 => {
                let len = *data.get(*pos).ok_or_else(|| {
                    "Invalid frame: truncated metalayer content length".to_string()
                })? as usize;
                *pos += 1;
                len
            }
            MSGPACK_BIN16 => {
                if *pos + 2 > limit {
                    return Err("Invalid frame: truncated metalayer content length".into());
                }
                let len = u16::from_be_bytes(data[*pos..*pos + 2].try_into().unwrap()) as usize;
                *pos += 2;
                len
            }
            MSGPACK_BIN32 => {
                if *pos + 4 > limit {
                    return Err("Invalid frame: truncated metalayer content length".into());
                }
                let len = u32::from_be_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
                *pos += 4;
                len
            }
            _ => return Err("Invalid frame: expected metalayer content bin".into()),
        };

        let end = (*pos)
            .checked_add(len)
            .ok_or_else(|| "Invalid frame: metalayer content size overflow".to_string())?;
        if end > limit {
            return Err("Invalid frame: truncated metalayer content".into());
        }
        let content = data[*pos..end].to_vec();
        *pos = end;
        Ok(content)
    }

    fn offsets_chunk_len(data: &[u8], pos: usize, frame_size: usize) -> Result<usize, String> {
        if pos >= frame_size {
            return Ok(0);
        }
        if pos + BLOSC_MIN_HEADER_LENGTH > frame_size {
            return Err("Invalid frame: truncated offsets chunk header".into());
        }
        let header = ChunkHeader::read(&data[pos..frame_size])
            .map_err(|_| "Invalid frame: invalid offsets chunk header".to_string())?;
        if header.cbytes < header.header_len() as i32 {
            return Err("Invalid frame: invalid offsets chunk size".into());
        }
        let cbytes = header.cbytes as usize;
        let end = pos
            .checked_add(cbytes)
            .ok_or_else(|| "Invalid frame: offsets chunk size overflow".to_string())?;
        if end > frame_size {
            return Err("Invalid frame: offsets chunk extends past frame".into());
        }
        Ok(cbytes)
    }

    fn parse_vlmetalayers(
        trailer: &[u8],
        has_vlmetalayers: bool,
    ) -> Result<Vec<Metalayer>, String> {
        if trailer.len() < 35 {
            return Err("Invalid frame: truncated trailer".into());
        }
        let mut pos = 0usize;
        if trailer.get(pos) != Some(&0x94) {
            return Err("Invalid frame: expected trailer array".into());
        }
        pos += 1;
        if trailer.get(pos) != Some(&0x01) {
            return Err("Invalid frame: unsupported trailer version".into());
        }
        pos += 1;
        if trailer.get(pos) != Some(&0x93) {
            return Err("Invalid frame: expected VL-metalayers array".into());
        }
        pos += 1;
        if trailer.get(pos) != Some(&MSGPACK_UINT16) {
            return Err("Invalid frame: expected VL-metalayer index size".into());
        }
        pos += 1;
        if pos + 2 > trailer.len() {
            return Err("Invalid frame: truncated VL-metalayer index size".into());
        }
        let index_size = u16::from_be_bytes(trailer[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let index_end = 3usize
            .checked_add(index_size)
            .ok_or_else(|| "Invalid frame: VL-metalayer index size overflow".to_string())?;
        if index_end > trailer.len() {
            return Err("Invalid frame: truncated VL-metalayer index".into());
        }

        if trailer.get(pos) != Some(&MSGPACK_MAP16) {
            return Err("Invalid frame: expected VL-metalayer index map".into());
        }
        pos += 1;
        if pos + 2 > index_end {
            return Err("Invalid frame: truncated VL-metalayer count".into());
        }
        let count = u16::from_be_bytes(trailer[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let name = decode_msgpack_str(trailer, &mut pos, index_end)?;
            validate_vlmetalayer_name(&name)
                .map_err(|_| "Invalid frame: invalid VL-metalayer name".to_string())?;
            if trailer.get(pos) != Some(&MSGPACK_INT32) {
                return Err("Invalid frame: expected VL-metalayer offset".into());
            }
            pos += 1;
            if pos + 4 > index_end {
                return Err("Invalid frame: truncated VL-metalayer offset".into());
            }
            let offset = i32::from_be_bytes(trailer[pos..pos + 4].try_into().unwrap());
            pos += 4;
            if offset < 0 || offset as usize >= trailer.len() {
                return Err("Invalid frame: invalid VL-metalayer offset".into());
            }
            entries.push((name, offset as usize));
        }
        if pos != index_end {
            return Err("Invalid frame: trailing bytes in VL-metalayer index".into());
        }

        if trailer.get(pos) != Some(&MSGPACK_ARRAY16) {
            return Err("Invalid frame: expected VL-metalayer value array".into());
        }
        pos += 1;
        if pos + 2 > trailer.len() {
            return Err("Invalid frame: truncated VL-metalayer value count".into());
        }
        let value_count = u16::from_be_bytes(trailer[pos..pos + 2].try_into().unwrap()) as usize;
        if value_count != count {
            return Err("Invalid frame: VL-metalayer index/value count mismatch".into());
        }

        let mut metalayers = Vec::with_capacity(count);
        for (name, offset) in entries {
            let mut value_pos = offset;
            let compressed = decode_msgpack_bin(trailer, &mut value_pos, trailer.len())?;
            let content = compress::decompress(&compressed)
                .map_err(|_| "Invalid frame: invalid VL-metalayer payload".to_string())?;
            metalayers.push(Metalayer { name, content });
        }

        if has_vlmetalayers == metalayers.is_empty() {
            return Err("Invalid frame: VL-metalayer flag mismatch".into());
        }

        Ok(metalayers)
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
        let header_size_i32 = i32::from_be_bytes(data[11..15].try_into().unwrap());
        if header_size_i32 < FRAME_HEADER_MIN_LEN as i32 {
            return Err("Invalid frame header size".into());
        }
        let header_size = header_size_i32 as usize;
        if header_size > data.len() {
            return Err("Frame truncated before data section".into());
        }
        let metalayers = parse_metalayers(&data[..header_size])?;

        // [15-23] frame_size
        if data[15] != MSGPACK_UINT64 {
            return Err("Expected uint64 for frame_size".into());
        }
        let frame_size = u64::from_be_bytes(data[16..24].try_into().unwrap());
        if frame_size < header_size as u64 || frame_size > data.len() as u64 {
            return Err("Invalid frame size".into());
        }

        // [24-28] flags string
        if data[24] != MSGPACK_STR4 {
            return Err("Expected fixstr(4) for flags".into());
        }
        let general_flags = data[25];
        let frame_version = general_flags & 0x0F;
        if frame_version > BLOSC2_VERSION_FRAME_FORMAT {
            return Err("Invalid frame: unsupported frame version".into());
        }
        if general_flags & FRAME_VL_BLOCKS != 0 {
            return Err("Invalid frame: unsupported VL-block frame".into());
        }
        let variable_chunks = general_flags & FRAME_VARIABLE_CHUNKS != 0;
        let frame_type = data[26];
        let codec_flags = data[27];
        let other_flags = data[28];
        if frame_type != 0 {
            return Err("Invalid frame: unsupported frame type".into());
        }

        let compcode = codec_flags & 0x0F;
        let clevel = (codec_flags >> 4) & 0x0F;
        if !matches!(
            compcode,
            BLOSC_BLOSCLZ | BLOSC_LZ4 | BLOSC_LZ4HC | BLOSC_ZLIB | BLOSC_ZSTD
        ) {
            return Err("Invalid frame: unsupported codec".into());
        }
        if clevel > 9 {
            return Err("Invalid frame: invalid compression level".into());
        }
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
        if nbytes < 0 {
            return Err("Invalid frame: negative nbytes".into());
        }

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
        if !(1..=BLOSC_MAX_TYPESIZE as i32).contains(&typesize) {
            return Err("Invalid frame: invalid typesize".into());
        }

        // [52-56] blocksize
        if data[52] != MSGPACK_INT32 {
            return Err("Expected int32 for blocksize".into());
        }
        let blocksize = i32::from_be_bytes(data[53..57].try_into().unwrap());
        if blocksize < 0 {
            return Err("Invalid frame: negative blocksize".into());
        }

        // [57-61] chunksize
        if data[57] != MSGPACK_INT32 {
            return Err("Expected int32 for chunksize".into());
        }
        let chunksize_i32 = i32::from_be_bytes(data[58..62].try_into().unwrap());
        if chunksize_i32 < 0 {
            return Err("Invalid frame: negative chunksize".into());
        }
        let chunksize = chunksize_i32 as usize;
        if variable_chunks {
            if chunksize != 0 {
                return Err("Invalid frame: variable chunk flag with nonzero chunksize".into());
            }
            if frame_version < BLOSC2_VERSION_FRAME_FORMAT {
                return Err("Invalid frame: variable chunks require frame version 3".into());
            }
        }

        // [62-64] nthreads_comp
        if data[62] != MSGPACK_INT16 {
            return Err("Expected int16 for nthreads_comp".into());
        }
        let nthreads_comp = i16::from_be_bytes(data[63..65].try_into().unwrap());
        if nthreads_comp < 1 {
            return Err("Invalid frame: invalid compression thread count".into());
        }

        // [65-67] nthreads_decomp
        if data[65] != MSGPACK_INT16 {
            return Err("Expected int16 for nthreads_decomp".into());
        }
        let nthreads_decomp = i16::from_be_bytes(data[66..68].try_into().unwrap());
        if nthreads_decomp < 1 {
            return Err("Invalid frame: invalid decompression thread count".into());
        }

        // [68] has_vlmetalayers
        let has_vlmeta = match data[68] {
            MSGPACK_TRUE => true,
            MSGPACK_FALSE => false,
            _ => return Err("Invalid frame: invalid VL-metalayer flag".into()),
        };

        // [69-86] filter info: fixext16
        if data[69] != MSGPACK_FIXEXT16 {
            return Err("Expected fixext16 for filters".into());
        }
        let _nfilters = data[70];
        let mut filters = [0u8; BLOSC2_MAX_FILTERS];
        let mut filters_meta = [0u8; BLOSC2_MAX_FILTERS];
        filters.copy_from_slice(&data[71..71 + BLOSC2_MAX_FILTERS]);
        filters_meta.copy_from_slice(&data[79..79 + BLOSC2_MAX_FILTERS]);
        for &filter in &filters {
            if !matches!(
                filter,
                BLOSC_NOFILTER | BLOSC_SHUFFLE | BLOSC_BITSHUFFLE | BLOSC_DELTA | BLOSC_TRUNC_PREC
            ) {
                return Err("Invalid frame: unsupported filter".into());
            }
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
        let data_end = data_start
            .checked_add(cbytes as usize)
            .ok_or_else(|| "Invalid frame: cbytes overflow".to_string())?;
        if data_end > data.len() {
            return Err("Invalid frame: truncated data section".into());
        }
        if frame_size < data_end as u64 {
            return Err("Invalid frame: frame size smaller than data section".into());
        }

        let mut total_nbytes = 0i64;
        let mut total_cbytes = 0i64;
        while pos < data_end {
            if pos + BLOSC_MIN_HEADER_LENGTH > data_end {
                return Err("Invalid frame: data section ends inside chunk header".into());
            }

            let ch = ChunkHeader::read(&data[pos..])
                .map_err(|_| "Invalid frame: invalid chunk header".to_string())?;
            validate_embedded_chunk_header(&ch, compcode, typesize, &filters, &filters_meta)?;

            let chunk_cbytes = ch.cbytes as usize;
            let chunk_end = pos
                .checked_add(chunk_cbytes)
                .ok_or_else(|| "Invalid frame: chunk size overflow".to_string())?;
            if chunk_end > data_end {
                return Err("Invalid frame: chunk extends past data section".into());
            }

            total_nbytes = total_nbytes
                .checked_add(ch.nbytes as i64)
                .ok_or_else(|| "Invalid frame: nbytes overflow".to_string())?;
            total_cbytes = total_cbytes
                .checked_add(ch.cbytes as i64)
                .ok_or_else(|| "Invalid frame: cbytes overflow".to_string())?;
            chunks.push(data[pos..chunk_end].to_vec());
            pos = chunk_end;
        }

        if total_cbytes != cbytes {
            return Err("Invalid frame: chunk cbytes total does not match frame".into());
        }
        if total_nbytes != nbytes {
            return Err("Invalid frame: chunk nbytes total does not match frame".into());
        }
        let offsets_len = if chunks.is_empty() {
            0
        } else {
            offsets_chunk_len(data, data_end, frame_size as usize)?
        };
        let trailer_start = data_end
            .checked_add(offsets_len)
            .ok_or_else(|| "Invalid frame: trailer offset overflow".to_string())?;
        if trailer_start > frame_size as usize {
            return Err("Invalid frame: trailer starts past frame".into());
        }
        let vlmetalayers =
            parse_vlmetalayers(&data[trailer_start..frame_size as usize], has_vlmeta)?;
        if variable_chunks && chunks.len() > 1 {
            let mut sizes = chunks
                .iter()
                .map(|chunk| ChunkHeader::read(chunk).map(|header| header.nbytes));
            if let Some(Ok(first_size)) = sizes.next() {
                if sizes.all(|size| size == Ok(first_size)) {
                    return Err("Invalid frame: variable chunk flag without variable chunks".into());
                }
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
            metalayers,
            vlmetalayers,
            variable_chunks,
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
        assert!(schunk.decompress_chunk(-1).is_err());
    }

    #[test]
    fn test_schunk_mutation_and_slice_operations() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());

        schunk.append_buffer(b"aaaa").unwrap();
        schunk.append_buffer(b"cccc").unwrap();
        schunk.insert_buffer(1, b"bbbb").unwrap();
        assert_eq!(schunk.decompress_all().unwrap(), b"aaaabbbbcccc");
        assert_eq!(schunk.nchunks(), 3);
        assert_eq!(schunk.nbytes, 12);

        schunk.update_chunk(1, b"BBBB").unwrap();
        assert_eq!(schunk.decompress_all().unwrap(), b"aaaaBBBBcccc");

        let removed = schunk.delete_chunk(0).unwrap();
        assert_eq!(removed, b"aaaa");
        assert_eq!(schunk.decompress_all().unwrap(), b"BBBBcccc");
        assert_eq!(schunk.chunksize, 4);

        assert_eq!(schunk.get_slice(2, 4).unwrap(), b"BBcc");
        schunk.set_slice(2, b"xyzz").unwrap();
        assert_eq!(schunk.decompress_all().unwrap(), b"BBxyzzcc");
        assert!(schunk.get_slice(7, 2).is_err());
        assert!(schunk.set_slice(7, b"zz").is_err());

        let copied = schunk.copy_schunk();
        schunk.update_chunk(0, b"1111").unwrap();
        assert_eq!(copied.decompress_all().unwrap(), b"BBxyzzcc");
        assert_eq!(schunk.decompress_all().unwrap(), b"1111zzcc");
    }

    #[test]
    fn test_schunk_reorder_and_offset_queries() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.append_buffer(b"aaaa").unwrap();
        schunk.append_buffer(b"bbbbbb").unwrap();
        schunk.append_buffer(b"cc").unwrap();

        let offsets = schunk.chunk_offsets();
        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets[0], 0);
        assert_eq!(offsets[1], schunk.chunks[0].len() as u64);
        assert_eq!(
            offsets[2],
            (schunk.chunks[0].len() + schunk.chunks[1].len()) as u64
        );

        assert_eq!(schunk.chunk_range_for_byte_slice(2, 6).unwrap(), 0..2);
        assert_eq!(schunk.chunk_range_for_byte_slice(4, 6).unwrap(), 1..2);
        assert_eq!(schunk.chunk_range_for_byte_slice(12, 0).unwrap(), 3..3);
        assert!(schunk.chunk_range_for_byte_slice(12, 1).is_err());

        schunk.reorder_chunks(&[2, 0, 1]).unwrap();
        assert_eq!(schunk.decompress_all().unwrap(), b"ccaaaabbbbbb");
        assert_eq!(schunk.chunksize, 0);

        assert!(schunk.reorder_chunks(&[0, 0, 1]).is_err());
        assert!(schunk.reorder_chunks(&[0, 1]).is_err());
        assert_eq!(schunk.decompress_all().unwrap(), b"ccaaaabbbbbb");
    }

    #[test]
    fn test_variable_chunks_frame_flag_roundtrip() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.append_buffer(b"alpha\0").unwrap();
        schunk.append_buffer(b"bravo bravo\0").unwrap();
        schunk.append_buffer(b"charlie-charlie-charlie\0").unwrap();

        assert_eq!(schunk.chunksize, 0);

        let frame = schunk.to_frame();
        assert_eq!(frame[25] & 0x0F, BLOSC2_VERSION_FRAME_FORMAT);
        assert_ne!(frame[25] & FRAME_VARIABLE_CHUNKS, 0);
        assert_eq!(i32::from_be_bytes(frame[58..62].try_into().unwrap()), 0);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.chunksize, 0);
        assert_eq!(restored.decompress_chunk(0).unwrap(), b"alpha\0");
        assert_eq!(restored.decompress_chunk(1).unwrap(), b"bravo bravo\0");
        assert_eq!(
            restored.decompress_chunk(2).unwrap(),
            b"charlie-charlie-charlie\0"
        );
    }

    #[test]
    fn test_fixed_chunks_keep_fixed_frame_flag() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.append_buffer(b"one\0").unwrap();
        schunk.append_buffer(b"two\0").unwrap();
        schunk.append_buffer(b"six\0").unwrap();

        assert_eq!(schunk.chunksize, 4);

        let frame = schunk.to_frame();
        assert_eq!(frame[25] & 0x0F, BLOSC2_VERSION_FRAME_FORMAT_RC1);
        assert_eq!(frame[25] & FRAME_VARIABLE_CHUNKS, 0);
        assert_eq!(i32::from_be_bytes(frame[58..62].try_into().unwrap()), 4);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.chunksize, 4);
        assert_eq!(restored.decompress_all().unwrap(), b"one\0two\0six\0");
    }

    #[test]
    fn test_schunk_append_rejects_overflowed_totals() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.nbytes = i64::MAX;

        assert!(schunk.append_buffer(&[1, 2, 3, 4]).is_err());
        assert_eq!(schunk.chunksize, 0);
        assert!(schunk.chunks.is_empty());

        schunk.nbytes = 0;
        schunk.cbytes = i64::MAX;
        assert!(schunk.append_buffer(&[1, 2, 3, 4]).is_err());
        assert_eq!(schunk.nbytes, 0);
        assert_eq!(schunk.chunksize, 0);
        assert!(schunk.chunks.is_empty());
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

    #[test]
    fn test_schunk_frame_roundtrip_matrix() {
        for &compcode in &[
            BLOSC_BLOSCLZ,
            BLOSC_LZ4,
            BLOSC_LZ4HC,
            BLOSC_ZLIB,
            BLOSC_ZSTD,
        ] {
            let cparams = CParams {
                compcode,
                clevel: 5,
                typesize: 4,
                splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            };
            let mut schunk = Schunk::new(cparams, DParams::default());
            let data1: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
            let data2: Vec<u8> = (4096..8192u32).flat_map(|i| i.to_le_bytes()).collect();
            schunk.append_buffer(&data1).unwrap();
            schunk.append_buffer(&data2).unwrap();

            let frame = schunk.to_frame();
            let from_memory = Schunk::from_frame(&frame).unwrap();
            assert_eq!(from_memory.nchunks(), 2);
            assert_eq!(from_memory.decompress_chunk(0).unwrap(), data1);
            assert_eq!(from_memory.decompress_chunk(1).unwrap(), data2);

            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(format!("codec-{compcode}.b2frame"));
            schunk.to_file(path.to_str().unwrap()).unwrap();
            let from_file = Schunk::open(path.to_str().unwrap()).unwrap();
            assert_eq!(from_file.nchunks(), 2);
            assert_eq!(from_file.decompress_chunk(0).unwrap(), data1);
            assert_eq!(from_file.decompress_chunk(1).unwrap(), data2);
        }
    }

    #[test]
    fn test_frame_writer_derives_totals_from_chunks() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        let data: Vec<u8> = (0..5000u32).flat_map(|i| i.to_le_bytes()).collect();
        schunk.append_buffer(&data).unwrap();

        schunk.nbytes = 1;
        schunk.cbytes = 1;
        schunk.chunksize = 1;

        let frame = schunk.to_frame();
        let schunk2 = Schunk::from_frame(&frame).unwrap();

        assert_eq!(schunk2.nbytes, data.len() as i64);
        assert_eq!(schunk2.cbytes, schunk.chunks[0].len() as i64);
        assert_eq!(schunk2.chunksize, data.len());
        assert_eq!(schunk2.decompress_chunk(0).unwrap(), data);
    }

    #[test]
    fn test_schunk_metalayers_roundtrip_in_frame() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.append_buffer(b"payload").unwrap();

        schunk.add_metalayer("author", b"pure-rust").unwrap();
        schunk.add_metalayer("revision", &[1, 2, 3, 4]).unwrap();
        schunk.add_metalayer("author", b"updated").unwrap();

        assert_eq!(schunk.metalayers.len(), 2);
        assert_eq!(schunk.metalayer("author"), Some(&b"updated"[..]));
        assert_eq!(schunk.remove_metalayer("revision"), Some(vec![1, 2, 3, 4]));
        schunk.add_metalayer("revision", &[5, 6]).unwrap();

        let frame = schunk.to_frame();
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        assert!(header_size > frame::FRAME_HEADER_MIN_LEN);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.decompress_all().unwrap(), b"payload");
        assert_eq!(restored.metalayer("author"), Some(&b"updated"[..]));
        assert_eq!(restored.metalayer("revision"), Some(&[5, 6][..]));
    }

    #[test]
    fn test_schunk_metalayers_reject_invalid_inputs() {
        let mut schunk = Schunk::new(CParams::default(), DParams::default());

        assert!(schunk.add_metalayer("", b"data").is_err());

        let large_name = "x".repeat(u16::MAX as usize + 1);
        assert!(schunk.add_metalayer(&large_name, b"data").is_err());

        let too_large_payload = vec![0u8; u16::MAX as usize];
        assert!(schunk
            .add_metalayer("too-large", &too_large_payload)
            .is_err());
    }

    #[test]
    fn test_frame_rejects_malformed_metalayers() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.append_buffer(b"payload").unwrap();
        schunk.add_metalayer("name", b"value").unwrap();
        let frame = schunk.to_frame();
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;

        let mut bad_marker = frame.clone();
        bad_marker[frame::FRAME_HEADER_MIN_LEN] = 0x90;
        assert!(Schunk::from_frame(&bad_marker).is_err());

        let mut bad_size = frame.clone();
        bad_size[frame::FRAME_HEADER_MIN_LEN + 2..frame::FRAME_HEADER_MIN_LEN + 4]
            .copy_from_slice(&u16::MAX.to_be_bytes());
        assert!(Schunk::from_frame(&bad_size).is_err());

        let mut bad_name = frame.clone();
        let name_marker_pos = frame::FRAME_HEADER_MIN_LEN + 7;
        bad_name[name_marker_pos] = 0xC1;
        assert!(Schunk::from_frame(&bad_name).is_err());

        let mut extra = frame.clone();
        extra.insert(header_size, 0);
        let new_header_size = (header_size + 1) as i32;
        extra[11..15].copy_from_slice(&new_header_size.to_be_bytes());
        let new_frame_size = extra.len() as u64;
        extra[16..24].copy_from_slice(&new_frame_size.to_be_bytes());
        assert!(Schunk::from_frame(&extra).is_err());
    }

    #[test]
    fn test_schunk_vlmetalayers_roundtrip_in_frame() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.append_buffer(b"payload").unwrap();

        let long_content = b"variable metalayer payload ".repeat(32);
        schunk.add_vlmetalayer("vlmeta1", &long_content).unwrap();
        schunk.add_vlmetalayer("vlmeta2", b"small").unwrap();
        schunk.add_vlmetalayer("vlmeta2", b"updated").unwrap();

        assert_eq!(schunk.vlmetalayer("vlmeta1"), Some(long_content.as_slice()));
        assert_eq!(
            schunk.remove_vlmetalayer("vlmeta2"),
            Some(b"updated".to_vec())
        );
        schunk.add_vlmetalayer("vlmeta2", b"restored").unwrap();

        let frame = schunk.to_frame();
        assert_eq!(frame[68], 0xC3);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.decompress_all().unwrap(), b"payload");
        assert_eq!(
            restored.vlmetalayer("vlmeta1"),
            Some(long_content.as_slice())
        );
        assert_eq!(restored.vlmetalayer("vlmeta2"), Some(&b"restored"[..]));
    }

    #[test]
    fn test_schunk_vlmetalayers_reject_invalid_inputs() {
        let mut schunk = Schunk::new(CParams::default(), DParams::default());

        assert!(schunk.add_vlmetalayer("", b"data").is_err());
        assert!(schunk.add_vlmetalayer(&"x".repeat(32), b"data").is_err());
    }

    #[test]
    fn test_frame_rejects_malformed_vlmetalayers() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.append_buffer(b"payload").unwrap();
        schunk.add_vlmetalayer("vlmeta", b"content").unwrap();
        let frame = schunk.to_frame();

        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        let data_end = header_size + schunk.cbytes as usize;
        let offsets_header = ChunkHeader::read(&frame[data_end..]).unwrap();
        let trailer_start = data_end + offsets_header.cbytes as usize;

        let mut bad_trailer_marker = frame.clone();
        bad_trailer_marker[trailer_start] = 0x90;
        assert!(Schunk::from_frame(&bad_trailer_marker).is_err());

        let mut bad_index_size = frame.clone();
        bad_index_size[trailer_start + 4..trailer_start + 6]
            .copy_from_slice(&u16::MAX.to_be_bytes());
        assert!(Schunk::from_frame(&bad_index_size).is_err());

        let mut bad_offset = frame.clone();
        let offset_pos = trailer_start + 6 + 3 + 1 + "vlmeta".len() + 1;
        bad_offset[offset_pos..offset_pos + 4].copy_from_slice(&(-1i32).to_be_bytes());
        assert!(Schunk::from_frame(&bad_offset).is_err());

        let mut flag_mismatch = frame.clone();
        flag_mismatch[68] = 0xC2;
        assert!(Schunk::from_frame(&flag_mismatch).is_err());
    }

    #[test]
    fn test_frame_rejects_invalid_signed_sizes() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        let data: Vec<u8> = (0..1000u32).flat_map(|i| i.to_le_bytes()).collect();
        schunk.append_buffer(&data).unwrap();
        let frame = schunk.to_frame();

        let mut bad_header_size = frame.clone();
        bad_header_size[11..15].copy_from_slice(&(-1i32).to_be_bytes());
        assert!(Schunk::from_frame(&bad_header_size).is_err());

        let mut bad_nbytes = frame.clone();
        bad_nbytes[30..38].copy_from_slice(&(-1i64).to_be_bytes());
        assert!(Schunk::from_frame(&bad_nbytes).is_err());

        let mut bad_cbytes = frame.clone();
        bad_cbytes[39..47].copy_from_slice(&(-1i64).to_be_bytes());
        assert!(Schunk::from_frame(&bad_cbytes).is_err());

        let mut bad_typesize = frame.clone();
        bad_typesize[48..52].copy_from_slice(&0i32.to_be_bytes());
        assert!(Schunk::from_frame(&bad_typesize).is_err());

        let mut bad_chunksize = frame.clone();
        bad_chunksize[58..62].copy_from_slice(&(-1i32).to_be_bytes());
        assert!(Schunk::from_frame(&bad_chunksize).is_err());

        let mut bad_blocksize = frame.clone();
        bad_blocksize[53..57].copy_from_slice(&(-1i32).to_be_bytes());
        assert!(Schunk::from_frame(&bad_blocksize).is_err());
    }

    #[test]
    fn test_frame_rejects_invalid_codec_level_threads_and_size() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        let data: Vec<u8> = (0..1000u32).flat_map(|i| i.to_le_bytes()).collect();
        schunk.append_buffer(&data).unwrap();
        let frame = schunk.to_frame();

        let mut bad_codec = frame.clone();
        bad_codec[27] = 0x06 | (5 << 4);
        assert!(Schunk::from_frame(&bad_codec).is_err());

        let mut bad_frame_type = frame.clone();
        bad_frame_type[26] = 1;
        assert!(Schunk::from_frame(&bad_frame_type).is_err());

        let mut bad_clevel = frame.clone();
        bad_clevel[27] = BLOSC_LZ4 | (10 << 4);
        assert!(Schunk::from_frame(&bad_clevel).is_err());

        let mut bad_filter = frame.clone();
        bad_filter[71 + BLOSC2_MAX_FILTERS - 1] = 99;
        assert!(Schunk::from_frame(&bad_filter).is_err());

        let mut bad_comp_threads = frame.clone();
        bad_comp_threads[63..65].copy_from_slice(&0i16.to_be_bytes());
        assert!(Schunk::from_frame(&bad_comp_threads).is_err());

        let mut bad_decomp_threads = frame.clone();
        bad_decomp_threads[66..68].copy_from_slice(&0i16.to_be_bytes());
        assert!(Schunk::from_frame(&bad_decomp_threads).is_err());

        let mut too_large_frame_size = frame.clone();
        too_large_frame_size[16..24].copy_from_slice(&((frame.len() as u64) + 1).to_be_bytes());
        assert!(Schunk::from_frame(&too_large_frame_size).is_err());

        let mut too_small_frame_size = frame.clone();
        too_small_frame_size[16..24].copy_from_slice(&87u64.to_be_bytes());
        assert!(Schunk::from_frame(&too_small_frame_size).is_err());
    }

    #[test]
    fn test_frame_rejects_inconsistent_chunk_totals() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        let data: Vec<u8> = (0..1000u32).flat_map(|i| i.to_le_bytes()).collect();
        schunk.append_buffer(&data).unwrap();
        let frame = schunk.to_frame();

        let mut bad_total_nbytes = frame.clone();
        bad_total_nbytes[30..38].copy_from_slice(&(schunk.nbytes + 1).to_be_bytes());
        assert!(Schunk::from_frame(&bad_total_nbytes).is_err());

        let mut bad_total_cbytes = frame.clone();
        bad_total_cbytes[39..47].copy_from_slice(&(schunk.cbytes - 1).to_be_bytes());
        assert!(Schunk::from_frame(&bad_total_cbytes).is_err());
    }

    #[test]
    fn test_frame_rejects_invalid_embedded_chunk_headers() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        let data: Vec<u8> = (0..1000u32).flat_map(|i| i.to_le_bytes()).collect();
        schunk.append_buffer(&data).unwrap();
        let frame = schunk.to_frame();
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;

        let mut bad_typesize = frame.clone();
        bad_typesize[header_size + BLOSC2_CHUNK_TYPESIZE] = 0;
        assert!(Schunk::from_frame(&bad_typesize).is_err());

        let mut bad_filter = frame.clone();
        bad_filter[header_size + BLOSC2_CHUNK_FILTER_CODES + 5] = 99;
        assert!(Schunk::from_frame(&bad_filter).is_err());

        let mut bad_flags = frame.clone();
        bad_flags[header_size + BLOSC2_CHUNK_BLOSC2_FLAGS2] = BLOSC2_VL_BLOCKS;
        assert!(Schunk::from_frame(&bad_flags).is_err());

        let mut mismatched_codec = frame.clone();
        mismatched_codec[27] = BLOSC_BLOSCLZ | (5 << 4);
        assert!(Schunk::from_frame(&mismatched_codec).is_err());

        let mut mismatched_filter = frame.clone();
        mismatched_filter[71 + BLOSC2_MAX_FILTERS - 1] = BLOSC_NOFILTER;
        assert!(Schunk::from_frame(&mismatched_filter).is_err());
    }
}
