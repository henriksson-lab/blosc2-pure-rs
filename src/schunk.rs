//! Super-chunks and the Blosc2 frame container format.
//!
//! A super-chunk ([`Schunk`]) is an ordered collection of independently
//! compressed chunks that share a single set of compression and
//! decompression parameters. It is the unit of data that Blosc2 reads from
//! and writes to a frame on disk:
//!
//! * Append, insert, update or delete chunks one at a time (or in bulk).
//! * Decompress an individual chunk, a contiguous byte range, or the whole
//!   super-chunk.
//! * Attach named fixed-size [`Metalayer`]s in the frame header and
//!   variable-length metalayers in the trailer.
//!
//! Serialization lives in the [`frame`] submodule. Two on-disk forms are
//! supported, both byte-compatible with C-Blosc2:
//!
//! * **Contiguous frame** — header, chunks, offsets index and trailer in
//!   one file or buffer ([`Schunk::to_frame`], [`Schunk::from_frame`]).
//! * **Sparse frame directory** — the header/offsets index in
//!   `chunks.b2frame` with one file per compressed chunk
//!   ([`Schunk::to_sframe_dir`], [`Schunk::open_sframe`]).
//!
//! [`LazySchunk`] gives random read access to a frame on disk without
//! decompressing or even loading every chunk up front.

use crate::codecs;
use crate::compress::{self, CParams, DParams};
use crate::constants::*;
use crate::header::ChunkHeader;
use rayon::prelude::*;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Named fixed-size metadata stored in a super-chunk frame header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Metalayer {
    pub name: String,
    pub content: Vec<u8>,
}

/// Borrowed view of a compressed chunk stored inside a [`Schunk`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompressedChunkView<'a> {
    bytes: &'a [u8],
}

impl<'a> CompressedChunkView<'a> {
    /// Return the raw compressed chunk bytes.
    pub fn as_slice(&self) -> &'a [u8] {
        self.bytes
    }

    /// Return `(nbytes, cbytes, blocksize)` from the chunk header.
    pub fn sizes(&self) -> Result<(usize, usize, usize), &'static str> {
        compress::cbuffer_sizes(self.bytes)
    }
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
    vlblocks: bool,
}

/// File-backed reference to a compressed chunk in a frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LazyChunkRef {
    /// Absolute byte offset of the compressed chunk in the frame file.
    pub offset: u64,
    /// Compressed chunk size in bytes.
    pub cbytes: usize,
    /// Uncompressed chunk size in bytes.
    pub nbytes: usize,
    special: Option<u8>,
}

/// A file-backed super-chunk that loads compressed chunks on demand.
#[derive(Clone, Debug)]
pub struct LazySchunk {
    /// Compression parameters recorded in the frame header.
    pub cparams: CParams,
    /// Decompression parameters recorded in the frame header.
    pub dparams: DParams,
    /// Uncompressed size for fixed-size chunks, or zero for variable chunks.
    pub chunksize: usize,
    /// Total uncompressed bytes across all chunks.
    pub nbytes: i64,
    /// Total compressed bytes across all chunks.
    pub cbytes: i64,
    /// Fixed-size metadata layers stored in the frame header.
    pub metalayers: Vec<Metalayer>,
    /// Variable-length metadata layers stored in the frame trailer.
    pub vlmetalayers: Vec<Metalayer>,
    path: PathBuf,
    chunks: Vec<LazyChunkRef>,
    sframe: bool,
}

impl LazySchunk {
    /// Number of chunks in the frame.
    pub fn nchunks(&self) -> i64 {
        self.chunks.len() as i64
    }

    /// Return lazy chunk references with file offsets and sizes.
    pub fn chunk_refs(&self) -> &[LazyChunkRef] {
        &self.chunks
    }

    /// Decompress a chunk by index, reading only that compressed chunk from the frame file.
    pub fn decompress_chunk(&self, nchunk: i64) -> Result<Vec<u8>, String> {
        let chunk = self.read_chunk_bytes(nchunk)?;
        let mut dparams = self.dparams.clone();
        dparams.nchunk = nchunk;
        compress::decompress_with_dparams(&chunk, &dparams).map_err(str::to_string)
    }

    /// Return decompressed bytes spanning the whole super-chunk.
    pub fn decompress_all(&self) -> Result<Vec<u8>, String> {
        let capacity = usize::try_from(self.nbytes).map_err(|_| "Invalid schunk nbytes")?;
        let mut out = Vec::with_capacity(capacity);
        for idx in 0..self.chunks.len() {
            out.extend(self.decompress_chunk(idx as i64)?);
        }
        Ok(out)
    }

    /// Read a byte slice by loading only the compressed chunks touched by the range.
    pub fn get_slice(&self, start: usize, len: usize) -> Result<Vec<u8>, String> {
        let end = checked_slice_end(start, len, self.nbytes).map_err(str::to_string)?;
        if len == 0 {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(len);
        let mut chunk_start = 0usize;
        for (idx, chunk_ref) in self.chunks.iter().enumerate() {
            let chunk_end = chunk_start
                .checked_add(chunk_ref.nbytes)
                .ok_or_else(|| "Slice offset overflow".to_string())?;
            if chunk_end > start && chunk_start < end {
                let chunk = self.decompress_chunk(idx as i64)?;
                if chunk.len() != chunk_ref.nbytes {
                    return Err("Lazy chunk uncompressed size mismatch".into());
                }
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

    /// Return the chunk index range touched by a byte slice without loading chunk payloads.
    pub fn chunk_range_for_byte_slice(
        &self,
        start: usize,
        len: usize,
    ) -> Result<std::ops::Range<usize>, String> {
        let end = checked_slice_end(start, len, self.nbytes).map_err(str::to_string)?;
        if len == 0 {
            let mut offset = 0usize;
            for (idx, chunk_ref) in self.chunks.iter().enumerate() {
                if start <= offset {
                    return Ok(idx..idx);
                }
                offset = offset
                    .checked_add(chunk_ref.nbytes)
                    .ok_or_else(|| "Slice offset overflow".to_string())?;
            }
            return Ok(self.chunks.len()..self.chunks.len());
        }

        let mut first = None;
        let mut last = None;
        let mut chunk_start = 0usize;
        for (idx, chunk_ref) in self.chunks.iter().enumerate() {
            let chunk_end = chunk_start
                .checked_add(chunk_ref.nbytes)
                .ok_or_else(|| "Slice offset overflow".to_string())?;
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

    fn read_chunk_bytes(&self, nchunk: i64) -> Result<Vec<u8>, String> {
        if nchunk < 0 {
            return Err("Chunk index out of range".into());
        }
        let chunk_ref = self
            .chunks
            .get(nchunk as usize)
            .ok_or_else(|| "Chunk index out of range".to_string())?;
        if let Some(special) = chunk_ref.special {
            return synthetic_special_chunk_for_params(special, chunk_ref.nbytes, &self.cparams);
        }
        if self.sframe {
            let chunk = std::fs::read(sframe_chunk_path(&self.path, chunk_ref.offset))
                .map_err(|e| format!("Failed to read sparse frame chunk: {e}"))?;
            compress::cbuffer_validate(&chunk).map_err(|err| format!("Invalid frame: {err}"))?;
            return Ok(chunk);
        }
        let mut file = std::fs::File::open(&self.path)
            .map_err(|e| format!("Failed to open frame file: {e}"))?;
        use std::io::{Read, Seek, SeekFrom};
        file.seek(SeekFrom::Start(chunk_ref.offset))
            .map_err(|e| format!("Failed to seek to chunk: {e}"))?;
        let mut chunk = vec![0u8; chunk_ref.cbytes];
        file.read_exact(&mut chunk)
            .map_err(|e| format!("Failed to read chunk: {e}"))?;
        compress::cbuffer_validate(&chunk).map_err(|err| format!("Invalid frame: {err}"))?;
        Ok(chunk)
    }
}

fn synthetic_special_chunk_for_params(
    special_type: u8,
    nbytes: usize,
    cparams: &CParams,
) -> Result<Vec<u8>, String> {
    if nbytes > i32::MAX as usize {
        return Err("Invalid frame: special chunk is too large".to_string());
    }
    compress::validate_cparams(cparams, nbytes).map_err(|err| format!("Invalid frame: {err}"))?;
    let normalized_cparams = compress::normalized_cparams(cparams);
    let typesize = normalized_cparams.typesize as usize;
    if nbytes != 0 && !nbytes.is_multiple_of(typesize) {
        return Err("Invalid frame: special chunk size is not a multiple of typesize".to_string());
    }
    let blocksize = compress::compute_blocksize(&normalized_cparams, nbytes as i32);
    let mut chunk = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH];
    let header = ChunkHeader {
        version: BLOSC2_VERSION_FORMAT_STABLE,
        versionlz: BLOSC_BLOSCLZ_VERSION_FORMAT,
        flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
        typesize: chunk_header_typesize(normalized_cparams.typesize),
        nbytes: nbytes as i32,
        blocksize,
        cbytes: BLOSC_EXTENDED_HEADER_LENGTH as i32,
        blosc2_flags: special_type << 4,
        ..Default::default()
    };
    header
        .try_write(&mut chunk)
        .map_err(|_| "Invalid frame: cannot build special chunk".to_string())?;
    Ok(chunk)
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
            vlblocks: false,
        }
    }

    /// Number of chunks.
    pub fn nchunks(&self) -> i64 {
        self.chunks.len() as i64
    }

    /// Compress and append a data buffer as a new chunk.
    /// Returns the resulting number of chunks, matching the C API.
    pub fn append_buffer(&mut self, data: &[u8]) -> Result<i64, &'static str> {
        if self.vlblocks {
            return Err("Cannot mix regular and VL-block chunks");
        }
        let mut cparams = self.cparams.clone();
        cparams.nchunk = self.chunks.len() as i64;
        let chunk = compress::compress(data, &cparams)?;

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

        Ok(self.chunks.len() as i64)
    }

    /// Compress and append multiple regular chunks, preserving input order.
    pub fn append_buffers(
        &mut self,
        buffers: &[&[u8]],
    ) -> Result<std::ops::Range<i64>, &'static str> {
        if self.vlblocks {
            return Err("Cannot mix regular and VL-block chunks");
        }
        if buffers.is_empty() {
            let idx = self.chunks.len() as i64;
            return Ok(idx..idx);
        }

        let start = self.chunks.len() as i64;
        let chunks: Result<Vec<_>, _> = buffers
            .iter()
            .enumerate()
            .map(|(idx, buffer)| {
                let mut cparams = self.cparams.clone();
                cparams.nchunk = start + idx as i64;
                compress::compress(buffer, &cparams)
            })
            .collect();
        let chunks = chunks?;
        let add_nbytes = buffers.iter().try_fold(0i64, |acc, buffer| {
            acc.checked_add(buffer.len() as i64)
                .ok_or("Schunk nbytes overflow")
        })?;
        let add_cbytes = chunks.iter().try_fold(0i64, |acc, chunk| {
            acc.checked_add(chunk.len() as i64)
                .ok_or("Schunk cbytes overflow")
        })?;
        self.nbytes = self
            .nbytes
            .checked_add(add_nbytes)
            .ok_or("Schunk nbytes overflow")?;
        self.cbytes = self
            .cbytes
            .checked_add(add_cbytes)
            .ok_or("Schunk cbytes overflow")?;
        self.chunks.extend(chunks);
        if self.chunksize == 0 {
            self.chunksize = buffers[0].len();
        }
        self.refresh_chunk_shape()?;
        Ok(start..self.chunks.len() as i64)
    }

    /// Compress and append independent variable-length blocks as one VL-block chunk.
    /// Returns the resulting number of chunks, matching the C API.
    pub fn append_vlblocks(&mut self, blocks: &[&[u8]]) -> Result<i64, &'static str> {
        if !self.chunks.is_empty() && !self.vlblocks {
            return Err("Cannot mix regular and VL-block chunks");
        }
        let mut cparams = self.cparams.clone();
        cparams.nchunk = self.chunks.len() as i64;
        let chunk = compress::vlcompress(blocks, &cparams)?;
        let (chunk_nbytes, chunk_cbytes, _) = compress::cbuffer_sizes(&chunk)?;
        let new_nbytes = self
            .nbytes
            .checked_add(chunk_nbytes as i64)
            .ok_or("Schunk nbytes overflow")?;
        let new_cbytes = self
            .cbytes
            .checked_add(chunk_cbytes as i64)
            .ok_or("Schunk cbytes overflow")?;
        self.nbytes = new_nbytes;
        self.cbytes = new_cbytes;
        self.chunksize = 0;
        self.variable_chunks = true;
        self.vlblocks = true;
        self.chunks.push(chunk);
        Ok(self.chunks.len() as i64)
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
        let mut dparams = self.dparams.clone();
        dparams.nchunk = nchunk;
        compress::decompress_with_dparams(&self.chunks[idx], &dparams)
    }

    /// Borrow the raw compressed bytes for a chunk by index.
    pub fn compressed_chunk(&self, nchunk: i64) -> Result<&[u8], &'static str> {
        if nchunk < 0 {
            return Err("Chunk index out of range");
        }
        let idx = nchunk as usize;
        self.chunks
            .get(idx)
            .map(Vec::as_slice)
            .ok_or("Chunk index out of range")
    }

    /// Borrow a view over the raw compressed bytes for a chunk by index.
    pub fn compressed_chunk_view(
        &self,
        nchunk: i64,
    ) -> Result<CompressedChunkView<'_>, &'static str> {
        Ok(CompressedChunkView {
            bytes: self.compressed_chunk(nchunk)?,
        })
    }

    /// Decompress a chunk by index into a caller-provided destination buffer.
    /// Returns the number of bytes written.
    pub fn decompress_chunk_into(
        &self,
        nchunk: i64,
        dest: &mut [u8],
    ) -> Result<usize, &'static str> {
        if nchunk < 0 {
            return Err("Chunk index out of range");
        }
        let idx = nchunk as usize;
        if idx >= self.chunks.len() {
            return Err("Chunk index out of range");
        }
        let mut dparams = self.dparams.clone();
        dparams.nchunk = nchunk;
        compress::decompress_into_with_dparams(&self.chunks[idx], dest, &dparams)
    }

    /// Compress and insert a data buffer before `nchunk`.
    /// Returns the resulting number of chunks, matching the C API.
    pub fn insert_buffer(&mut self, nchunk: i64, data: &[u8]) -> Result<i64, &'static str> {
        if self.vlblocks {
            return Err("Cannot mix regular and VL-block chunks");
        }
        if nchunk < 0 || nchunk as usize > self.chunks.len() {
            return Err("Chunk index out of range");
        }

        let mut cparams = self.cparams.clone();
        cparams.nchunk = nchunk;
        let chunk = compress::compress(data, &cparams)?;
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

        Ok(self.chunks.len() as i64)
    }

    pub(crate) fn insert_special_zero_chunk(
        &mut self,
        nchunk: i64,
        nbytes: usize,
    ) -> Result<i64, &'static str> {
        if self.vlblocks {
            return Err("Cannot mix regular and VL-block chunks");
        }
        if nchunk < 0 || nchunk as usize > self.chunks.len() {
            return Err("Chunk index out of range");
        }

        let chunk = synthetic_special_chunk_for_params(BLOSC2_SPECIAL_ZERO, nbytes, &self.cparams)
            .map_err(|_| "Invalid special zero chunk")?;
        self.chunks.insert(nchunk as usize, chunk);
        self.recompute_metadata()?;
        Ok(self.chunks.len() as i64)
    }

    /// Delete a chunk and return the resulting number of chunks, matching the C API.
    pub fn delete_chunk(&mut self, nchunk: i64) -> Result<i64, &'static str> {
        if nchunk < 0 || nchunk as usize >= self.chunks.len() {
            return Err("Chunk index out of range");
        }
        let idx = nchunk as usize;

        self.chunks.remove(idx);
        self.recompute_metadata()?;

        Ok(self.chunks.len() as i64)
    }

    /// Delete a chunk and return its decompressed data.
    pub fn delete_chunk_data(&mut self, nchunk: i64) -> Result<Vec<u8>, &'static str> {
        let data = self.decompress_chunk(nchunk)?;
        self.delete_chunk(nchunk)?;
        Ok(data)
    }

    /// Replace a chunk with newly compressed data.
    pub fn update_chunk(&mut self, nchunk: i64, data: &[u8]) -> Result<i64, &'static str> {
        if self.vlblocks {
            return Err("Cannot update a VL-block schunk with regular chunks");
        }
        if nchunk < 0 || nchunk as usize >= self.chunks.len() {
            return Err("Chunk index out of range");
        }

        let idx = nchunk as usize;
        let mut cparams = self.cparams.clone();
        cparams.nchunk = nchunk;
        let chunk = compress::compress(data, &cparams)?;
        self.chunks[idx] = chunk;
        self.recompute_metadata()?;
        Ok(self.chunks.len() as i64)
    }

    /// Deep-copy the super-chunk.
    pub fn copy_schunk(&self) -> Self {
        self.clone()
    }

    /// Add a named fixed-size metalayer.
    pub fn add_metalayer(&mut self, name: &str, content: &[u8]) -> Result<(), &'static str> {
        validate_metalayer_name(name)?;
        if self.metalayers.iter().any(|layer| layer.name == name) {
            return Err("Metalayer already exists");
        }
        validate_metalayers_encoded_size(
            self.metalayers
                .iter()
                .map(|layer| (layer.name.as_str(), layer.content.as_slice()))
                .chain(std::iter::once((name, content))),
        )?;

        self.metalayers.push(Metalayer {
            name: name.to_string(),
            content: content.to_vec(),
        });
        Ok(())
    }

    /// Replace an existing fixed-size metalayer payload.
    pub fn update_metalayer(&mut self, name: &str, content: &[u8]) -> Result<(), &'static str> {
        validate_metalayer_name(name)?;
        let pos = self
            .metalayers
            .iter()
            .position(|layer| layer.name == name)
            .ok_or("Metalayer does not exist")?;
        validate_metalayers_encoded_size(self.metalayers.iter().enumerate().map(
            |(idx, layer)| {
                if idx == pos {
                    (name, content)
                } else {
                    (layer.name.as_str(), layer.content.as_slice())
                }
            },
        ))?;
        self.metalayers[pos].content.clear();
        self.metalayers[pos].content.extend_from_slice(content);
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

    /// Add a named variable-length metalayer.
    pub fn add_vlmetalayer(&mut self, name: &str, content: &[u8]) -> Result<(), &'static str> {
        validate_vlmetalayer_name(name)?;
        if self.vlmetalayers.iter().any(|layer| layer.name == name) {
            return Err("VL-metalayer already exists");
        }
        let compressed = compress::compress(content, &CParams::default())?;
        let mut compressed_layers = Vec::with_capacity(self.vlmetalayers.len() + 1);
        for layer in &self.vlmetalayers {
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

        self.vlmetalayers.push(Metalayer {
            name: name.to_string(),
            content: content.to_vec(),
        });
        Ok(())
    }

    /// Replace an existing variable-length metalayer payload.
    pub fn update_vlmetalayer(&mut self, name: &str, content: &[u8]) -> Result<(), &'static str> {
        validate_vlmetalayer_name(name)?;
        let pos = self
            .vlmetalayers
            .iter()
            .position(|layer| layer.name == name)
            .ok_or("VL-metalayer does not exist")?;
        let compressed = compress::compress(content, &CParams::default())?;
        let mut compressed_layers = Vec::with_capacity(self.vlmetalayers.len());
        for (idx, layer) in self.vlmetalayers.iter().enumerate() {
            if idx == pos {
                compressed_layers.push((name, compressed.clone()));
            } else {
                compressed_layers.push((
                    layer.name.as_str(),
                    compress::compress(&layer.content, &CParams::default())?,
                ));
            }
        }
        validate_vlmetalayers_encoded_size(
            compressed_layers
                .iter()
                .map(|(layer_name, compressed)| (*layer_name, compressed.as_slice())),
        )?;
        self.vlmetalayers[pos].content.clear();
        self.vlmetalayers[pos].content.extend_from_slice(content);
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
        if self.dparams.nthreads > 1 && self.chunks.len() > 1 {
            let mut dparams = self.dparams.clone();
            dparams.nthreads = 1;
            let chunks: Vec<Vec<u8>> = compress::with_thread_pool(self.dparams.nthreads, || {
                self.chunks
                    .par_iter()
                    .map(|chunk| compress::decompress_with_dparams(chunk, &dparams))
                    .collect::<Result<Vec<_>, _>>()
            })?;
            let mut out = Vec::with_capacity(capacity);
            for chunk in chunks {
                out.extend(chunk);
            }
            return Ok(out);
        }
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
            let (chunk_nbytes, _, _) = compress::cbuffer_sizes(&self.chunks[idx])?;
            let chunk_end = chunk_start
                .checked_add(chunk_nbytes)
                .ok_or("Slice offset overflow")?;
            if chunk_end > start && chunk_start < end {
                let local_start = start.saturating_sub(chunk_start);
                let local_end = end.min(chunk_end) - chunk_start;
                let copy_len = local_end - local_start;
                let replacement = &data[replacement_pos..replacement_pos + copy_len];
                replacement_pos += copy_len;

                if let Some(chunk) = compress::replace_aligned_blocks(
                    &self.chunks[idx],
                    local_start,
                    replacement,
                    &self.cparams,
                )? {
                    replacements.push((idx, chunk));
                } else {
                    let mut chunk_data = self.decompress_chunk(idx as i64)?;
                    chunk_data[local_start..local_end].copy_from_slice(replacement);
                    replacements.push((idx, compress::compress(&chunk_data, &self.cparams)?));
                }
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
        let encode_special_offsets = false;
        let mut offset = 0u64;
        for chunk in &self.chunks {
            if encode_special_offsets {
                if let Some(special) = frame::special_offset_for_chunk(chunk) {
                    offsets.push(special);
                    continue;
                }
            }
            offsets.push(offset);
            offset =
                offset.saturating_add(
                    frame::stored_frame_chunk_len(chunk, encode_special_offsets) as u64
                );
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

    /// Recompute `nbytes`, `cbytes` and `chunksize` after the chunk list has
    /// been mutated.
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

    /// Refresh `chunksize`, `variable_chunks` and `vlblocks` to reflect the
    /// current chunk list.
    fn refresh_chunk_shape(&mut self) -> Result<(), &'static str> {
        if self.chunks.is_empty() {
            self.chunksize = 0;
            self.variable_chunks = false;
            self.vlblocks = false;
            return Ok(());
        }
        self.vlblocks = self
            .chunks
            .iter()
            .any(|chunk| ChunkHeader::read(chunk).is_ok_and(|header| header.vl_blocks()));
        if self.vlblocks {
            self.variable_chunks = true;
            self.chunksize = 0;
        } else {
            let chunksize = fixed_tail_chunksize(&self.chunks)?;
            self.variable_chunks = chunksize == 0 && !self.chunks.is_empty();
            self.chunksize = chunksize;
        }
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
        let file = std::fs::File::create(path)?;
        let mut writer = BufWriter::new(file);
        frame::write_frame_to_writer(self, &mut writer)?;
        writer.flush()
    }

    /// Write to a sparse frame directory.
    pub fn to_sframe_dir(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        frame::write_sframe_dir(self, path.as_ref())
    }

    /// Open a b2frame file or sparse frame directory.
    pub fn open(path: &str) -> Result<Self, String> {
        if Path::new(path).is_dir() {
            return frame::read_sframe_dir(Path::new(path));
        }
        let data = std::fs::read(path).map_err(|e| format!("Failed to read file: {e}"))?;
        let frame_len = frame::declared_frame_size(&data).unwrap_or(data.len());
        Self::from_frame(&data[..frame_len])
    }

    /// Open a sparse frame directory.
    pub fn open_sframe(path: impl AsRef<Path>) -> Result<Self, String> {
        frame::read_sframe_dir(path.as_ref())
    }

    /// Open a b2frame file or sparse frame directory lazily, keeping compressed chunks on disk until read.
    pub fn open_lazy(path: impl AsRef<Path>) -> Result<LazySchunk, String> {
        if path.as_ref().is_dir() {
            return frame::read_lazy_sframe_dir(path.as_ref());
        }
        frame::read_lazy_frame(path.as_ref())
    }

    /// Open a sparse frame directory lazily.
    pub fn open_lazy_sframe(path: impl AsRef<Path>) -> Result<LazySchunk, String> {
        frame::read_lazy_sframe_dir(path.as_ref())
    }
}

fn chunk_header_typesize(typesize: i32) -> u8 {
    if typesize > BLOSC_MAX_TYPESIZE as i32 {
        1
    } else {
        typesize as u8
    }
}

/// Path of one chunk file inside a sparse frame directory.
fn sframe_chunk_path(dir: &Path, chunk_id: u64) -> PathBuf {
    dir.join(format!("{chunk_id:08X}.chunk"))
}

/// Check that a metalayer name is non-empty and fits in the on-disk format.
fn validate_metalayer_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Metalayer name cannot be empty");
    }
    if name.len() > 31 {
        return Err("Metalayer name too large");
    }
    Ok(())
}

/// Check that a VL-metalayer name is non-empty and fits in the on-disk format.
fn validate_vlmetalayer_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("VL-metalayer name cannot be empty");
    }
    if name.len() > 31 {
        return Err("VL-metalayer name too large");
    }
    Ok(())
}

/// Reject VL-metalayer sets whose msgpack-encoded trailer would overflow the
/// signed 32-bit size fields used by the frame format.
fn validate_vlmetalayers_encoded_size<'a>(
    layers: impl Iterator<Item = (&'a str, &'a [u8])>,
) -> Result<(), &'static str> {
    let mut index_len = 3usize;
    let mut values_len = 3usize;
    let mut count = 0usize;
    for (name, compressed_content) in layers {
        validate_vlmetalayer_name(name)?;
        count += 1;
        if count > BLOSC2_MAX_METALAYERS {
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

/// Encoded length in bytes of a msgpack string carrying the given name.
fn encoded_str_len(name: &str) -> usize {
    if name.len() <= 31 {
        1 + name.len()
    } else if name.len() <= u8::MAX as usize {
        2 + name.len()
    } else {
        3 + name.len()
    }
}

/// Reject metalayer sets whose msgpack-encoded header would overflow the
/// signed 32-bit size fields used by the frame format.
fn validate_metalayers_encoded_size<'a>(
    layers: impl Iterator<Item = (&'a str, &'a [u8])>,
) -> Result<(), &'static str> {
    let mut index_len = 1usize + 1 + 2 + 3; // array3 + uint16 size + map16 count
    let mut values_len = 3usize; // array16 count
    let mut count = 0usize;
    for (name, content) in layers {
        validate_metalayer_name(name)?;
        count += 1;
        if count > BLOSC2_MAX_METALAYERS {
            return Err("Too many metalayers");
        }
        index_len = index_len
            .checked_add(encoded_str_len(name))
            .and_then(|len| len.checked_add(5))
            .ok_or("Metalayers too large")?;
        values_len = values_len
            .checked_add(5)
            .and_then(|len| len.checked_add(content.len()))
            .ok_or("Metalayers too large")?;
    }
    if index_len > u16::MAX as usize {
        return Err("Metalayers too large");
    }
    if index_len
        .checked_add(values_len)
        .and_then(|len| len.checked_add(frame::FRAME_HEADER_MIN_LEN))
        .is_none_or(|len| len > i32::MAX as usize)
    {
        return Err("Metalayers too large");
    }
    Ok(())
}

/// Compute `start + len` and verify the resulting byte range fits inside the
/// uncompressed super-chunk.
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

fn fixed_tail_chunksize(chunks: &[Vec<u8>]) -> Result<usize, &'static str> {
    let Some((first, rest)) = chunks.split_first() else {
        return Ok(0);
    };
    let (first_nbytes, _, _) = compress::cbuffer_sizes(first)?;
    for (idx, chunk) in rest.iter().enumerate() {
        let (chunk_nbytes, _, _) = compress::cbuffer_sizes(chunk)?;
        let is_last = idx + 1 == rest.len();
        if is_last {
            if chunk_nbytes > first_nbytes {
                return Ok(0);
            }
        } else if chunk_nbytes != first_nbytes {
            return Ok(0);
        }
    }
    Ok(first_nbytes)
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

    pub(super) fn declared_frame_size(data: &[u8]) -> Option<usize> {
        if data.len() < 24
            || data[0] != MSGPACK_FIXARRAY_14
            || data[1] != MSGPACK_STR8
            || &data[2..10] != FRAME_MAGIC
            || data[15] != MSGPACK_UINT64
        {
            return None;
        }
        let frame_size = u64::from_be_bytes(data[16..24].try_into().ok()?) as usize;
        (frame_size <= data.len()).then_some(frame_size)
    }

    /// Write a frame from a schunk.
    pub fn write_frame(schunk: &Schunk) -> Vec<u8> {
        let nbytes: i64 = schunk
            .chunks
            .iter()
            .filter_map(|chunk| ChunkHeader::read(chunk).ok())
            .map(|header| i64::from(header.nbytes))
            .sum();
        let chunksize = derive_frame_chunksize(schunk);
        let encode_special_offsets = false;
        let cbytes: i64 = schunk
            .chunks
            .iter()
            .map(|chunk| stored_frame_chunk_len(chunk, encode_special_offsets) as i64)
            .sum();

        // Build header first to know its size
        let header = build_header(schunk, nbytes, cbytes, chunksize);
        let header_size = header.len();

        // Build the offset index as a Blosc2 chunk with int64 offsets
        let offsets_data = build_offsets(schunk, header_size, encode_special_offsets);
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
            if stored_frame_chunk_len(chunk, encode_special_offsets) != 0 {
                frame.extend_from_slice(chunk);
            }
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

    /// Write a frame directly to a writer without materializing the whole frame.
    pub fn write_frame_to_writer<W: Write>(schunk: &Schunk, writer: &mut W) -> std::io::Result<()> {
        let nbytes: i64 = schunk
            .chunks
            .iter()
            .filter_map(|chunk| ChunkHeader::read(chunk).ok())
            .map(|header| i64::from(header.nbytes))
            .sum();
        let chunksize = derive_frame_chunksize(schunk);
        let encode_special_offsets = false;
        let cbytes: i64 = schunk
            .chunks
            .iter()
            .map(|chunk| stored_frame_chunk_len(chunk, encode_special_offsets) as i64)
            .sum();

        let mut header = build_header(schunk, nbytes, cbytes, chunksize);
        let offsets_data = build_offsets(schunk, header.len(), encode_special_offsets);
        let offsets_chunk = if offsets_data.is_empty() {
            Vec::new()
        } else {
            build_offsets_chunk(&offsets_data)
        };
        let trailer = build_trailer(schunk);

        let frame_size = header.len() + cbytes as usize + offsets_chunk.len() + trailer.len();
        header[16..24].copy_from_slice(&(frame_size as u64).to_be_bytes());

        writer.write_all(&header)?;
        for chunk in &schunk.chunks {
            if stored_frame_chunk_len(chunk, encode_special_offsets) != 0 {
                writer.write_all(chunk)?;
            }
        }
        writer.write_all(&offsets_chunk)?;
        writer.write_all(&trailer)?;
        Ok(())
    }

    /// Write a sparse frame directory with c-blosc2-compatible chunk files.
    pub fn write_sframe_dir(schunk: &Schunk, path: &Path) -> std::io::Result<()> {
        std::fs::create_dir(path)?;

        let nbytes: i64 = schunk
            .chunks
            .iter()
            .filter_map(|chunk| ChunkHeader::read(chunk).ok())
            .map(|header| i64::from(header.nbytes))
            .sum();
        let chunksize = derive_frame_chunksize(schunk);
        let encode_special_offsets = chunksize > 0;
        let cbytes: i64 = schunk
            .chunks
            .iter()
            .map(|chunk| stored_frame_chunk_len(chunk, encode_special_offsets) as i64)
            .sum();

        let mut header = build_header(schunk, nbytes, cbytes, chunksize);
        header[26] = 1;

        if schunk.chunks.len() > u32::MAX as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "too many sparse frame chunks",
            ));
        }
        let offsets_data = build_sframe_offsets(schunk, encode_special_offsets);
        let offsets_chunk = if offsets_data.is_empty() {
            Vec::new()
        } else {
            build_offsets_chunk(&offsets_data)
        };
        let trailer = build_trailer(schunk);

        let frame_size = header.len() + offsets_chunk.len() + trailer.len();
        header[16..24].copy_from_slice(&(frame_size as u64).to_be_bytes());

        let mut next_chunk_id = 0u64;
        for chunk in &schunk.chunks {
            if !encode_special_offsets || special_offset_for_chunk(chunk).is_none() {
                std::fs::write(sframe_chunk_path(path, next_chunk_id), chunk)?;
                next_chunk_id += 1;
            }
        }

        let mut index = Vec::with_capacity(frame_size);
        index.extend_from_slice(&header);
        index.extend_from_slice(&offsets_chunk);
        index.extend_from_slice(&trailer);
        std::fs::write(path.join("chunks.b2frame"), index)
    }

    /// Build the contiguous-frame offsets array: little-endian `u64` offsets
    /// for each chunk relative to the start of the data section (offset 0
    /// being the first chunk just after the header).
    fn build_offsets(
        schunk: &Schunk,
        _header_size: usize,
        encode_special_offsets: bool,
    ) -> Vec<u8> {
        let nchunks = schunk.chunks.len();
        if nchunks == 0 {
            return Vec::new();
        }

        let mut offsets = Vec::with_capacity(nchunks * 8);
        let mut coffset: u64 = 0;

        for chunk in &schunk.chunks {
            if encode_special_offsets {
                if let Some(special) = special_offset_for_chunk(chunk) {
                    offsets.extend_from_slice(&special.to_le_bytes());
                    continue;
                }
            }
            {
                offsets.extend_from_slice(&coffset.to_le_bytes());
                coffset += chunk.len() as u64;
            }
        }

        offsets
    }

    /// Build the sparse-frame offsets array: little-endian `u64` chunk file
    /// identifiers used to locate each chunk file inside the sparse frame
    /// directory.
    fn build_sframe_offsets(schunk: &Schunk, encode_special_offsets: bool) -> Vec<u8> {
        let nchunks = schunk.chunks.len();
        let mut offsets = Vec::with_capacity(nchunks * 8);
        let mut next_chunk_id = 0u64;
        for chunk in &schunk.chunks {
            let offset = if encode_special_offsets {
                if let Some(special) = special_offset_for_chunk(chunk) {
                    special
                } else {
                    let chunk_id = next_chunk_id;
                    next_chunk_id += 1;
                    chunk_id
                }
            } else {
                let chunk_id = next_chunk_id;
                next_chunk_id += 1;
                chunk_id
            };
            offsets.extend_from_slice(&offset.to_le_bytes());
        }
        offsets
    }

    pub(super) fn stored_frame_chunk_len(chunk: &[u8], encode_special_offsets: bool) -> usize {
        if encode_special_offsets && special_offset_for_chunk(chunk).is_some() {
            0
        } else {
            chunk.len()
        }
    }

    pub(super) fn special_offset_for_chunk(chunk: &[u8]) -> Option<u64> {
        let header = ChunkHeader::read(chunk).ok()?;
        match header.special_type() {
            BLOSC2_SPECIAL_ZERO | BLOSC2_SPECIAL_NAN | BLOSC2_SPECIAL_UNINIT => {
                Some(encoded_special_offset(header.special_type()))
            }
            _ => None,
        }
    }

    pub(super) fn encoded_special_offset(special: u8) -> u64 {
        (1u64 << 63) | ((special as u64) << 56)
    }

    pub(super) fn special_type_from_offset(offset: u64) -> Option<u8> {
        if offset & (1u64 << 63) == 0 {
            return None;
        }
        let special = ((offset >> 56) as u8) & BLOSC2_SPECIAL_MASK;
        if matches!(
            special,
            BLOSC2_SPECIAL_ZERO | BLOSC2_SPECIAL_NAN | BLOSC2_SPECIAL_UNINIT
        ) {
            Some(special)
        } else {
            None
        }
    }

    fn special_chunk_from_offset(
        offset: u64,
        logical_idx: usize,
        nchunks: usize,
        nbytes: i64,
        chunksize: usize,
        blocksize: i32,
        spec: &FrameChunkSpec<'_>,
    ) -> Result<Vec<u8>, String> {
        let special_type = special_type_from_offset(offset)
            .ok_or_else(|| "Invalid frame: invalid special chunk offset".to_string())?;
        let chunk_nbytes = special_chunk_nbytes(logical_idx, nchunks, nbytes, chunksize)?;
        synthetic_special_chunk_with_spec(special_type, chunk_nbytes, blocksize, spec)
    }

    fn special_chunk_nbytes(
        logical_idx: usize,
        nchunks: usize,
        nbytes: i64,
        chunksize: usize,
    ) -> Result<usize, String> {
        if nbytes < 0 {
            return Err("Invalid frame: negative nbytes".into());
        }
        if chunksize > 0 {
            if logical_idx + 1 == nchunks {
                (nbytes as usize)
                    .checked_sub(chunksize.saturating_mul(nchunks.saturating_sub(1)))
                    .ok_or_else(|| "Invalid frame: special chunk nbytes underflow".to_string())
            } else {
                Ok(chunksize)
            }
        } else if nchunks == 0 {
            Ok(0)
        } else {
            Err("Invalid frame: special chunk offset requires fixed chunksize".into())
        }
    }

    fn synthetic_special_chunk_with_spec(
        special_type: u8,
        nbytes: usize,
        blocksize: i32,
        spec: &FrameChunkSpec<'_>,
    ) -> Result<Vec<u8>, String> {
        if nbytes > i32::MAX as usize {
            return Err("Invalid frame: special chunk is too large".to_string());
        }
        let mut cparams = CParams {
            compcode: spec.compcode,
            typesize: spec.typesize,
            blocksize,
            ..Default::default()
        };
        cparams = compress::normalized_cparams(&cparams);
        let typesize = cparams.typesize as usize;
        if nbytes != 0 && !nbytes.is_multiple_of(typesize) {
            return Err("Invalid frame: special chunk size is not a multiple of typesize".into());
        }
        let blocksize = compress::compute_blocksize(&cparams, nbytes as i32);
        let mut chunk = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        let header = ChunkHeader {
            version: BLOSC2_VERSION_FORMAT_STABLE,
            versionlz: BLOSC_BLOSCLZ_VERSION_FORMAT,
            flags: BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE,
            typesize: chunk_header_typesize(cparams.typesize),
            nbytes: nbytes as i32,
            blocksize,
            cbytes: BLOSC_EXTENDED_HEADER_LENGTH as i32,
            blosc2_flags: special_type << 4,
            ..Default::default()
        };
        header
            .try_write(&mut chunk)
            .map_err(|_| "Invalid frame: cannot build special chunk".to_string())?;
        Ok(chunk)
    }

    /// Determine the `chunksize` value to record in the frame header.
    /// Returns the fixed chunk size, allowing only the final chunk to be
    /// shorter, or `0` when chunks are variable-sized.
    pub(super) fn derive_frame_chunksize(schunk: &Schunk) -> i32 {
        if schunk.chunks.is_empty() {
            return -1;
        }
        fixed_tail_chunksize(&schunk.chunks)
            .ok()
            .and_then(|chunksize| i32::try_from(chunksize).ok())
            .unwrap_or(0)
    }

    fn expected_nchunks_from_frame(nbytes: i64, chunksize: usize) -> Result<Option<usize>, String> {
        if chunksize == 0 {
            return Ok(None);
        }
        let nbytes = usize::try_from(nbytes)
            .map_err(|_| "Invalid frame: invalid uncompressed size".to_string())?;
        Ok(Some(nbytes.div_ceil(chunksize)))
    }

    fn validate_frame_offsets_count(
        offsets_len: usize,
        nbytes: i64,
        chunksize: usize,
    ) -> Result<(), String> {
        if let Some(expected) = expected_nchunks_from_frame(nbytes, chunksize)? {
            if offsets_len != expected {
                return Err("Invalid frame: offsets count does not match fixed chunksize".into());
            }
        }
        Ok(())
    }

    fn validate_frame_data_intervals(
        intervals: &mut [(usize, usize)],
        cbytes: usize,
    ) -> Result<(), String> {
        intervals.sort_unstable_by_key(|&(start, end)| (start, end));
        let mut expected_start = 0usize;
        for &(start, end) in intervals.iter() {
            if start != expected_start || end < start {
                return Err("Invalid frame: chunk offsets do not partition data section".into());
            }
            expected_start = end;
        }
        if expected_start != cbytes {
            return Err("Invalid frame: chunk offsets do not cover data section".into());
        }
        Ok(())
    }

    /// Wrap the offsets payload in a Blosc2 chunk using C-Blosc2's default
    /// one-shot frame parameters for the offset index.
    fn build_offsets_chunk(data: &[u8]) -> Vec<u8> {
        let cparams = CParams {
            compcode: BLOSC_BLOSCLZ,
            clevel: 5,
            typesize: 8,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        compress::compress(data, &cparams).expect("offset index compression uses valid parameters")
    }

    // Frame format constants matching C code (frame.h)
    const FRAME_UDCODEC: usize = 77;
    const FRAME_CODEC_META: usize = 78;
    const FRAME_OTHER_FLAGS2: usize = 85;

    /// Build the msgpack frame header. Layout matches the C reference
    /// implementation byte for byte: 14-entry fixarray of `b2frame\0` magic,
    /// sizes, codec flags, filter pipeline, and any metalayers.
    fn build_header(schunk: &Schunk, nbytes: i64, cbytes: i64, chunksize: i32) -> Vec<u8> {
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
            | if (schunk.chunksize == 0 && chunksize == 0 && !schunk.chunks.is_empty())
                || schunk.vlblocks
            {
                BLOSC2_VERSION_FRAME_FORMAT
            } else {
                BLOSC2_VERSION_FRAME_FORMAT_RC1
            };
        if chunksize == 0 {
            h[pos] |= FRAME_VARIABLE_CHUNKS;
        }
        if schunk.vlblocks {
            h[pos] |= FRAME_VL_BLOCKS;
        }
        pos += 1;

        // [26] frame_type: 0 = contiguous
        h[pos] = 0;
        pos += 1;

        // [27] codec_flags: codec in bits 0-3, clevel in bits 4-7
        let codec_frame_id =
            if compcode_to_compformat(schunk.cparams.compcode) == BLOSC_UDCODEC_FORMAT {
                BLOSC_UDCODEC_FORMAT
            } else {
                schunk.cparams.compcode & 0x0F
            };
        h[pos] = codec_frame_id | ((schunk.cparams.clevel & 0x0F) << 4);
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

        // [52-56] int32: blocksize
        h[pos] = MSGPACK_INT32;
        pos += 1;
        h[pos..pos + 4].copy_from_slice(&schunk.cparams.blocksize.to_be_bytes());
        pos += 4;

        // [57-61] int32: chunksize
        h[pos] = MSGPACK_INT32;
        pos += 1;
        h[pos..pos + 4].copy_from_slice(&chunksize.to_be_bytes());
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
        h[FRAME_CODEC_META] = schunk.cparams.compcode_meta;

        // [85] other_flags2: bit 0 = use_dict
        h[FRAME_OTHER_FLAGS2] = if schunk.cparams.use_dict { 1 } else { 0 };

        assert_eq!(h.len(), FRAME_HEADER_MIN_LEN);

        h.extend_from_slice(&encode_metalayers(&schunk.metalayers));

        // Update header_size
        let header_size = h.len() as i32;
        h[header_size_pos..header_size_pos + 4].copy_from_slice(&header_size.to_be_bytes());

        h
    }

    /// Encode the metalayers section of the frame header: an index of
    /// `(name, offset)` pairs followed by an array of msgpack `bin32` payloads.
    fn encode_metalayers(metalayers: &[Metalayer]) -> Vec<u8> {
        let mut section = vec![0x93, MSGPACK_UINT16, 0, 0, MSGPACK_MAP16];
        section.extend_from_slice(&(metalayers.len() as u16).to_be_bytes());

        let mut offset_positions = Vec::with_capacity(metalayers.len());
        for layer in metalayers {
            encode_msgpack_str(&mut section, &layer.name);
            section.push(MSGPACK_INT32);
            offset_positions.push(section.len());
            section.extend_from_slice(&0i32.to_be_bytes());
        }

        let index_size = u16::try_from(section.len())
            .expect("metalayer index size is validated before insertion");
        section[2..4].copy_from_slice(&index_size.to_be_bytes());

        section.push(MSGPACK_ARRAY16);
        section.extend_from_slice(&(metalayers.len() as u16).to_be_bytes());
        for (layer, offset_pos) in metalayers.iter().zip(offset_positions) {
            let offset = i32::try_from(FRAME_HEADER_MIN_LEN + section.len())
                .expect("metalayer offset fits i32");
            section[offset_pos..offset_pos + 4].copy_from_slice(&offset.to_be_bytes());
            section.push(MSGPACK_BIN32);
            section.extend_from_slice(&(layer.content.len() as u32).to_be_bytes());
            section.extend_from_slice(&layer.content);
        }

        section
    }

    /// Append a msgpack string (`fixstr`, `str8` or `str16`) to `out`.
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

    /// Build the frame trailer: the VL-metalayers index and payloads followed
    /// by the trailer length and 16-byte fingerprint placeholder.
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

    /// Append a VL-metalayer name as a msgpack `fixstr` (names are limited to
    /// 31 bytes).
    fn encode_vlmeta_name(out: &mut Vec<u8>, name: &str) {
        let bytes = name.as_bytes();
        debug_assert!(bytes.len() <= 31);
        out.push(0xA0 | bytes.len() as u8);
        out.extend_from_slice(bytes);
    }

    /// Cached frame-level parameters that every embedded chunk header must
    /// agree with during frame validation.
    struct FrameChunkSpec<'a> {
        compcode: u8,
        compcode_meta: u8,
        typesize: i32,
        nfilters: usize,
        filters: &'a [u8; BLOSC2_MAX_FILTERS],
        filters_meta: &'a [u8; BLOSC2_MAX_FILTERS],
        vlblocks: bool,
    }

    struct FrameMetadata {
        cparams: CParams,
        dparams: DParams,
        chunksize: usize,
        nbytes: i64,
        cbytes: i64,
        metalayers: Vec<Metalayer>,
        vlmetalayers: Vec<Metalayer>,
        vlblocks: bool,
        nfilters: usize,
    }

    fn decode_frame_splitmode(other_flags: u8) -> i32 {
        match (other_flags & 0x03) + 1 {
            1 => BLOSC_ALWAYS_SPLIT,
            2 => BLOSC_NEVER_SPLIT,
            3 => BLOSC_AUTO_SPLIT,
            _ => BLOSC_FORWARD_COMPAT_SPLIT,
        }
    }

    /// Check that an embedded chunk header is well-formed and matches the
    /// codec, typesize and filter pipeline advertised by the frame header.
    fn validate_embedded_chunk_header(
        ch: &ChunkHeader,
        spec: &FrameChunkSpec<'_>,
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
        if ch.memcpyed() && !ch.use_dict() {
            let expected = ch
                .header_len()
                .checked_add(ch.nbytes as usize)
                .ok_or_else(|| "Invalid frame: invalid memcpyed chunk size".to_string())?;
            if ch.cbytes as usize != expected {
                return Err("Invalid frame: invalid memcpyed chunk size".into());
            }
        }
        if ch.nbytes > 0 {
            if ch.typesize == 0 || ch.typesize as usize > BLOSC_MAX_TYPESIZE {
                return Err("Invalid frame: invalid chunk typesize".into());
            }
            if ch.blocksize <= 0 || ch.blocksize as usize > BLOSC2_MAXBLOCKSIZE {
                return Err("Invalid frame: invalid chunk blocksize".into());
            }
        }
        if ch.special_type() == BLOSC2_NO_SPECIAL {
            if ch.blosc2_flags & (BLOSC2_INSTR_CODEC | BLOSC2_LAZY_CHUNK) != 0 {
                return Err("Invalid frame: unsupported chunk flags".into());
            }
            if ch.vl_blocks() != spec.vlblocks {
                return Err("Invalid frame: chunk VL-block flag does not match frame".into());
            }
        }
        match ch.special_type() {
            BLOSC2_SPECIAL_VALUE => {
                if ch.use_dict() {
                    return Ok(());
                }
                let value_size = (ch.cbytes as usize)
                    .checked_sub(ch.header_len())
                    .ok_or_else(|| "Invalid frame: invalid special value size".to_string())?;
                if value_size == 0
                    || value_size > BLOSC2_MAXTYPESIZE
                    || value_size > ch.nbytes as usize
                {
                    return Err("Invalid frame: invalid special value size".into());
                }
                if !(ch.nbytes as usize).is_multiple_of(value_size) {
                    return Err("Invalid frame: invalid special value nbytes".into());
                }
            }
            BLOSC2_SPECIAL_NAN | BLOSC2_SPECIAL_ZERO | BLOSC2_SPECIAL_UNINIT => {
                if !ch.use_dict() && ch.cbytes as usize != ch.header_len() {
                    return Err("Invalid frame: invalid special chunk size".into());
                }
                if ch.nbytes > 0 && !(ch.nbytes as usize).is_multiple_of(ch.typesize as usize) {
                    return Err("Invalid frame: invalid special value nbytes".into());
                }
            }
            BLOSC2_NO_SPECIAL => {}
            _ => return Err("Invalid frame: unknown special value type".into()),
        }
        if ch.special_type() == BLOSC2_NO_SPECIAL && !ch.memcpyed() {
            if !matches!(
                ch.compformat(),
                BLOSC_BLOSCLZ_FORMAT
                    | BLOSC_LZ4_FORMAT
                    | BLOSC_ZLIB_FORMAT
                    | BLOSC_ZSTD_FORMAT
                    | BLOSC_UDCODEC_FORMAT
            ) {
                return Err("Invalid frame: unsupported chunk codec format".into());
            }
            if ch.compformat() == BLOSC_UDCODEC_FORMAT
                && !codecs::is_registered_codec(ch.udcompcode)
            {
                return Err("Invalid frame: unsupported chunk codec".into());
            }
            if ch.use_dict() && !codecs::codec_supports_dict(ch.compcode()) {
                return Err(
                    "Invalid frame: dictionary compression is only supported for Zstd, LZ4, and LZ4HC"
                        .into(),
                );
            }
            if !ch.vl_blocks() {
                let nblocks = (ch.nbytes as usize).div_ceil(ch.blocksize as usize);
                let min_block_table_len = nblocks
                    .checked_mul(4)
                    .and_then(|len| ch.header_len().checked_add(len))
                    .ok_or_else(|| "Invalid frame: invalid block table size".to_string())?;
                if (ch.cbytes as usize) < min_block_table_len {
                    return Err("Invalid frame: chunk too small for block table".into());
                }
            }
        }
        if ch.special_type() == BLOSC2_NO_SPECIAL
            && !ch.memcpyed()
            && !matches!(
                ch.compcode(),
                BLOSC_BLOSCLZ | BLOSC_LZ4 | BLOSC_LZ4HC | BLOSC_ZLIB | BLOSC_ZSTD
            )
            && !crate::codecs::is_registered_codec(ch.compcode())
        {
            return Err("Invalid frame: unsupported chunk codec".into());
        }
        if !ch.memcpyed() && ch.special_type() == BLOSC2_NO_SPECIAL {
            let codec_matches = ch.compcode() == spec.compcode
                || (spec.compcode == BLOSC_LZ4HC && ch.compcode() == BLOSC_LZ4);
            if !codec_matches {
                return Err("Invalid frame: chunk codec does not match frame".into());
            }
            if ch.compcode_meta != spec.compcode_meta {
                return Err("Invalid frame: chunk codec metadata does not match frame".into());
            }
        }
        let expected_chunk_typesize = if spec.typesize > BLOSC_MAX_TYPESIZE as i32 {
            1
        } else {
            spec.typesize
        };
        if ch.nbytes > 0 && ch.typesize as i32 != expected_chunk_typesize {
            return Err("Invalid frame: chunk typesize does not match frame".into());
        }
        if ch.special_type() == BLOSC2_NO_SPECIAL && !ch.memcpyed() {
            if ch.filters[..spec.nfilters] != spec.filters[..spec.nfilters]
                || ch.filters_meta[..spec.nfilters] != spec.filters_meta[..spec.nfilters]
            {
                return Err("Invalid frame: chunk filters do not match frame".into());
            }
            for &filter in &ch.filters[..spec.nfilters] {
                if !matches!(
                    filter,
                    BLOSC_NOFILTER
                        | BLOSC_SHUFFLE
                        | BLOSC_BITSHUFFLE
                        | BLOSC_DELTA
                        | BLOSC_TRUNC_PREC
                ) && !crate::filters::is_registered_filter(filter)
                {
                    return Err("Invalid frame: unsupported chunk filter".into());
                }
            }
        }

        Ok(())
    }

    fn read_frame_filters(
        data: &[u8],
        filter_start: usize,
        meta_start: usize,
        nfilters: usize,
    ) -> ([u8; BLOSC2_MAX_FILTERS], [u8; BLOSC2_MAX_FILTERS]) {
        let mut filters = [0u8; BLOSC2_MAX_FILTERS];
        let mut filters_meta = [0u8; BLOSC2_MAX_FILTERS];
        filters[..nfilters].copy_from_slice(&data[filter_start..filter_start + nfilters]);
        filters_meta[..nfilters].copy_from_slice(&data[meta_start..meta_start + nfilters]);
        (filters, filters_meta)
    }

    /// Decode the metalayers section that may follow the 87-byte minimum
    /// frame header.
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
            return Err("Invalid frame: expected metalayers index size".into());
        }
        pos += 1;
        if pos + 2 > header.len() {
            return Err("Invalid frame: truncated metalayers index size".into());
        }
        let index_size = u16::from_be_bytes(header[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if index_size < 7 {
            return Err("Invalid frame: invalid metalayers index size".into());
        }
        let index_end = FRAME_HEADER_MIN_LEN
            .checked_add(index_size)
            .ok_or_else(|| "Invalid frame: metalayers index size overflow".to_string())?;
        if index_end > header.len() {
            return Err("Invalid frame: truncated metalayers index".into());
        }

        if header.get(pos) != Some(&MSGPACK_MAP16) {
            return Err("Invalid frame: expected metalayers index map".into());
        }
        pos += 1;
        if pos + 2 > index_end {
            return Err("Invalid frame: truncated metalayers index map".into());
        }
        let count = u16::from_be_bytes(header[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        let mut index = Vec::with_capacity(count);
        for _ in 0..count {
            let name = decode_msgpack_str(header, &mut pos, index_end)?;
            validate_metalayer_name(&name).map_err(|err| format!("Invalid frame: {err}"))?;
            if header.get(pos) != Some(&MSGPACK_INT32) {
                return Err("Invalid frame: expected metalayer content offset".into());
            }
            pos += 1;
            if pos + 4 > index_end {
                return Err("Invalid frame: truncated metalayer content offset".into());
            }
            let offset = i32::from_be_bytes(header[pos..pos + 4].try_into().unwrap());
            pos += 4;
            if offset < 0 {
                return Err("Invalid frame: invalid metalayer content offset".into());
            }
            let offset = usize::try_from(offset)
                .map_err(|_| "Invalid frame: invalid metalayer content offset".to_string())?;
            if offset >= header.len() {
                return Err("Invalid frame: metalayer content offset out of range".into());
            }
            index.push((name, offset));
        }

        if pos != index_end {
            return Err("Invalid frame: trailing bytes in metalayers index".into());
        }

        if header.get(pos) != Some(&MSGPACK_ARRAY16) {
            return Err("Invalid frame: expected metalayers value array".into());
        }
        pos += 1;
        if pos + 2 > header.len() {
            return Err("Invalid frame: truncated metalayers value array".into());
        }
        let value_count = u16::from_be_bytes(header[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if value_count != count {
            return Err("Invalid frame: metalayer index/value count mismatch".into());
        }

        let values_start = pos;
        let mut metalayers = Vec::with_capacity(count);
        let mut values_end = pos;
        for (name, offset) in index {
            if offset < values_start {
                return Err("Invalid frame: metalayer content offset before values".into());
            }
            let mut value_pos = offset;
            let content = decode_msgpack_bin(header, &mut value_pos, header.len())?;
            values_end = values_end.max(value_pos);
            metalayers.push(Metalayer { name, content });
        }

        if values_end != header.len() {
            return Err("Invalid frame: unsupported header extension after metalayers".into());
        }

        Ok(metalayers)
    }

    /// Decode the msgpack `fixstr` name form used by C frame metalayers.
    fn decode_msgpack_str(data: &[u8], pos: &mut usize, limit: usize) -> Result<String, String> {
        let marker = *data
            .get(*pos)
            .ok_or_else(|| "Invalid frame: truncated metalayer name".to_string())?;
        *pos += 1;

        if marker & 0xE0 != 0xA0 {
            return Err("Invalid frame: expected metalayer name string".into());
        }
        let len = (marker & 0x1F) as usize;

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

    /// Decode the msgpack `bin32` payload form used by C frame metalayers.
    fn decode_msgpack_bin(data: &[u8], pos: &mut usize, limit: usize) -> Result<Vec<u8>, String> {
        let marker = *data
            .get(*pos)
            .ok_or_else(|| "Invalid frame: truncated metalayer content".to_string())?;
        *pos += 1;

        if marker != MSGPACK_BIN32 {
            return Err("Invalid frame: expected metalayer content bin".into());
        }
        if *pos + 4 > limit {
            return Err("Invalid frame: truncated metalayer content length".into());
        }
        let len = u32::from_be_bytes(data[*pos..*pos + 4].try_into().unwrap()) as usize;
        *pos += 4;

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

    /// Total in-frame size of the offsets chunk that starts at `pos`, or `0`
    /// if no offsets chunk is present.
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

    /// Decode the VL-metalayers stored in the frame trailer, decompressing
    /// each payload to recover the original metalayer bytes.
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
        pos += 2;
        if value_count != count {
            return Err("Invalid frame: VL-metalayer index/value count mismatch".into());
        }
        let values_start = pos;
        let mut value_offsets = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            value_offsets.push(pos);
            let _ = decode_msgpack_bin(trailer, &mut pos, trailer.len())?;
        }
        let values_end = pos;
        if values_end
            .checked_add(23)
            .is_none_or(|footer_end| footer_end != trailer.len())
        {
            return Err("Invalid frame: invalid trailer footer length".into());
        }
        if trailer.get(values_end) != Some(&MSGPACK_UINT32) {
            return Err("Invalid frame: expected trailer length".into());
        }
        let declared_len =
            u32::from_be_bytes(trailer[values_end + 1..values_end + 5].try_into().unwrap())
                as usize;
        if declared_len != trailer.len() {
            return Err("Invalid frame: trailer length mismatch".into());
        }
        if trailer.get(values_end + 5) != Some(&MSGPACK_FIXEXT16) {
            return Err("Invalid frame: expected trailer fingerprint".into());
        }
        let _fingerprint_type = trailer[values_end + 6];
        let _fingerprint = &trailer[values_end + 7..values_end + 23];

        let mut metalayers = Vec::with_capacity(count);
        for (name, offset) in entries {
            if offset < values_start || offset >= values_end || !value_offsets.contains(&offset) {
                return Err("Invalid frame: invalid VL-metalayer offset".into());
            }
            let mut value_pos = offset;
            let compressed = decode_msgpack_bin(trailer, &mut value_pos, values_end)?;
            let content = compress::decompress(&compressed)
                .map_err(|_| "Invalid frame: invalid VL-metalayer payload".to_string())?;
            metalayers.push(Metalayer { name, content });
        }

        if has_vlmetalayers == metalayers.is_empty() {
            return Err("Invalid frame: VL-metalayer flag mismatch".into());
        }

        Ok(metalayers)
    }

    /// Seek to `offset` and fill `buf` from the file. `context` is used as a
    /// prefix in error messages.
    fn read_exact_at(
        file: &mut std::fs::File,
        offset: u64,
        buf: &mut [u8],
        context: &str,
    ) -> Result<(), String> {
        use std::io::{Read, Seek, SeekFrom};

        file.seek(SeekFrom::Start(offset))
            .map_err(|e| format!("{context}: seek failed: {e}"))?;
        file.read_exact(buf)
            .map_err(|e| format!("{context}: read failed: {e}"))
    }

    /// Read either a 16-byte or 32-byte chunk header from a frame file
    /// starting at `pos`, depending on whether the chunk uses the extended
    /// header format.
    fn read_chunk_header_at(
        file: &mut std::fs::File,
        pos: u64,
        data_end: u64,
    ) -> Result<ChunkHeader, String> {
        if pos
            .checked_add(BLOSC_MIN_HEADER_LENGTH as u64)
            .is_none_or(|end| end > data_end)
        {
            return Err("Invalid frame: data section ends inside chunk header".into());
        }

        let mut min_header = [0u8; BLOSC_MIN_HEADER_LENGTH];
        read_exact_at(file, pos, &mut min_header, "Failed to read chunk header")?;
        let extended = (min_header[BLOSC2_CHUNK_FLAGS] & BLOSC_DOSHUFFLE != 0)
            && (min_header[BLOSC2_CHUNK_FLAGS] & BLOSC_DOBITSHUFFLE != 0);
        if !extended {
            return ChunkHeader::read(&min_header)
                .map_err(|_| "Invalid frame: invalid chunk header".to_string());
        }

        if pos
            .checked_add(BLOSC_EXTENDED_HEADER_LENGTH as u64)
            .is_none_or(|end| end > data_end)
        {
            return Err("Invalid frame: data section ends inside extended chunk header".into());
        }
        let mut extended_header = [0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        read_exact_at(
            file,
            pos,
            &mut extended_header,
            "Failed to read extended chunk header",
        )?;
        ChunkHeader::read(&extended_header)
            .map_err(|_| "Invalid frame: invalid chunk header".to_string())
    }

    /// File-backed counterpart of [`offsets_chunk_len`] used by the lazy
    /// frame reader.
    fn offsets_chunk_len_from_file(
        file: &mut std::fs::File,
        pos: usize,
        frame_size: usize,
    ) -> Result<usize, String> {
        if pos >= frame_size {
            return Ok(0);
        }
        let header = read_chunk_header_at(file, pos as u64, frame_size as u64)?;
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

    fn offsets_payload_from_file(
        file: &mut std::fs::File,
        pos: usize,
        len: usize,
    ) -> Result<Vec<u64>, String> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let mut offsets_chunk = vec![0u8; len];
        read_exact_at(
            file,
            pos as u64,
            &mut offsets_chunk,
            "Failed to read offsets chunk",
        )?;
        let offsets_payload = compress::decompress(&offsets_chunk)
            .map_err(|_| "Invalid frame: invalid offsets chunk".to_string())?;
        if offsets_payload.len() % 8 != 0 {
            return Err("Invalid frame: offsets payload has invalid length".into());
        }
        Ok(offsets_payload
            .chunks_exact(8)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
            .collect())
    }

    /// Load the `chunks.b2frame` index of a sparse frame directory and
    /// return its raw bytes, header size, decoded chunk-file IDs, and the
    /// byte position where the offsets chunk ends.
    fn read_sframe_index(path: &Path) -> Result<(Vec<u8>, usize, Vec<u64>, usize), String> {
        let index_path = path.join("chunks.b2frame");
        let index =
            std::fs::read(&index_path).map_err(|e| format!("Failed to read sframe index: {e}"))?;
        if index.len() < FRAME_HEADER_MIN_LEN {
            return Err("Sparse frame index too small".into());
        }
        if index[0] != MSGPACK_FIXARRAY_14 {
            return Err(format!("Invalid frame marker: 0x{:02X}", index[0]));
        }
        if index[1] != MSGPACK_STR8 || &index[2..10] != FRAME_MAGIC {
            return Err("Invalid frame magic".into());
        }
        if index[10] != MSGPACK_INT32 {
            return Err("Expected int32 for header_size".into());
        }
        let header_size_i32 = i32::from_be_bytes(index[11..15].try_into().unwrap());
        if header_size_i32 < FRAME_HEADER_MIN_LEN as i32 {
            return Err("Invalid frame header size".into());
        }
        let header_size = header_size_i32 as usize;
        if header_size > index.len() {
            return Err("Sparse frame index truncated before offsets".into());
        }
        if index[15] != MSGPACK_UINT64 {
            return Err("Expected uint64 for frame_size".into());
        }
        let frame_size = u64::from_be_bytes(index[16..24].try_into().unwrap());
        if frame_size < header_size as u64 || frame_size > index.len() as u64 {
            return Err("Invalid sparse frame index size".into());
        }
        if index[24] != MSGPACK_STR4 {
            return Err("Expected fixstr(4) for flags".into());
        }
        if index[26] != 1 {
            return Err("Invalid frame: expected sparse directory frame type".into());
        }

        let frame_size = frame_size as usize;
        if index[38] != MSGPACK_INT64 {
            return Err("Expected int64 for cbytes".into());
        }
        let cbytes = i64::from_be_bytes(index[39..47].try_into().unwrap());
        if cbytes < 0 {
            return Err("Invalid frame: negative cbytes".into());
        }
        if cbytes == 0 && index[30..38] == 0i64.to_be_bytes() {
            return Ok((
                index[..frame_size].to_vec(),
                header_size,
                Vec::new(),
                header_size,
            ));
        }

        let offsets_len = offsets_chunk_len(&index, header_size, frame_size)?;
        let offsets_end = header_size
            .checked_add(offsets_len)
            .ok_or_else(|| "Invalid frame: offsets chunk overflow".to_string())?;
        if offsets_end > frame_size {
            return Err("Invalid frame: sparse offsets extend past index".into());
        }
        let offsets_payload = if offsets_len == 0 {
            Vec::new()
        } else {
            compress::decompress(&index[header_size..offsets_end])
                .map_err(|_| "Invalid frame: invalid sparse offsets chunk".to_string())?
        };
        if offsets_payload.len() % 8 != 0 {
            return Err("Invalid frame: sparse offsets payload has invalid length".into());
        }
        let mut offsets = Vec::with_capacity(offsets_payload.len() / 8);
        for bytes in offsets_payload.chunks_exact(8) {
            let offset = u64::from_le_bytes(bytes.try_into().unwrap());
            if offset & (1u64 << 63) != 0 && special_type_from_offset(offset).is_none() {
                return Err("Invalid frame: invalid special chunk offset".into());
            }
            if offset & (1u64 << 63) == 0 && offset > u32::MAX as u64 {
                return Err("Invalid frame: sparse chunk id is too large".into());
            }
            offsets.push(offset);
        }
        let nbytes = i64::from_be_bytes(index[30..38].try_into().unwrap());
        let chunksize = i32::from_be_bytes(index[58..62].try_into().unwrap());
        if chunksize < 0 {
            return Err("Invalid frame: negative chunksize".into());
        }
        validate_frame_offsets_count(offsets.len(), nbytes, chunksize as usize)?;

        Ok((
            index[..frame_size].to_vec(),
            header_size,
            offsets,
            offsets_end,
        ))
    }

    fn parse_sframe_index_metadata(
        index: &[u8],
        header_size: usize,
        offsets_end: usize,
    ) -> Result<FrameMetadata, String> {
        if index.len() < header_size || offsets_end > index.len() {
            return Err("Invalid frame: sparse index truncated".into());
        }
        let metalayers = parse_metalayers(&index[..header_size])?;
        if index[24] != MSGPACK_STR4 {
            return Err("Expected fixstr(4) for flags".into());
        }
        let general_flags = index[25];
        let frame_version = general_flags & 0x0F;
        if frame_version > BLOSC2_VERSION_FRAME_FORMAT {
            return Err("Invalid frame: unsupported frame version".into());
        }
        let vlblocks = general_flags & FRAME_VL_BLOCKS != 0;
        let variable_chunks = general_flags & FRAME_VARIABLE_CHUNKS != 0;
        if index[26] != 1 {
            return Err("Invalid frame: expected sparse directory frame type".into());
        }

        let codec_flags = index[27];
        let frame_compcode = codec_flags & 0x0F;
        let compcode = if frame_compcode == BLOSC_UDCODEC_FORMAT {
            index[FRAME_UDCODEC]
        } else {
            frame_compcode
        };
        let compcode_meta = index[FRAME_CODEC_META];
        let clevel = (codec_flags >> 4) & 0x0F;
        let splitmode = decode_frame_splitmode(index[28]);

        if index[29] != MSGPACK_INT64 {
            return Err("Expected int64 for nbytes".into());
        }
        let nbytes = i64::from_be_bytes(index[30..38].try_into().unwrap());
        if nbytes < 0 {
            return Err("Invalid frame: negative nbytes".into());
        }
        if index[38] != MSGPACK_INT64 {
            return Err("Expected int64 for cbytes".into());
        }
        let cbytes = i64::from_be_bytes(index[39..47].try_into().unwrap());
        if cbytes < 0 {
            return Err("Invalid frame: negative cbytes".into());
        }
        if index[47] != MSGPACK_INT32 {
            return Err("Expected int32 for typesize".into());
        }
        let typesize = i32::from_be_bytes(index[48..52].try_into().unwrap());
        if !(1..=BLOSC2_MAXTYPESIZE as i32).contains(&typesize) {
            return Err("Invalid frame: invalid typesize".into());
        }
        if index[52] != MSGPACK_INT32 {
            return Err("Expected int32 for blocksize".into());
        }
        let blocksize = i32::from_be_bytes(index[53..57].try_into().unwrap());
        if blocksize < 0 {
            return Err("Invalid frame: negative blocksize".into());
        }
        if index[57] != MSGPACK_INT32 {
            return Err("Expected int32 for chunksize".into());
        }
        let chunksize_i32 = i32::from_be_bytes(index[58..62].try_into().unwrap());
        if chunksize_i32 < -1 || (chunksize_i32 == -1 && (nbytes != 0 || cbytes != 0)) {
            return Err("Invalid frame: negative chunksize".into());
        }
        let chunksize = chunksize_i32.max(0) as usize;
        if variable_chunks {
            if chunksize != 0 {
                return Err("Invalid frame: variable chunk flag with nonzero chunksize".into());
            }
        }

        if index[62] != MSGPACK_INT16 {
            return Err("Expected int16 for nthreads_comp".into());
        }
        let nthreads_comp = i16::from_be_bytes(index[63..65].try_into().unwrap());
        if nthreads_comp < 1 {
            return Err("Invalid frame: invalid compression thread count".into());
        }
        if index[65] != MSGPACK_INT16 {
            return Err("Expected int16 for nthreads_decomp".into());
        }
        let nthreads_decomp = i16::from_be_bytes(index[66..68].try_into().unwrap());
        if nthreads_decomp < 1 {
            return Err("Invalid frame: invalid decompression thread count".into());
        }
        let has_vlmeta = match index[68] {
            MSGPACK_TRUE => true,
            MSGPACK_FALSE => false,
            _ => return Err("Invalid frame: invalid VL-metalayer flag".into()),
        };
        if index[69] != MSGPACK_FIXEXT16 {
            return Err("Expected fixext16 for filters".into());
        }
        if index[70] as usize > BLOSC2_MAX_FILTERS {
            return Err("Invalid frame: too many filters".into());
        }
        let nfilters = index[70] as usize;
        let (filters, filters_meta) = read_frame_filters(&index, 71, 79, nfilters);
        let use_dict = index[FRAME_OTHER_FLAGS2] & 0x01 != 0;
        let vlmetalayers = parse_vlmetalayers(&index[offsets_end..], has_vlmeta)?;
        Ok(FrameMetadata {
            cparams: CParams {
                compcode,
                clevel,
                typesize,
                blocksize,
                splitmode,
                filters,
                filters_meta,
                compcode_meta,
                use_dict,
                nthreads: nthreads_comp,
                ..Default::default()
            },
            dparams: DParams {
                nthreads: nthreads_decomp,
                ..Default::default()
            },
            chunksize,
            nbytes,
            cbytes,
            metalayers,
            vlmetalayers,
            vlblocks,
            nfilters,
        })
    }

    /// Assemble a contiguous frame in memory from the sparse-frame index and
    /// the per-chunk files in `path` so the regular contiguous-frame reader
    /// can be reused.
    fn contiguous_frame_from_sframe_index(
        index: &[u8],
        header_size: usize,
        offsets: &[u64],
        old_offsets_end: usize,
        path: &Path,
    ) -> Result<Vec<u8>, String> {
        let trailer = &index[old_offsets_end..];
        let mut chunks = Vec::with_capacity(offsets.len());
        let mut offsets_data = Vec::with_capacity(offsets.len() * 8);
        let mut data_cbytes = 0u64;
        for &chunk_id in offsets {
            if special_type_from_offset(chunk_id).is_some() {
                offsets_data.extend_from_slice(&chunk_id.to_le_bytes());
                continue;
            }
            let chunk = std::fs::read(sframe_chunk_path(path, chunk_id))
                .map_err(|e| format!("Failed to read sparse frame chunk: {e}"))?;
            offsets_data.extend_from_slice(&data_cbytes.to_le_bytes());
            data_cbytes = data_cbytes
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| "Invalid frame: sparse chunk size overflow".to_string())?;
            chunks.push(chunk);
        }

        let offsets_chunk = if offsets_data.is_empty() {
            Vec::new()
        } else {
            build_offsets_chunk(&offsets_data)
        };
        let frame_size = header_size
            .checked_add(data_cbytes as usize)
            .and_then(|len| len.checked_add(offsets_chunk.len()))
            .and_then(|len| len.checked_add(trailer.len()))
            .ok_or_else(|| "Invalid frame: sparse frame size overflow".to_string())?;

        let mut frame = Vec::with_capacity(frame_size);
        frame.extend_from_slice(&index[..header_size]);
        frame[16..24].copy_from_slice(&(frame_size as u64).to_be_bytes());
        frame[26] = 0;
        for chunk in chunks {
            frame.extend_from_slice(&chunk);
        }
        frame.extend_from_slice(&offsets_chunk);
        frame.extend_from_slice(trailer);
        Ok(frame)
    }

    /// Read a sparse frame directory eagerly.
    pub fn read_sframe_dir(path: &Path) -> Result<Schunk, String> {
        let (index, header_size, offsets, old_offsets_end) = read_sframe_index(path)?;
        let frame = contiguous_frame_from_sframe_index(
            &index,
            header_size,
            &offsets,
            old_offsets_end,
            path,
        )?;
        read_frame(&frame)
    }

    /// Read a sparse frame directory lazily.
    pub fn read_lazy_sframe_dir(path: &Path) -> Result<LazySchunk, String> {
        let (index, header_size, offsets, offsets_end) = read_sframe_index(path)?;
        let meta = parse_sframe_index_metadata(&index, header_size, offsets_end)?;
        let chunk_spec = FrameChunkSpec {
            compcode: meta.cparams.compcode,
            compcode_meta: meta.cparams.compcode_meta,
            typesize: meta.cparams.typesize,
            nfilters: meta.nfilters,
            filters: &meta.cparams.filters,
            filters_meta: &meta.cparams.filters_meta,
            vlblocks: meta.vlblocks,
        };
        let mut chunks = Vec::with_capacity(offsets.len());
        let mut total_nbytes = 0i64;
        let mut total_cbytes = 0i64;
        for (idx, &chunk_id) in offsets.iter().enumerate() {
            if let Some(special) = special_type_from_offset(chunk_id) {
                let nbytes = special_chunk_nbytes(idx, offsets.len(), meta.nbytes, meta.chunksize)?;
                let chunk = synthetic_special_chunk_with_spec(
                    special,
                    nbytes,
                    meta.cparams.blocksize,
                    &chunk_spec,
                )?;
                let header = ChunkHeader::read(&chunk)
                    .map_err(|_| "Invalid frame: invalid special chunk header".to_string())?;
                validate_embedded_chunk_header(&header, &chunk_spec)?;
                total_nbytes = total_nbytes
                    .checked_add(nbytes as i64)
                    .ok_or_else(|| "Invalid frame: nbytes overflow".to_string())?;
                chunks.push(LazyChunkRef {
                    offset: chunk_id,
                    cbytes: BLOSC_EXTENDED_HEADER_LENGTH,
                    nbytes,
                    special: Some(special),
                });
                continue;
            }
            let chunk_path = sframe_chunk_path(path, chunk_id);
            let file_len = std::fs::metadata(&chunk_path)
                .map_err(|e| format!("Failed to stat sparse frame chunk: {e}"))?
                .len();
            let mut file = std::fs::File::open(&chunk_path)
                .map_err(|e| format!("Failed to open sparse frame chunk: {e}"))?;
            let header = read_chunk_header_at(&mut file, 0, file_len)?;
            validate_embedded_chunk_header(&header, &chunk_spec)?;
            if header.cbytes as u64 != file_len {
                return Err("Invalid frame: sparse chunk size mismatch".into());
            }
            let cbytes = usize::try_from(file_len)
                .map_err(|_| "Invalid frame: sparse chunk too large".to_string())?;
            let chunk = std::fs::read(&chunk_path)
                .map_err(|e| format!("Failed to read sparse frame chunk: {e}"))?;
            compress::cbuffer_validate(&chunk).map_err(|err| format!("Invalid frame: {err}"))?;
            total_nbytes = total_nbytes
                .checked_add(header.nbytes as i64)
                .ok_or_else(|| "Invalid frame: nbytes overflow".to_string())?;
            total_cbytes = total_cbytes
                .checked_add(header.cbytes as i64)
                .ok_or_else(|| "Invalid frame: cbytes overflow".to_string())?;
            chunks.push(LazyChunkRef {
                offset: chunk_id,
                cbytes,
                nbytes: header.nbytes as usize,
                special: None,
            });
        }
        if total_cbytes != meta.cbytes {
            return Err("Invalid frame: chunk cbytes total does not match frame".into());
        }
        if total_nbytes != meta.nbytes {
            return Err("Invalid frame: chunk nbytes total does not match frame".into());
        }
        Ok(LazySchunk {
            cparams: meta.cparams,
            dparams: meta.dparams,
            chunksize: meta.chunksize,
            nbytes: meta.nbytes,
            cbytes: meta.cbytes,
            metalayers: meta.metalayers,
            vlmetalayers: meta.vlmetalayers,
            path: path.to_path_buf(),
            chunks,
            sframe: true,
        })
    }

    /// Read a frame lazily and return file-backed chunk references.
    pub fn read_lazy_frame(path: &Path) -> Result<LazySchunk, String> {
        let mut file =
            std::fs::File::open(path).map_err(|e| format!("Failed to open frame file: {e}"))?;
        let file_len = file
            .metadata()
            .map_err(|e| format!("Failed to stat frame file: {e}"))?
            .len();
        if file_len < FRAME_HEADER_MIN_LEN as u64 {
            return Err("Frame too small".into());
        }

        let mut header = vec![0u8; FRAME_HEADER_MIN_LEN];
        read_exact_at(&mut file, 0, &mut header, "Failed to read frame header")?;

        if header[0] != MSGPACK_FIXARRAY_14 {
            return Err(format!("Invalid frame marker: 0x{:02X}", header[0]));
        }
        if header[1] != MSGPACK_STR8 || &header[2..10] != FRAME_MAGIC {
            return Err("Invalid frame magic".into());
        }
        if header[10] != MSGPACK_INT32 {
            return Err("Expected int32 for header_size".into());
        }
        let header_size_i32 = i32::from_be_bytes(header[11..15].try_into().unwrap());
        if header_size_i32 < FRAME_HEADER_MIN_LEN as i32 {
            return Err("Invalid frame header size".into());
        }
        let header_size = header_size_i32 as usize;
        if header_size as u64 > file_len {
            return Err("Frame truncated before data section".into());
        }
        header.resize(header_size, 0);
        if header_size > FRAME_HEADER_MIN_LEN {
            read_exact_at(
                &mut file,
                FRAME_HEADER_MIN_LEN as u64,
                &mut header[FRAME_HEADER_MIN_LEN..],
                "Failed to read extended frame header",
            )?;
        }
        let metalayers = parse_metalayers(&header)?;

        if header[15] != MSGPACK_UINT64 {
            return Err("Expected uint64 for frame_size".into());
        }
        let frame_size_u64 = u64::from_be_bytes(header[16..24].try_into().unwrap());
        if frame_size_u64 < header_size as u64 || frame_size_u64 > file_len {
            return Err("Invalid frame size".into());
        }
        let frame_size =
            usize::try_from(frame_size_u64).map_err(|_| "Invalid frame size".to_string())?;

        if header[24] != MSGPACK_STR4 {
            return Err("Expected fixstr(4) for flags".into());
        }
        let general_flags = header[25];
        let frame_version = general_flags & 0x0F;
        if frame_version > BLOSC2_VERSION_FRAME_FORMAT {
            return Err("Invalid frame: unsupported frame version".into());
        }
        let vlblocks = general_flags & FRAME_VL_BLOCKS != 0;
        let variable_chunks = general_flags & FRAME_VARIABLE_CHUNKS != 0;
        if header[26] != 0 {
            return Err("Invalid frame: unsupported frame type".into());
        }

        let codec_flags = header[27];
        let frame_compcode = codec_flags & 0x0F;
        let compcode = if frame_compcode == BLOSC_UDCODEC_FORMAT {
            header[FRAME_UDCODEC]
        } else {
            frame_compcode
        };
        let compcode_meta = header[FRAME_CODEC_META];
        let clevel = (codec_flags >> 4) & 0x0F;
        let splitmode = decode_frame_splitmode(header[28]);

        if header[29] != MSGPACK_INT64 {
            return Err("Expected int64 for nbytes".into());
        }
        let nbytes = i64::from_be_bytes(header[30..38].try_into().unwrap());
        if nbytes < 0 {
            return Err("Invalid frame: negative nbytes".into());
        }
        if header[38] != MSGPACK_INT64 {
            return Err("Expected int64 for cbytes".into());
        }
        let cbytes = i64::from_be_bytes(header[39..47].try_into().unwrap());
        if cbytes < 0 {
            return Err("Invalid frame: negative cbytes".into());
        }

        if header[47] != MSGPACK_INT32 {
            return Err("Expected int32 for typesize".into());
        }
        let typesize = i32::from_be_bytes(header[48..52].try_into().unwrap());
        if !(1..=BLOSC2_MAXTYPESIZE as i32).contains(&typesize) {
            return Err("Invalid frame: invalid typesize".into());
        }
        if header[52] != MSGPACK_INT32 {
            return Err("Expected int32 for blocksize".into());
        }
        let blocksize = i32::from_be_bytes(header[53..57].try_into().unwrap());
        if blocksize < 0 {
            return Err("Invalid frame: negative blocksize".into());
        }
        if header[57] != MSGPACK_INT32 {
            return Err("Expected int32 for chunksize".into());
        }
        let chunksize_i32 = i32::from_be_bytes(header[58..62].try_into().unwrap());
        if chunksize_i32 < -1 || (chunksize_i32 == -1 && (nbytes != 0 || cbytes != 0)) {
            return Err("Invalid frame: negative chunksize".into());
        }
        let chunksize = chunksize_i32.max(0) as usize;
        if variable_chunks {
            if chunksize != 0 {
                return Err("Invalid frame: variable chunk flag with nonzero chunksize".into());
            }
        }

        if header[62] != MSGPACK_INT16 {
            return Err("Expected int16 for nthreads_comp".into());
        }
        let nthreads_comp = i16::from_be_bytes(header[63..65].try_into().unwrap());
        if nthreads_comp < 1 {
            return Err("Invalid frame: invalid compression thread count".into());
        }
        if header[65] != MSGPACK_INT16 {
            return Err("Expected int16 for nthreads_decomp".into());
        }
        let nthreads_decomp = i16::from_be_bytes(header[66..68].try_into().unwrap());
        if nthreads_decomp < 1 {
            return Err("Invalid frame: invalid decompression thread count".into());
        }
        let has_vlmeta = match header[68] {
            MSGPACK_TRUE => true,
            MSGPACK_FALSE => false,
            _ => return Err("Invalid frame: invalid VL-metalayer flag".into()),
        };

        if header[69] != MSGPACK_FIXEXT16 {
            return Err("Expected fixext16 for filters".into());
        }
        if header[70] as usize > BLOSC2_MAX_FILTERS {
            return Err("Invalid frame: too many filters".into());
        }
        let nfilters = header[70] as usize;
        let (filters, filters_meta) = read_frame_filters(&header, 71, 79, nfilters);
        let use_dict = header[FRAME_OTHER_FLAGS2] & 0x01 != 0;
        let data_start = header_size;
        let data_end = data_start
            .checked_add(cbytes as usize)
            .ok_or_else(|| "Invalid frame: cbytes overflow".to_string())?;
        if data_end > frame_size {
            return Err("Invalid frame: truncated data section".into());
        }

        let mut total_nbytes = 0i64;
        let mut total_cbytes = 0i64;
        let chunk_spec = FrameChunkSpec {
            compcode,
            compcode_meta,
            typesize,
            nfilters,
            filters: &filters,
            filters_meta: &filters_meta,
            vlblocks,
        };
        let offsets_len = if cbytes == 0 && nbytes == 0 {
            0
        } else {
            offsets_chunk_len_from_file(&mut file, data_end, frame_size)?
        };
        let offsets = offsets_payload_from_file(&mut file, data_end, offsets_len)?;
        validate_frame_offsets_count(offsets.len(), nbytes, chunksize)?;
        let mut chunks = Vec::new();
        if offsets.is_empty() {
            let mut pos = data_start;
            while pos < data_end {
                let ch = read_chunk_header_at(&mut file, pos as u64, data_end as u64)?;
                validate_embedded_chunk_header(&ch, &chunk_spec)?;
                let chunk_cbytes = ch.cbytes as usize;
                let chunk_end = pos
                    .checked_add(chunk_cbytes)
                    .ok_or_else(|| "Invalid frame: chunk size overflow".to_string())?;
                if chunk_end > data_end {
                    return Err("Invalid frame: chunk extends past data section".into());
                }
                let mut chunk = vec![0u8; chunk_cbytes];
                read_exact_at(
                    &mut file,
                    pos as u64,
                    &mut chunk,
                    "Failed to read frame chunk",
                )?;
                compress::cbuffer_validate(&chunk)
                    .map_err(|err| format!("Invalid frame: {err}"))?;
                total_nbytes = total_nbytes
                    .checked_add(ch.nbytes as i64)
                    .ok_or_else(|| "Invalid frame: nbytes overflow".to_string())?;
                total_cbytes = total_cbytes
                    .checked_add(ch.cbytes as i64)
                    .ok_or_else(|| "Invalid frame: cbytes overflow".to_string())?;
                chunks.push(LazyChunkRef {
                    offset: pos as u64,
                    cbytes: chunk_cbytes,
                    nbytes: ch.nbytes as usize,
                    special: None,
                });
                pos = chunk_end;
            }
        } else {
            chunks.reserve(offsets.len());
            let mut intervals = Vec::with_capacity(offsets.len());
            for (logical_idx, &offset) in offsets.iter().enumerate() {
                if let Some(special) = special_type_from_offset(offset) {
                    let nbytes =
                        special_chunk_nbytes(logical_idx, offsets.len(), nbytes, chunksize)?;
                    let chunk =
                        synthetic_special_chunk_with_spec(special, nbytes, blocksize, &chunk_spec)?;
                    let header = ChunkHeader::read(&chunk)
                        .map_err(|_| "Invalid frame: invalid special chunk header".to_string())?;
                    validate_embedded_chunk_header(&header, &chunk_spec)?;
                    total_nbytes = total_nbytes
                        .checked_add(nbytes as i64)
                        .ok_or_else(|| "Invalid frame: nbytes overflow".to_string())?;
                    chunks.push(LazyChunkRef {
                        offset,
                        cbytes: BLOSC_EXTENDED_HEADER_LENGTH,
                        nbytes,
                        special: Some(special),
                    });
                    continue;
                }
                if offset & (1u64 << 63) != 0 {
                    return Err("Invalid frame: invalid special chunk offset".into());
                }
                let pos = data_start
                    .checked_add(offset as usize)
                    .ok_or_else(|| "Invalid frame: chunk offset overflow".to_string())?;
                let ch = read_chunk_header_at(&mut file, pos as u64, data_end as u64)?;
                validate_embedded_chunk_header(&ch, &chunk_spec)?;
                let chunk_cbytes = ch.cbytes as usize;
                let chunk_end = pos
                    .checked_add(chunk_cbytes)
                    .ok_or_else(|| "Invalid frame: chunk size overflow".to_string())?;
                if chunk_end > data_end {
                    return Err("Invalid frame: chunk extends past data section".into());
                }
                let mut chunk = vec![0u8; chunk_cbytes];
                read_exact_at(
                    &mut file,
                    pos as u64,
                    &mut chunk,
                    "Failed to read frame chunk",
                )?;
                compress::cbuffer_validate(&chunk)
                    .map_err(|err| format!("Invalid frame: {err}"))?;
                intervals.push((pos - data_start, chunk_end - data_start));
                total_nbytes = total_nbytes
                    .checked_add(ch.nbytes as i64)
                    .ok_or_else(|| "Invalid frame: nbytes overflow".to_string())?;
                total_cbytes = total_cbytes
                    .checked_add(ch.cbytes as i64)
                    .ok_or_else(|| "Invalid frame: cbytes overflow".to_string())?;
                chunks.push(LazyChunkRef {
                    offset: pos as u64,
                    cbytes: chunk_cbytes,
                    nbytes: ch.nbytes as usize,
                    special: None,
                });
            }
            validate_frame_data_intervals(&mut intervals, cbytes as usize)?;
        }

        if total_cbytes != cbytes {
            return Err("Invalid frame: chunk cbytes total does not match frame".into());
        }
        if total_nbytes != nbytes {
            return Err("Invalid frame: chunk nbytes total does not match frame".into());
        }
        let trailer_start = data_end
            .checked_add(offsets_len)
            .ok_or_else(|| "Invalid frame: trailer offset overflow".to_string())?;
        if trailer_start > frame_size {
            return Err("Invalid frame: trailer starts past frame".into());
        }
        let trailer_len = frame_size - trailer_start;
        let mut trailer = vec![0u8; trailer_len];
        if trailer_len > 0 {
            read_exact_at(
                &mut file,
                trailer_start as u64,
                &mut trailer,
                "Failed to read frame trailer",
            )?;
        }
        let vlmetalayers = parse_vlmetalayers(&trailer, has_vlmeta)?;

        Ok(LazySchunk {
            cparams: CParams {
                compcode,
                clevel,
                typesize,
                blocksize,
                splitmode,
                filters,
                filters_meta,
                compcode_meta,
                use_dict,
                nthreads: nthreads_comp,
                ..Default::default()
            },
            dparams: DParams {
                nthreads: nthreads_decomp,
                ..Default::default()
            },
            chunksize,
            nbytes,
            cbytes,
            metalayers,
            vlmetalayers,
            path: path.to_path_buf(),
            chunks,
            sframe: false,
        })
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
        if frame_size != data.len() as u64 {
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
        let vlblocks = general_flags & FRAME_VL_BLOCKS != 0;
        let variable_chunks = general_flags & FRAME_VARIABLE_CHUNKS != 0;
        let frame_type = data[26];
        let codec_flags = data[27];
        let other_flags = data[28];
        if frame_type != 0 {
            return Err("Invalid frame: unsupported frame type".into());
        }

        let frame_compcode = codec_flags & 0x0F;
        let compcode = if frame_compcode == BLOSC_UDCODEC_FORMAT {
            data[FRAME_UDCODEC]
        } else {
            frame_compcode
        };
        let compcode_meta = data[FRAME_CODEC_META];
        let clevel = (codec_flags >> 4) & 0x0F;
        let splitmode = decode_frame_splitmode(other_flags);

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
        if !(1..=BLOSC2_MAXTYPESIZE as i32).contains(&typesize) {
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
        if chunksize_i32 < -1 || (chunksize_i32 == -1 && (nbytes != 0 || cbytes != 0)) {
            return Err("Invalid frame: negative chunksize".into());
        }
        let chunksize = chunksize_i32.max(0) as usize;
        if variable_chunks {
            if chunksize != 0 {
                return Err("Invalid frame: variable chunk flag with nonzero chunksize".into());
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
        let nfilters = data[70];
        if nfilters as usize > BLOSC2_MAX_FILTERS {
            return Err("Invalid frame: too many filters".into());
        }
        let nfilters = nfilters as usize;
        let (filters, filters_meta) = read_frame_filters(data, 71, 79, nfilters);
        let use_dict = data[FRAME_OTHER_FLAGS2] & 0x01 != 0;
        // Now we need to find and read the chunks
        // The offset index is after the data chunks
        // First, find data start (= header_size) and read the offset index

        // Read chunks from the frame
        let data_start = header_size;

        // The offsets chunk is after all data chunks
        // We need to scan forward from data_start, reading chunk headers to find all chunks
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
        let mut materialized_cbytes = 0i64;
        let chunk_spec = FrameChunkSpec {
            compcode,
            compcode_meta,
            typesize,
            nfilters,
            filters: &filters,
            filters_meta: &filters_meta,
            vlblocks,
        };
        let offsets_len = if cbytes == 0 && nbytes == 0 {
            0
        } else {
            offsets_chunk_len(data, data_end, frame_size as usize)?
        };
        let mut chunks = Vec::new();
        if offsets_len > 0 {
            let offsets_end = data_end
                .checked_add(offsets_len)
                .ok_or_else(|| "Invalid frame: offsets chunk overflow".to_string())?;
            let offsets_payload = compress::decompress(&data[data_end..offsets_end])
                .map_err(|_| "Invalid frame: invalid offsets chunk".to_string())?;
            if offsets_payload.len() % 8 != 0 {
                return Err("Invalid frame: offsets payload has invalid length".into());
            }
            let nchunks = offsets_payload.len() / 8;
            validate_frame_offsets_count(nchunks, nbytes, chunksize)?;
            let mut intervals = Vec::with_capacity(nchunks);
            for (logical_idx, bytes) in offsets_payload.chunks_exact(8).enumerate() {
                let offset = u64::from_le_bytes(bytes.try_into().unwrap());
                if special_type_from_offset(offset).is_some() {
                    let chunk = special_chunk_from_offset(
                        offset,
                        logical_idx,
                        nchunks,
                        nbytes,
                        chunksize,
                        blocksize,
                        &chunk_spec,
                    )?;
                    let ch = ChunkHeader::read(&chunk)
                        .map_err(|_| "Invalid frame: invalid special chunk header".to_string())?;
                    validate_embedded_chunk_header(&ch, &chunk_spec)?;
                    total_nbytes = total_nbytes
                        .checked_add(ch.nbytes as i64)
                        .ok_or_else(|| "Invalid frame: nbytes overflow".to_string())?;
                    materialized_cbytes = materialized_cbytes
                        .checked_add(ch.cbytes as i64)
                        .ok_or_else(|| "Invalid frame: cbytes overflow".to_string())?;
                    chunks.push(chunk);
                    continue;
                }
                if offset & (1u64 << 63) != 0 {
                    return Err("Invalid frame: invalid special chunk offset".into());
                }

                let pos = data_start
                    .checked_add(offset as usize)
                    .ok_or_else(|| "Invalid frame: chunk offset overflow".to_string())?;
                if pos + BLOSC_MIN_HEADER_LENGTH > data_end {
                    return Err("Invalid frame: chunk offset outside data section".into());
                }
                let ch = ChunkHeader::read(&data[pos..])
                    .map_err(|_| "Invalid frame: invalid chunk header".to_string())?;
                validate_embedded_chunk_header(&ch, &chunk_spec)?;
                let chunk_cbytes = ch.cbytes as usize;
                let chunk_end = pos
                    .checked_add(chunk_cbytes)
                    .ok_or_else(|| "Invalid frame: chunk size overflow".to_string())?;
                if chunk_end > data_end {
                    return Err("Invalid frame: chunk extends past data section".into());
                }
                compress::cbuffer_validate(&data[pos..chunk_end])
                    .map_err(|err| format!("Invalid frame: {err}"))?;
                intervals.push((pos - data_start, chunk_end - data_start));
                total_nbytes = total_nbytes
                    .checked_add(ch.nbytes as i64)
                    .ok_or_else(|| "Invalid frame: nbytes overflow".to_string())?;
                total_cbytes = total_cbytes
                    .checked_add(ch.cbytes as i64)
                    .ok_or_else(|| "Invalid frame: cbytes overflow".to_string())?;
                materialized_cbytes = materialized_cbytes
                    .checked_add(ch.cbytes as i64)
                    .ok_or_else(|| "Invalid frame: cbytes overflow".to_string())?;
                chunks.push(data[pos..chunk_end].to_vec());
            }
            validate_frame_data_intervals(&mut intervals, cbytes as usize)?;
        } else {
            let mut pos = data_start;
            while pos < data_end {
                if pos + BLOSC_MIN_HEADER_LENGTH > data_end {
                    return Err("Invalid frame: data section ends inside chunk header".into());
                }

                let ch = ChunkHeader::read(&data[pos..])
                    .map_err(|_| "Invalid frame: invalid chunk header".to_string())?;
                validate_embedded_chunk_header(&ch, &chunk_spec)?;

                let chunk_cbytes = ch.cbytes as usize;
                let chunk_end = pos
                    .checked_add(chunk_cbytes)
                    .ok_or_else(|| "Invalid frame: chunk size overflow".to_string())?;
                if chunk_end > data_end {
                    return Err("Invalid frame: chunk extends past data section".into());
                }
                compress::cbuffer_validate(&data[pos..chunk_end])
                    .map_err(|err| format!("Invalid frame: {err}"))?;

                total_nbytes = total_nbytes
                    .checked_add(ch.nbytes as i64)
                    .ok_or_else(|| "Invalid frame: nbytes overflow".to_string())?;
                total_cbytes = total_cbytes
                    .checked_add(ch.cbytes as i64)
                    .ok_or_else(|| "Invalid frame: cbytes overflow".to_string())?;
                materialized_cbytes = materialized_cbytes
                    .checked_add(ch.cbytes as i64)
                    .ok_or_else(|| "Invalid frame: cbytes overflow".to_string())?;
                chunks.push(data[pos..chunk_end].to_vec());
                pos = chunk_end;
            }
        }

        if total_cbytes != cbytes {
            return Err("Invalid frame: chunk cbytes total does not match frame".into());
        }
        if total_nbytes != nbytes {
            return Err("Invalid frame: chunk nbytes total does not match frame".into());
        }
        let trailer_start = data_end
            .checked_add(offsets_len)
            .ok_or_else(|| "Invalid frame: trailer offset overflow".to_string())?;
        if trailer_start > frame_size as usize {
            return Err("Invalid frame: trailer starts past frame".into());
        }
        let vlmetalayers =
            parse_vlmetalayers(&data[trailer_start..frame_size as usize], has_vlmeta)?;
        let cparams = CParams {
            compcode,
            clevel,
            typesize,
            blocksize,
            splitmode,
            filters,
            filters_meta,
            compcode_meta,
            use_dict,
            nthreads: nthreads_comp,
            ..Default::default()
        };

        let dparams = DParams {
            nthreads: nthreads_decomp,
            ..Default::default()
        };

        Ok(Schunk {
            cparams,
            dparams,
            chunks,
            chunksize,
            nbytes,
            cbytes: materialized_cbytes,
            metalayers,
            vlmetalayers,
            variable_chunks,
            vlblocks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};

    static LAST_PREFILTER_NCHUNK: AtomicI64 = AtomicI64::new(-99);
    static LAST_POSTFILTER_NCHUNK: AtomicI64 = AtomicI64::new(-99);

    fn record_prefilter_nchunk(params: &mut compress::PrefilterParams<'_>) -> i32 {
        LAST_PREFILTER_NCHUNK.store(params.nchunk, AtomicOrdering::SeqCst);
        params.output.copy_from_slice(params.input);
        0
    }

    fn record_postfilter_nchunk(params: &mut compress::PostfilterParams<'_>) -> i32 {
        LAST_POSTFILTER_NCHUNK.store(params.nchunk, AtomicOrdering::SeqCst);
        params.output.copy_from_slice(params.input);
        0
    }

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
    fn test_filter_callbacks_receive_schunk_chunk_index() {
        LAST_PREFILTER_NCHUNK.store(-99, AtomicOrdering::SeqCst);
        LAST_POSTFILTER_NCHUNK.store(-99, AtomicOrdering::SeqCst);

        let mut schunk = Schunk::new(
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                filters: [0; BLOSC2_MAX_FILTERS],
                prefilter: Some(record_prefilter_nchunk),
                ..Default::default()
            },
            DParams {
                postfilter: Some(record_postfilter_nchunk),
                typesize: 1,
                ..Default::default()
            },
        );

        let first = vec![b'a'; 1024];
        let second = vec![b'b'; 1024];
        let updated = vec![b'c'; 1024];

        schunk.append_buffer(&first).unwrap();
        assert_eq!(LAST_PREFILTER_NCHUNK.load(AtomicOrdering::SeqCst), 0);
        schunk.append_buffer(&second).unwrap();
        assert_eq!(LAST_PREFILTER_NCHUNK.load(AtomicOrdering::SeqCst), 1);
        schunk.update_chunk(1, &updated).unwrap();
        assert_eq!(LAST_PREFILTER_NCHUNK.load(AtomicOrdering::SeqCst), 1);

        assert_eq!(schunk.decompress_chunk(1).unwrap(), updated);
        assert_eq!(LAST_POSTFILTER_NCHUNK.load(AtomicOrdering::SeqCst), 1);
    }

    #[test]
    fn test_special_offset_decoding_accepts_c_mask_form() {
        let canonical = frame::encoded_special_offset(BLOSC2_SPECIAL_ZERO);
        assert_eq!(
            frame::special_type_from_offset(canonical | 0x1234),
            Some(BLOSC2_SPECIAL_ZERO)
        );
        assert_eq!(
            frame::special_type_from_offset(
                frame::encoded_special_offset(BLOSC2_SPECIAL_UNINIT) | 0x00ff_ffff
            ),
            Some(BLOSC2_SPECIAL_UNINIT)
        );
        assert_eq!(frame::special_type_from_offset(0x8500_0000_0000_0001), None);
    }

    #[test]
    fn test_lazy_special_offset_reports_frame_cbytes() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams.clone(), DParams::default());
        schunk.append_buffer(&[1u8; 8]).unwrap();
        schunk.append_buffer(&[0u8; 8]).unwrap();

        let frame = schunk.to_frame();
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        let cbytes = i64::from_be_bytes(frame[39..47].try_into().unwrap()) as usize;
        let data_start = header_size;
        let data_end = data_start + cbytes;
        let offsets_header = ChunkHeader::read(&frame[data_end..]).unwrap();
        let offsets_end = data_end + offsets_header.cbytes as usize;
        let trailer = &frame[offsets_end..];

        let first_chunk_len = schunk.chunks[0].len();
        let mut offsets_payload = vec![0u8; 16];
        offsets_payload[..8].copy_from_slice(&0u64.to_le_bytes());
        offsets_payload[8..]
            .copy_from_slice(&frame::encoded_special_offset(BLOSC2_SPECIAL_ZERO).to_le_bytes());
        let offsets_chunk = {
            let cparams = CParams {
                compcode: BLOSC_BLOSCLZ,
                clevel: 5,
                typesize: 8,
                splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            };
            compress::compress(&offsets_payload, &cparams).unwrap()
        };

        let mut special_frame = frame[..header_size].to_vec();
        let new_cbytes = first_chunk_len as i64;
        let new_frame_size = header_size + first_chunk_len + offsets_chunk.len() + trailer.len();
        special_frame[16..24].copy_from_slice(&(new_frame_size as u64).to_be_bytes());
        special_frame[39..47].copy_from_slice(&new_cbytes.to_be_bytes());
        special_frame.extend_from_slice(&frame[data_start..data_start + first_chunk_len]);
        special_frame.extend_from_slice(&offsets_chunk);
        special_frame.extend_from_slice(trailer);

        let eager = Schunk::from_frame(&special_frame).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("special-offset.b2frame");
        std::fs::write(&path, &special_frame).unwrap();
        let lazy = Schunk::open_lazy(&path).unwrap();
        assert_eq!(lazy.chunk_refs()[1].cbytes, BLOSC_EXTENDED_HEADER_LENGTH);
        assert_eq!(lazy.cbytes, new_cbytes);
        assert_ne!(lazy.cbytes, eager.cbytes);
        assert_eq!(lazy.decompress_chunk(1).unwrap(), vec![0u8; 8]);
    }

    #[test]
    fn test_synthetic_special_zero_validates_cparams_and_sizes() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 100,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let chunk = synthetic_special_chunk_for_params(BLOSC2_SPECIAL_ZERO, 16, &cparams).unwrap();
        let header = ChunkHeader::read(&chunk).unwrap();
        assert_eq!(header.special_type(), BLOSC2_SPECIAL_ZERO);
        assert_eq!(header.typesize, 4);
        assert_eq!(header.nbytes, 16);
        assert_eq!(header.blocksize, 16);
        assert_eq!(header.cbytes as usize, BLOSC_EXTENDED_HEADER_LENGTH);

        assert!(synthetic_special_chunk_for_params(BLOSC2_SPECIAL_ZERO, 18, &cparams).is_err());
        assert!(synthetic_special_chunk_for_params(
            BLOSC2_SPECIAL_ZERO,
            16,
            &CParams {
                typesize: 0,
                ..cparams.clone()
            },
        )
        .is_err());
        assert!(synthetic_special_chunk_for_params(
            BLOSC2_SPECIAL_ZERO,
            16,
            &CParams {
                blocksize: -1,
                ..cparams
            },
        )
        .is_err());
    }

    #[test]
    fn test_schunk_parallel_append_buffers_and_decompress_all() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            nthreads: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let dparams = DParams {
            nthreads: 4,
            ..Default::default()
        };
        let chunks: Vec<Vec<u8>> = (0..8)
            .map(|chunk| {
                (0..4096u32)
                    .flat_map(|i| (i + chunk * 4096).to_le_bytes())
                    .collect()
            })
            .collect();
        let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
        let expected: Vec<u8> = chunks.iter().flatten().copied().collect();

        let mut schunk = Schunk::new(cparams.clone(), dparams);
        assert_eq!(schunk.append_buffers(&refs).unwrap(), 0..8);
        assert_eq!(schunk.nchunks(), 8);
        assert_eq!(schunk.decompress_all().unwrap(), expected);

        let mut sequential = Schunk::new(
            CParams {
                nthreads: 1,
                ..cparams
            },
            DParams::default(),
        );
        for chunk in &chunks {
            sequential.append_buffer(chunk).unwrap();
        }
        assert_eq!(schunk.chunks, sequential.chunks);
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
        let mut schunk = Schunk::new(cparams.clone(), DParams::default());

        schunk.append_buffer(b"aaaa").unwrap();
        schunk.append_buffer(b"cccc").unwrap();
        schunk.insert_buffer(1, b"bbbb").unwrap();
        assert_eq!(schunk.decompress_all().unwrap(), b"aaaabbbbcccc");
        assert_eq!(schunk.nchunks(), 3);
        assert_eq!(schunk.nbytes, 12);

        schunk.update_chunk(1, b"BBBB").unwrap();
        assert_eq!(schunk.decompress_all().unwrap(), b"aaaaBBBBcccc");

        let removed = schunk.delete_chunk_data(0).unwrap();
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
    fn test_schunk_set_slice_updates_aligned_blocks_without_touching_others() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 128,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams.clone(), DParams::default());
        let data: Vec<u8> = (0..512u32).flat_map(|i| i.to_le_bytes()).collect();
        schunk.append_buffer(&data).unwrap();

        let replacement = vec![0x5au8; 128];
        schunk.set_slice(128, &replacement).unwrap();

        let mut expected = data;
        expected[128..256].copy_from_slice(&replacement);
        assert_eq!(schunk.decompress_all().unwrap(), expected);
    }

    #[test]
    fn test_schunk_set_slice_aligned_update_ignores_untouched_block_payloads() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 128,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        let data: Vec<u8> = (0..512u32).flat_map(|i| i.to_le_bytes()).collect();
        schunk.append_buffer(&data).unwrap();

        let header = ChunkHeader::read(&schunk.chunks[0]).unwrap();
        let block2_bstart_pos = header.header_len() + 2 * 4;
        let block2_start = i32::from_le_bytes(
            schunk.chunks[0][block2_bstart_pos..block2_bstart_pos + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        schunk.chunks[0][block2_start..block2_start + 4].copy_from_slice(&i32::MAX.to_le_bytes());
        assert!(schunk.decompress_all().is_err());

        let replacement = vec![0xa5u8; 128];
        schunk.set_slice(0, &replacement).unwrap();
        assert_eq!(
            compress::getitem(&schunk.chunks[0], 0, 128 / 4).unwrap(),
            replacement
        );
        assert!(schunk.decompress_all().is_err());
    }

    #[test]
    fn test_schunk_set_slice_unaligned_update_ignores_untouched_block_payloads() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 128,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        let data: Vec<u8> = (0..512u32).flat_map(|i| i.to_le_bytes()).collect();
        schunk.append_buffer(&data).unwrap();

        let header = ChunkHeader::read(&schunk.chunks[0]).unwrap();
        let block2_bstart_pos = header.header_len() + 2 * 4;
        let block2_start = i32::from_le_bytes(
            schunk.chunks[0][block2_bstart_pos..block2_bstart_pos + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        schunk.chunks[0][block2_start..block2_start + 4].copy_from_slice(&i32::MAX.to_le_bytes());
        assert!(schunk.decompress_all().is_err());

        let replacement = vec![0x3cu8; 32];
        schunk.set_slice(16, &replacement).unwrap();
        assert_eq!(
            compress::getitem(&schunk.chunks[0], 4, 8).unwrap(),
            replacement
        );
        assert!(schunk.decompress_all().is_err());
    }

    fn xor_filter(meta: u8, _typesize: usize, _block_offset: usize, src: &[u8], dest: &mut [u8]) {
        for (out, inp) in dest.iter_mut().zip(src) {
            *out = *inp ^ meta;
        }
    }

    fn sequence_frame_codec_compress(_clevel: u8, meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
        if src.len() < 2 || dest.len() < 3 {
            return 0;
        }
        dest[0] = src[0];
        dest[1] = src[1].wrapping_sub(src[0]);
        dest[2] = meta;
        3
    }

    fn sequence_frame_codec_decompress(meta: u8, src: &[u8], dest: &mut [u8]) -> i32 {
        if src.len() != 3 || src[2] != meta {
            return -1;
        }
        for (idx, byte) in dest.iter_mut().enumerate() {
            *byte = src[0].wrapping_add(src[1].wrapping_mul(idx as u8));
        }
        dest.len() as i32
    }

    #[test]
    fn test_user_defined_filter_frame_roundtrip_and_metadata() {
        const FILTER_ID: u8 = 201;
        crate::filters::register_filter(FILTER_ID, xor_filter, xor_filter).unwrap();

        let data: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut filters = [0; BLOSC2_MAX_FILTERS];
        let mut filters_meta = [0; BLOSC2_MAX_FILTERS];
        filters[BLOSC2_MAX_FILTERS - 1] = FILTER_ID;
        filters_meta[BLOSC2_MAX_FILTERS - 1] = 0x5a;

        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 1024,
            splitmode: BLOSC_NEVER_SPLIT,
            filters,
            filters_meta,
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.append_buffer(&data).unwrap();

        let frame = schunk.to_frame();
        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.cparams.filters, filters);
        assert_eq!(restored.cparams.filters_meta, filters_meta);
        assert_eq!(restored.decompress_all().unwrap(), data);
    }

    #[test]
    fn test_plugin_codec_frame_roundtrip_and_metadata() {
        const CODEC_ID: u8 = 32;
        codecs::register_codec(
            CODEC_ID,
            sequence_frame_codec_compress,
            sequence_frame_codec_decompress,
        )
        .unwrap();

        let data: Vec<u8> = (0..200u8).collect();
        let cparams = CParams {
            compcode: CODEC_ID,
            compcode_meta: 19,
            clevel: 5,
            typesize: 1,
            blocksize: 200,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.append_buffer(&data).unwrap();

        let frame = schunk.to_frame();
        assert_eq!(frame[27] & 0x0f, BLOSC_UDCODEC_FORMAT);
        assert_eq!(frame[77], CODEC_ID);
        assert_eq!(frame[78], 19);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.cparams.compcode, CODEC_ID);
        assert_eq!(restored.cparams.compcode_meta, 19);
        assert_eq!(restored.decompress_all().unwrap(), data);
    }

    #[test]
    fn test_frame_stores_builtin_codec_metadata_byte() {
        let cparams = CParams {
            compcode: BLOSC_LZ4HC,
            compcode_meta: 7,
            clevel: 5,
            typesize: 1,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk
            .append_buffer(b"builtin codec frame metadata")
            .unwrap();

        let frame = schunk.to_frame();
        assert_eq!(frame[27] & 0x0f, BLOSC_LZ4HC);
        assert_eq!(frame[77], BLOSC_LZ4HC);
        assert_eq!(frame[78], 7);
        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.cparams.compcode, BLOSC_LZ4HC);
        assert_eq!(
            restored.decompress_all().unwrap(),
            b"builtin codec frame metadata"
        );
    }

    #[test]
    fn test_unregistered_frame_udcodec_allowed_until_codec_needed() {
        let data = b"frame memcpy payload".repeat(32);
        let mut memcpy_schunk = Schunk::new(
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 0,
                typesize: 1,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
            DParams::default(),
        );
        memcpy_schunk.append_buffer(&data).unwrap();
        let mut memcpy_frame = memcpy_schunk.to_frame();
        let header_size = i32::from_be_bytes(memcpy_frame[11..15].try_into().unwrap()) as usize;
        memcpy_frame[27] = (memcpy_frame[27] & 0xf0) | BLOSC_UDCODEC_FORMAT;
        memcpy_frame[77] = 160;
        memcpy_frame[85] |= 0x01;
        memcpy_frame[71 + BLOSC2_MAX_FILTERS - 1] = 99;
        memcpy_frame[header_size + BLOSC2_CHUNK_FILTER_CODES + 5] = 99;
        assert_eq!(
            Schunk::from_frame(&memcpy_frame)
                .unwrap()
                .decompress_all()
                .unwrap(),
            data
        );

        let mut special_schunk = Schunk::new(
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
            DParams::default(),
        );
        special_schunk.append_buffer(&vec![0u8; 128]).unwrap();
        let mut special_frame = special_schunk.to_frame();
        special_frame[27] = (special_frame[27] & 0xf0) | BLOSC_UDCODEC_FORMAT;
        special_frame[77] = 160;
        special_frame[85] |= 0x01;
        special_frame[71 + BLOSC2_MAX_FILTERS - 1] = 99;
        assert_eq!(
            Schunk::from_frame(&special_frame)
                .unwrap()
                .decompress_all()
                .unwrap(),
            vec![0u8; 128]
        );

        let mut regular_schunk = Schunk::new(
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
            DParams::default(),
        );
        regular_schunk.append_buffer(&data).unwrap();
        let mut regular_frame = regular_schunk.to_frame();
        let header_size = i32::from_be_bytes(regular_frame[11..15].try_into().unwrap()) as usize;
        regular_frame[27] = (regular_frame[27] & 0xf0) | BLOSC_UDCODEC_FORMAT;
        regular_frame[77] = 160;
        regular_frame[header_size + BLOSC2_CHUNK_FLAGS] =
            (regular_frame[header_size + BLOSC2_CHUNK_FLAGS] & !0xe0) | (BLOSC_UDCODEC_FORMAT << 5);
        regular_frame[header_size + BLOSC2_CHUNK_UDCOMPCODE] = 160;
        assert!(Schunk::from_frame(&regular_frame).is_err());

        let mut bad_regular_filter = regular_schunk.to_frame();
        let header_size =
            i32::from_be_bytes(bad_regular_filter[11..15].try_into().unwrap()) as usize;
        bad_regular_filter[71 + BLOSC2_MAX_FILTERS - 1] = 99;
        bad_regular_filter[header_size + BLOSC2_CHUNK_FILTER_CODES + 5] = 99;
        assert!(Schunk::from_frame(&bad_regular_filter).is_err());
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
        let zeros = vec![0u8; 8];
        schunk.append_buffer(&zeros).unwrap();
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
        assert_eq!(restored.decompress_chunk(1).unwrap(), zeros);
        assert_eq!(restored.decompress_chunk(2).unwrap(), b"bravo bravo\0");
        assert_eq!(
            restored.decompress_chunk(3).unwrap(),
            b"charlie-charlie-charlie\0"
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("variable-special.b2frame");
        schunk.to_sframe_dir(&path).unwrap();
        assert!(path.join("00000001.chunk").is_file());
        let restored = Schunk::open_sframe(&path).unwrap();
        assert_eq!(restored.decompress_chunk(1).unwrap(), zeros);
    }

    #[test]
    fn test_final_short_chunk_keeps_fixed_frame_chunksize() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.append_buffer(b"first chunk").unwrap();
        schunk.append_buffer(b"secondchunk").unwrap();
        schunk.append_buffer(b"short").unwrap();

        assert_eq!(schunk.chunksize, 11);

        let frame = schunk.to_frame();
        assert_eq!(frame[25] & 0x0F, BLOSC2_VERSION_FRAME_FORMAT_RC1);
        assert_eq!(frame[25] & FRAME_VARIABLE_CHUNKS, 0);
        assert_eq!(i32::from_be_bytes(frame[58..62].try_into().unwrap()), 11);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.chunksize, 11);
        assert_eq!(
            restored.decompress_all().unwrap(),
            b"first chunksecondchunkshort"
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("final-short.b2frame");
        schunk.to_file(path.to_str().unwrap()).unwrap();
        let lazy = Schunk::open_lazy(&path).unwrap();
        assert_eq!(lazy.chunksize, 11);
        assert_eq!(
            lazy.decompress_all().unwrap(),
            b"first chunksecondchunkshort"
        );
    }

    #[test]
    fn test_empty_frame_uses_c_negative_chunksize() {
        let schunk = Schunk::new(CParams::default(), DParams::default());
        let frame = schunk.to_frame();
        assert_eq!(frame[25] & 0x0F, BLOSC2_VERSION_FRAME_FORMAT_RC1);
        assert_eq!(frame[25] & FRAME_VARIABLE_CHUNKS, 0);
        assert_eq!(i32::from_be_bytes(frame[58..62].try_into().unwrap()), -1);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.nchunks(), 0);
        assert_eq!(restored.chunksize, 0);
        assert_eq!(restored.nbytes, 0);
        assert_eq!(restored.cbytes, 0);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.b2frame");
        std::fs::write(&path, &frame).unwrap();
        let lazy = Schunk::open_lazy(&path).unwrap();
        assert_eq!(lazy.nchunks(), 0);
        assert_eq!(lazy.chunksize, 0);

        let sframe = dir.path().join("empty-sframe.b2frame");
        schunk.to_sframe_dir(&sframe).unwrap();
        let index = std::fs::read(sframe.join("chunks.b2frame")).unwrap();
        assert_eq!(index[25] & 0x0F, BLOSC2_VERSION_FRAME_FORMAT_RC1);
        assert_eq!(index[25] & FRAME_VARIABLE_CHUNKS, 0);
        assert_eq!(i32::from_be_bytes(index[58..62].try_into().unwrap()), -1);
        let restored = Schunk::open_sframe(&sframe).unwrap();
        assert_eq!(restored.nchunks(), 0);
        let lazy = Schunk::open_lazy_sframe(&sframe).unwrap();
        assert_eq!(lazy.nchunks(), 0);
    }

    #[test]
    fn test_non_tail_short_chunk_uses_variable_frame_flag() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.append_buffer(b"first chunk").unwrap();
        schunk.append_buffer(b"short").unwrap();
        schunk.append_buffer(b"secondchunk").unwrap();

        assert_eq!(schunk.chunksize, 0);

        let frame = schunk.to_frame();
        assert_eq!(frame[25] & 0x0F, BLOSC2_VERSION_FRAME_FORMAT);
        assert_ne!(frame[25] & FRAME_VARIABLE_CHUNKS, 0);
        assert_eq!(i32::from_be_bytes(frame[58..62].try_into().unwrap()), 0);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.chunksize, 0);
        assert_eq!(
            restored.decompress_all().unwrap(),
            b"first chunkshortsecondchunk"
        );
    }

    #[test]
    fn test_zstd_dictionary_frame_roundtrip_preserves_flag() {
        let cparams = CParams {
            compcode: BLOSC_ZSTD,
            clevel: 5,
            typesize: 4,
            blocksize: 4096,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            use_dict: true,
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        let data: Vec<u8> = (0..200_000u32)
            .flat_map(|i| (i % 4096).to_le_bytes())
            .collect();

        schunk.append_buffer(&data).unwrap();
        let frame = schunk.to_frame();
        assert_eq!(frame[85] & 0x01, 0x01);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert!(restored.cparams.use_dict);
        assert_eq!(restored.decompress_chunk(0).unwrap(), data);

        let dir = tempfile::tempdir().unwrap();
        let frame_path = dir.path().join("dict-fallback.b2frame");
        schunk.to_file(frame_path.to_str().unwrap()).unwrap();
        let lazy = Schunk::open_lazy(&frame_path).unwrap();
        assert!(lazy.cparams.use_dict);
        assert_eq!(lazy.decompress_chunk(0).unwrap(), data);

        let sframe_path = dir.path().join("dict-fallback-sframe.b2frame");
        schunk.to_sframe_dir(&sframe_path).unwrap();
        let eager_sframe = Schunk::open_sframe(&sframe_path).unwrap();
        assert!(eager_sframe.cparams.use_dict);
        assert_eq!(eager_sframe.decompress_chunk(0).unwrap(), data);
        let lazy_sframe = Schunk::open_lazy_sframe(&sframe_path).unwrap();
        assert!(lazy_sframe.cparams.use_dict);
        assert_eq!(lazy_sframe.decompress_chunk(0).unwrap(), data);
    }

    #[test]
    fn test_lz4_dictionary_frame_roundtrip_preserves_flag() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 4096,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            use_dict: true,
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        let data: Vec<u8> = (0..200_000u32)
            .flat_map(|i| (i % 4096).to_le_bytes())
            .collect();

        schunk.append_buffer(&data).unwrap();
        let frame = schunk.to_frame();
        assert_eq!(frame[85] & 0x01, 0x01);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert!(restored.cparams.use_dict);
        assert_eq!(restored.decompress_chunk(0).unwrap(), data);
    }

    #[test]
    fn test_dictionary_frame_accepts_per_chunk_fallback() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            blocksize: 64,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            use_dict: true,
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        let data = b"small non-special payload".to_vec();

        schunk.append_buffer(&data).unwrap();
        let chunk_header = ChunkHeader::read(schunk.compressed_chunk(0).unwrap()).unwrap();
        assert_eq!(chunk_header.special_type(), BLOSC2_NO_SPECIAL);
        assert!(!chunk_header.use_dict());

        let frame = schunk.to_frame();
        assert_eq!(frame[85] & 0x01, 0x01);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert!(restored.cparams.use_dict);
        assert_eq!(restored.decompress_chunk(0).unwrap(), data);
    }

    #[test]
    fn test_frame_accepts_chunk_dictionary_without_frame_flag() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 4096,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            use_dict: true,
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        let data: Vec<u8> = (0..200_000u32)
            .flat_map(|i| (i % 4096).to_le_bytes())
            .collect();
        schunk.append_buffer(&data).unwrap();
        assert!(ChunkHeader::read(schunk.compressed_chunk(0).unwrap())
            .unwrap()
            .use_dict());

        let mut frame = schunk.to_frame();
        frame[85] &= !0x01;
        let restored = Schunk::from_frame(&frame).unwrap();
        assert!(!restored.cparams.use_dict);
        assert_eq!(restored.decompress_chunk(0).unwrap(), data);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chunk-dict-no-frame-flag.b2frame");
        std::fs::write(&path, &frame).unwrap();
        let lazy = Schunk::open_lazy(&path).unwrap();
        assert!(!lazy.cparams.use_dict);
        assert_eq!(lazy.decompress_chunk(0).unwrap(), data);
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
    fn test_fixed_frame_writer_materializes_special_chunks() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        let first = vec![1u8; 4096];
        let zeroes = vec![0u8; 4096];
        let last = vec![2u8; 4096];
        let expected = [first.clone(), zeroes.clone(), last.clone()].concat();
        schunk.append_buffer(&first).unwrap();
        schunk.append_buffer(&zeroes).unwrap();
        schunk.append_buffer(&last).unwrap();
        assert_eq!(schunk.chunksize, 4096);
        assert_eq!(
            ChunkHeader::read(&schunk.chunks[1]).unwrap().special_type(),
            BLOSC2_SPECIAL_ZERO
        );
        assert_eq!(
            schunk.chunk_offsets(),
            vec![
                0,
                schunk.chunks[0].len() as u64,
                (schunk.chunks[0].len() + schunk.chunks[1].len()) as u64
            ]
        );

        let frame = schunk.to_frame();
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        let cbytes = i64::from_be_bytes(frame[39..47].try_into().unwrap()) as usize;
        let expected_cbytes = schunk.chunks.iter().map(Vec::len).sum::<usize>();
        assert_eq!(cbytes, expected_cbytes);

        let data_start = header_size;
        let data_end = data_start + cbytes;
        let mut pos = data_start;
        for chunk in &schunk.chunks {
            assert_eq!(&frame[pos..pos + chunk.len()], chunk);
            pos += chunk.len();
        }

        let offsets_header = ChunkHeader::read(&frame[data_end..]).unwrap();
        let offsets_end = data_end + offsets_header.cbytes as usize;
        let offsets_payload = compress::decompress(&frame[data_end..offsets_end]).unwrap();
        let offsets: Vec<u64> = offsets_payload
            .chunks_exact(8)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
            .collect();
        assert_eq!(
            offsets,
            vec![
                0,
                schunk.chunks[0].len() as u64,
                (schunk.chunks[0].len() + schunk.chunks[1].len()) as u64
            ]
        );
        assert_eq!(
            Schunk::from_frame(&frame)
                .unwrap()
                .decompress_all()
                .unwrap(),
            expected
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fixed-special.b2frame");
        schunk.to_sframe_dir(&path).unwrap();
        assert!(path.join("00000000.chunk").is_file());
        assert!(path.join("00000001.chunk").is_file());
        assert!(!path.join("00000002.chunk").exists());
        assert_eq!(
            Schunk::open_sframe(&path)
                .unwrap()
                .decompress_all()
                .unwrap(),
            expected
        );
    }

    #[test]
    fn test_compressed_chunk_accessors_borrow_raw_chunk_bytes() {
        let data: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.append_buffer(&data).unwrap();

        let raw = schunk.compressed_chunk(0).unwrap();
        assert_eq!(raw, schunk.chunks[0].as_slice());

        let view = schunk.compressed_chunk_view(0).unwrap();
        assert_eq!(view.as_slice(), raw);
        let (nbytes, cbytes, blocksize) = view.sizes().unwrap();
        assert_eq!(nbytes, data.len());
        assert_eq!(cbytes, raw.len());
        assert!(blocksize > 0);
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
        let codecs = vec![
            BLOSC_BLOSCLZ,
            BLOSC_LZ4,
            BLOSC_LZ4HC,
            BLOSC_ZLIB,
            BLOSC_ZSTD,
        ];

        for compcode in codecs {
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
    fn test_lazy_schunk_file_backed_roundtrip() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.add_metalayer("codec", b"lz4").unwrap();
        schunk.add_vlmetalayer("owner", b"lazy").unwrap();

        let data1: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
        let data2: Vec<u8> = (4096..8192u32).flat_map(|i| i.to_le_bytes()).collect();
        let data3: Vec<u8> = (8192..12288u32).flat_map(|i| i.to_le_bytes()).collect();
        schunk.append_buffer(&data1).unwrap();
        schunk.append_buffer(&data2).unwrap();
        schunk.append_buffer(&data3).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lazy.b2frame");
        schunk.to_file(path.to_str().unwrap()).unwrap();

        let lazy = Schunk::open_lazy(&path).unwrap();
        assert_eq!(lazy.nchunks(), 3);
        assert_eq!(lazy.nbytes, schunk.nbytes);
        assert_eq!(lazy.cbytes, schunk.cbytes);
        assert_eq!(lazy.metalayers[0].content, b"lz4");
        assert_eq!(lazy.vlmetalayers[0].content, b"lazy");
        assert_eq!(lazy.chunk_refs().len(), 3);
        assert!(lazy
            .chunk_refs()
            .windows(2)
            .all(|pair| pair[0].offset + pair[0].cbytes as u64 == pair[1].offset));

        assert_eq!(lazy.decompress_chunk(0).unwrap(), data1);
        assert_eq!(lazy.decompress_chunk(2).unwrap(), data3);

        let start = data1.len() - 8;
        let len = 24;
        let expected: Vec<u8> = data1[data1.len() - 8..]
            .iter()
            .chain(data2[..16].iter())
            .copied()
            .collect();
        assert_eq!(lazy.get_slice(start, len).unwrap(), expected);
        assert_eq!(lazy.chunk_range_for_byte_slice(start, len).unwrap(), 0..2);
        assert!(lazy.decompress_chunk(-1).is_err());
        assert!(lazy.get_slice(schunk.nbytes as usize, 1).is_err());
    }

    #[test]
    fn test_file_open_uses_declared_frame_size_with_trailing_bytes() {
        let mut schunk = Schunk::new(CParams::default(), DParams::default());
        schunk.append_buffer(b"first chunk").unwrap();
        schunk.append_buffer(b"second chunk").unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trailing.b2frame");
        let mut frame = schunk.to_frame();
        frame.extend_from_slice(b"trailing bytes after first frame");
        std::fs::write(&path, frame).unwrap();

        let eager = Schunk::open(path.to_str().unwrap()).unwrap();
        let lazy = Schunk::open_lazy(&path).unwrap();
        assert_eq!(
            eager.decompress_all().unwrap(),
            schunk.decompress_all().unwrap()
        );
        assert_eq!(
            lazy.decompress_all().unwrap(),
            schunk.decompress_all().unwrap()
        );
    }

    #[test]
    fn test_lazy_schunk_variable_chunks() {
        let cparams = CParams {
            compcode: BLOSC_ZSTD,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.append_buffer(b"alpha").unwrap();
        schunk.append_buffer(b"bravo-bravo").unwrap();
        schunk.append_buffer(b"charlie-charlie-charlie").unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lazy-variable.b2frame");
        schunk.to_file(path.to_str().unwrap()).unwrap();

        let lazy = Schunk::open_lazy(&path).unwrap();
        assert_eq!(lazy.chunksize, 0);
        assert_eq!(
            lazy.decompress_all().unwrap(),
            b"alphabravo-bravocharlie-charlie-charlie"
        );
        assert_eq!(lazy.get_slice(3, 10).unwrap(), b"habravo-br");
        assert_eq!(lazy.chunk_range_for_byte_slice(3, 10).unwrap(), 0..2);
    }

    #[test]
    fn test_sparse_frame_directory_roundtrip() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.add_metalayer("kind", b"sframe").unwrap();
        schunk.add_vlmetalayer("owner", b"rust").unwrap();
        let chunks: Vec<Vec<u8>> = (0..3)
            .map(|chunk| {
                (0..1024u32)
                    .flat_map(|i| (i + chunk * 1024).to_le_bytes())
                    .collect()
            })
            .collect();
        for chunk in &chunks {
            schunk.append_buffer(chunk).unwrap();
        }
        let zero_chunk = vec![0u8; chunks[0].len()];
        schunk.append_buffer(&zero_chunk).unwrap();
        let special_header = ChunkHeader::read(schunk.compressed_chunk(3).unwrap()).unwrap();
        assert_eq!(special_header.special_type(), BLOSC2_SPECIAL_ZERO);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("array.b2frame");
        schunk.to_sframe_dir(&path).unwrap();
        assert!(path.join("chunks.b2frame").is_file());
        assert!(path.join("00000000.chunk").is_file());
        assert!(path.join("00000001.chunk").is_file());
        assert!(path.join("00000002.chunk").is_file());
        assert!(!path.join("00000003.chunk").exists());

        let restored = Schunk::open_sframe(&path).unwrap();
        let restored_cbytes: i64 = (0..restored.nchunks())
            .map(|idx| {
                ChunkHeader::read(restored.compressed_chunk(idx).unwrap())
                    .unwrap()
                    .cbytes as i64
            })
            .sum();
        assert_eq!(restored.cbytes, restored_cbytes);
        assert_eq!(restored.metalayer("kind"), Some(&b"sframe"[..]));
        assert_eq!(restored.vlmetalayer("owner"), Some(&b"rust"[..]));
        for (idx, expected) in chunks.iter().enumerate() {
            assert_eq!(
                restored.decompress_chunk(idx as i64).unwrap(),
                expected.as_slice()
            );
        }
        assert_eq!(restored.decompress_chunk(3).unwrap(), zero_chunk);
        let restored_special = ChunkHeader::read(restored.compressed_chunk(3).unwrap()).unwrap();
        assert_eq!(restored_special.special_type(), BLOSC2_SPECIAL_ZERO);

        let opened = Schunk::open(path.to_str().unwrap()).unwrap();
        assert_eq!(
            opened.decompress_all().unwrap(),
            schunk.decompress_all().unwrap()
        );

        let lazy = Schunk::open_lazy_sframe(&path).unwrap();
        assert_eq!(lazy.nchunks(), 4);
        assert_eq!(lazy.chunk_refs()[1].offset, 1);
        assert_eq!(
            lazy.chunk_refs()[3].offset,
            frame::encoded_special_offset(BLOSC2_SPECIAL_ZERO)
        );
        assert_eq!(lazy.chunk_refs()[3].special, Some(BLOSC2_SPECIAL_ZERO));
        assert!(lazy.chunk_refs()[3].cbytes > 0);
        assert_eq!(lazy.decompress_chunk(2).unwrap(), chunks[2]);
        assert_eq!(lazy.get_slice(chunks[0].len() - 4, 12).unwrap(), {
            let mut expected = Vec::new();
            expected.extend_from_slice(&chunks[0][chunks[0].len() - 4..]);
            expected.extend_from_slice(&chunks[1][..8]);
            expected
        });
    }

    #[test]
    fn test_frame_roundtrip_preserves_splitmode() {
        for splitmode in [
            BLOSC_ALWAYS_SPLIT,
            BLOSC_NEVER_SPLIT,
            BLOSC_AUTO_SPLIT,
            BLOSC_FORWARD_COMPAT_SPLIT,
        ] {
            let cparams = CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                splitmode,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            };
            let mut schunk = Schunk::new(cparams, DParams::default());
            schunk.append_buffer(b"splitmode payload").unwrap();

            let frame = schunk.to_frame();
            let restored = Schunk::from_frame(&frame).unwrap();
            assert_eq!(restored.cparams.splitmode, splitmode);

            let dir = tempfile::tempdir().unwrap();
            let frame_path = dir.path().join("splitmode.b2frame");
            std::fs::write(&frame_path, &frame).unwrap();
            let lazy = Schunk::open_lazy(&frame_path).unwrap();
            assert_eq!(lazy.cparams.splitmode, splitmode);

            let sframe_path = dir.path().join("splitmode-sframe.b2frame");
            schunk.to_sframe_dir(&sframe_path).unwrap();
            let restored = Schunk::open_sframe(&sframe_path).unwrap();
            assert_eq!(restored.cparams.splitmode, splitmode);
            let lazy = Schunk::open_lazy_sframe(&sframe_path).unwrap();
            assert_eq!(lazy.cparams.splitmode, splitmode);
        }
    }

    #[test]
    fn test_vlblocks_schunk_frame_roundtrip() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            nthreads: 4,
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        let blocks: [&[u8]; 3] = [b"red\0", b"green-green\0", b"blue-blue-blue-blue\0"];
        schunk.append_vlblocks(&blocks).unwrap();
        assert_eq!(schunk.nchunks(), 1);
        assert_eq!(schunk.chunksize, 0);
        assert!(schunk.append_buffer(b"regular").is_err());
        assert_eq!(
            schunk.decompress_chunk(0).unwrap(),
            b"red\0green-green\0blue-blue-blue-blue\0"
        );

        let frame = schunk.to_frame();
        assert_ne!(frame[25] & FRAME_VL_BLOCKS, 0);
        assert_eq!(frame[25] & FRAME_VARIABLE_CHUNKS, 0);
        assert_eq!(frame[25] & 0x0F, BLOSC2_VERSION_FRAME_FORMAT);
        assert_eq!(
            i32::from_be_bytes(frame[58..62].try_into().unwrap()) as usize,
            b"red\0green-green\0blue-blue-blue-blue\0".len()
        );

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(
            restored.decompress_chunk(0).unwrap(),
            schunk.decompress_chunk(0).unwrap()
        );
        assert!(ChunkHeader::read(&restored.chunks[0]).unwrap().vl_blocks());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vlblocks.b2frame");
        schunk.to_file(path.to_str().unwrap()).unwrap();
        let lazy = Schunk::open_lazy(&path).unwrap();
        assert_eq!(
            lazy.decompress_chunk(0).unwrap(),
            b"red\0green-green\0blue-blue-blue-blue\0"
        );

        let sframe_dir = dir.path().join("vlblocks-sframe.b2frame");
        schunk.to_sframe_dir(&sframe_dir).unwrap();
        let restored = Schunk::open_sframe(&sframe_dir).unwrap();
        assert_eq!(
            restored.decompress_chunk(0).unwrap(),
            b"red\0green-green\0blue-blue-blue-blue\0"
        );
        let lazy = Schunk::open_lazy_sframe(&sframe_dir).unwrap();
        assert_eq!(
            lazy.decompress_chunk(0).unwrap(),
            b"red\0green-green\0blue-blue-blue-blue\0"
        );

        let mut variable = Schunk::new(
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        );
        variable.append_vlblocks(&[b"one".as_slice()]).unwrap();
        variable
            .append_vlblocks(&[b"longer".as_slice(), b"chunk".as_slice()])
            .unwrap();
        let frame = variable.to_frame();
        assert_ne!(frame[25] & FRAME_VL_BLOCKS, 0);
        assert_ne!(frame[25] & FRAME_VARIABLE_CHUNKS, 0);
        assert_eq!(i32::from_be_bytes(frame[58..62].try_into().unwrap()), 0);
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
        assert!(schunk.add_metalayer("author", b"duplicate").is_err());
        schunk.update_metalayer("author", b"updated").unwrap();

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

        let large_name = "x".repeat(32);
        assert!(schunk.add_metalayer(&large_name, b"data").is_err());
        assert!(schunk.update_metalayer("missing", b"data").is_err());
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

        let mut str8_name = frame.clone();
        str8_name[name_marker_pos] = 0xD9;
        assert!(Schunk::from_frame(&str8_name).is_err());

        let mut bin8_content = frame.clone();
        let content_marker_pos = frame
            .windows(b"value".len())
            .position(|window| window == b"value")
            .unwrap()
            - 5;
        bin8_content[content_marker_pos] = 0xC4;
        assert!(Schunk::from_frame(&bin8_content).is_err());

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
        assert!(schunk.add_vlmetalayer("vlmeta2", b"duplicate").is_err());
        schunk.update_vlmetalayer("vlmeta2", b"updated").unwrap();

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
        assert!(schunk.update_vlmetalayer("missing", b"data").is_err());
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

        let mut str8_name = frame.clone();
        let name_marker_pos = trailer_start + 6 + 3;
        str8_name[name_marker_pos] = 0xD9;
        assert!(Schunk::from_frame(&str8_name).is_err());

        let mut bin8_content = frame.clone();
        let index_size = u16::from_be_bytes(
            frame[trailer_start + 4..trailer_start + 6]
                .try_into()
                .unwrap(),
        ) as usize;
        let content_marker_pos = trailer_start + 3 + index_size + 3;
        bin8_content[content_marker_pos] = 0xC4;
        assert!(Schunk::from_frame(&bin8_content).is_err());

        let mut flag_mismatch = frame.clone();
        flag_mismatch[68] = 0xC2;
        assert!(Schunk::from_frame(&flag_mismatch).is_err());

        let footer_start = frame.len() - 23;
        let mut nonzero_fingerprint = frame.clone();
        nonzero_fingerprint[footer_start + 6] = 17;
        nonzero_fingerprint[footer_start + 7] = 0xA5;
        let restored = Schunk::from_frame(&nonzero_fingerprint).unwrap();
        assert_eq!(restored.vlmetalayer("vlmeta"), Some(&b"content"[..]));
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
        bad_codec[27] = 0x07 | (5 << 4);
        assert!(Schunk::from_frame(&bad_codec).is_err());

        let mut bad_frame_type = frame.clone();
        bad_frame_type[26] = 1;
        assert!(Schunk::from_frame(&bad_frame_type).is_err());

        let mut bad_clevel = frame.clone();
        bad_clevel[27] = BLOSC_LZ4 | (10 << 4);
        assert_eq!(
            Schunk::from_frame(&bad_clevel)
                .unwrap()
                .decompress_all()
                .unwrap(),
            data
        );

        let mut bad_filter_count = frame.clone();
        bad_filter_count[70] = BLOSC2_MAX_FILTERS as u8 + 1;
        assert!(Schunk::from_frame(&bad_filter_count).is_err());

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
        let mut schunk = Schunk::new(cparams.clone(), DParams::default());
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

        let mut reserved_compformat = frame.clone();
        reserved_compformat[header_size + BLOSC2_CHUNK_FLAGS] =
            (reserved_compformat[header_size + BLOSC2_CHUNK_FLAGS] & !0xe0) | (2 << 5);
        assert!(Schunk::from_frame(&reserved_compformat).is_err());

        let mut memcpy_schunk = Schunk::new(
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 0,
                typesize: 4,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
            DParams::default(),
        );
        memcpy_schunk.append_buffer(&data).unwrap();
        let mut memcpy_frame = memcpy_schunk.to_frame();
        let memcpy_header_size =
            i32::from_be_bytes(memcpy_frame[11..15].try_into().unwrap()) as usize;
        memcpy_frame[memcpy_header_size + BLOSC2_CHUNK_FLAGS] =
            (memcpy_frame[memcpy_header_size + BLOSC2_CHUNK_FLAGS] & !0xe0)
                | (BLOSC_SCHUNK_FORMAT << 5);
        assert_eq!(
            Schunk::from_frame(&memcpy_frame)
                .unwrap()
                .decompress_chunk(0)
                .unwrap(),
            data
        );

        let mut special_schunk = Schunk::new(cparams, DParams::default());
        special_schunk.append_buffer(b"alpha").unwrap();
        special_schunk.append_buffer(&[0u8; 8]).unwrap();
        special_schunk.append_buffer(b"charlie-charlie").unwrap();
        let mut special_frame = special_schunk.to_frame();
        let special_header_size =
            i32::from_be_bytes(special_frame[11..15].try_into().unwrap()) as usize;
        let special_pos = special_header_size + special_schunk.chunks[0].len();
        special_frame[special_pos + BLOSC2_CHUNK_FLAGS] =
            (special_frame[special_pos + BLOSC2_CHUNK_FLAGS] & !0xe0) | (BLOSC_UDCODEC_FORMAT << 5);
        assert_eq!(
            Schunk::from_frame(&special_frame)
                .unwrap()
                .decompress_chunk(1)
                .unwrap(),
            vec![0u8; 8]
        );

        let mut bad_flags = frame.clone();
        bad_flags[header_size + BLOSC2_CHUNK_BLOSC2_FLAGS2] = BLOSC2_VL_BLOCKS;
        assert!(Schunk::from_frame(&bad_flags).is_err());

        let mut mismatched_codec = frame.clone();
        mismatched_codec[27] = BLOSC_BLOSCLZ | (5 << 4);
        assert!(Schunk::from_frame(&mismatched_codec).is_err());

        let mut mismatched_filter = frame.clone();
        mismatched_filter[71 + BLOSC2_MAX_FILTERS - 1] = BLOSC_NOFILTER;
        assert!(Schunk::from_frame(&mismatched_filter).is_err());

        let mut ignored_filter_slots = frame.clone();
        ignored_filter_slots[70] = 1;
        ignored_filter_slots[71] = BLOSC_NOFILTER;
        ignored_filter_slots[79] = 0;
        ignored_filter_slots[72..77].fill(99);
        ignored_filter_slots[80..85].fill(99);
        assert_eq!(
            Schunk::from_frame(&ignored_filter_slots)
                .unwrap()
                .decompress_chunk(0)
                .unwrap(),
            data
        );
    }

    #[test]
    fn test_frame_rejects_malformed_materialized_chunks_and_offsets() {
        let data = b"abcdefgh".repeat(16);
        let mut memcpy_schunk = Schunk::new(
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 0,
                typesize: 1,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
            DParams::default(),
        );
        memcpy_schunk.append_buffer(&data).unwrap();
        let frame = memcpy_schunk.to_frame();
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        let old_cbytes = i64::from_be_bytes(frame[39..47].try_into().unwrap()) as usize;
        let chunk_cbytes = i32::from_le_bytes(
            frame[header_size + 12..header_size + 16]
                .try_into()
                .unwrap(),
        );

        let mut bad_memcpy = frame.clone();
        bad_memcpy[header_size + 12..header_size + 16]
            .copy_from_slice(&(chunk_cbytes - 1).to_le_bytes());
        bad_memcpy.remove(header_size + old_cbytes - 1);
        bad_memcpy[39..47].copy_from_slice(&((old_cbytes - 1) as i64).to_be_bytes());
        let bad_memcpy_len = bad_memcpy.len() as u64;
        bad_memcpy[16..24].copy_from_slice(&bad_memcpy_len.to_be_bytes());
        assert!(Schunk::from_frame(&bad_memcpy).is_err());

        let mut bad_blocksize = frame.clone();
        bad_blocksize
            [header_size + BLOSC2_CHUNK_BLOCKSIZE..header_size + BLOSC2_CHUNK_BLOCKSIZE + 4]
            .copy_from_slice(&((BLOSC2_MAXBLOCKSIZE as i32).wrapping_add(1)).to_le_bytes());
        assert!(Schunk::from_frame(&bad_blocksize).is_err());
        let dir = tempfile::tempdir().unwrap();
        let bad_blocksize_path = dir.path().join("bad-blocksize.b2frame");
        std::fs::write(&bad_blocksize_path, &bad_blocksize).unwrap();
        assert!(Schunk::open_lazy(&bad_blocksize_path).is_err());

        let mut equal_chunks = Schunk::new(
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 0,
                typesize: 1,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
            DParams::default(),
        );
        equal_chunks.append_buffer(b"chunk-one").unwrap();
        equal_chunks.append_buffer(b"chunk-two").unwrap();
        let frame = equal_chunks.to_frame();
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        let data_cbytes = i64::from_be_bytes(frame[39..47].try_into().unwrap()) as usize;
        let data_end = header_size + data_cbytes;
        let offsets_header = ChunkHeader::read(&frame[data_end..]).unwrap();
        let offsets_end = data_end + offsets_header.cbytes as usize;
        let mut offsets_payload = compress::decompress(&frame[data_end..offsets_end]).unwrap();
        offsets_payload[8..16].copy_from_slice(&0u64.to_le_bytes());
        let offsets_chunk = compress::compress(
            &offsets_payload,
            &CParams {
                compcode: BLOSC_BLOSCLZ,
                clevel: 5,
                typesize: 8,
                splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            },
        )
        .unwrap();
        let mut duplicate_offset = Vec::new();
        duplicate_offset.extend_from_slice(&frame[..data_end]);
        duplicate_offset.extend_from_slice(&offsets_chunk);
        duplicate_offset.extend_from_slice(&frame[offsets_end..]);
        let duplicate_offset_len = duplicate_offset.len() as u64;
        duplicate_offset[16..24].copy_from_slice(&duplicate_offset_len.to_be_bytes());

        assert!(Schunk::from_frame(&duplicate_offset).is_err());

        let path = dir.path().join("duplicate-offset.b2frame");
        std::fs::write(&path, &duplicate_offset).unwrap();
        assert!(Schunk::open_lazy(&path).is_err());
    }
}
