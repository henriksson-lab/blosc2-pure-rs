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
use crate::utils::{normalize_urlpath, normalized_path};
use rayon::prelude::*;
use std::borrow::Cow;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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
pub struct Schunk {
    /// Compression parameters used when adding or replacing chunks.
    pub cparams: CParams,
    /// Decompression parameters used when reading chunks.
    pub dparams: DParams,
    /// Compressed chunks stored in memory.
    ///
    /// For B2ND metadata views, this public vector is a compatibility snapshot:
    /// use [`Schunk`] chunk mutator methods instead of mutating it directly so
    /// sibling views can observe shared backing changes.
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
    vlmetalayer_encoded: Vec<Option<Vec<u8>>>,
    storage: FrameStorage,
    frame_offsets: Option<Vec<u64>>,
    attached_frame_len: Option<i64>,
    attached_frame: Option<AttachedFrame>,
    variable_chunks: bool,
    vlblocks: bool,
    shared_chunks: Option<Arc<Mutex<SharedChunks>>>,
    shared_chunks_generation: AtomicU64,
}

impl Clone for Schunk {
    fn clone(&self) -> Self {
        Self {
            cparams: self.cparams.clone(),
            dparams: self.dparams.clone(),
            chunks: self.chunks.clone(),
            chunksize: self.chunksize,
            nbytes: self.nbytes,
            cbytes: self.cbytes,
            metalayers: self.metalayers.clone(),
            vlmetalayers: self.vlmetalayers.clone(),
            vlmetalayer_encoded: self.vlmetalayer_encoded.clone(),
            storage: self.storage,
            frame_offsets: self.frame_offsets.clone(),
            attached_frame_len: self.attached_frame_len,
            attached_frame: None,
            variable_chunks: self.variable_chunks,
            vlblocks: self.vlblocks,
            shared_chunks: None,
            shared_chunks_generation: AtomicU64::new(0),
        }
    }
}

struct SharedChunks {
    chunks: Vec<Vec<u8>>,
    generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FrameStorage {
    Contiguous,
    Sparse,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AttachedFrame {
    path: PathBuf,
    storage: FrameStorage,
}

/// File-backed reference to a compressed chunk in a frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LazyChunkRef {
    /// Absolute byte offset for contiguous frames, or chunk-file ID for sparse frames.
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
    frame_offset: u64,
    chunks: Vec<LazyChunkRef>,
    sframe: bool,
}

/// Shared source for `blosc2_schunk_get_lazychunk`-style access.
///
/// In-memory [`Schunk`] values return their stored compressed chunk, while
/// file-backed [`LazySchunk`] values return a synthesized lazy chunk that
/// points back into the frame.
pub trait SchunkLazyChunkAccessor {
    /// Return `(chunk, needs_free)`, following the C accessor's ownership flag.
    fn lazychunk_for_c(&self, nchunk: i64) -> Result<(Vec<u8>, bool), String>;
}

impl SchunkLazyChunkAccessor for Schunk {
    fn lazychunk_for_c(&self, nchunk: i64) -> Result<(Vec<u8>, bool), String> {
        self.compressed_chunk(nchunk)
            .map(|chunk| (chunk.to_vec(), false))
            .map_err(str::to_string)
    }
}

impl SchunkLazyChunkAccessor for LazySchunk {
    fn lazychunk_for_c(&self, nchunk: i64) -> Result<(Vec<u8>, bool), String> {
        self.lazy_chunk(nchunk).map(|chunk| (chunk, true))
    }
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

    /// Return a compressed chunk by index, reading only that chunk from disk.
    pub fn compressed_chunk(&self, nchunk: i64) -> Result<Vec<u8>, String> {
        self.read_chunk_bytes(nchunk)
    }

    pub fn lazy_chunk(&self, nchunk: i64) -> Result<Vec<u8>, String> {
        self.read_lazy_chunk(nchunk)
    }

    /// Decompress one VL-block from a chunk in this file-backed super-chunk.
    pub fn decompress_vlblock(&self, nchunk: i64, nblock: usize) -> Result<Vec<u8>, String> {
        let chunk = self.read_chunk_bytes(nchunk)?;
        if !ChunkHeader::read(&chunk)
            .map_err(|err| err.to_string())?
            .vl_blocks()
        {
            return Err("Schunk does not contain VL-block chunks".into());
        }
        let mut dparams = self.dparams.clone();
        dparams.nchunk = nchunk;
        compress::vldecompress_block_with_params(&chunk, nblock, &dparams).map_err(str::to_string)
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

    /// Read an item slice `[start, stop)` using `self.cparams.typesize`.
    pub fn get_slice_items(&self, start: usize, stop: usize) -> Result<Vec<u8>, String> {
        let (byte_start, byte_len) =
            item_slice_to_byte_range(start, stop, self.cparams.typesize as usize)
                .map_err(str::to_string)?;
        self.get_slice(byte_start, byte_len)
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
        use std::io::{Read, Seek, SeekFrom};
        if self.sframe {
            let mut file = std::fs::File::open(sframe_chunk_path(&self.path, chunk_ref.offset))
                .map_err(|e| format!("Failed to open sparse frame chunk: {e}"))?;
            let mut chunk = vec![0u8; chunk_ref.cbytes];
            file.read_exact(&mut chunk)
                .map_err(|e| format!("Failed to read sparse frame chunk: {e}"))?;
            compress::cbuffer_validate(&chunk).map_err(|err| format!("Invalid frame: {err}"))?;
            return Ok(chunk);
        }
        let mut file = std::fs::File::open(&self.path)
            .map_err(|e| format!("Failed to open frame file: {e}"))?;
        file.seek(SeekFrom::Start(chunk_ref.offset))
            .map_err(|e| format!("Failed to seek to chunk: {e}"))?;
        let mut chunk = vec![0u8; chunk_ref.cbytes];
        file.read_exact(&mut chunk)
            .map_err(|e| format!("Failed to read chunk: {e}"))?;
        compress::cbuffer_validate(&chunk).map_err(|err| format!("Invalid frame: {err}"))?;
        Ok(chunk)
    }

    fn read_lazy_chunk(&self, nchunk: i64) -> Result<Vec<u8>, String> {
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

        let mut file = if self.sframe {
            std::fs::File::open(sframe_chunk_path(&self.path, chunk_ref.offset))
                .map_err(|e| format!("Failed to open sparse frame chunk: {e}"))?
        } else {
            std::fs::File::open(&self.path)
                .map_err(|e| format!("Failed to open frame file: {e}"))?
        };
        let chunk_offset = if self.sframe { 0 } else { chunk_ref.offset };
        let mut header_buf = vec![0u8; BLOSC_EXTENDED_HEADER_LENGTH];
        read_lazy_exact_at(
            &mut file,
            chunk_offset,
            &mut header_buf,
            if self.sframe {
                "Failed to read sparse frame chunk"
            } else {
                "Failed to read chunk"
            },
        )?;
        let header = ChunkHeader::read(&header_buf)
            .map_err(|_| "Invalid frame: invalid chunk header".to_string())?;
        if header.cbytes < BLOSC_EXTENDED_HEADER_LENGTH as i32 || header.blocksize <= 0 {
            return Err("Invalid frame: invalid chunk header".into());
        }
        if header.cbytes as usize != chunk_ref.cbytes {
            return Err("Invalid frame: chunk size mismatch".into());
        }

        let special_type = header.special_type();
        if special_type == BLOSC2_SPECIAL_VALUE {
            let typesize = usize::from(header.typesize);
            let lazy_len = BLOSC_EXTENDED_HEADER_LENGTH
                .checked_add(typesize)
                .ok_or_else(|| "Invalid frame: lazy chunk size overflow".to_string())?;
            if lazy_len > chunk_ref.cbytes {
                return Err("Invalid frame: lazy chunk extends past chunk".into());
            }
            let mut lazy = vec![0u8; lazy_len];
            read_lazy_exact_at(
                &mut file,
                chunk_offset,
                &mut lazy,
                if self.sframe {
                    "Failed to read sparse frame chunk"
                } else {
                    "Failed to read chunk"
                },
            )?;
            return Ok(lazy);
        }
        if special_type != BLOSC2_NO_SPECIAL {
            return Err("Invalid frame: invalid lazy chunk special type".into());
        }

        let nblocks = if header.vl_blocks() {
            usize::try_from(header.blocksize)
                .map_err(|_| "Invalid frame: invalid chunk block count".to_string())?
        } else {
            header.nblocks()
        };
        if nblocks == 0 || nblocks > i32::MAX as usize {
            return Err("Invalid frame: invalid chunk block count".into());
        }
        let bstarts_len = nblocks
            .checked_mul(std::mem::size_of::<i32>())
            .ok_or_else(|| "Invalid frame: lazy chunk size overflow".to_string())?;
        let trailer_offset = BLOSC_EXTENDED_HEADER_LENGTH
            .checked_add(bstarts_len)
            .ok_or_else(|| "Invalid frame: lazy chunk size overflow".to_string())?;
        let trailer_len = std::mem::size_of::<i32>()
            .checked_add(std::mem::size_of::<i64>())
            .and_then(|len| len.checked_add(bstarts_len))
            .ok_or_else(|| "Invalid frame: lazy chunk size overflow".to_string())?;
        let lazy_len = trailer_offset
            .checked_add(trailer_len)
            .ok_or_else(|| "Invalid frame: lazy chunk size overflow".to_string())?;
        if lazy_len > i32::MAX as usize {
            return Err("Invalid frame: lazy chunk too large".into());
        }

        let memcpyed = header.memcpyed();
        let streams_offset = if memcpyed {
            BLOSC_EXTENDED_HEADER_LENGTH
        } else {
            trailer_offset
        };
        if streams_offset > chunk_ref.cbytes {
            return Err("Invalid frame: lazy chunk extends past chunk".into());
        }
        let mut lazy = vec![0u8; lazy_len];
        read_lazy_exact_at(
            &mut file,
            chunk_offset,
            &mut lazy[..streams_offset],
            if self.sframe {
                "Failed to read sparse frame chunk"
            } else {
                "Failed to read chunk"
            },
        )?;
        lazy[BLOSC2_CHUNK_BLOSC2_FLAGS] |= BLOSC2_LAZY_CHUNK;

        if self.sframe {
            let chunk_id = chunk_ref.offset as u32 as i32;
            lazy[trailer_offset..trailer_offset + 4].copy_from_slice(&chunk_id.to_le_bytes());
            lazy[trailer_offset + 4..trailer_offset + 12]
                .copy_from_slice(&(chunk_ref.offset as i64).to_le_bytes());
        } else {
            let nchunk_i32 = i32::try_from(nchunk)
                .map_err(|_| "Invalid frame: chunk index out of range".to_string())?;
            let frame_relative_offset = chunk_ref
                .offset
                .checked_sub(self.frame_offset)
                .ok_or_else(|| "Invalid frame: chunk offset before frame".to_string())?;
            lazy[trailer_offset..trailer_offset + 4].copy_from_slice(&nchunk_i32.to_le_bytes());
            lazy[trailer_offset + 4..trailer_offset + 12]
                .copy_from_slice(&(frame_relative_offset as i64).to_le_bytes());
        }

        let block_csizes = if memcpyed {
            lazy_memcpy_block_csizes(&header, nblocks)?
        } else {
            lazy_compressed_block_csizes(
                &lazy[BLOSC_EXTENDED_HEADER_LENGTH..BLOSC_EXTENDED_HEADER_LENGTH + bstarts_len],
                header.cbytes as usize,
            )?
        };
        let mut csize_pos = lazy_len - bstarts_len;
        for csize in block_csizes {
            lazy[csize_pos..csize_pos + 4].copy_from_slice(&csize.to_le_bytes());
            csize_pos += 4;
        }
        Ok(lazy)
    }
}

fn read_lazy_exact_at(
    file: &mut std::fs::File,
    offset: u64,
    buf: &mut [u8],
    context: &str,
) -> Result<(), String> {
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| format!("{context}: seek failed: {e}"))?;
    file.read_exact(buf)
        .map_err(|e| format!("{context}: read failed: {e}"))
}

fn lazy_memcpy_block_csizes(header: &ChunkHeader, nblocks: usize) -> Result<Vec<i32>, String> {
    let blocksize = usize::try_from(header.blocksize)
        .map_err(|_| "Invalid frame: invalid chunk blocksize".to_string())?;
    let nbytes = usize::try_from(header.nbytes)
        .map_err(|_| "Invalid frame: invalid chunk nbytes".to_string())?;
    if blocksize == 0 || nblocks == 0 {
        return Err("Invalid frame: invalid chunk block count".into());
    }
    let mut csizes = vec![
        i32::try_from(blocksize)
            .map_err(|_| "Invalid frame: chunk blocksize too large".to_string())?;
        nblocks
    ];
    let leftover = nbytes % blocksize;
    if leftover != 0 {
        csizes[nblocks - 1] = i32::try_from(leftover)
            .map_err(|_| "Invalid frame: chunk blocksize too large".to_string())?;
    }
    Ok(csizes)
}

fn lazy_compressed_block_csizes(bstarts: &[u8], chunk_cbytes: usize) -> Result<Vec<i32>, String> {
    if bstarts.is_empty() || !bstarts.len().is_multiple_of(4) {
        return Err("Invalid frame: invalid block starts".into());
    }
    let mut starts = Vec::with_capacity(bstarts.len() / 4);
    for (idx, bytes) in bstarts.chunks_exact(4).enumerate() {
        let start = i32::from_le_bytes(bytes.try_into().unwrap());
        if start < 0 {
            return Err("Invalid frame: invalid block start".into());
        }
        let start =
            usize::try_from(start).map_err(|_| "Invalid frame: invalid block start".to_string())?;
        if start > chunk_cbytes {
            return Err("Invalid frame: block start out of range".into());
        }
        starts.push((start, idx));
    }
    let mut sorted = starts.clone();
    sorted.sort_by_key(|&(start, _)| start);
    let mut csizes = vec![0i32; starts.len()];
    for pos in 0..sorted.len() {
        let (start, idx) = sorted[pos];
        let end = sorted
            .get(pos + 1)
            .map(|&(next, _)| next)
            .unwrap_or(chunk_cbytes);
        if end < start {
            return Err("Invalid frame: invalid block start order".into());
        }
        csizes[idx] = i32::try_from(end - start)
            .map_err(|_| "Invalid frame: block compressed size too large".to_string())?;
    }
    Ok(csizes)
}

fn synthetic_special_chunk_for_params(
    special_type: u8,
    nbytes: usize,
    cparams: &CParams,
) -> Result<Vec<u8>, String> {
    if nbytes > i32::MAX as usize {
        return Err("Invalid frame: special chunk is too large".to_string());
    }
    let normalized_cparams = compress::normalized_cparams(cparams);
    let typesize = normalized_cparams.typesize as usize;
    if typesize == 0 || normalized_cparams.blocksize < 0 {
        return Err("Invalid frame: invalid special chunk parameters".to_string());
    }
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

fn validate_compressed_chunk_for_schunk(
    schunk: &Schunk,
    chunk: &[u8],
    replacing: Option<usize>,
) -> Result<(), &'static str> {
    let header = ChunkHeader::read(chunk)?;
    let (_, cbytes, _) = compress::cbuffer_sizes(chunk)?;
    if cbytes != chunk.len() {
        return Err("Compressed chunk size mismatch");
    }

    let new_vl = header.vl_blocks();
    let existing_vl = schunk
        .chunks
        .iter()
        .enumerate()
        .filter(|(idx, _)| Some(*idx) != replacing)
        .try_fold(None, |seen, (_, stored)| {
            let stored_vl = ChunkHeader::read(stored)?.vl_blocks();
            match seen {
                Some(prev) if prev != stored_vl => Err("Cannot mix regular and VL-block chunks"),
                Some(prev) => Ok(Some(prev)),
                None => Ok(Some(stored_vl)),
            }
        })?;

    if existing_vl.is_some_and(|existing_vl| existing_vl != new_vl) {
        return Err("Cannot mix regular and VL-block chunks");
    }
    Ok(())
}

fn schunk_cparams_raw_copy_compatible(src: &CParams, dst: &CParams) -> bool {
    src.compcode == dst.compcode
        && src.clevel == dst.clevel
        && src.typesize == dst.typesize
        && src.blocksize == dst.blocksize
        && src.use_dict == dst.use_dict
        && src.filters == dst.filters
        && src.filters_meta == dst.filters_meta
        && src.prefilter.is_none()
        && dst.prefilter.is_none()
}

fn schunk_b2nd_metalayer(schunk: &Schunk) -> Option<Vec<u8>> {
    schunk
        .metalayer("b2nd")
        .map(<[u8]>::to_vec)
        .or_else(|| schunk.cparams.b2nd_metalayer.clone())
        .or_else(|| schunk.dparams.b2nd_metalayer.clone())
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
            vlmetalayer_encoded: Vec::new(),
            storage: FrameStorage::Contiguous,
            frame_offsets: None,
            attached_frame_len: None,
            attached_frame: None,
            variable_chunks: false,
            vlblocks: false,
            shared_chunks: None,
        }
    }

    pub(crate) fn enable_shared_chunks(&mut self) {
        if self.shared_chunks.is_none() {
            self.shared_chunks = Some(Arc::new(Mutex::new(self.chunks.clone())));
        }
    }

    pub(crate) fn clone_with_shared_chunks(&self) -> Self {
        let mut cloned = self.clone();
        cloned.shared_chunks = Some(
            self.shared_chunks
                .as_ref()
                .cloned()
                .unwrap_or_else(|| Arc::new(Mutex::new(self.chunks.clone()))),
        );
        cloned
    }

    fn active_chunks_len(&self) -> usize {
        self.shared_chunks
            .as_ref()
            .map(|chunks| chunks.lock().expect("shared chunks lock poisoned").len())
            .unwrap_or(self.chunks.len())
    }

    fn replace_active_chunk(&mut self, idx: usize, chunk: Vec<u8>) -> Result<(), &'static str> {
        if let Some(shared_chunks) = &self.shared_chunks {
            let mut chunks = shared_chunks.lock().expect("shared chunks lock poisoned");
            if idx >= chunks.len() {
                return Err("Chunk index out of range");
            }
            chunks[idx] = chunk;
            self.chunks = chunks.clone();
        } else {
            self.chunks[idx] = chunk;
        }
        Ok(())
    }

    fn insert_active_chunk(&mut self, idx: usize, chunk: Vec<u8>) {
        if let Some(shared_chunks) = &self.shared_chunks {
            let mut chunks = shared_chunks.lock().expect("shared chunks lock poisoned");
            chunks.insert(idx, chunk);
            self.chunks = chunks.clone();
        } else {
            self.chunks.insert(idx, chunk);
        }
    }

    fn push_active_chunk(&mut self, chunk: Vec<u8>) {
        if let Some(shared_chunks) = &self.shared_chunks {
            let mut chunks = shared_chunks.lock().expect("shared chunks lock poisoned");
            chunks.push(chunk);
            self.chunks = chunks.clone();
        } else {
            self.chunks.push(chunk);
        }
    }

    fn remove_active_chunk(&mut self, idx: usize) -> Result<Vec<u8>, &'static str> {
        if let Some(shared_chunks) = &self.shared_chunks {
            let mut chunks = shared_chunks.lock().expect("shared chunks lock poisoned");
            if idx >= chunks.len() {
                return Err("Chunk index out of range");
            }
            let removed = chunks.remove(idx);
            self.chunks = chunks.clone();
            Ok(removed)
        } else {
            Ok(self.chunks.remove(idx))
        }
    }

    /// Number of chunks.
    pub fn nchunks(&self) -> i64 {
        self.active_chunks_len() as i64
    }

    /// C-name-style accessor returning compression parameters by value.
    pub fn get_cparams(&self) -> CParams {
        self.cparams.clone()
    }

    /// C-name-style accessor returning decompression parameters by value.
    pub fn get_dparams(&self) -> DParams {
        self.dparams.clone()
    }

    /// Compress and append a data buffer as a new chunk.
    /// Returns the resulting number of chunks, matching the C API.
    pub fn append_buffer(&mut self, data: &[u8]) -> Result<i64, &'static str> {
        if self.vlblocks {
            return Err("Cannot mix regular and VL-block chunks");
        }
        let mut cparams = self.cparams.clone();
        cparams.nchunk = self.active_chunks_len() as i64;
        cparams.schunk = self as *const Self as usize;
        cparams.b2nd_metalayer = schunk_b2nd_metalayer(self);
        let chunk = compress::compress(data, &cparams)?;

        let new_chunksize = if self.active_chunks_len() == 0 {
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
        self.push_active_chunk(chunk);
        self.frame_offsets = None;
        self.attached_frame_len = None;
        self.refresh_chunk_shape()?;
        self.persist_attached_frame()?;

        Ok(self.active_chunks_len() as i64)
    }

    /// Append an already-compressed chunk.
    ///
    /// The chunk is stored byte-for-byte after validating its Blosc header and
    /// regular/VL-block consistency with the existing super-chunk.
    pub fn append_chunk(&mut self, chunk: &[u8]) -> Result<i64, &'static str> {
        validate_compressed_chunk_for_schunk(self, chunk, None)?;
        self.push_active_chunk(chunk.to_vec());
        self.recompute_metadata()?;
        Ok(self.active_chunks_len() as i64)
    }

    /// C-style `blosc2_schunk_append_chunk` adapter.
    pub fn append_chunk_c(&mut self, chunk: &[u8], _copy: bool) -> i64 {
        self.append_chunk(chunk)
            .unwrap_or(i64::from(BLOSC2_ERROR_CHUNK_APPEND))
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
                cparams.schunk = self as *const Self as usize;
                cparams.b2nd_metalayer = schunk_b2nd_metalayer(self);
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
        self.frame_offsets = None;
        self.attached_frame_len = None;
        if self.chunksize == 0 {
            self.chunksize = buffers[0].len();
        }
        self.refresh_chunk_shape()?;
        self.persist_attached_frame()?;
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
        cparams.schunk = self as *const Self as usize;
        cparams.b2nd_metalayer = schunk_b2nd_metalayer(self);
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
        self.frame_offsets = None;
        self.attached_frame_len = None;
        self.persist_attached_frame()?;
        Ok(self.chunks.len() as i64)
    }

    /// Compress independent variable-length blocks and insert them as one
    /// VL-block chunk at `nchunk`.
    pub fn insert_vlblocks(&mut self, nchunk: i64, blocks: &[&[u8]]) -> Result<i64, &'static str> {
        if !self.chunks.is_empty() && !self.vlblocks {
            return Err("Cannot mix regular and VL-block chunks");
        }
        if nchunk < 0 || nchunk as usize > self.chunks.len() {
            return Err("Chunk index out of range");
        }
        let idx = nchunk as usize;
        let mut cparams = self.cparams.clone();
        cparams.nchunk = nchunk;
        cparams.schunk = self as *const Self as usize;
        cparams.b2nd_metalayer = schunk_b2nd_metalayer(self);
        let chunk = compress::vlcompress(blocks, &cparams)?;
        self.chunks.insert(idx, chunk);
        self.frame_offsets = None;
        self.attached_frame_len = None;
        self.recompute_metadata()?;
        Ok(self.chunks.len() as i64)
    }

    /// Compress independent variable-length blocks and replace VL-block chunk
    /// `nchunk` with the result.
    pub fn update_vlblocks(&mut self, nchunk: i64, blocks: &[&[u8]]) -> Result<i64, &'static str> {
        if !self.vlblocks {
            return Err("Schunk does not contain VL-block chunks");
        }
        if nchunk < 0 || nchunk as usize >= self.chunks.len() {
            return Err("Chunk index out of range");
        }
        let mut cparams = self.cparams.clone();
        cparams.nchunk = nchunk;
        cparams.schunk = self as *const Self as usize;
        cparams.b2nd_metalayer = schunk_b2nd_metalayer(self);
        let chunk = compress::vlcompress(blocks, &cparams)?;
        self.chunks[nchunk as usize] = chunk;
        self.frame_offsets = None;
        self.attached_frame_len = None;
        self.recompute_metadata()?;
        Ok(self.chunks.len() as i64)
    }

    /// Decompress a chunk by index.
    /// Returns the decompressed data.
    pub fn decompress_chunk(&self, nchunk: i64) -> Result<Vec<u8>, &'static str> {
        if nchunk < 0 {
            return Err("Chunk index out of range");
        }
        let idx = nchunk as usize;
        if idx >= self.active_chunks_len() {
            return Err("Chunk index out of range");
        }
        let mut dparams = self.dparams.clone();
        dparams.nchunk = nchunk;
        dparams.schunk = self as *const Self as usize;
        dparams.b2nd_metalayer = schunk_b2nd_metalayer(self);
        if let Some(shared_chunks) = &self.shared_chunks {
            let chunks = shared_chunks.lock().expect("shared chunks lock poisoned");
            return compress::decompress_with_dparams(&chunks[idx], &dparams);
        }
        compress::decompress_with_dparams(&self.chunks[idx], &dparams)
    }

    /// Decompress one variable-length block from a VL-block chunk.
    pub fn decompress_vlblock(&self, nchunk: i64, nblock: usize) -> Result<Vec<u8>, &'static str> {
        if !self.vlblocks {
            return Err("Schunk does not contain VL-block chunks");
        }
        if nchunk < 0 {
            return Err("Chunk index out of range");
        }
        let idx = nchunk as usize;
        if idx >= self.chunks.len() {
            return Err("Chunk index out of range");
        }
        let mut dparams = self.dparams.clone();
        dparams.nchunk = nchunk;
        dparams.schunk = self as *const Self as usize;
        dparams.b2nd_metalayer = schunk_b2nd_metalayer(self);
        compress::vldecompress_block_with_params(&self.chunks[idx], nblock, &dparams)
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

    pub(crate) fn compressed_chunks(&self) -> impl Iterator<Item = &[u8]> {
        self.chunks.iter().map(Vec::as_slice)
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
        if idx >= self.active_chunks_len() {
            return Err("Chunk index out of range");
        }
        let mut dparams = self.dparams.clone();
        dparams.nchunk = nchunk;
        dparams.schunk = self as *const Self as usize;
        dparams.b2nd_metalayer = schunk_b2nd_metalayer(self);
        if let Some(shared_chunks) = &self.shared_chunks {
            let chunks = shared_chunks.lock().expect("shared chunks lock poisoned");
            return compress::decompress_into_with_dparams(&chunks[idx], dest, &dparams);
        }
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
        cparams.schunk = self as *const Self as usize;
        cparams.b2nd_metalayer = schunk_b2nd_metalayer(self);
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

        self.insert_active_chunk(nchunk as usize, chunk);
        self.frame_offsets = None;
        self.attached_frame_len = None;
        self.chunksize = new_chunksize;
        self.nbytes = new_nbytes;
        self.cbytes = new_cbytes;
        self.refresh_chunk_shape()?;
        self.persist_attached_frame()?;

        Ok(self.chunks.len() as i64)
    }

    /// Insert an already-compressed chunk before `nchunk`.
    ///
    /// The chunk is stored byte-for-byte after validating its Blosc header and
    /// regular/VL-block consistency with the existing super-chunk.
    pub fn insert_chunk(&mut self, nchunk: i64, chunk: &[u8]) -> Result<i64, &'static str> {
        if nchunk < 0 || nchunk as usize > self.chunks.len() {
            return Err("Chunk index out of range");
        }
        validate_compressed_chunk_for_schunk(self, chunk, None)?;
        self.insert_active_chunk(nchunk as usize, chunk.to_vec());
        self.recompute_metadata()?;
        Ok(self.active_chunks_len() as i64)
    }

    /// C-style `blosc2_schunk_insert_chunk` adapter.
    pub fn insert_chunk_c(&mut self, nchunk: i64, chunk: &[u8], _copy: bool) -> i64 {
        self.insert_chunk(nchunk, chunk)
            .unwrap_or(i64::from(BLOSC2_ERROR_CHUNK_INSERT))
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
        self.insert_active_chunk(nchunk as usize, chunk);
        self.recompute_metadata()?;
        Ok(self.active_chunks_len() as i64)
    }

    /// Fill an empty super-chunk with zero, NaN, or uninitialized special chunks.
    ///
    /// `nitems` is measured in logical items of `self.cparams.typesize`.
    /// `chunksize` is measured in bytes and must be non-zero when `nitems` is
    /// non-zero.
    pub fn fill_special(
        &mut self,
        nitems: usize,
        special: u8,
        chunksize: usize,
    ) -> Result<i64, &'static str> {
        if !self.chunks.is_empty() || self.nbytes != 0 || self.cbytes != 0 {
            return Err("Can only fill an empty schunk");
        }
        if self.vlblocks {
            return Err("Cannot mix regular and VL-block chunks");
        }
        if !matches!(
            special,
            BLOSC2_SPECIAL_ZERO | BLOSC2_SPECIAL_NAN | BLOSC2_SPECIAL_UNINIT
        ) {
            return Err("Unsupported special value");
        }
        let cparams = compress::normalized_cparams(&self.cparams);
        let typesize = cparams.typesize as usize;
        if typesize == 0 {
            return Err("Invalid typesize");
        }
        let total_nbytes = nitems
            .checked_mul(typesize)
            .ok_or("Schunk nbytes overflow")?;
        if total_nbytes != 0 && chunksize == 0 {
            return Err("Invalid chunksize");
        }
        let chunk_nbytes = if chunksize != 0 {
            chunksize
        } else if self.chunksize != 0 {
            self.chunksize
        } else {
            total_nbytes
        };
        if total_nbytes != 0
            && (chunk_nbytes == 0
                || chunk_nbytes > BLOSC2_MAX_BUFFERSIZE as usize
                || !chunk_nbytes.is_multiple_of(typesize))
        {
            return Err("Invalid chunksize");
        }
        let nchunks = if total_nbytes == 0 {
            0
        } else {
            total_nbytes.div_ceil(chunk_nbytes)
        };
        if nchunks > i32::MAX as usize {
            return Err("Too many chunks");
        }
        for idx in 0..nchunks {
            let start = idx * chunk_nbytes;
            let nbytes = (total_nbytes - start).min(chunk_nbytes);
            let chunk = match special {
                BLOSC2_SPECIAL_ZERO => compress::blosc2_chunk_zeros_with_cparams(nbytes, &cparams),
                BLOSC2_SPECIAL_NAN => compress::blosc2_chunk_nans_with_cparams(nbytes, &cparams),
                BLOSC2_SPECIAL_UNINIT => {
                    compress::blosc2_chunk_uninit_with_cparams(nbytes, &cparams)
                }
                _ => unreachable!(),
            }?;
            self.chunks.push(chunk);
        }
        self.frame_offsets = None;
        self.attached_frame_len = None;
        self.chunksize = chunk_nbytes;
        self.nbytes = i64::try_from(total_nbytes).map_err(|_| "Schunk nbytes overflow")?;
        self.cbytes = self.chunks.iter().try_fold(0i64, |acc, chunk| {
            acc.checked_add(chunk.len() as i64)
                .ok_or("Schunk cbytes overflow")
        })?;
        self.refresh_chunk_shape()?;
        if nchunks == 0 {
            self.chunksize = chunk_nbytes;
        }
        self.persist_attached_frame()?;
        Ok(self.chunks.len() as i64)
    }

    /// Fill an empty super-chunk with repeat-value special chunks.
    ///
    /// `nitems` is measured in logical items of `self.cparams.typesize`.
    /// `chunksize` is measured in bytes; pass `0` to use `self.chunksize`.
    pub fn fill_repeatval(
        &mut self,
        nitems: usize,
        value: &[u8],
        chunksize: usize,
    ) -> Result<i64, &'static str> {
        if !self.chunks.is_empty() || self.nbytes != 0 || self.cbytes != 0 {
            return Err("Can only fill an empty schunk");
        }
        if self.vlblocks {
            return Err("Cannot mix regular and VL-block chunks");
        }
        let cparams = compress::normalized_cparams(&self.cparams);
        let typesize = cparams.typesize as usize;
        if typesize == 0 || value.len() != typesize {
            return Err("Invalid repeat value size");
        }
        let total_nbytes = nitems
            .checked_mul(typesize)
            .ok_or("Schunk nbytes overflow")?;
        let chunk_nbytes = if chunksize != 0 {
            chunksize
        } else if self.chunksize != 0 {
            self.chunksize
        } else {
            total_nbytes
        };
        if total_nbytes != 0
            && (chunk_nbytes == 0
                || chunk_nbytes > BLOSC2_MAX_BUFFERSIZE as usize
                || !chunk_nbytes.is_multiple_of(typesize))
        {
            return Err("Invalid chunksize");
        }
        let nchunks = if total_nbytes == 0 {
            0
        } else {
            total_nbytes.div_ceil(chunk_nbytes)
        };
        for idx in 0..nchunks {
            let start = idx * chunk_nbytes;
            let nbytes = (total_nbytes - start).min(chunk_nbytes);
            self.chunks
                .push(compress::blosc2_chunk_repeatval_with_cparams(
                    nbytes, value, &cparams,
                )?);
        }
        self.frame_offsets = None;
        self.attached_frame_len = None;
        self.chunksize = chunk_nbytes;
        self.nbytes = i64::try_from(total_nbytes).map_err(|_| "Schunk nbytes overflow")?;
        self.cbytes = self.chunks.iter().try_fold(0i64, |acc, chunk| {
            acc.checked_add(chunk.len() as i64)
                .ok_or("Schunk cbytes overflow")
        })?;
        self.refresh_chunk_shape()?;
        if nchunks == 0 {
            self.chunksize = chunk_nbytes;
        }
        self.persist_attached_frame()?;
        Ok(self.chunks.len() as i64)
    }

    /// Delete a chunk and return the resulting number of chunks, matching the C API.
    pub fn delete_chunk(&mut self, nchunk: i64) -> Result<i64, &'static str> {
        if nchunk < 0 || nchunk as usize >= self.active_chunks_len() {
            return Err("Chunk index out of range");
        }
        let idx = nchunk as usize;

        self.remove_active_chunk(idx)?;
        self.recompute_metadata()?;

        Ok(self.active_chunks_len() as i64)
    }

    /// C-style `blosc2_schunk_delete_chunk` adapter.
    pub fn delete_chunk_c(&mut self, nchunk: i64) -> i64 {
        self.delete_chunk(nchunk).unwrap_or(-1)
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
        if nchunk < 0 || nchunk as usize >= self.active_chunks_len() {
            return Err("Chunk index out of range");
        }

        let idx = nchunk as usize;
        let mut cparams = self.cparams.clone();
        cparams.nchunk = nchunk;
        cparams.schunk = self as *const Self as usize;
        cparams.b2nd_metalayer = schunk_b2nd_metalayer(self);
        let chunk = compress::compress(data, &cparams)?;
        self.replace_active_chunk(idx, chunk)?;
        self.recompute_metadata()?;
        Ok(self.active_chunks_len() as i64)
    }

    /// Replace a chunk with an already-compressed chunk.
    ///
    /// The chunk is stored byte-for-byte after validating its Blosc header and
    /// regular/VL-block consistency with the existing super-chunk.
    pub fn update_compressed_chunk(
        &mut self,
        nchunk: i64,
        chunk: &[u8],
    ) -> Result<i64, &'static str> {
        if nchunk < 0 || nchunk as usize >= self.active_chunks_len() {
            return Err("Chunk index out of range");
        }
        validate_compressed_chunk_for_schunk(self, chunk, Some(nchunk as usize))?;
        self.replace_active_chunk(nchunk as usize, chunk.to_vec())?;
        self.recompute_metadata()?;
        Ok(self.active_chunks_len() as i64)
    }

    /// C-style `blosc2_schunk_update_chunk` adapter.
    pub fn update_compressed_chunk_c(&mut self, nchunk: i64, chunk: &[u8], _copy: bool) -> i64 {
        self.update_compressed_chunk(nchunk, chunk)
            .unwrap_or(i64::from(BLOSC2_ERROR_CHUNK_UPDATE))
    }

    /// Deep-copy the super-chunk.
    pub fn copy_schunk(&self) -> Self {
        self.clone()
    }

    /// Deep-copy the super-chunk using destination compression and
    /// decompression parameters.
    pub fn copy_schunk_with_params(
        &self,
        cparams: CParams,
        dparams: DParams,
    ) -> Result<Self, &'static str> {
        let mut copied = Schunk::new(cparams, dparams);
        copied.metalayers = self.metalayers.clone();
        copied.vlmetalayers = self.vlmetalayers.clone();
        copied.vlmetalayer_encoded = vec![None; copied.vlmetalayers.len()];

        if self.vlblocks
            || (schunk_cparams_raw_copy_compatible(&self.cparams, &copied.cparams)
                && self.dparams.postfilter.is_none())
        {
            for chunk in &self.chunks {
                copied.append_chunk(chunk)?;
            }
        } else {
            for idx in 0..self.chunks.len() {
                let data = self.decompress_chunk(idx as i64)?;
                copied.append_buffer(&data)?;
            }
        }

        Ok(copied)
    }

    /// Add a named fixed-size metalayer.
    pub fn add_metalayer(&mut self, name: &str, content: &[u8]) -> Result<(), &'static str> {
        self.add_metalayer_index(name, content).map(|_| ())
    }

    /// Add a fixed-size metalayer and return its frame-order index.
    pub fn add_metalayer_index(
        &mut self,
        name: &str,
        content: &[u8],
    ) -> Result<usize, &'static str> {
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
        self.persist_attached_frame()?;
        Ok(self.metalayers.len() - 1)
    }

    /// Replace an existing fixed-size metalayer payload.
    pub fn update_metalayer(&mut self, name: &str, content: &[u8]) -> Result<(), &'static str> {
        self.update_metalayer_index(name, content).map(|_| ())
    }

    /// Replace a fixed-size metalayer payload and return its frame-order index.
    pub fn update_metalayer_index(
        &mut self,
        name: &str,
        content: &[u8],
    ) -> Result<usize, &'static str> {
        validate_metalayer_name(name)?;
        let pos = self
            .metalayers
            .iter()
            .position(|layer| layer.name == name)
            .ok_or("Metalayer does not exist")?;
        if content.len() > self.metalayers[pos].content.len() {
            return Err("Fixed-size metalayer cannot grow");
        }
        self.metalayers[pos].content[..content.len()].copy_from_slice(content);
        self.persist_attached_frame()?;
        Ok(pos)
    }

    /// Return a metalayer payload by name.
    pub fn metalayer(&self, name: &str) -> Option<&[u8]> {
        self.metalayers
            .iter()
            .find(|layer| layer.name == name)
            .map(|layer| layer.content.as_slice())
    }

    /// Return the frame-order index of a fixed-size metalayer by name.
    pub fn metalayer_index(&self, name: &str) -> Option<usize> {
        self.metalayers.iter().position(|layer| layer.name == name)
    }

    /// Return whether a fixed-size metalayer exists.
    pub fn metalayer_exists(&self, name: &str) -> bool {
        self.metalayer_index(name).is_some()
    }

    /// Return fixed-size metalayer names in frame order.
    pub fn metalayer_names(&self) -> Vec<&str> {
        self.metalayers
            .iter()
            .map(|layer| layer.name.as_str())
            .collect()
    }

    /// Remove a metalayer by name and return its payload.
    pub fn remove_metalayer(&mut self, name: &str) -> Option<Vec<u8>> {
        let pos = self
            .metalayers
            .iter()
            .position(|layer| layer.name == name)?;
        let content = self.metalayers.remove(pos).content;
        let _ = self.persist_attached_frame();
        Some(content)
    }

    /// C-style fixed-size metalayer existence check: returns the frame-order
    /// index on success, or a negative `BLOSC2_ERROR_*` code.
    pub fn meta_exists_c(&self, name: &str) -> i32 {
        if validate_metalayer_name(name).is_err() {
            return BLOSC2_ERROR_INVALID_PARAM;
        }
        self.metalayer_index(name)
            .map(|idx| idx as i32)
            .unwrap_or(BLOSC2_ERROR_NOT_FOUND)
    }

    /// C-style fixed-size metalayer add: returns the new frame-order index, or
    /// a negative `BLOSC2_ERROR_*` code.
    pub fn meta_add_c(&mut self, name: &str, content: &[u8]) -> i32 {
        match self.add_metalayer_index_c(name, content) {
            Ok(idx) => idx as i32,
            Err(err) => metalayer_error_code(err),
        }
    }

    fn add_metalayer_index_c(&mut self, name: &str, content: &[u8]) -> Result<usize, &'static str> {
        if self.metalayers.iter().any(|layer| layer.name == name) {
            return Err("Metalayer already exists");
        }
        if self.metalayers.len() >= BLOSC2_MAX_METALAYERS {
            return Err("Too many metalayers");
        }
        self.metalayers.push(Metalayer {
            name: name.to_string(),
            content: content.to_vec(),
        });
        self.persist_attached_frame()?;
        Ok(self.metalayers.len() - 1)
    }

    /// C-style fixed-size metalayer update: returns the existing frame-order
    /// index. Matching C-Blosc2, an oversized update returns the index and
    /// leaves the fixed-size payload unchanged.
    pub fn meta_update_c(&mut self, name: &str, content: &[u8]) -> i32 {
        if validate_metalayer_name(name).is_err() {
            return BLOSC2_ERROR_INVALID_PARAM;
        }
        let Some(pos) = self.metalayer_index(name) else {
            return BLOSC2_ERROR_NOT_FOUND;
        };
        if content.len() > self.metalayers[pos].content.len() {
            return pos as i32;
        }
        self.metalayers[pos].content[..content.len()].copy_from_slice(content);
        if let Err(err) = self.persist_attached_frame() {
            return schunk_error_code(err);
        }
        pos as i32
    }

    /// C-style fixed-size metalayer delete: returns the remaining fixed
    /// metalayer count, or a negative `BLOSC2_ERROR_*` code.
    pub fn meta_delete_c(&mut self, name: &str) -> i32 {
        if validate_metalayer_name(name).is_err() {
            return BLOSC2_ERROR_INVALID_PARAM;
        }
        match self.metalayers.iter().position(|layer| layer.name == name) {
            Some(pos) => {
                self.metalayers.remove(pos);
                if let Err(err) = self.persist_attached_frame() {
                    return schunk_error_code(err);
                }
                self.metalayers.len() as i32
            }
            None => BLOSC2_ERROR_NOT_FOUND,
        }
    }

    /// C-style fixed-size metalayer names query: returns `(count, names)`.
    pub fn meta_get_names_c(&self) -> (i32, Vec<&str>) {
        (self.metalayers.len() as i32, self.metalayer_names())
    }

    /// Add a named variable-length metalayer.
    pub fn add_vlmetalayer(&mut self, name: &str, content: &[u8]) -> Result<(), &'static str> {
        self.add_vlmetalayer_inner(name, content, None).map(|_| ())
    }

    /// Add a named variable-length metalayer and return its frame-order index.
    pub fn add_vlmetalayer_index(
        &mut self,
        name: &str,
        content: &[u8],
    ) -> Result<usize, &'static str> {
        self.add_vlmetalayer_inner(name, content, None)
    }

    /// Add a named variable-length metalayer compressed with explicit
    /// parameters when serialized into a frame.
    pub fn add_vlmetalayer_with_cparams(
        &mut self,
        name: &str,
        content: &[u8],
        cparams: CParams,
    ) -> Result<(), &'static str> {
        self.add_vlmetalayer_inner(name, content, Some(cparams))
            .map(|_| ())
    }

    fn add_vlmetalayer_inner(
        &mut self,
        name: &str,
        content: &[u8],
        cparams: Option<CParams>,
    ) -> Result<usize, &'static str> {
        validate_vlmetalayer_name(name)?;
        if self.vlmetalayers.iter().any(|layer| layer.name == name) {
            return Err("VL-metalayer already exists");
        }
        let encoded = match cparams {
            Some(params) => Some(compress_vlmetalayer_content_with_cparams(content, &params)?),
            None => None,
        };

        self.vlmetalayers.push(Metalayer {
            name: name.to_string(),
            content: content.to_vec(),
        });
        self.vlmetalayer_encoded.push(encoded);
        self.persist_attached_frame()?;
        Ok(self.vlmetalayers.len() - 1)
    }

    /// Replace an existing variable-length metalayer payload.
    pub fn update_vlmetalayer(&mut self, name: &str, content: &[u8]) -> Result<(), &'static str> {
        self.update_vlmetalayer_inner(name, content, None)
            .map(|_| ())
    }

    /// Replace an existing variable-length metalayer payload and return its
    /// frame-order index.
    pub fn update_vlmetalayer_index(
        &mut self,
        name: &str,
        content: &[u8],
    ) -> Result<usize, &'static str> {
        self.update_vlmetalayer_inner(name, content, None)
    }

    /// Replace an existing variable-length metalayer payload and compression
    /// parameters used when serialized into a frame.
    pub fn update_vlmetalayer_with_cparams(
        &mut self,
        name: &str,
        content: &[u8],
        cparams: CParams,
    ) -> Result<(), &'static str> {
        self.update_vlmetalayer_inner(name, content, Some(cparams))
            .map(|_| ())
    }

    fn update_vlmetalayer_inner(
        &mut self,
        name: &str,
        content: &[u8],
        cparams: Option<CParams>,
    ) -> Result<usize, &'static str> {
        validate_vlmetalayer_name(name)?;
        let pos = self
            .vlmetalayers
            .iter()
            .position(|layer| layer.name == name)
            .ok_or("VL-metalayer does not exist")?;
        let encoded = match cparams {
            Some(params) => Some(compress_vlmetalayer_content_with_cparams(content, &params)?),
            None => None,
        };
        let encoded_len = encoded.as_ref().map_or_else(
            || compress_vlmetalayer_content(content).map(|compressed| compressed.len()),
            |compressed| Ok(compressed.len()),
        )?;
        validate_vlmetalayers_encoded_size_parts(self.vlmetalayer_encoded_sizes().map(
            |(layer_name, len)| {
                if layer_name == self.vlmetalayers[pos].name {
                    (name, encoded_len)
                } else {
                    (layer_name, len)
                }
            },
        ))?;
        self.vlmetalayers[pos].content.clear();
        self.vlmetalayers[pos].content.extend_from_slice(content);
        self.vlmetalayer_encoded[pos] = encoded;
        self.persist_attached_frame()?;
        Ok(pos)
    }

    /// Return a variable-length metalayer payload by name.
    pub fn vlmetalayer(&self, name: &str) -> Option<&[u8]> {
        self.vlmetalayers
            .iter()
            .find(|layer| layer.name == name)
            .map(|layer| layer.content.as_slice())
    }

    /// Return the frame-order index of a variable-length metalayer by name.
    pub fn vlmetalayer_index(&self, name: &str) -> Option<usize> {
        self.vlmetalayers
            .iter()
            .position(|layer| layer.name == name)
    }

    /// Return whether a variable-length metalayer exists.
    pub fn vlmetalayer_exists(&self, name: &str) -> bool {
        self.vlmetalayer_index(name).is_some()
    }

    /// Remove a variable-length metalayer by name and return its payload.
    pub fn remove_vlmetalayer(&mut self, name: &str) -> Option<Vec<u8>> {
        let pos = self
            .vlmetalayers
            .iter()
            .position(|layer| layer.name == name)?;
        if pos < self.vlmetalayer_encoded.len() {
            self.vlmetalayer_encoded.remove(pos);
        }
        let content = self.vlmetalayers.remove(pos).content;
        let _ = self.persist_attached_frame();
        Some(content)
    }

    /// Return VL-metalayer names in frame order.
    pub fn vlmetalayer_names(&self) -> Vec<&str> {
        self.vlmetalayers
            .iter()
            .map(|layer| layer.name.as_str())
            .collect()
    }

    /// C-style variable-length metalayer existence check: returns the
    /// frame-order index on success, or a negative `BLOSC2_ERROR_*` code.
    pub fn vlmeta_exists_c(&self, name: &str) -> i32 {
        if validate_vlmetalayer_name(name).is_err() {
            return BLOSC2_ERROR_INVALID_PARAM;
        }
        self.vlmetalayer_index(name)
            .map(|idx| idx as i32)
            .unwrap_or(BLOSC2_ERROR_NOT_FOUND)
    }

    /// C-style variable-length metalayer add: returns the new frame-order
    /// index, or a negative `BLOSC2_ERROR_*` code.
    pub fn vlmeta_add_c(&mut self, name: &str, content: &[u8]) -> i32 {
        match self.add_vlmetalayer_index_c(name, content, None) {
            Ok(idx) => idx as i32,
            Err(err) => metalayer_error_code(err),
        }
    }

    fn add_vlmetalayer_index_c(
        &mut self,
        name: &str,
        content: &[u8],
        cparams: Option<CParams>,
    ) -> Result<usize, &'static str> {
        if self.vlmetalayers.iter().any(|layer| layer.name == name) {
            return Err("VL-metalayer already exists");
        }
        if self.vlmetalayers.len() >= BLOSC2_MAX_VLMETALAYERS {
            return Err("Too many VL-metalayers");
        }
        let encoded = match cparams {
            Some(params) => Some(compress_vlmetalayer_content_with_cparams(content, &params)?),
            None => None,
        };
        self.vlmetalayers.push(Metalayer {
            name: name.to_string(),
            content: content.to_vec(),
        });
        self.vlmetalayer_encoded.push(encoded);
        self.persist_attached_frame()?;
        Ok(self.vlmetalayers.len() - 1)
    }

    /// C-style variable-length metalayer update: returns the existing
    /// frame-order index, or a negative `BLOSC2_ERROR_*` code.
    pub fn vlmeta_update_c(&mut self, name: &str, content: &[u8]) -> i32 {
        match self.update_vlmetalayer_index(name, content) {
            Ok(idx) => idx as i32,
            Err(err) => metalayer_error_code(err),
        }
    }

    /// C-style variable-length metalayer delete: returns the remaining
    /// VL-metalayer count, or a negative `BLOSC2_ERROR_*` code.
    pub fn vlmeta_delete_c(&mut self, name: &str) -> i32 {
        if validate_vlmetalayer_name(name).is_err() {
            return BLOSC2_ERROR_INVALID_PARAM;
        }
        match self
            .vlmetalayers
            .iter()
            .position(|layer| layer.name == name)
        {
            Some(pos) => {
                self.vlmetalayers.remove(pos);
                if pos < self.vlmetalayer_encoded.len() {
                    self.vlmetalayer_encoded.remove(pos);
                }
                if let Err(err) = self.persist_attached_frame() {
                    return schunk_error_code(err);
                }
                self.vlmetalayers.len() as i32
            }
            None => BLOSC2_ERROR_NOT_FOUND,
        }
    }

    /// C-style variable-length metalayer names query: returns `(count, names)`.
    pub fn vlmeta_get_names_c(&self) -> (i32, Vec<&str>) {
        (self.vlmetalayers.len() as i32, self.vlmetalayer_names())
    }

    pub(crate) fn copy_vlmetalayers_to(&self, dst: &mut Schunk) -> Result<(), &'static str> {
        for (idx, layer) in self.vlmetalayers.iter().enumerate() {
            if let Some(encoded) = self.vlmetalayer_encoded.get(idx).and_then(Option::as_ref) {
                dst.vlmetalayers.push(layer.clone());
                dst.vlmetalayer_encoded.push(Some(encoded.clone()));
                validate_vlmetalayers_encoded_size_parts(dst.vlmetalayer_encoded_sizes())?;
            } else {
                dst.add_vlmetalayer(&layer.name, &layer.content)?;
            }
        }
        Ok(())
    }

    fn vlmetalayer_encoded_sizes(&self) -> impl Iterator<Item = (&str, usize)> {
        self.vlmetalayers.iter().enumerate().map(|(idx, layer)| {
            let len = self
                .vlmetalayer_encoded
                .get(idx)
                .and_then(Option::as_ref)
                .map_or_else(
                    || {
                        compress_vlmetalayer_content(&layer.content)
                            .map(|compressed| compressed.len())
                            .unwrap_or(usize::MAX)
                    },
                    Vec::len,
                );
            (layer.name.as_str(), len)
        })
    }

    /// Return decompressed bytes spanning the whole super-chunk.
    pub fn decompress_all(&self) -> Result<Vec<u8>, &'static str> {
        let capacity = usize::try_from(self.nbytes).map_err(|_| "Invalid schunk nbytes")?;
        if self.dparams.nthreads > 1 && self.chunks.len() > 1 {
            let mut dparams = self.dparams.clone();
            dparams.nthreads = 1;
            dparams.schunk = self as *const Self as usize;
            dparams.b2nd_metalayer = schunk_b2nd_metalayer(self);
            let chunks: Vec<Vec<u8>> = compress::with_thread_pool(self.dparams.nthreads, || {
                self.chunks
                    .par_iter()
                    .enumerate()
                    .map(|(idx, chunk)| {
                        let mut chunk_dparams = dparams.clone();
                        chunk_dparams.nchunk = idx as i64;
                        chunk_dparams.schunk = self as *const Self as usize;
                        chunk_dparams.b2nd_metalayer = schunk_b2nd_metalayer(self);
                        compress::decompress_with_dparams(chunk, &chunk_dparams)
                    })
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

    /// Read an item slice `[start, stop)` using `self.cparams.typesize`.
    pub fn get_slice_items(&self, start: usize, stop: usize) -> Result<Vec<u8>, &'static str> {
        let (byte_start, byte_len) =
            item_slice_to_byte_range(start, stop, self.cparams.typesize as usize)?;
        self.get_slice(byte_start, byte_len)
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
        if self.vlblocks {
            return Err("Cannot set byte slices on VL-block chunks");
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
                    let mut cparams = self.cparams.clone();
                    cparams.nchunk = idx as i64;
                    cparams.schunk = self as *const Self as usize;
                    cparams.b2nd_metalayer = schunk_b2nd_metalayer(self);
                    replacements.push((idx, compress::compress(&chunk_data, &cparams)?));
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

    /// Overwrite an item slice `[start, stop)` using `self.cparams.typesize`.
    pub fn set_slice_items(
        &mut self,
        start: usize,
        stop: usize,
        data: &[u8],
    ) -> Result<(), &'static str> {
        let (byte_start, byte_len) =
            item_slice_to_byte_range(start, stop, self.cparams.typesize as usize)?;
        if data.len() != byte_len {
            return Err("Slice data size does not match item range");
        }
        self.set_slice(byte_start, data)
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

    /// C-style `blosc2_schunk_reorder_offsets` adapter.
    pub fn reorder_offsets_c(&mut self, order: &[i64]) -> i32 {
        self.reorder_chunks(order)
            .map(|()| BLOSC2_ERROR_SUCCESS)
            .unwrap_or_else(schunk_error_code)
    }

    /// Return compressed chunk offsets relative to the frame data section.
    pub fn chunk_offsets(&self) -> Vec<u64> {
        let mut offsets = Vec::with_capacity(self.chunks.len());
        let encode_special_offsets = self.chunksize > 0;
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

    /// C-name-style frame offsets accessor.
    pub fn frame_get_offsets(&self) -> Result<Vec<i64>, &'static str> {
        self.frame_offsets
            .as_ref()
            .map_or_else(|| self.chunk_offsets(), Clone::clone)
            .into_iter()
            .map(|offset| i64::try_from(offset).map_err(|_| "Frame offset too large"))
            .collect()
    }

    /// Return the chunk indexes touched by the half-open byte slice `start..stop`.
    pub fn get_slice_nchunks(&self, start: usize, stop: usize) -> Result<Vec<i64>, &'static str> {
        if stop < start {
            return Err("Invalid slice bounds");
        }
        let range = self.chunk_range_for_byte_slice(start, stop - start)?;
        range
            .map(|idx| i64::try_from(idx).map_err(|_| "Chunk index too large"))
            .collect()
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
                let next_offset = offset.checked_add(nbytes).ok_or("Slice offset overflow")?;
                if start == offset {
                    return Ok(idx..idx);
                }
                if start < next_offset {
                    return Ok(idx..idx + 1);
                }
                offset = next_offset;
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
        self.frame_offsets = None;
        self.attached_frame_len = None;
        if let Some(shared_chunks) = &self.shared_chunks {
            self.chunks = shared_chunks
                .lock()
                .expect("shared chunks lock poisoned")
                .clone();
        }
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
        self.persist_attached_frame()
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

    /// Return the serialized contiguous frame length in bytes.
    pub fn frame_len(&self) -> Result<i64, &'static str> {
        i64::try_from(self.to_frame().len()).map_err(|_| "Frame too large")
    }

    /// Deserialize from a contiguous frame buffer.
    pub fn from_frame(data: &[u8]) -> Result<Self, String> {
        frame::read_frame(data)
    }

    /// Write to a file in b2frame format.
    pub fn to_file(&self, path: &str) -> std::io::Result<()> {
        self.to_file_len(path).map(|_| ())
    }

    /// Write to a file in b2frame format, returning the written frame length.
    pub fn to_file_len(&self, path: &str) -> std::io::Result<i64> {
        validate_c_frame_metalayers(self)?;
        let path = normalize_urlpath(path);
        let file = std::fs::File::create(path)?;
        let mut writer = BufWriter::new(file);
        frame::write_frame_to_writer(self, &mut writer)?;
        writer.flush()?;
        self.frame_len()
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
    }

    /// Append this schunk as a contiguous frame to `path`.
    ///
    /// Returns the byte offset where the frame starts.
    pub fn append_file(&self, path: impl AsRef<Path>) -> std::io::Result<u64> {
        validate_c_frame_metalayers(self)?;
        let path = normalized_path(path.as_ref());
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path.as_ref())?;
        let offset = file.metadata()?.len();
        let mut writer = BufWriter::new(file);
        frame::write_frame_to_writer(self, &mut writer)?;
        writer.flush()?;
        Ok(offset)
    }

    /// Write to a sparse frame directory.
    pub fn to_sframe_dir(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        validate_c_frame_metalayers(self)?;
        let path = normalized_path(path.as_ref());
        frame::write_sframe_dir(self, path.as_ref())
    }

    /// Write this super-chunk using its current frame storage kind.
    pub fn save(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        match self.storage {
            FrameStorage::Contiguous => self.to_file(path.as_ref().to_string_lossy().as_ref()),
            FrameStorage::Sparse => self.to_sframe_dir(path),
        }
    }

    /// Open a b2frame file or sparse frame directory.
    pub fn open(path: &str) -> Result<Self, String> {
        let path = normalize_urlpath(path);
        if Path::new(path).is_dir() {
            return Self::open_sframe(Path::new(path));
        }
        let data = std::fs::read(path).map_err(|e| format!("Failed to read file: {e}"))?;
        let frame_len = frame::declared_frame_size(&data).unwrap_or(data.len());
        let mut schunk = Self::from_frame(&data[..frame_len])?;
        schunk.attach_frame(PathBuf::from(path), FrameStorage::Contiguous);
        Ok(schunk)
    }

    /// Open a b2frame file starting at `offset` bytes into `path`.
    ///
    /// This is useful for files containing multiple concatenated frames or an
    /// embedded frame preceded by an application-specific prefix.
    pub fn open_offset(path: impl AsRef<Path>, offset: u64) -> Result<Self, String> {
        let path = normalized_path(path.as_ref());
        if path.as_ref().is_dir() {
            return frame::read_sframe_dir_at(path.as_ref(), offset);
        }
        let data = std::fs::read(path.as_ref()).map_err(|e| format!("Failed to read file: {e}"))?;
        let offset = usize::try_from(offset).map_err(|_| "Frame offset too large".to_string())?;
        if offset > data.len() {
            return Err("Frame offset beyond end of file".into());
        }
        let frame = &data[offset..];
        let frame_len = frame::declared_frame_size(frame).unwrap_or(frame.len());
        let mut schunk = Self::from_frame(&frame[..frame_len])?;
        if offset == 0 {
            schunk.attach_frame(path.as_ref().to_path_buf(), FrameStorage::Contiguous);
        }
        Ok(schunk)
    }

    /// Open a contiguous frame from any reader/seek source at `offset`.
    ///
    /// This mirrors the common custom-I/O path in C-Blosc2 while keeping Rust's
    /// public API generic over `Read + Seek`.
    pub fn open_from_reader_at<R: Read + Seek>(mut reader: R, offset: u64) -> Result<Self, String> {
        reader
            .seek(SeekFrom::Start(offset))
            .map_err(|e| format!("Failed to seek to frame: {e}"))?;
        let mut data = Vec::new();
        reader
            .read_to_end(&mut data)
            .map_err(|e| format!("Failed to read frame: {e}"))?;
        let frame_len = frame::declared_frame_size(&data).unwrap_or(data.len());
        Self::from_frame(&data[..frame_len])
    }

    /// Open a sparse frame directory.
    pub fn open_sframe(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = normalized_path(path.as_ref());
        let mut schunk = frame::read_sframe_dir(path.as_ref())?;
        schunk.attach_frame(path.into_owned(), FrameStorage::Sparse);
        Ok(schunk)
    }

    /// Open a b2frame file or sparse frame directory lazily, keeping compressed chunks on disk until read.
    pub fn open_lazy(path: impl AsRef<Path>) -> Result<LazySchunk, String> {
        let path = normalized_path(path.as_ref());
        if path.as_ref().is_dir() {
            return frame::read_lazy_sframe_dir(path.as_ref());
        }
        frame::read_lazy_frame(path.as_ref())
    }

    /// Open a contiguous frame lazily from `offset` bytes in a file.
    pub fn open_lazy_offset(path: impl AsRef<Path>, offset: u64) -> Result<LazySchunk, String> {
        let path = normalized_path(path.as_ref());
        if path.as_ref().is_dir() {
            return frame::read_lazy_sframe_dir_at(path.as_ref(), offset);
        }
        frame::read_lazy_frame_at(path.as_ref(), offset)
    }

    /// Open a sparse frame directory lazily.
    pub fn open_lazy_sframe(path: impl AsRef<Path>) -> Result<LazySchunk, String> {
        let path = normalized_path(path.as_ref());
        frame::read_lazy_sframe_dir(path.as_ref())
    }

    pub(crate) fn storage(&self) -> FrameStorage {
        self.storage
    }

    pub(crate) fn set_storage(&mut self, storage: FrameStorage) {
        self.storage = storage;
    }

    fn attach_frame(&mut self, path: PathBuf, storage: FrameStorage) {
        self.storage = storage;
        self.attached_frame = Some(AttachedFrame { path, storage });
    }

    fn persist_attached_frame(&mut self) -> Result<(), &'static str> {
        let Some(attached) = self.attached_frame.clone() else {
            return Ok(());
        };
        match attached.storage {
            FrameStorage::Contiguous => {
                self.to_file_len(attached.path.to_string_lossy().as_ref())
                    .map_err(|_| "Failed to write attached frame")?;
                self.attached_frame_len = self.frame_len().ok();
                self.frame_offsets = None;
            }
            FrameStorage::Sparse => {
                self.to_sframe_dir(&attached.path)
                    .map_err(|_| "Failed to write attached frame")?;
                let index_len = std::fs::metadata(attached.path.join("chunks.b2frame"))
                    .map_err(|_| "Failed to write attached frame")?
                    .len();
                self.attached_frame_len = i64::try_from(index_len).ok();
                self.frame_offsets = None;
            }
        }
        self.storage = attached.storage;
        self.attached_frame = Some(attached);
        Ok(())
    }
}

/// C-name adapter for [`Schunk::append_chunk_c`].
pub fn blosc2_schunk_append_chunk(schunk: &mut Schunk, chunk: &[u8], copy: bool) -> i64 {
    schunk.append_chunk_c(chunk, copy)
}

/// C-style explicit-size adapter for [`Schunk::append_chunk_c`].
pub fn blosc2_schunk_append_chunk_c(
    schunk: &mut Schunk,
    chunk: &[u8],
    cbytes: i64,
    copy: bool,
) -> i64 {
    let chunk = match schunk_checked_buffer_prefix(chunk, cbytes) {
        Ok(chunk) => chunk,
        Err(code) => return i64::from(code),
    };
    blosc2_schunk_append_chunk(schunk, chunk, copy)
}

/// C-style constructor using defaults for omitted parameter blocks.
pub fn blosc2_schunk_new_c(
    cparams: Option<CParams>,
    dparams: Option<DParams>,
) -> (i32, Option<Schunk>) {
    (
        BLOSC2_ERROR_SUCCESS,
        Some(Schunk::new(
            cparams.unwrap_or_default(),
            dparams.unwrap_or_default(),
        )),
    )
}

/// Nullable lifecycle adapter for C API parity.
pub fn blosc2_schunk_free_c(_schunk: Option<Schunk>) -> i32 {
    BLOSC2_ERROR_SUCCESS
}

/// Rust-friendly alias for [`Schunk::from_frame`].
pub fn blosc2_schunk_from_buffer_vec(frame: &[u8]) -> Result<Schunk, String> {
    Schunk::from_frame(frame)
}

/// C-name adapter for [`Schunk::from_frame`].
pub fn blosc2_schunk_from_buffer(frame: &[u8], len: i64, _copy: bool) -> Result<Schunk, String> {
    if len < 0 {
        return Err("Invalid frame length".into());
    }
    let len = len as usize;
    if len > frame.len() {
        return Err("Invalid frame length".into());
    }
    let frame = &frame[..len];
    if let Some(frame_size) = frame::declared_frame_size(frame) {
        if frame_size != len {
            return Err("Invalid frame length".into());
        }
    }
    Schunk::from_frame(frame)
}

/// C-style status adapter for [`Schunk::from_frame`].
pub fn blosc2_schunk_from_buffer_c(frame: &[u8], len: i64, copy: bool) -> (i32, Option<Schunk>) {
    if len < 0 {
        return (BLOSC2_ERROR_INVALID_PARAM, None);
    }
    match blosc2_schunk_from_buffer(frame, len, copy) {
        Ok(schunk) => (BLOSC2_ERROR_SUCCESS, Some(schunk)),
        Err(err) => (schunk_string_error_code(&err), None),
    }
}

/// C-name adapter for [`Schunk::open`].
pub fn blosc2_schunk_open(path: &str) -> Result<Schunk, String> {
    Schunk::open(path)
}

/// C-style status adapter for [`Schunk::open`].
pub fn blosc2_schunk_open_c(path: &str) -> (i32, Option<Schunk>) {
    match Schunk::open(path) {
        Ok(schunk) => (BLOSC2_ERROR_SUCCESS, Some(schunk)),
        Err(err) => (schunk_string_error_code(&err), None),
    }
}

/// C-name adapter for [`Schunk::open_offset`].
pub fn blosc2_schunk_open_offset(path: impl AsRef<Path>, offset: i64) -> Result<Schunk, String> {
    if offset < 0 {
        return Err("Invalid frame offset".into());
    }
    Schunk::open_offset(path, offset as u64)
}

/// C-style status adapter for [`Schunk::open_offset`].
pub fn blosc2_schunk_open_offset_c(path: impl AsRef<Path>, offset: i64) -> (i32, Option<Schunk>) {
    let offset = match u64::try_from(offset) {
        Ok(offset) => offset,
        Err(_) => return (BLOSC2_ERROR_FILE_OPEN, None),
    };
    match Schunk::open_offset(path, offset) {
        Ok(schunk) => (BLOSC2_ERROR_SUCCESS, Some(schunk)),
        Err(err) => (schunk_string_error_code(&err), None),
    }
}

/// C-name-style lazy open helper for [`Schunk::open_lazy`].
pub fn blosc2_schunk_open_lazy(path: impl AsRef<Path>) -> Result<LazySchunk, String> {
    Schunk::open_lazy(path)
}

/// C-style status adapter for [`Schunk::open_lazy`].
pub fn blosc2_schunk_open_lazy_c(path: impl AsRef<Path>) -> (i32, Option<LazySchunk>) {
    match Schunk::open_lazy(path) {
        Ok(schunk) => (BLOSC2_ERROR_SUCCESS, Some(schunk)),
        Err(err) => (schunk_string_error_code(&err), None),
    }
}

/// C-name-style lazy offset open helper for [`Schunk::open_lazy_offset`].
pub fn blosc2_schunk_open_lazy_offset(
    path: impl AsRef<Path>,
    offset: i64,
) -> Result<LazySchunk, String> {
    if offset < 0 {
        return Err("Invalid frame offset".into());
    }
    Schunk::open_lazy_offset(path, offset as u64)
}

/// C-style status adapter for [`Schunk::open_lazy_offset`].
pub fn blosc2_schunk_open_lazy_offset_c(
    path: impl AsRef<Path>,
    offset: i64,
) -> (i32, Option<LazySchunk>) {
    let offset = match u64::try_from(offset) {
        Ok(offset) => offset,
        Err(_) => return (BLOSC2_ERROR_FILE_OPEN, None),
    };
    match Schunk::open_lazy_offset(path, offset) {
        Ok(schunk) => (BLOSC2_ERROR_SUCCESS, Some(schunk)),
        Err(err) => (schunk_string_error_code(&err), None),
    }
}

/// Rust-friendly alias for [`Schunk::to_frame`].
pub fn blosc2_schunk_to_buffer_vec(schunk: &Schunk) -> Vec<u8> {
    schunk.to_frame()
}

/// C-name adapter for [`Schunk::to_frame`].
pub fn blosc2_schunk_to_buffer(schunk: &Schunk) -> (i64, Option<Vec<u8>>, bool) {
    if validate_c_frame_metalayers(schunk).is_err() {
        return (i64::from(BLOSC2_ERROR_INVALID_PARAM), None, false);
    }
    let frame = schunk.to_frame();
    (
        i64::try_from(frame.len()).unwrap_or(i64::from(BLOSC2_ERROR_2GB_LIMIT)),
        Some(frame),
        true,
    )
}

fn validate_c_frame_metalayers(schunk: &Schunk) -> std::io::Result<()> {
    if schunk.vlmetalayers.len() > BLOSC2_MAX_METALAYERS {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Too many VL-metalayers",
        ));
    }
    validate_metalayers_encoded_size(
        schunk
            .metalayers
            .iter()
            .map(|layer| (layer.name.as_str(), layer.content.as_slice())),
    )
    .and_then(|_| validate_vlmetalayers_encoded_size_parts(schunk.vlmetalayer_encoded_sizes()))
    .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

fn schunk_file_write_error_code(err: &std::io::Error) -> i32 {
    match err.kind() {
        std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied => {
            BLOSC2_ERROR_FILE_OPEN
        }
        std::io::ErrorKind::InvalidData => BLOSC2_ERROR_SCHUNK_COPY,
        _ => BLOSC2_ERROR_FILE_WRITE,
    }
}

/// C-name adapter for [`Schunk::to_file_len`].
pub fn blosc2_schunk_to_file(schunk: &Schunk, path: &str) -> i64 {
    schunk
        .to_file_len(path)
        .unwrap_or_else(|err| i64::from(schunk_file_write_error_code(&err)))
}

/// C-name adapter for [`Schunk::append_file`].
pub fn blosc2_schunk_append_file(schunk: &Schunk, path: impl AsRef<Path>) -> i64 {
    match schunk.append_file(path) {
        Ok(offset) => i64::try_from(offset).unwrap_or(i64::from(BLOSC2_ERROR_FILE_WRITE)),
        Err(err) => i64::from(schunk_file_write_error_code(&err)),
    }
}

/// C-name adapter for [`Schunk::copy_schunk_with_params`].
pub fn blosc2_schunk_copy(
    schunk: &Schunk,
    cparams: Option<CParams>,
    dparams: Option<DParams>,
) -> Result<Schunk, &'static str> {
    match (cparams, dparams) {
        (Some(cparams), Some(dparams)) => schunk.copy_schunk_with_params(cparams, dparams),
        (Some(cparams), None) => schunk.copy_schunk_with_params(cparams, schunk.dparams.clone()),
        (None, Some(dparams)) => schunk.copy_schunk_with_params(schunk.cparams.clone(), dparams),
        (None, None) => Ok(schunk.copy_schunk()),
    }
}

/// C-style status adapter for [`blosc2_schunk_copy`].
pub fn blosc2_schunk_copy_c(
    schunk: &Schunk,
    cparams: Option<CParams>,
    dparams: Option<DParams>,
) -> (i32, Option<Schunk>) {
    match blosc2_schunk_copy(schunk, cparams, dparams) {
        Ok(copy) => (BLOSC2_ERROR_SUCCESS, Some(copy)),
        Err(err) => (schunk_error_code(err), None),
    }
}

/// C-name adapter for [`Schunk::get_cparams`].
pub fn blosc2_schunk_get_cparams(schunk: &Schunk) -> CParams {
    schunk.get_cparams()
}

/// C-style status adapter for [`Schunk::get_cparams`].
pub fn blosc2_schunk_get_cparams_c(schunk: &Schunk) -> (i32, CParams) {
    (BLOSC2_ERROR_SUCCESS, schunk.get_cparams())
}

/// C-name adapter for [`Schunk::get_dparams`].
pub fn blosc2_schunk_get_dparams(schunk: &Schunk) -> DParams {
    schunk.get_dparams()
}

/// C-style status adapter for [`Schunk::get_dparams`].
pub fn blosc2_schunk_get_dparams_c(schunk: &Schunk) -> (i32, DParams) {
    (BLOSC2_ERROR_SUCCESS, schunk.get_dparams())
}

/// C-name adapter for [`Schunk::append_buffer`].
pub fn blosc2_schunk_append_buffer(schunk: &mut Schunk, data: &[u8]) -> i64 {
    schunk
        .append_buffer(data)
        .unwrap_or_else(|err| i64::from(schunk_error_code(err)))
}

/// C-style explicit-length adapter for [`Schunk::append_buffer`].
pub fn blosc2_schunk_append_buffer_c(schunk: &mut Schunk, data: &[u8], nbytes: i64) -> i64 {
    let data = match schunk_checked_buffer_prefix(data, nbytes) {
        Ok(data) => data,
        Err(code) => return i64::from(code),
    };
    blosc2_schunk_append_buffer(schunk, data)
}

/// C-name adapter for [`Schunk::update_compressed_chunk_c`].
pub fn blosc2_schunk_update_chunk(
    schunk: &mut Schunk,
    nchunk: i64,
    chunk: &[u8],
    copy: bool,
) -> i64 {
    schunk.update_compressed_chunk_c(nchunk, chunk, copy)
}

/// C-style explicit-size adapter for [`Schunk::update_compressed_chunk_c`].
pub fn blosc2_schunk_update_chunk_c(
    schunk: &mut Schunk,
    nchunk: i64,
    chunk: &[u8],
    cbytes: i64,
    copy: bool,
) -> i64 {
    let chunk = match schunk_checked_buffer_prefix(chunk, cbytes) {
        Ok(chunk) => chunk,
        Err(code) => return i64::from(code),
    };
    blosc2_schunk_update_chunk(schunk, nchunk, chunk, copy)
}

/// C-name adapter for [`Schunk::insert_chunk_c`].
pub fn blosc2_schunk_insert_chunk(
    schunk: &mut Schunk,
    nchunk: i64,
    chunk: &[u8],
    copy: bool,
) -> i64 {
    schunk.insert_chunk_c(nchunk, chunk, copy)
}

/// C-style explicit-size adapter for [`Schunk::insert_chunk_c`].
pub fn blosc2_schunk_insert_chunk_c(
    schunk: &mut Schunk,
    nchunk: i64,
    chunk: &[u8],
    cbytes: i64,
    copy: bool,
) -> i64 {
    let chunk = match schunk_checked_buffer_prefix(chunk, cbytes) {
        Ok(chunk) => chunk,
        Err(code) => return i64::from(code),
    };
    blosc2_schunk_insert_chunk(schunk, nchunk, chunk, copy)
}

/// C-name adapter for [`Schunk::insert_buffer`].
pub fn blosc2_schunk_insert_buffer(schunk: &mut Schunk, nchunk: i64, data: &[u8]) -> i64 {
    schunk
        .insert_buffer(nchunk, data)
        .unwrap_or_else(|err| i64::from(schunk_error_code(err)))
}

/// C-style explicit-length adapter for [`Schunk::insert_buffer`].
pub fn blosc2_schunk_insert_buffer_c(
    schunk: &mut Schunk,
    nchunk: i64,
    data: &[u8],
    nbytes: i64,
) -> i64 {
    let data = match schunk_checked_buffer_prefix(data, nbytes) {
        Ok(data) => data,
        Err(code) => return i64::from(code),
    };
    blosc2_schunk_insert_buffer(schunk, nchunk, data)
}

/// C-name adapter for [`Schunk::update_chunk`].
pub fn blosc2_schunk_update_buffer(schunk: &mut Schunk, nchunk: i64, data: &[u8]) -> i64 {
    schunk
        .update_chunk(nchunk, data)
        .unwrap_or_else(|err| i64::from(schunk_error_code(err)))
}

/// C-style explicit-length adapter for [`Schunk::update_chunk`].
pub fn blosc2_schunk_update_buffer_c(
    schunk: &mut Schunk,
    nchunk: i64,
    data: &[u8],
    nbytes: i64,
) -> i64 {
    let data = match schunk_checked_buffer_prefix(data, nbytes) {
        Ok(data) => data,
        Err(code) => return i64::from(code),
    };
    blosc2_schunk_update_buffer(schunk, nchunk, data)
}

/// C-style VL-block append adapter with explicit block count and sizes.
pub fn blosc2_schunk_append_vlblocks_c(
    schunk: &mut Schunk,
    blocks: &[&[u8]],
    block_sizes: &[i32],
    nblocks: i32,
) -> i64 {
    let sized_blocks = match checked_vlblock_prefixes(blocks, block_sizes, nblocks) {
        Ok(blocks) => blocks,
        Err(code) => return i64::from(code),
    };
    schunk
        .append_vlblocks(&sized_blocks)
        .unwrap_or_else(|err| i64::from(schunk_error_code(err)))
}

/// C-style VL-block insert adapter with explicit block count and sizes.
pub fn blosc2_schunk_insert_vlblocks_c(
    schunk: &mut Schunk,
    nchunk: i64,
    blocks: &[&[u8]],
    block_sizes: &[i32],
    nblocks: i32,
) -> i64 {
    let sized_blocks = match checked_vlblock_prefixes(blocks, block_sizes, nblocks) {
        Ok(blocks) => blocks,
        Err(code) => return i64::from(code),
    };
    schunk
        .insert_vlblocks(nchunk, &sized_blocks)
        .unwrap_or_else(|err| i64::from(schunk_error_code(err)))
}

/// C-style VL-block update adapter with explicit block count and sizes.
pub fn blosc2_schunk_update_vlblocks_c(
    schunk: &mut Schunk,
    nchunk: i64,
    blocks: &[&[u8]],
    block_sizes: &[i32],
    nblocks: i32,
) -> i64 {
    let sized_blocks = match checked_vlblock_prefixes(blocks, block_sizes, nblocks) {
        Ok(blocks) => blocks,
        Err(code) => return i64::from(code),
    };
    schunk
        .update_vlblocks(nchunk, &sized_blocks)
        .unwrap_or_else(|err| i64::from(schunk_error_code(err)))
}

fn checked_vlblock_prefixes<'a>(
    blocks: &'a [&'a [u8]],
    block_sizes: &[i32],
    nblocks: i32,
) -> Result<Vec<&'a [u8]>, i32> {
    let nblocks = match usize::try_from(nblocks) {
        Ok(nblocks) if nblocks <= blocks.len() => nblocks,
        _ => return Err(BLOSC2_ERROR_INVALID_PARAM),
    };
    if block_sizes.len() < nblocks {
        return Err(BLOSC2_ERROR_INVALID_PARAM);
    }
    let mut sized_blocks = Vec::with_capacity(nblocks);
    for (block, &declared_size) in blocks.iter().zip(block_sizes.iter()).take(nblocks) {
        let declared_size = match usize::try_from(declared_size) {
            Ok(size) => size,
            Err(_) => return Err(BLOSC2_ERROR_INVALID_PARAM),
        };
        if declared_size > block.len() {
            return Err(BLOSC2_ERROR_INVALID_PARAM);
        }
        sized_blocks.push(&block[..declared_size]);
    }
    Ok(sized_blocks)
}

/// C-style VL-block decompression adapter with signed index and destination size.
pub fn blosc2_schunk_decompress_vlblock_c(
    schunk: &Schunk,
    nchunk: i64,
    nblock: i32,
    dest: &mut [u8],
    destsize: i64,
) -> i32 {
    if nblock < 0 {
        return BLOSC2_ERROR_INVALID_PARAM;
    }
    let destsize = match schunk_checked_dest_len(dest.len(), destsize) {
        Ok(destsize) => destsize,
        Err(code) => return code,
    };
    match schunk.decompress_vlblock(nchunk, nblock as usize) {
        Ok(block) => {
            if destsize < block.len() {
                return BLOSC2_ERROR_WRITE_BUFFER;
            }
            dest[..block.len()].copy_from_slice(&block);
            i32::try_from(block.len()).unwrap_or(BLOSC2_ERROR_2GB_LIMIT)
        }
        Err(err) => schunk_error_code(err),
    }
}

fn schunk_checked_buffer_prefix(data: &[u8], nbytes: i64) -> Result<&[u8], i32> {
    let nbytes = usize::try_from(nbytes).map_err(|_| BLOSC2_ERROR_INVALID_PARAM)?;
    data.get(..nbytes).ok_or(BLOSC2_ERROR_INVALID_PARAM)
}

fn schunk_checked_dest_len(available: usize, nbytes: i64) -> Result<usize, i32> {
    let nbytes = usize::try_from(nbytes).map_err(|_| BLOSC2_ERROR_INVALID_PARAM)?;
    if nbytes > available {
        return Err(BLOSC2_ERROR_INVALID_PARAM);
    }
    Ok(nbytes)
}

/// C-name adapter for [`Schunk::delete_chunk_c`].
pub fn blosc2_schunk_delete_chunk(schunk: &mut Schunk, nchunk: i64) -> i64 {
    schunk.delete_chunk_c(nchunk)
}

/// C-name adapter for [`Schunk::compressed_chunk`].
pub fn blosc2_schunk_get_chunk(schunk: &Schunk, nchunk: i64) -> (i32, Option<Vec<u8>>, bool) {
    match schunk.compressed_chunk(nchunk) {
        Ok(chunk) => (
            i32::try_from(chunk.len()).unwrap_or(BLOSC2_ERROR_2GB_LIMIT),
            Some(chunk.to_vec()),
            false,
        ),
        Err("Chunk index out of range") => (BLOSC2_ERROR_INVALID_PARAM, None, false),
        Err(err) => (schunk_error_code(err), None, false),
    }
}

/// Borrow-preserving C-style chunk accessor for in-memory [`Schunk`] values.
pub fn blosc2_schunk_get_chunk_ref(schunk: &Schunk, nchunk: i64) -> (i32, Option<&[u8]>, bool) {
    match schunk.compressed_chunk(nchunk) {
        Ok(chunk) => (
            i32::try_from(chunk.len()).unwrap_or(BLOSC2_ERROR_2GB_LIMIT),
            Some(chunk),
            false,
        ),
        Err("Chunk index out of range") => (BLOSC2_ERROR_INVALID_PARAM, None, false),
        Err(err) => (schunk_error_code(err), None, false),
    }
}

/// C-name-style lazy chunk accessor for [`Schunk`] and [`LazySchunk`].
pub fn blosc2_schunk_get_lazychunk<S: SchunkLazyChunkAccessor + ?Sized>(
    schunk: &S,
    nchunk: i64,
) -> (i32, Option<Vec<u8>>) {
    match schunk.lazychunk_for_c(nchunk) {
        Ok((chunk, _needs_free)) => (
            i32::try_from(chunk.len()).unwrap_or(BLOSC2_ERROR_2GB_LIMIT),
            Some(chunk),
        ),
        Err(err) if err == "Chunk index out of range" => (BLOSC2_ERROR_INVALID_PARAM, None),
        Err(err) => (schunk_string_error_code(&err), None),
    }
}

/// C-style lazy chunk accessor including ownership information.
pub fn blosc2_schunk_get_lazychunk_c<S: SchunkLazyChunkAccessor + ?Sized>(
    schunk: &S,
    nchunk: i64,
) -> (i32, Option<Vec<u8>>, bool) {
    match schunk.lazychunk_for_c(nchunk) {
        Ok((chunk, needs_free)) => (
            i32::try_from(chunk.len()).unwrap_or(BLOSC2_ERROR_2GB_LIMIT),
            Some(chunk),
            needs_free,
        ),
        Err(err) if err == "Chunk index out of range" => (BLOSC2_ERROR_INVALID_PARAM, None, false),
        Err(err) => (schunk_string_error_code(&err), None, false),
    }
}

/// C-name adapter for [`Schunk::decompress_chunk_into`].
pub fn blosc2_schunk_decompress_chunk(schunk: &Schunk, nchunk: i64, dest: &mut [u8]) -> i32 {
    match schunk.decompress_chunk_into(nchunk, dest) {
        Ok(len) => i32::try_from(len).unwrap_or(BLOSC2_ERROR_2GB_LIMIT),
        Err("Chunk index out of range") => BLOSC2_ERROR_INVALID_PARAM,
        Err("Destination too small") => BLOSC2_ERROR_WRITE_BUFFER,
        Err(err) => schunk_error_code(err),
    }
}

/// C-style adapter for [`Schunk::decompress_chunk_into`] with explicit destination size.
pub fn blosc2_schunk_decompress_chunk_c(
    schunk: &Schunk,
    nchunk: i64,
    dest: &mut [u8],
    destsize: i64,
) -> i32 {
    let destsize = match schunk_checked_dest_len(dest.len(), destsize) {
        Ok(destsize) => destsize,
        Err(code) => return code,
    };
    match schunk.decompress_chunk(nchunk) {
        Ok(data) => {
            if destsize < data.len() {
                return BLOSC2_ERROR_WRITE_BUFFER;
            }
            dest[..data.len()].copy_from_slice(&data);
            i32::try_from(data.len()).unwrap_or(BLOSC2_ERROR_2GB_LIMIT)
        }
        Err("Chunk index out of range") => BLOSC2_ERROR_INVALID_PARAM,
        Err(err) => schunk_error_code(err),
    }
}

/// C-name-style VL-block accessor returning an owned block and its size.
pub fn blosc2_schunk_get_vlblock(
    schunk: &Schunk,
    nchunk: i64,
    nblock: i32,
) -> (i32, Option<Vec<u8>>, i32) {
    if nblock < 0 {
        return (BLOSC2_ERROR_INVALID_PARAM, None, 0);
    }
    match schunk.decompress_vlblock(nchunk, nblock as usize) {
        Ok(block) => {
            let len = match i32::try_from(block.len()) {
                Ok(len) => len,
                Err(_) => return (BLOSC2_ERROR_2GB_LIMIT, None, 0),
            };
            (len, Some(block), len)
        }
        Err(err) => (schunk_error_code(err), None, 0),
    }
}

/// C-name adapter for [`Schunk::reorder_offsets_c`].
pub fn blosc2_schunk_reorder_offsets(schunk: &mut Schunk, order: &[i64]) -> i32 {
    schunk.reorder_offsets_c(order)
}

/// C-name adapter for [`Schunk::frame_get_offsets`].
pub fn blosc2_schunk_frame_get_offsets(schunk: &Schunk) -> (i32, Option<Vec<i64>>) {
    if schunk.attached_frame_len.is_none() || schunk.nchunks() == 0 {
        return (BLOSC2_ERROR_FAILURE, None);
    }
    match schunk.frame_get_offsets() {
        Ok(offsets) => (BLOSC2_ERROR_SUCCESS, Some(offsets)),
        Err(err) => (schunk_error_code(err), None),
    }
}

/// Exact C-name alias for [`Schunk::frame_get_offsets`].
pub fn blosc2_frame_get_offsets(schunk: &Schunk) -> (i32, Option<Vec<i64>>) {
    blosc2_schunk_frame_get_offsets(schunk)
}

/// C-name adapter for [`Schunk::frame_len`].
pub fn blosc2_schunk_frame_len(schunk: &Schunk) -> i64 {
    if let Some(frame_len) = schunk.attached_frame_len {
        return frame_len;
    }
    schunk
        .cbytes
        .checked_add(schunk.nchunks().saturating_mul(8))
        .unwrap_or(i64::from(BLOSC2_ERROR_2GB_LIMIT))
}

fn schunk_c_slice_nchunks(schunk: &Schunk, start: usize, stop: usize) -> (i32, Option<Vec<i64>>) {
    if schunk.nchunks() == 0 {
        return (0, Some(Vec::new()));
    }
    let typesize = match usize::try_from(schunk.cparams.typesize) {
        Ok(typesize) if typesize > 0 => typesize,
        _ => return (BLOSC2_ERROR_INVALID_PARAM, None),
    };
    let (byte_start, byte_len) = match item_slice_to_byte_range(start, stop, typesize) {
        Ok(range) => range,
        Err(err) => return (schunk_error_code(err), None),
    };
    let byte_stop = match byte_start.checked_add(byte_len) {
        Some(byte_stop) => byte_stop,
        None => return (BLOSC2_ERROR_INVALID_PARAM, None),
    };
    match schunk.get_slice_nchunks(byte_start, byte_stop) {
        Ok(chunks) => (
            i32::try_from(chunks.len()).unwrap_or(BLOSC2_ERROR_2GB_LIMIT),
            Some(chunks),
        ),
        Err(err) => (schunk_error_code(err), None),
    }
}

/// C-name adapter for [`Schunk::get_slice_nchunks`].
pub fn blosc2_schunk_get_slice_nchunks(
    schunk: &Schunk,
    start: usize,
    stop: usize,
) -> (i32, Option<Vec<i64>>) {
    schunk_c_slice_nchunks(schunk, start, stop)
}

/// C-style signed adapter for [`Schunk::get_slice_nchunks`].
pub fn blosc2_schunk_get_slice_nchunks_c(
    schunk: &Schunk,
    start: i64,
    stop: i64,
) -> (i32, Option<Vec<i64>>) {
    let (start, stop) = match schunk_checked_slice_bounds(start, stop) {
        Ok(bounds) => bounds,
        Err(code) => return (code, None),
    };
    blosc2_schunk_get_slice_nchunks(schunk, start, stop)
}

/// Exact C-name alias for [`Schunk::get_slice_nchunks`].
pub fn blosc2_get_slice_nchunks(
    schunk: &Schunk,
    start: usize,
    stop: usize,
) -> (i32, Option<Vec<i64>>) {
    schunk_c_slice_nchunks(schunk, start, stop)
}

/// C-name adapter for [`Schunk::get_slice_items`].
pub fn blosc2_schunk_get_slice_buffer(
    schunk: &Schunk,
    start: usize,
    stop: usize,
    dest: &mut [u8],
) -> i32 {
    match schunk.get_slice_items(start, stop) {
        Ok(data) => {
            if dest.len() < data.len() {
                return BLOSC2_ERROR_WRITE_BUFFER;
            }
            dest[..data.len()].copy_from_slice(&data);
            BLOSC2_ERROR_SUCCESS
        }
        Err(
            "Invalid item slice range"
            | "Slice range out of bounds"
            | "Slice range overflow"
            | "Slice data size does not match item range",
        ) => BLOSC2_ERROR_INVALID_PARAM,
        Err(err) => schunk_error_code(err),
    }
}

/// C-style signed adapter for [`Schunk::get_slice_items`].
pub fn blosc2_schunk_get_slice_buffer_c(
    schunk: &Schunk,
    start: i64,
    stop: i64,
    dest: &mut [u8],
) -> i32 {
    let (start, stop) = match schunk_checked_slice_bounds(start, stop) {
        Ok(bounds) => bounds,
        Err(code) => return code,
    };
    blosc2_schunk_get_slice_buffer(schunk, start, stop, dest)
}

/// C-style signed adapter for [`Schunk::get_slice_items`] with explicit destination size.
pub fn blosc2_schunk_get_slice_buffer_size_c(
    schunk: &Schunk,
    start: i64,
    stop: i64,
    dest: &mut [u8],
    destsize: i64,
) -> i32 {
    let destsize = match schunk_checked_dest_len(dest.len(), destsize) {
        Ok(destsize) => destsize,
        Err(code) => return code,
    };
    let (start, stop) = match schunk_checked_slice_bounds(start, stop) {
        Ok(bounds) => bounds,
        Err(code) => return code,
    };
    match schunk.get_slice_items(start, stop) {
        Ok(data) => {
            if destsize < data.len() {
                return BLOSC2_ERROR_WRITE_BUFFER;
            }
            dest[..destsize].fill(0);
            dest[..data.len()].copy_from_slice(&data);
            BLOSC2_ERROR_SUCCESS
        }
        Err(
            "Invalid item slice range"
            | "Slice range out of bounds"
            | "Slice range overflow"
            | "Slice data size does not match item range",
        ) => BLOSC2_ERROR_INVALID_PARAM,
        Err(err) => schunk_error_code(err),
    }
}

/// C-name adapter for [`Schunk::set_slice_items`].
pub fn blosc2_schunk_set_slice_buffer(
    schunk: &mut Schunk,
    start: usize,
    stop: usize,
    data: &[u8],
) -> i32 {
    match schunk.set_slice_items(start, stop, data) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(
            "Invalid item slice range"
            | "Slice range out of bounds"
            | "Slice range overflow"
            | "Slice data size does not match item range",
        ) => BLOSC2_ERROR_INVALID_PARAM,
        Err(err) => schunk_error_code(err),
    }
}

/// C-style signed adapter for [`Schunk::set_slice_items`].
pub fn blosc2_schunk_set_slice_buffer_c(
    schunk: &mut Schunk,
    start: i64,
    stop: i64,
    data: &[u8],
) -> i32 {
    let (start, stop) = match schunk_checked_slice_bounds(start, stop) {
        Ok(bounds) => bounds,
        Err(code) => return code,
    };
    blosc2_schunk_set_slice_buffer(schunk, start, stop, data)
}

/// C-style signed adapter for [`Schunk::set_slice_items`] with explicit input size.
pub fn blosc2_schunk_set_slice_buffer_size_c(
    schunk: &mut Schunk,
    start: i64,
    stop: i64,
    data: &[u8],
    buffersize: i64,
) -> i32 {
    let data = match schunk_checked_buffer_prefix(data, buffersize) {
        Ok(data) => data,
        Err(code) => return code,
    };
    let (start, stop) = match schunk_checked_slice_bounds(start, stop) {
        Ok(bounds) => bounds,
        Err(code) => return code,
    };
    blosc2_schunk_set_slice_buffer(schunk, start, stop, data)
}

fn schunk_checked_slice_bounds(start: i64, stop: i64) -> Result<(usize, usize), i32> {
    let start = usize::try_from(start).map_err(|_| BLOSC2_ERROR_INVALID_PARAM)?;
    let stop = usize::try_from(stop).map_err(|_| BLOSC2_ERROR_INVALID_PARAM)?;
    Ok((start, stop))
}

/// C-name adapter for [`Schunk::fill_special`].
pub fn blosc2_schunk_fill_special(
    schunk: &mut Schunk,
    nitems: usize,
    special: u8,
    chunksize: usize,
) -> i64 {
    if nitems == 0 {
        return 0;
    }
    match schunk.fill_special(nitems, special, chunksize) {
        Ok(nchunks) => nchunks,
        Err("Can only fill an empty schunk") => i64::from(BLOSC2_ERROR_FRAME_SPECIAL),
        Err("Invalid chunksize" | "Invalid typesize") => i64::from(BLOSC2_ERROR_INVALID_PARAM),
        Err("Unsupported special value") | Err("Too many chunks") => {
            i64::from(BLOSC2_ERROR_SCHUNK_SPECIAL)
        }
        Err(err) => i64::from(schunk_error_code(err)),
    }
}

/// C-name adapter for [`Schunk::meta_exists_c`].
pub fn blosc2_meta_exists(schunk: &Schunk, name: &str) -> i32 {
    schunk.meta_exists_c(name)
}

/// C-name adapter for [`Schunk::meta_add_c`].
pub fn blosc2_meta_add(schunk: &mut Schunk, name: &str, content: &[u8]) -> i32 {
    schunk.meta_add_c(name, content)
}

/// C-style fixed metalayer add adapter with explicit content length.
pub fn blosc2_meta_add_c(schunk: &mut Schunk, name: &str, content: &[u8], content_len: i64) -> i32 {
    let content = match schunk_checked_buffer_prefix(content, content_len) {
        Ok(content) => content,
        Err(code) => return code,
    };
    blosc2_meta_add(schunk, name, content)
}

/// C-name adapter for [`Schunk::meta_update_c`].
pub fn blosc2_meta_update(schunk: &mut Schunk, name: &str, content: &[u8]) -> i32 {
    schunk.meta_update_c(name, content)
}

/// C-style fixed metalayer update adapter with explicit content length.
pub fn blosc2_meta_update_c(
    schunk: &mut Schunk,
    name: &str,
    content: &[u8],
    content_len: i64,
) -> i32 {
    let content = match schunk_checked_buffer_prefix(content, content_len) {
        Ok(content) => content,
        Err(code) => return code,
    };
    blosc2_meta_update(schunk, name, content)
}

/// C-name adapter for fixed metalayer retrieval. Returns the metalayer index
/// and a copied payload on success, or a negative `BLOSC2_ERROR_*` code.
pub fn blosc2_meta_get(schunk: &Schunk, name: &str) -> (i32, Option<Vec<u8>>) {
    let idx = schunk.meta_exists_c(name);
    if idx < 0 {
        return (idx, None);
    }
    match schunk.metalayer(name) {
        Some(content) => (idx, Some(content.to_vec())),
        None => (BLOSC2_ERROR_NOT_FOUND, None),
    }
}

/// Rust extension matching the fixed-metalayer delete semantics used by this
/// crate. C-Blosc2 only exposes delete/get-names for VL metalayers.
pub fn blosc2_meta_delete(schunk: &mut Schunk, name: &str) -> i32 {
    schunk.meta_delete_c(name)
}

/// Rust extension matching the fixed-metalayer names query used by this crate.
/// C-Blosc2 only exposes delete/get-names for VL metalayers.
pub fn blosc2_meta_get_names(schunk: &Schunk) -> (i32, Vec<&str>) {
    schunk.meta_get_names_c()
}

/// C-name adapter for [`Schunk::vlmeta_exists_c`].
pub fn blosc2_vlmeta_exists(schunk: &Schunk, name: &str) -> i32 {
    schunk.vlmeta_exists_c(name)
}

/// C-name adapter for [`Schunk::vlmeta_add_c`].
pub fn blosc2_vlmeta_add(
    schunk: &mut Schunk,
    name: &str,
    content: &[u8],
    cparams: Option<CParams>,
) -> i32 {
    match cparams {
        Some(cparams) => match schunk.add_vlmetalayer_index_c(name, content, Some(cparams)) {
            Ok(idx) => idx as i32,
            Err(err) => metalayer_error_code(err),
        },
        None => schunk.vlmeta_add_c(name, content),
    }
}

/// C-style variable-length metalayer add adapter with explicit content length.
pub fn blosc2_vlmeta_add_c(
    schunk: &mut Schunk,
    name: &str,
    content: &[u8],
    content_len: i64,
    cparams: Option<CParams>,
) -> i32 {
    let content = match schunk_checked_buffer_prefix(content, content_len) {
        Ok(content) => content,
        Err(code) => return code,
    };
    blosc2_vlmeta_add(schunk, name, content, cparams)
}

/// C-name adapter for variable-length metalayer retrieval. Returns the
/// metalayer index and a copied payload on success, or a negative
/// `BLOSC2_ERROR_*` code.
pub fn blosc2_vlmeta_get(schunk: &Schunk, name: &str) -> (i32, Option<Vec<u8>>) {
    let idx = schunk.vlmeta_exists_c(name);
    if idx < 0 {
        return (idx, None);
    }
    match schunk.vlmetalayer(name) {
        Some(content) => (idx, Some(content.to_vec())),
        None => (BLOSC2_ERROR_NOT_FOUND, None),
    }
}

/// C-name adapter for [`Schunk::vlmeta_update_c`].
pub fn blosc2_vlmeta_update(
    schunk: &mut Schunk,
    name: &str,
    content: &[u8],
    cparams: Option<CParams>,
) -> i32 {
    match cparams {
        Some(cparams) => match schunk.update_vlmetalayer_inner(name, content, Some(cparams)) {
            Ok(idx) => idx as i32,
            Err(err) => metalayer_error_code(err),
        },
        None => schunk.vlmeta_update_c(name, content),
    }
}

/// C-style variable-length metalayer update adapter with explicit content length.
pub fn blosc2_vlmeta_update_c(
    schunk: &mut Schunk,
    name: &str,
    content: &[u8],
    content_len: i64,
    cparams: Option<CParams>,
) -> i32 {
    let content = match schunk_checked_buffer_prefix(content, content_len) {
        Ok(content) => content,
        Err(code) => return code,
    };
    blosc2_vlmeta_update(schunk, name, content, cparams)
}

/// C-name adapter for [`Schunk::vlmeta_delete_c`].
pub fn blosc2_vlmeta_delete(schunk: &mut Schunk, name: &str) -> i32 {
    schunk.vlmeta_delete_c(name)
}

/// C-name adapter for [`Schunk::vlmeta_get_names_c`].
pub fn blosc2_vlmeta_get_names(schunk: &Schunk) -> (i32, Vec<&str>) {
    schunk.vlmeta_get_names_c()
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
    dir.join(format!("{:08X}.chunk", chunk_id as u32))
}

/// Check that a metalayer name fits in the on-disk format.
fn validate_metalayer_name(name: &str) -> Result<(), &'static str> {
    if name.len() > 31 {
        return Err("Metalayer name too large");
    }
    Ok(())
}

/// Check that a VL-metalayer name fits in the on-disk format.
fn validate_vlmetalayer_name(name: &str) -> Result<(), &'static str> {
    if name.len() > 31 {
        return Err("VL-metalayer name too large");
    }
    Ok(())
}

fn metalayer_error_code(err: &str) -> i32 {
    match err {
        "Metalayer does not exist" | "VL-metalayer does not exist" => BLOSC2_ERROR_NOT_FOUND,
        "Metalayer name too large"
        | "VL-metalayer name too large"
        | "Metalayer already exists"
        | "VL-metalayer already exists" => BLOSC2_ERROR_INVALID_PARAM,
        "Fixed-size metalayer cannot grow" => BLOSC2_ERROR_INVALID_PARAM,
        _ => compress::blosc2_error_code(err),
    }
}

fn schunk_error_code(err: &str) -> i32 {
    match err {
        "Invalid chunk permutation" => BLOSC2_ERROR_DATA,
        "Chunk index out of range" => BLOSC2_ERROR_INVALID_INDEX,
        "Chunk does not use VL-blocks" | "VL-block index out of range" => {
            BLOSC2_ERROR_INVALID_PARAM
        }
        "Chunk too small"
        | "Chunk too small for header"
        | "Chunk too small for block table"
        | "Chunk too small for compressed block" => BLOSC2_ERROR_READ_BUFFER,
        err if err.contains("Invalid")
            || err.contains("Malformed")
            || err.contains("Unsupported")
            || err.contains("mismatch") =>
        {
            BLOSC2_ERROR_INVALID_HEADER
        }
        _ => BLOSC2_ERROR_FAILURE,
    }
}

fn schunk_string_error_code(err: &str) -> i32 {
    if err.contains("Failed to read file")
        || err.contains("Failed to open")
        || err.contains("No such file")
        || err.contains("Permission denied")
    {
        BLOSC2_ERROR_FILE_OPEN
    } else if err.contains("Invalid frame length") || err.contains("Invalid frame offset") {
        BLOSC2_ERROR_INVALID_PARAM
    } else {
        schunk_error_code(err)
    }
}

fn compress_vlmetalayer_content(content: &[u8]) -> Result<Vec<u8>, &'static str> {
    compress_vlmetalayer_content_with_cparams(content, &CParams::default())
}

fn compress_vlmetalayer_content_with_cparams(
    content: &[u8],
    cparams: &CParams,
) -> Result<Vec<u8>, &'static str> {
    compress::compress(content, cparams)
}

/// Reject VL-metalayer sets whose msgpack-encoded trailer would overflow the
/// signed 32-bit size fields used by the frame format.
fn validate_vlmetalayers_encoded_size_parts<'a>(
    layers: impl Iterator<Item = (&'a str, usize)>,
) -> Result<(), &'static str> {
    let mut index_len = 3usize;
    let mut values_len = 3usize;
    for (name, compressed_len) in layers {
        validate_vlmetalayer_name(name)?;
        index_len = index_len
            .checked_add(encoded_str_len(name))
            .and_then(|len| len.checked_add(5))
            .ok_or("VL-metalayers too large")?;
        values_len = values_len
            .checked_add(5)
            .and_then(|len| len.checked_add(compressed_len))
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

fn item_slice_to_byte_range(
    start: usize,
    stop: usize,
    typesize: usize,
) -> Result<(usize, usize), &'static str> {
    if stop < start || typesize == 0 {
        return Err("Invalid item slice range");
    }
    let byte_start = start.checked_mul(typesize).ok_or("Slice range overflow")?;
    let byte_len = stop
        .checked_sub(start)
        .and_then(|len| len.checked_mul(typesize))
        .ok_or("Slice range overflow")?;
    Ok((byte_start, byte_len))
}

fn fixed_tail_chunksize(chunks: &[Vec<u8>]) -> Result<usize, &'static str> {
    let Some((first, rest)) = chunks.split_first() else {
        return Ok(0);
    };
    let (first_nbytes, _, _) = compress::cbuffer_sizes(first)?;
    for (idx, chunk) in rest.iter().enumerate() {
        let (chunk_nbytes, _, _) = compress::cbuffer_sizes(chunk)?;
        let is_last = idx + 1 == rest.len();
        if chunk_nbytes != first_nbytes && (!is_last || chunk_nbytes > first_nbytes) {
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

    struct RepeatValueFrameLayout {
        physical_indices: Vec<usize>,
        contiguous_offsets: Vec<u64>,
        sparse_offsets: Vec<u64>,
        cbytes: i64,
    }

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
        let encode_special_offsets = chunksize > 0;
        let repeat_layout = repeat_value_frame_layout(schunk, encode_special_offsets);
        let cbytes: i64 = repeat_layout.as_ref().map_or_else(
            || {
                schunk
                    .chunks
                    .iter()
                    .map(|chunk| stored_frame_chunk_len(chunk, encode_special_offsets) as i64)
                    .sum()
            },
            |layout| layout.cbytes,
        );

        // Build header first to know its size
        let header = build_header(schunk, nbytes, cbytes, chunksize);
        let header_size = header.len();

        // Build the offset index as a Blosc2 chunk with int64 offsets
        let offsets_data = repeat_layout.as_ref().map_or_else(
            || build_offsets(schunk, header_size, encode_special_offsets),
            |layout| offsets_bytes(&layout.contiguous_offsets),
        );
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
        let physical_indices: Cow<'_, [usize]> = repeat_layout.as_ref().map_or_else(
            || Cow::Owned((0..schunk.chunks.len()).collect()),
            |layout| Cow::Borrowed(layout.physical_indices.as_slice()),
        );
        for &idx in physical_indices.iter() {
            if let Some(stored) = stored_frame_chunk(&schunk.chunks[idx], encode_special_offsets) {
                frame.extend_from_slice(&stored);
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
        let encode_special_offsets = chunksize > 0;
        let repeat_layout = repeat_value_frame_layout(schunk, encode_special_offsets);
        let cbytes: i64 = repeat_layout.as_ref().map_or_else(
            || {
                schunk
                    .chunks
                    .iter()
                    .map(|chunk| stored_frame_chunk_len(chunk, encode_special_offsets) as i64)
                    .sum()
            },
            |layout| layout.cbytes,
        );

        let mut header = build_header(schunk, nbytes, cbytes, chunksize);
        let offsets_data = repeat_layout.as_ref().map_or_else(
            || build_offsets(schunk, header.len(), encode_special_offsets),
            |layout| offsets_bytes(&layout.contiguous_offsets),
        );
        let offsets_chunk = if offsets_data.is_empty() {
            Vec::new()
        } else {
            build_offsets_chunk(&offsets_data)
        };
        let trailer = build_trailer(schunk);

        let frame_size = header.len() + cbytes as usize + offsets_chunk.len() + trailer.len();
        header[16..24].copy_from_slice(&(frame_size as u64).to_be_bytes());

        writer.write_all(&header)?;
        let physical_indices: Cow<'_, [usize]> = repeat_layout.as_ref().map_or_else(
            || Cow::Owned((0..schunk.chunks.len()).collect()),
            |layout| Cow::Borrowed(layout.physical_indices.as_slice()),
        );
        for &idx in physical_indices.iter() {
            if let Some(stored) = stored_frame_chunk(&schunk.chunks[idx], encode_special_offsets) {
                writer.write_all(&stored)?;
            }
        }
        writer.write_all(&offsets_chunk)?;
        writer.write_all(&trailer)?;
        Ok(())
    }

    /// Write a sparse frame directory with c-blosc2-compatible chunk files.
    pub fn write_sframe_dir(schunk: &Schunk, path: &Path) -> std::io::Result<()> {
        match std::fs::create_dir(path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => {}
            Err(err) => return Err(err),
        }

        let nbytes: i64 = schunk
            .chunks
            .iter()
            .filter_map(|chunk| ChunkHeader::read(chunk).ok())
            .map(|header| i64::from(header.nbytes))
            .sum();
        let chunksize = derive_frame_chunksize(schunk);
        let encode_special_offsets = chunksize > 0;
        let repeat_layout = repeat_value_frame_layout(schunk, encode_special_offsets);
        let cbytes: i64 = repeat_layout.as_ref().map_or_else(
            || {
                schunk
                    .chunks
                    .iter()
                    .map(|chunk| stored_frame_chunk_len(chunk, encode_special_offsets) as i64)
                    .sum()
            },
            |layout| layout.cbytes,
        );

        let mut header = build_header(schunk, nbytes, cbytes, chunksize);
        header[26] = 1;

        if schunk.chunks.len() > u32::MAX as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "too many sparse frame chunks",
            ));
        }
        let offsets_data = repeat_layout.as_ref().map_or_else(
            || build_sframe_offsets(schunk, encode_special_offsets),
            |layout| offsets_bytes(&layout.sparse_offsets),
        );
        let offsets_chunk = if offsets_data.is_empty() {
            Vec::new()
        } else {
            build_offsets_chunk(&offsets_data)
        };
        let trailer = build_trailer(schunk);

        let frame_size = header.len() + offsets_chunk.len() + trailer.len();
        header[16..24].copy_from_slice(&(frame_size as u64).to_be_bytes());

        let physical_indices: Cow<'_, [usize]> = repeat_layout.as_ref().map_or_else(
            || Cow::Owned((0..schunk.chunks.len()).collect()),
            |layout| Cow::Borrowed(layout.physical_indices.as_slice()),
        );
        let mut next_chunk_id = 0u64;
        for &idx in physical_indices.iter() {
            if let Some(stored) = stored_frame_chunk(&schunk.chunks[idx], encode_special_offsets) {
                std::fs::write(sframe_chunk_path(path, next_chunk_id), &stored)?;
                next_chunk_id += 1;
            }
        }
        remove_stale_sframe_chunks(path, next_chunk_id)?;

        let mut index = Vec::with_capacity(frame_size);
        index.extend_from_slice(&header);
        index.extend_from_slice(&offsets_chunk);
        index.extend_from_slice(&trailer);
        std::fs::write(path.join("chunks.b2frame"), index)
    }

    fn remove_stale_sframe_chunks(path: &Path, keep_count: u64) -> std::io::Result<()> {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            let Some(stem) = file_name.strip_suffix(".chunk") else {
                continue;
            };
            if stem.len() != 8 {
                continue;
            }
            let Ok(chunk_id) = u64::from_str_radix(stem, 16) else {
                continue;
            };
            if chunk_id >= keep_count {
                std::fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    fn offsets_bytes(offsets: &[u64]) -> Vec<u8> {
        let mut data = Vec::with_capacity(offsets.len() * 8);
        for &offset in offsets {
            data.extend_from_slice(&offset.to_le_bytes());
        }
        data
    }

    fn repeat_value_frame_layout(
        schunk: &Schunk,
        encode_special_offsets: bool,
    ) -> Option<RepeatValueFrameLayout> {
        if !encode_special_offsets || schunk.chunks.len() < 2 {
            return None;
        }

        let mut first_value: Option<&[u8]> = None;
        let mut physical_indices: Vec<usize> = Vec::new();
        let mut physical_offsets = Vec::new();
        let mut contiguous_offsets = Vec::with_capacity(schunk.chunks.len());
        let mut sparse_offsets = Vec::with_capacity(schunk.chunks.len());
        let mut cbytes = 0i64;

        for (logical_idx, chunk) in schunk.chunks.iter().enumerate() {
            let header = ChunkHeader::read(chunk).ok()?;
            if header.special_type() != BLOSC2_SPECIAL_VALUE {
                return None;
            }
            let chunk_cbytes = usize::try_from(header.cbytes).ok()?;
            if chunk_cbytes > chunk.len() || header.header_len() > chunk_cbytes {
                return None;
            }
            let value = &chunk[header.header_len()..chunk_cbytes];
            if let Some(first) = first_value {
                if value != first {
                    return None;
                }
            } else {
                first_value = Some(value);
            }

            let physical_idx = physical_indices
                .iter()
                .position(|&idx| schunk.chunks[idx].as_slice() == &chunk[..chunk_cbytes]);
            let physical_idx = if let Some(idx) = physical_idx {
                idx
            } else {
                let idx = physical_indices.len();
                physical_indices.push(logical_idx);
                physical_offsets.push(cbytes as u64);
                cbytes = cbytes.checked_add(header.cbytes as i64)?;
                idx
            };
            contiguous_offsets.push(physical_offsets[physical_idx]);
            sparse_offsets.push(physical_idx as u64);
        }

        if physical_indices.len() == schunk.chunks.len() {
            return None;
        }

        Some(RepeatValueFrameLayout {
            physical_indices,
            contiguous_offsets,
            sparse_offsets,
            cbytes,
        })
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
                coffset += stored_frame_chunk_len(chunk, encode_special_offsets) as u64;
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
        stored_frame_chunk(chunk, encode_special_offsets).map_or(0, |stored| stored.len())
    }

    fn stored_frame_chunk(chunk: &[u8], encode_special_offsets: bool) -> Option<Cow<'_, [u8]>> {
        if special_offset_for_chunk(chunk).is_some() {
            if encode_special_offsets {
                None
            } else {
                Some(materialized_special_chunk_for_frame(chunk).unwrap_or(Cow::Borrowed(chunk)))
            }
        } else {
            Some(Cow::Borrowed(chunk))
        }
    }

    fn materialized_special_chunk_for_frame(chunk: &[u8]) -> Result<Cow<'_, [u8]>, &'static str> {
        let header = ChunkHeader::read(chunk)?;
        match header.special_type() {
            BLOSC2_SPECIAL_ZERO | BLOSC2_SPECIAL_NAN | BLOSC2_SPECIAL_UNINIT => {
                let data = compress::decompress(chunk)?;
                let cparams = CParams {
                    compcode: header.compcode(),
                    compcode_meta: header.compcode_meta,
                    clevel: 0,
                    typesize: i32::from(header.typesize),
                    blocksize: header.blocksize,
                    filters: header.filters,
                    filters_meta: header.filters_meta,
                    ..Default::default()
                };
                let blocksize = header.blocksize.max(1) as usize;
                Ok(Cow::Owned(compress::memcpy_chunk(
                    &data, &cparams, blocksize,
                )))
            }
            _ => Ok(Cow::Borrowed(chunk)),
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
        let special = ((offset >> 56) as u8) & !(1 << 7);
        if offset != encoded_special_offset(special) {
            return None;
        }
        match special {
            BLOSC2_SPECIAL_ZERO | BLOSC2_SPECIAL_UNINIT | BLOSC2_SPECIAL_NAN => Some(special),
            _ => None,
        }
    }

    fn special_chunk_from_offset(
        offset: u64,
        logical_idx: usize,
        nchunks: usize,
        nbytes: i64,
        chunksize: usize,
        blocksize: i32,
        spec: &FrameChunkSpec,
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
        spec: &FrameChunkSpec,
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
    /// Returns the fixed chunk size when every chunk has the same uncompressed
    /// size, or `0` when chunks are variable-sized.
    pub(super) fn derive_frame_chunksize(schunk: &Schunk) -> i32 {
        if schunk.chunks.is_empty() {
            return i32::try_from(schunk.chunksize)
                .ok()
                .filter(|&chunksize| chunksize > 0)
                .unwrap_or(-1);
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
        data_len: usize,
    ) -> Result<(), String> {
        intervals.sort_unstable_by_key(|&(start, end)| (start, end));
        let mut covered_until = 0usize;
        let mut last_unique: Option<(usize, usize)> = None;
        for &(start, end) in intervals.iter() {
            if end < start {
                return Err("Invalid frame: invalid chunk interval".into());
            }
            if let Some((prev_start, prev_end)) = last_unique {
                if (start, end) == (prev_start, prev_end) {
                    continue;
                }
                if start < prev_end {
                    return Err("Invalid frame: chunk offsets partially overlap".into());
                }
            }
            if last_unique != Some((start, end)) {
                if start != covered_until {
                    return Err("Invalid frame: chunk offsets leave data gaps".into());
                }
                covered_until = end;
            }
            last_unique = Some((start, end));
        }
        if covered_until != data_len {
            return Err("Invalid frame: chunk offsets leave data gaps".into());
        }
        Ok(())
    }

    fn validate_frame_vlblocks_flag(
        frame_vlblocks: bool,
        variable_chunks: bool,
        actual_vlblocks: bool,
        actual_regular_chunks: bool,
    ) -> Result<(), String> {
        if actual_vlblocks && actual_regular_chunks {
            return Err("Invalid frame: VL-block flag mismatch".into());
        }
        if actual_vlblocks && !frame_vlblocks {
            return Err("Invalid frame: VL-block flag mismatch".into());
        }
        if frame_vlblocks && actual_regular_chunks && !variable_chunks {
            return Err("Invalid frame: VL-block flag mismatch".into());
        }
        if frame_vlblocks && !actual_regular_chunks && !actual_vlblocks {
            return Err("Invalid frame: VL-block flag mismatch".into());
        }
        Ok(())
    }

    /// Wrap the offsets payload in a Blosc2 chunk using C-Blosc2's default
    /// one-shot frame parameters for the offset index.
    pub(super) fn build_offsets_chunk(data: &[u8]) -> Vec<u8> {
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
            | if chunksize == 0 && !schunk.chunks.is_empty() || schunk.vlblocks {
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

    /// Build the frame trailer: the VL-metalayers index and compressed payload
    /// cbuffers followed by the trailer length and 16-byte fingerprint placeholder.
    fn build_trailer(schunk: &Schunk) -> Vec<u8> {
        let vlmetalayers: Vec<_> = schunk
            .vlmetalayers
            .iter()
            .enumerate()
            .map(|(idx, layer)| {
                (
                    layer.name.as_str(),
                    schunk
                        .vlmetalayer_encoded
                        .get(idx)
                        .and_then(Option::as_ref)
                        .cloned()
                        .unwrap_or_else(|| {
                            compress_vlmetalayer_content(&layer.content)
                                .expect("VL-metalayer content was validated before insertion")
                        }),
                )
            })
            .collect();

        let mut t = vec![0x94, 0x01, 0x93, MSGPACK_UINT16];
        let map_size_pos = t.len();
        t.extend_from_slice(&0u16.to_be_bytes());
        let index_start = map_size_pos - 1;

        t.push(MSGPACK_MAP16);
        t.extend_from_slice(&(vlmetalayers.len() as u16).to_be_bytes());

        let mut offset_positions = Vec::with_capacity(vlmetalayers.len());
        for (name, _) in &vlmetalayers {
            encode_vlmeta_name(&mut t, name);
            t.push(MSGPACK_INT32);
            offset_positions.push(t.len());
            t.extend_from_slice(&0i32.to_be_bytes());
        }

        let map_size = u16::try_from(t.len() - index_start)
            .expect("VL-metalayer index size is validated when inserting");
        t[map_size_pos..map_size_pos + 2].copy_from_slice(&map_size.to_be_bytes());

        t.push(MSGPACK_ARRAY16);
        t.extend_from_slice(&(vlmetalayers.len() as u16).to_be_bytes());
        for ((_, content), offset_pos) in vlmetalayers.iter().zip(offset_positions) {
            let offset = i32::try_from(t.len()).expect("VL-metalayer trailer offset fits i32");
            t[offset_pos..offset_pos + 4].copy_from_slice(&offset.to_be_bytes());
            t.push(MSGPACK_BIN32);
            t.extend_from_slice(&(content.len() as u32).to_be_bytes());
            t.extend_from_slice(content);
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

    /// Cached frame-level parameters needed for special-offset materialization
    /// and frame chunk validation.
    struct FrameChunkSpec {
        compcode: u8,
        typesize: i32,
    }

    struct FrameMetadata {
        cparams: CParams,
        dparams: DParams,
        chunksize: usize,
        nbytes: i64,
        cbytes: i64,
        metalayers: Vec<Metalayer>,
        vlmetalayers: Vec<Metalayer>,
        frame_vlblocks: bool,
    }

    #[derive(Debug)]
    pub(super) struct ParsedVlMetalayers {
        layers: Vec<Metalayer>,
        encoded: Vec<Option<Vec<u8>>>,
    }

    fn decode_frame_splitmode(other_flags: u8) -> i32 {
        match (other_flags & 0x03) + 1 {
            1 => BLOSC_ALWAYS_SPLIT,
            2 => BLOSC_NEVER_SPLIT,
            3 => BLOSC_AUTO_SPLIT,
            _ => BLOSC_FORWARD_COMPAT_SPLIT,
        }
    }

    fn validate_frame_codec_format(frame_compcode: u8) -> Result<(), String> {
        match frame_compcode {
            BLOSC_BLOSCLZ | BLOSC_LZ4 | BLOSC_LZ4HC | BLOSC_ZLIB | BLOSC_ZSTD
            | BLOSC_UDCODEC_FORMAT | BLOSC_CODEC_NDLZ => Ok(()),
            _ => Err("Invalid frame: unsupported codec".into()),
        }
    }

    /// Check that an embedded chunk header is well-formed and matches the
    /// codec, typesize and filter pipeline advertised by the frame header.
    fn validate_embedded_chunk_header(
        ch: &ChunkHeader,
        spec: &FrameChunkSpec,
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
        if ch.special_type() == BLOSC2_NO_SPECIAL
            && ch.blosc2_flags & (BLOSC2_INSTR_CODEC | BLOSC2_LAZY_CHUNK) != 0
        {
            return Err("Invalid frame: unsupported chunk flags".into());
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
                && ch.udcompcode != BLOSC_CODEC_NDLZ
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
                BLOSC_BLOSCLZ
                    | BLOSC_LZ4
                    | BLOSC_LZ4HC
                    | BLOSC_ZLIB
                    | BLOSC_ZSTD
                    | BLOSC_CODEC_NDLZ
            )
            && !crate::codecs::is_registered_codec(ch.compcode())
        {
            return Err("Invalid frame: unsupported chunk codec".into());
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
            for &filter in &ch.filters {
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
    pub(super) fn parse_metalayers(header: &[u8]) -> Result<Vec<Metalayer>, String> {
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
        if count > BLOSC2_MAX_METALAYERS {
            return Err("Invalid frame: too many metalayers".into());
        }

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
        let _value_count = u16::from_be_bytes(header[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        let values_start = pos;
        let values_end = header.len();

        let mut metalayers = Vec::with_capacity(count);
        for (name, offset) in index {
            if offset < values_start {
                return Err("Invalid frame: metalayer content offset before values".into());
            }
            if offset >= values_end {
                return Err("Invalid frame: invalid metalayer content offset".into());
            }
            let mut value_pos = offset;
            let content = decode_msgpack_bin(header, &mut value_pos, values_end)?;
            metalayers.push(Metalayer { name, content });
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
        if len > i32::MAX as usize {
            return Err("Invalid frame: metalayer content length out of range".into());
        }
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
    pub(super) fn offsets_chunk_len(
        data: &[u8],
        pos: usize,
        frame_size: usize,
    ) -> Result<usize, String> {
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
    pub(super) fn parse_vlmetalayers(
        trailer: &[u8],
        has_vlmetalayers: bool,
    ) -> Result<ParsedVlMetalayers, String> {
        if trailer.is_empty() && !has_vlmetalayers {
            return Ok(ParsedVlMetalayers {
                layers: Vec::new(),
                encoded: Vec::new(),
            });
        }
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
        if count > BLOSC2_MAX_VLMETALAYERS {
            return Err("Invalid frame: too many VL-metalayers".into());
        }

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
        let _value_count = u16::from_be_bytes(trailer[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let values_start = pos;
        if trailer.len() < 23 {
            return Err("Invalid frame: invalid trailer footer length".into());
        }
        let footer_start = trailer.len() - 23;
        if footer_start < values_start {
            return Err("Invalid frame: invalid trailer footer length".into());
        }
        let values_end = footer_start;
        if trailer.get(footer_start) != Some(&MSGPACK_UINT32) {
            return Err("Invalid frame: expected trailer length".into());
        }
        let declared_len = u32::from_be_bytes(
            trailer[footer_start + 1..footer_start + 5]
                .try_into()
                .unwrap(),
        ) as usize;
        if declared_len != trailer.len() {
            return Err("Invalid frame: trailer length mismatch".into());
        }
        if trailer.get(footer_start + 5) != Some(&MSGPACK_FIXEXT16) {
            return Err("Invalid frame: expected trailer fingerprint".into());
        }
        let _fingerprint_type = trailer[footer_start + 6];
        let _fingerprint = &trailer[footer_start + 7..footer_start + 23];

        let mut metalayers = Vec::with_capacity(count);
        let mut encoded = Vec::with_capacity(count);
        for (name, offset) in entries {
            if offset < values_start || offset >= values_end {
                return Err("Invalid frame: invalid VL-metalayer offset".into());
            }
            let mut value_pos = offset;
            let stored_content = decode_msgpack_bin(trailer, &mut value_pos, values_end)?;
            compress::cbuffer_validate(&stored_content)
                .map_err(|err| format!("Invalid frame: invalid VL-metalayer cbuffer: {err}"))?;
            let content = compress::decompress(&stored_content)
                .map_err(|err| format!("Invalid frame: invalid VL-metalayer cbuffer: {err}"))?;
            metalayers.push(Metalayer { name, content });
            encoded.push(Some(stored_content));
        }

        Ok(ParsedVlMetalayers {
            layers: metalayers,
            encoded,
        })
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
        base_offset: u64,
        pos: usize,
        frame_size: usize,
    ) -> Result<usize, String> {
        if pos >= frame_size {
            return Ok(0);
        }
        let absolute_pos = base_offset
            .checked_add(pos as u64)
            .ok_or_else(|| "Invalid frame: offsets chunk offset overflow".to_string())?;
        let absolute_end = base_offset
            .checked_add(frame_size as u64)
            .ok_or_else(|| "Invalid frame: frame size overflow".to_string())?;
        let header = read_chunk_header_at(file, absolute_pos, absolute_end)?;
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
        base_offset: u64,
        pos: usize,
        len: usize,
    ) -> Result<Vec<u64>, String> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let mut offsets_chunk = vec![0u8; len];
        let absolute_pos = base_offset
            .checked_add(pos as u64)
            .ok_or_else(|| "Invalid frame: offsets chunk offset overflow".to_string())?;
        read_exact_at(
            file,
            absolute_pos,
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
    fn read_sframe_index_at(
        path: &Path,
        offset: u64,
    ) -> Result<(Vec<u8>, usize, Vec<u64>, usize), String> {
        let index_path = path.join("chunks.b2frame");
        let full_index =
            std::fs::read(&index_path).map_err(|e| format!("Failed to read sframe index: {e}"))?;
        let offset =
            usize::try_from(offset).map_err(|_| "Sparse frame offset too large".to_string())?;
        if offset > full_index.len() {
            return Err("Sparse frame offset beyond end of index".into());
        }
        let index = &full_index[offset..];
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

        let offsets_len = offsets_chunk_len(index, header_size, frame_size)?;
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
        let frame_vlblocks = general_flags & FRAME_VL_BLOCKS != 0;
        let variable_chunks = general_flags & FRAME_VARIABLE_CHUNKS != 0;
        if index[26] != 1 {
            return Err("Invalid frame: expected sparse directory frame type".into());
        }

        let codec_flags = index[27];
        let frame_compcode = codec_flags & 0x0F;
        validate_frame_codec_format(frame_compcode)?;
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
        if variable_chunks && chunksize != 0 {
            return Err("Invalid frame: variable chunk flag with nonzero chunksize".into());
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
        let (filters, filters_meta) = read_frame_filters(index, 71, 79, nfilters);
        let use_dict = index[FRAME_OTHER_FLAGS2] & 0x01 != 0;
        let parsed_vlmetalayers = parse_vlmetalayers(&index[offsets_end..], has_vlmeta)?;
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
            vlmetalayers: parsed_vlmetalayers.layers,
            frame_vlblocks,
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
            let chunk_path = sframe_chunk_path(path, chunk_id);
            let mut file = std::fs::File::open(&chunk_path)
                .map_err(|e| format!("Failed to open sparse frame chunk: {e}"))?;
            let file_len = file
                .metadata()
                .map_err(|e| format!("Failed to stat sparse frame chunk: {e}"))?
                .len();
            let header = read_chunk_header_at(&mut file, 0, file_len)?;
            let chunk_cbytes = usize::try_from(header.cbytes)
                .map_err(|_| "Invalid frame: sparse chunk too large".to_string())?;
            let mut chunk = vec![0u8; chunk_cbytes];
            read_exact_at(
                &mut file,
                0,
                &mut chunk,
                "Failed to read sparse frame chunk",
            )?;
            compress::cbuffer_validate(&chunk).map_err(|err| format!("Invalid frame: {err}"))?;
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
        frame[39..47].copy_from_slice(&(data_cbytes as i64).to_be_bytes());
        for chunk in chunks {
            frame.extend_from_slice(&chunk);
        }
        frame.extend_from_slice(&offsets_chunk);
        frame.extend_from_slice(trailer);
        Ok(frame)
    }

    /// Read a sparse frame directory eagerly.
    pub fn read_sframe_dir(path: &Path) -> Result<Schunk, String> {
        read_sframe_dir_at(path, 0)
    }

    pub fn read_sframe_dir_at(path: &Path, offset: u64) -> Result<Schunk, String> {
        let (index, header_size, offsets, old_offsets_end) = read_sframe_index_at(path, offset)?;
        let frame = contiguous_frame_from_sframe_index(
            &index,
            header_size,
            &offsets,
            old_offsets_end,
            path,
        )?;
        let mut schunk = read_frame(&frame)?;
        schunk.storage = FrameStorage::Sparse;
        schunk.frame_offsets = Some(offsets);
        Ok(schunk)
    }

    /// Read a sparse frame directory lazily.
    pub fn read_lazy_sframe_dir(path: &Path) -> Result<LazySchunk, String> {
        read_lazy_sframe_dir_at(path, 0)
    }

    pub fn read_lazy_sframe_dir_at(path: &Path, offset: u64) -> Result<LazySchunk, String> {
        let (index, header_size, offsets, offsets_end) = read_sframe_index_at(path, offset)?;
        let meta = parse_sframe_index_metadata(&index, header_size, offsets_end)?;
        let chunk_spec = FrameChunkSpec {
            compcode: meta.cparams.compcode,
            typesize: meta.cparams.typesize,
        };
        let mut chunks = Vec::with_capacity(offsets.len());
        let mut total_nbytes = 0i64;
        let mut total_cbytes = 0i64;
        let mut unique_total_cbytes = 0i64;
        let mut unique_chunk_ids = std::collections::HashSet::new();
        let mut actual_vlblocks = false;
        let mut actual_regular_chunks = false;
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
                actual_regular_chunks = true;
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
            if header.vl_blocks() {
                actual_vlblocks = true;
            } else {
                actual_regular_chunks = true;
            }
            if header.cbytes as u64 > file_len {
                return Err("Invalid frame: sparse chunk size mismatch".into());
            }
            let cbytes = usize::try_from(header.cbytes)
                .map_err(|_| "Invalid frame: sparse chunk too large".to_string())?;
            total_nbytes = total_nbytes
                .checked_add(header.nbytes as i64)
                .ok_or_else(|| "Invalid frame: nbytes overflow".to_string())?;
            total_cbytes = total_cbytes
                .checked_add(header.cbytes as i64)
                .ok_or_else(|| "Invalid frame: cbytes overflow".to_string())?;
            if unique_chunk_ids.insert(chunk_id) {
                unique_total_cbytes = unique_total_cbytes
                    .checked_add(header.cbytes as i64)
                    .ok_or_else(|| "Invalid frame: cbytes overflow".to_string())?;
            }
            chunks.push(LazyChunkRef {
                offset: chunk_id,
                cbytes,
                nbytes: header.nbytes as usize,
                special: None,
            });
        }
        if total_cbytes != meta.cbytes && unique_total_cbytes != meta.cbytes {
            return Err("Invalid frame: chunk cbytes total does not match frame".into());
        }
        validate_frame_vlblocks_flag(
            meta.frame_vlblocks,
            meta.chunksize == 0,
            actual_vlblocks,
            actual_regular_chunks,
        )?;
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
            frame_offset: 0,
            chunks,
            sframe: true,
        })
    }

    /// Read a frame lazily and return file-backed chunk references.
    pub fn read_lazy_frame(path: &Path) -> Result<LazySchunk, String> {
        read_lazy_frame_at(path, 0)
    }

    /// Read a frame lazily from a byte offset and return file-backed chunk references.
    pub fn read_lazy_frame_at(path: &Path, base_offset: u64) -> Result<LazySchunk, String> {
        let mut file =
            std::fs::File::open(path).map_err(|e| format!("Failed to open frame file: {e}"))?;
        let file_len = file
            .metadata()
            .map_err(|e| format!("Failed to stat frame file: {e}"))?
            .len();
        let remaining_len = file_len
            .checked_sub(base_offset)
            .ok_or_else(|| "Frame offset beyond end of file".to_string())?;
        if remaining_len < FRAME_HEADER_MIN_LEN as u64 {
            return Err("Frame too small".into());
        }

        let mut header = vec![0u8; FRAME_HEADER_MIN_LEN];
        read_exact_at(
            &mut file,
            base_offset,
            &mut header,
            "Failed to read frame header",
        )?;

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
        if header_size as u64 > remaining_len {
            return Err("Frame truncated before data section".into());
        }
        header.resize(header_size, 0);
        if header_size > FRAME_HEADER_MIN_LEN {
            let extended_offset = base_offset
                .checked_add(FRAME_HEADER_MIN_LEN as u64)
                .ok_or_else(|| "Invalid frame: header offset overflow".to_string())?;
            read_exact_at(
                &mut file,
                extended_offset,
                &mut header[FRAME_HEADER_MIN_LEN..],
                "Failed to read extended frame header",
            )?;
        }
        let metalayers = parse_metalayers(&header)?;

        if header[15] != MSGPACK_UINT64 {
            return Err("Expected uint64 for frame_size".into());
        }
        let frame_size_u64 = u64::from_be_bytes(header[16..24].try_into().unwrap());
        if frame_size_u64 < header_size as u64 || frame_size_u64 > remaining_len {
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
        let frame_vlblocks = general_flags & FRAME_VL_BLOCKS != 0;
        let variable_chunks = general_flags & FRAME_VARIABLE_CHUNKS != 0;
        if header[26] != 0 {
            return Err("Invalid frame: unsupported frame type".into());
        }

        let codec_flags = header[27];
        let frame_compcode = codec_flags & 0x0F;
        validate_frame_codec_format(frame_compcode)?;
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
        if variable_chunks && chunksize != 0 {
            return Err("Invalid frame: variable chunk flag with nonzero chunksize".into());
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
        let chunk_spec = FrameChunkSpec { compcode, typesize };
        let offsets_len = if cbytes == 0 && nbytes == 0 {
            0
        } else {
            offsets_chunk_len_from_file(&mut file, base_offset, data_end, frame_size)?
        };
        let offsets = offsets_payload_from_file(&mut file, base_offset, data_end, offsets_len)?;
        validate_frame_offsets_count(offsets.len(), nbytes, chunksize)?;
        let mut actual_vlblocks = false;
        let mut actual_regular_chunks = false;
        let mut chunks = Vec::new();
        if offsets.is_empty() {
            let mut pos = data_start;
            while pos < data_end {
                let absolute_pos = base_offset
                    .checked_add(pos as u64)
                    .ok_or_else(|| "Invalid frame: chunk offset overflow".to_string())?;
                let absolute_data_end = base_offset
                    .checked_add(data_end as u64)
                    .ok_or_else(|| "Invalid frame: data section overflow".to_string())?;
                let ch = read_chunk_header_at(&mut file, absolute_pos, absolute_data_end)?;
                validate_embedded_chunk_header(&ch, &chunk_spec)?;
                if ch.vl_blocks() {
                    actual_vlblocks = true;
                } else {
                    actual_regular_chunks = true;
                }
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
                chunks.push(LazyChunkRef {
                    offset: absolute_pos,
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
                    actual_regular_chunks = true;
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
                let absolute_pos = base_offset
                    .checked_add(pos as u64)
                    .ok_or_else(|| "Invalid frame: chunk offset overflow".to_string())?;
                let absolute_data_end = base_offset
                    .checked_add(data_end as u64)
                    .ok_or_else(|| "Invalid frame: data section overflow".to_string())?;
                let ch = read_chunk_header_at(&mut file, absolute_pos, absolute_data_end)?;
                validate_embedded_chunk_header(&ch, &chunk_spec)?;
                if ch.vl_blocks() {
                    actual_vlblocks = true;
                } else {
                    actual_regular_chunks = true;
                }
                let chunk_cbytes = ch.cbytes as usize;
                let chunk_end = pos
                    .checked_add(chunk_cbytes)
                    .ok_or_else(|| "Invalid frame: chunk size overflow".to_string())?;
                if chunk_end > data_end {
                    return Err("Invalid frame: chunk extends past data section".into());
                }
                intervals.push((pos - data_start, chunk_end - data_start));
                total_nbytes = total_nbytes
                    .checked_add(ch.nbytes as i64)
                    .ok_or_else(|| "Invalid frame: nbytes overflow".to_string())?;
                total_cbytes = total_cbytes
                    .checked_add(ch.cbytes as i64)
                    .ok_or_else(|| "Invalid frame: cbytes overflow".to_string())?;
                chunks.push(LazyChunkRef {
                    offset: absolute_pos,
                    cbytes: chunk_cbytes,
                    nbytes: ch.nbytes as usize,
                    special: None,
                });
            }
            validate_frame_data_intervals(&mut intervals, cbytes as usize)?;
        }

        if offsets_len == 0 && total_cbytes != cbytes {
            return Err("Invalid frame: chunk cbytes total does not match frame".into());
        }
        validate_frame_vlblocks_flag(
            frame_vlblocks,
            variable_chunks,
            actual_vlblocks,
            actual_regular_chunks,
        )?;
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
            let absolute_trailer_start = base_offset
                .checked_add(trailer_start as u64)
                .ok_or_else(|| "Invalid frame: trailer offset overflow".to_string())?;
            read_exact_at(
                &mut file,
                absolute_trailer_start,
                &mut trailer,
                "Failed to read frame trailer",
            )?;
        }
        let parsed_vlmetalayers = parse_vlmetalayers(&trailer, has_vlmeta)?;

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
            vlmetalayers: parsed_vlmetalayers.layers,
            path: path.to_path_buf(),
            frame_offset: base_offset,
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
        let frame_vlblocks = general_flags & FRAME_VL_BLOCKS != 0;
        let variable_chunks = general_flags & FRAME_VARIABLE_CHUNKS != 0;
        let frame_type = data[26];
        let codec_flags = data[27];
        let other_flags = data[28];
        if frame_type != 0 {
            return Err("Invalid frame: unsupported frame type".into());
        }

        let frame_compcode = codec_flags & 0x0F;
        validate_frame_codec_format(frame_compcode)?;
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
        if variable_chunks && chunksize != 0 {
            return Err("Invalid frame: variable chunk flag with nonzero chunksize".into());
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
        let mut actual_vlblocks = false;
        let mut actual_regular_chunks = false;
        let chunk_spec = FrameChunkSpec { compcode, typesize };
        let offsets_len = if cbytes == 0 && nbytes == 0 {
            0
        } else {
            offsets_chunk_len(data, data_end, frame_size as usize)?
        };
        let mut chunks = Vec::new();
        let mut frame_offsets = Vec::new();
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
                frame_offsets.push(offset);
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
                    actual_regular_chunks = true;
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
                let ch = ChunkHeader::read(&data[pos..data_end])
                    .map_err(|_| "Invalid frame: invalid chunk header".to_string())?;
                validate_embedded_chunk_header(&ch, &chunk_spec)?;
                if ch.vl_blocks() {
                    actual_vlblocks = true;
                } else {
                    actual_regular_chunks = true;
                }
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

                let ch = ChunkHeader::read(&data[pos..data_end])
                    .map_err(|_| "Invalid frame: invalid chunk header".to_string())?;
                validate_embedded_chunk_header(&ch, &chunk_spec)?;
                if ch.vl_blocks() {
                    actual_vlblocks = true;
                } else {
                    actual_regular_chunks = true;
                }

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

        if offsets_len == 0 && total_cbytes != cbytes {
            return Err("Invalid frame: chunk cbytes total does not match frame".into());
        }
        validate_frame_vlblocks_flag(
            frame_vlblocks,
            variable_chunks,
            actual_vlblocks,
            actual_regular_chunks,
        )?;
        if total_nbytes != nbytes {
            return Err("Invalid frame: chunk nbytes total does not match frame".into());
        }
        let trailer_start = data_end
            .checked_add(offsets_len)
            .ok_or_else(|| "Invalid frame: trailer offset overflow".to_string())?;
        if trailer_start > frame_size as usize {
            return Err("Invalid frame: trailer starts past frame".into());
        }
        let parsed_vlmetalayers =
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
            vlmetalayers: parsed_vlmetalayers.layers,
            vlmetalayer_encoded: parsed_vlmetalayers.encoded,
            storage: FrameStorage::Contiguous,
            frame_offsets: Some(frame_offsets),
            attached_frame_len: Some(frame_size as i64),
            attached_frame: None,
            variable_chunks,
            vlblocks: actual_vlblocks,
            shared_chunks: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};

    static CONTEXT_FILTER_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    static LAST_PREFILTER_NCHUNK: AtomicI64 = AtomicI64::new(-99);
    static LAST_POSTFILTER_NCHUNK: AtomicI64 = AtomicI64::new(-99);
    static PARALLEL_POSTFILTER_NCHUNK_MASK: AtomicU64 = AtomicU64::new(0);
    static LAST_CONTEXT_FORWARD_SCHUNK: AtomicU64 = AtomicU64::new(0);
    static LAST_CONTEXT_BACKWARD_SCHUNK: AtomicU64 = AtomicU64::new(0);

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

    fn record_parallel_postfilter_nchunk(params: &mut compress::PostfilterParams<'_>) -> i32 {
        if (0..64).contains(&params.nchunk) {
            PARALLEL_POSTFILTER_NCHUNK_MASK.fetch_or(1u64 << params.nchunk, AtomicOrdering::SeqCst);
        }
        params.output.copy_from_slice(params.input);
        0
    }

    fn record_context_filter_schunk(
        ctx: &mut crate::filters::FilterCallbackContext<'_>,
        input: &[u8],
        output: &mut [u8],
    ) -> i32 {
        if ctx.cparams.is_some() {
            LAST_CONTEXT_FORWARD_SCHUNK.store(ctx.chunk.schunk as u64, AtomicOrdering::SeqCst);
        }
        if ctx.dparams.is_some() {
            LAST_CONTEXT_BACKWARD_SCHUNK.store(ctx.chunk.schunk as u64, AtomicOrdering::SeqCst);
        }
        output.copy_from_slice(input);
        crate::filters::PluginCallbackStatus::Success as i32
    }

    fn xor_record_prefilter_nchunk(params: &mut compress::PrefilterParams<'_>) -> i32 {
        LAST_PREFILTER_NCHUNK.store(params.nchunk, AtomicOrdering::SeqCst);
        for (dst, src) in params.output.iter_mut().zip(params.input.iter()) {
            *dst = *src ^ 0xA5;
        }
        0
    }

    fn xor_postfilter(params: &mut compress::PostfilterParams<'_>) -> i32 {
        for (dst, src) in params.output.iter_mut().zip(params.input.iter()) {
            *dst = *src ^ 0xA5;
        }
        0
    }

    fn first_vlmetalayer_cbuffer(frame: &[u8], data_cbytes: usize) -> Vec<u8> {
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        let data_end = header_size + data_cbytes;
        let offsets_header = ChunkHeader::read(&frame[data_end..]).unwrap();
        let trailer_start = data_end + offsets_header.cbytes as usize;
        let index_size = u16::from_be_bytes(
            frame[trailer_start + 4..trailer_start + 6]
                .try_into()
                .unwrap(),
        ) as usize;
        let content_marker_pos = trailer_start + 3 + index_size + 3;
        assert_eq!(frame[content_marker_pos], 0xC6);
        let content_len = u32::from_be_bytes(
            frame[content_marker_pos + 1..content_marker_pos + 5]
                .try_into()
                .unwrap(),
        ) as usize;
        frame[content_marker_pos + 5..content_marker_pos + 5 + content_len].to_vec()
    }

    #[test]
    fn test_schunk_compressed_chunk_mutators_preserve_bytes() {
        let cparams = CParams {
            typesize: 4,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut source = Schunk::new(cparams.clone(), DParams::default());
        source.append_buffer(&[1u8; 32]).unwrap();
        source.append_buffer(&[2u8; 32]).unwrap();
        source.append_buffer(&[3u8; 32]).unwrap();
        let first = source.compressed_chunk(0).unwrap().to_vec();
        let second = source.compressed_chunk(1).unwrap().to_vec();
        let third = source.compressed_chunk(2).unwrap().to_vec();

        let mut schunk = Schunk::new(cparams.clone(), DParams::default());
        assert_eq!(blosc2_schunk_append_chunk(&mut schunk, &first, true), 1);
        let mut third_with_tail = third.clone();
        third_with_tail.extend_from_slice(b"tail");
        assert_eq!(
            blosc2_schunk_insert_chunk_c(
                &mut schunk,
                1,
                &third_with_tail,
                third.len() as i64,
                true
            ),
            2
        );
        let mut second_with_tail = second.clone();
        second_with_tail.extend_from_slice(b"tail");
        assert_eq!(
            blosc2_schunk_update_chunk_c(
                &mut schunk,
                1,
                &second_with_tail,
                second.len() as i64,
                true
            ),
            2
        );
        assert_eq!(schunk.compressed_chunk(0).unwrap(), first.as_slice());
        assert_eq!(schunk.compressed_chunk(1).unwrap(), second.as_slice());
        assert_eq!(
            schunk.decompress_all().unwrap(),
            [[1u8; 32], [2u8; 32]].concat()
        );
        assert_eq!(blosc2_schunk_delete_chunk(&mut schunk, 0), 1);
        assert_eq!(schunk.decompress_all().unwrap(), [2u8; 32]);
        assert_eq!(
            blosc2_schunk_append_chunk_c(&mut schunk, &first, -1, true),
            i64::from(BLOSC2_ERROR_INVALID_PARAM)
        );
        assert_eq!(
            blosc2_schunk_append_chunk_c(&mut schunk, &first, (first.len() + 1) as i64, true),
            i64::from(BLOSC2_ERROR_INVALID_PARAM)
        );

        let bad = &first[..first.len() - 1];
        assert_eq!(
            schunk.append_chunk(bad),
            Err("Compressed chunk size mismatch")
        );
        assert!(blosc2_schunk_append_chunk(&mut schunk, bad, true) < 0);

        let mut vl = Schunk::new(CParams::default(), DParams::default());
        vl.append_vlblocks(&[b"one".as_slice()]).unwrap();
        assert_eq!(
            schunk.append_chunk(vl.compressed_chunk(0).unwrap()),
            Err("Cannot mix regular and VL-block chunks")
        );
        assert_eq!(
            blosc2_schunk_append_chunk(&mut schunk, vl.compressed_chunk(0).unwrap(), true),
            i64::from(BLOSC2_ERROR_CHUNK_APPEND)
        );
        assert_eq!(
            blosc2_schunk_insert_chunk(&mut schunk, 0, vl.compressed_chunk(0).unwrap(), true),
            i64::from(BLOSC2_ERROR_CHUNK_INSERT)
        );
        assert_eq!(blosc2_schunk_append_chunk(&mut schunk, &first, true), 2);
        assert_eq!(
            blosc2_schunk_update_chunk(&mut schunk, 0, vl.compressed_chunk(0).unwrap(), true),
            i64::from(BLOSC2_ERROR_CHUNK_UPDATE)
        );
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
    fn test_context_filter_callbacks_receive_live_schunk_handle() {
        let _guard = CONTEXT_FILTER_LOCK.lock().unwrap();
        const FILTER_ID: u8 = BLOSC2_USER_DEFINED_FILTERS_START + 83;
        let _ = crate::filters::register_context_filter(
            FILTER_ID,
            record_context_filter_schunk,
            record_context_filter_schunk,
        );
        LAST_CONTEXT_FORWARD_SCHUNK.store(0, AtomicOrdering::SeqCst);
        LAST_CONTEXT_BACKWARD_SCHUNK.store(0, AtomicOrdering::SeqCst);

        let mut schunk = Schunk::new(
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                filters: [0, 0, 0, 0, 0, FILTER_ID],
                ..Default::default()
            },
            DParams {
                typesize: 1,
                ..Default::default()
            },
        );
        let expected = &schunk as *const Schunk as u64;
        let data: Vec<u8> = (0..=255).cycle().take(4096).collect();

        schunk.append_buffer(&data).unwrap();
        assert_eq!(
            LAST_CONTEXT_FORWARD_SCHUNK.load(AtomicOrdering::SeqCst),
            expected
        );
        assert_eq!(schunk.decompress_chunk(0).unwrap(), data);
        assert_eq!(
            LAST_CONTEXT_BACKWARD_SCHUNK.load(AtomicOrdering::SeqCst),
            expected
        );
    }

    #[test]
    fn test_schunk_set_slice_runs_prefilter_with_chunk_index() {
        LAST_PREFILTER_NCHUNK.store(-99, AtomicOrdering::SeqCst);

        let mut schunk = Schunk::new(
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                blocksize: 16,
                filters: [0; BLOSC2_MAX_FILTERS],
                prefilter: Some(xor_record_prefilter_nchunk),
                ..Default::default()
            },
            DParams {
                postfilter: Some(xor_postfilter),
                typesize: 1,
                ..Default::default()
            },
        );

        let first = vec![1u8; 64];
        let second = vec![2u8; 64];
        let replacement = vec![3u8; 16];
        schunk.append_buffer(&first).unwrap();
        schunk.append_buffer(&second).unwrap();

        LAST_PREFILTER_NCHUNK.store(-99, AtomicOrdering::SeqCst);
        schunk.set_slice(64, &replacement).unwrap();
        assert_eq!(LAST_PREFILTER_NCHUNK.load(AtomicOrdering::SeqCst), 1);

        let mut expected_second = second;
        expected_second[..replacement.len()].copy_from_slice(&replacement);
        assert_eq!(schunk.decompress_chunk(0).unwrap(), first);
        assert_eq!(schunk.decompress_chunk(1).unwrap(), expected_second);
    }

    #[test]
    fn test_parallel_decompress_all_sets_postfilter_chunk_index() {
        PARALLEL_POSTFILTER_NCHUNK_MASK.store(0, AtomicOrdering::SeqCst);

        let mut schunk = Schunk::new(
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                filters: [0; BLOSC2_MAX_FILTERS],
                ..Default::default()
            },
            DParams {
                postfilter: Some(record_parallel_postfilter_nchunk),
                typesize: 1,
                nthreads: 2,
                ..Default::default()
            },
        );

        let first = vec![b'a'; 1024];
        let second = vec![b'b'; 1024];
        let third = vec![b'c'; 1024];
        schunk.append_buffer(&first).unwrap();
        schunk.append_buffer(&second).unwrap();
        schunk.append_buffer(&third).unwrap();

        let mut expected = first;
        expected.extend(second);
        expected.extend(third);
        assert_eq!(schunk.decompress_all().unwrap(), expected);
        assert_eq!(
            PARALLEL_POSTFILTER_NCHUNK_MASK.load(AtomicOrdering::SeqCst),
            0b111
        );
    }

    #[test]
    fn test_special_offset_decoding_requires_canonical_form() {
        let canonical = frame::encoded_special_offset(BLOSC2_SPECIAL_ZERO);
        assert_eq!(
            frame::special_type_from_offset(canonical),
            Some(BLOSC2_SPECIAL_ZERO)
        );
        assert_eq!(
            frame::special_type_from_offset(frame::encoded_special_offset(BLOSC2_SPECIAL_UNINIT)),
            Some(BLOSC2_SPECIAL_UNINIT)
        );
        assert_eq!(frame::special_type_from_offset(canonical | 0x1234), None);
        assert_eq!(
            frame::special_type_from_offset(
                frame::encoded_special_offset(BLOSC2_SPECIAL_UNINIT) | 0x00ff_ffff
            ),
            None
        );
        assert_eq!(frame::special_type_from_offset(0x8500_0000_0000_0001), None);
        assert_eq!(frame::special_type_from_offset(0x8000_0000_0000_0000), None);
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
    fn test_lazy_special_offset_ignores_unsupported_frame_filter() {
        let mut schunk = Schunk::new(
            CParams {
                typesize: 4,
                ..Default::default()
            },
            DParams::default(),
        );
        assert_eq!(
            blosc2_schunk_fill_special(&mut schunk, 4, BLOSC2_SPECIAL_ZERO, 16),
            1
        );

        let mut frame = schunk.to_frame();
        frame[71 + BLOSC2_MAX_FILTERS - 1] = BLOSC_FILTER_INT_TRUNC + 1;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lazy-special-unsupported-filter.b2frame");
        std::fs::write(&path, &frame).unwrap();

        let lazy = Schunk::open_lazy(&path).unwrap();
        assert!(lazy.compressed_chunk(0).is_ok());
        assert!(blosc2_schunk_get_lazychunk(&lazy, 0).0 > 0);
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

        assert_eq!(
            blosc2_schunk_get_cparams(&schunk).compcode,
            cparams.compcode
        );
        assert_eq!(
            blosc2_schunk_get_cparams(&schunk).typesize,
            cparams.typesize
        );
        assert_eq!(blosc2_schunk_get_dparams(&schunk).nthreads, 1);
        let (cparams_rc, cparams_c) = blosc2_schunk_get_cparams_c(&schunk);
        assert_eq!(cparams_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(cparams_c.compcode, cparams.compcode);
        assert_eq!(cparams_c.typesize, cparams.typesize);
        let (dparams_rc, dparams_c) = blosc2_schunk_get_dparams_c(&schunk);
        assert_eq!(dparams_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(dparams_c.nthreads, 1);
        schunk.append_buffer(b"aaaa").unwrap();
        schunk.append_buffer(b"cccc").unwrap();
        schunk.insert_buffer(1, b"bbbb").unwrap();
        assert_eq!(schunk.decompress_all().unwrap(), b"aaaabbbbcccc");
        assert_eq!(schunk.nchunks(), 3);
        assert_eq!(schunk.nbytes, 12);

        let mut wrapped = Schunk::new(cparams.clone(), DParams::default());
        assert_eq!(blosc2_schunk_append_buffer(&mut wrapped, b"aaaa"), 1);
        assert_eq!(blosc2_schunk_append_buffer(&mut wrapped, b"cccc"), 2);
        assert_eq!(blosc2_schunk_append_buffer_c(&mut wrapped, b"ddddxx", 4), 3);
        assert_eq!(
            blosc2_schunk_append_buffer_c(&mut wrapped, b"bad", -1),
            i64::from(BLOSC2_ERROR_INVALID_PARAM)
        );
        assert_eq!(
            blosc2_schunk_append_buffer_c(&mut wrapped, b"bad", 4),
            i64::from(BLOSC2_ERROR_INVALID_PARAM)
        );
        assert_eq!(
            blosc2_schunk_insert_buffer_c(&mut wrapped, 1, b"bbbbxx", 4),
            4
        );
        assert_eq!(
            blosc2_schunk_insert_buffer_c(&mut wrapped, 1, b"bad", -1),
            i64::from(BLOSC2_ERROR_INVALID_PARAM)
        );
        assert_eq!(
            blosc2_schunk_insert_buffer_c(&mut wrapped, 1, b"bad", 4),
            i64::from(BLOSC2_ERROR_INVALID_PARAM)
        );
        assert_eq!(blosc2_schunk_delete_chunk(&mut wrapped, 3), 3);
        assert_eq!(blosc2_schunk_delete_chunk(&mut wrapped, 99), -1);
        assert_eq!(wrapped.decompress_all().unwrap(), b"aaaabbbbcccc");
        let (chunk_rc, chunk, needs_free) = blosc2_schunk_get_chunk(&wrapped, 1);
        assert_eq!(chunk_rc, wrapped.compressed_chunk(1).unwrap().len() as i32);
        assert!(!needs_free);
        assert_eq!(
            chunk.unwrap(),
            wrapped.compressed_chunk(1).unwrap().to_vec()
        );
        let (chunk_ref_rc, chunk_ref, chunk_ref_needs_free) =
            blosc2_schunk_get_chunk_ref(&wrapped, 1);
        assert_eq!(
            chunk_ref_rc,
            wrapped.compressed_chunk(1).unwrap().len() as i32
        );
        assert!(!chunk_ref_needs_free);
        assert_eq!(chunk_ref.unwrap(), wrapped.compressed_chunk(1).unwrap());
        assert_eq!(
            blosc2_schunk_get_chunk(&wrapped, wrapped.nchunks()).0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_schunk_get_chunk(&wrapped, -1).0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        let mut chunk_dest = vec![0u8; 4];
        assert_eq!(
            blosc2_schunk_decompress_chunk(&wrapped, 1, &mut chunk_dest),
            4
        );
        assert_eq!(chunk_dest, b"bbbb");
        chunk_dest.fill(0);
        assert_eq!(
            blosc2_schunk_decompress_chunk_c(&wrapped, 1, &mut chunk_dest, 4),
            4
        );
        assert_eq!(chunk_dest, b"bbbb");
        assert_eq!(
            blosc2_schunk_decompress_chunk_c(&wrapped, 1, &mut chunk_dest, 3),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        assert_eq!(
            blosc2_schunk_decompress_chunk_c(&wrapped, 1, &mut chunk_dest, -1),
            BLOSC2_ERROR_INVALID_PARAM
        );
        let mut short_dest = vec![0u8; 3];
        assert_eq!(
            blosc2_schunk_decompress_chunk(&wrapped, 1, &mut short_dest),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        assert_eq!(
            blosc2_schunk_decompress_chunk(&wrapped, wrapped.nchunks(), &mut chunk_dest),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_schunk_update_buffer_c(&mut wrapped, 1, b"BBBBxx", 4),
            3
        );
        assert_eq!(wrapped.decompress_all().unwrap(), b"aaaaBBBBcccc");
        assert_eq!(
            blosc2_schunk_update_buffer_c(&mut wrapped, 1, b"bad", -1),
            i64::from(BLOSC2_ERROR_INVALID_PARAM)
        );
        assert_eq!(
            blosc2_schunk_update_buffer_c(&mut wrapped, 1, b"bad", 4),
            i64::from(BLOSC2_ERROR_INVALID_PARAM)
        );
        assert!(blosc2_schunk_update_buffer(&mut wrapped, 10, b"bad") < 0);

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
    fn test_schunk_item_slice_wrappers_are_typesize_based() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            blocksize: 16,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams.clone(), DParams::default());
        let data: Vec<u8> = (0..6u32).flat_map(u32::to_le_bytes).collect();
        schunk.append_buffer(&data[..12]).unwrap();
        schunk.append_buffer(&data[12..]).unwrap();

        assert_eq!(
            schunk.get_slice_items(1, 3).unwrap(),
            [1u32.to_le_bytes(), 2u32.to_le_bytes()].concat()
        );
        let mut slice_dest = vec![0u8; 8];
        assert_eq!(
            blosc2_schunk_get_slice_buffer(&schunk, 1, 3, &mut slice_dest),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            blosc2_schunk_get_slice_buffer_c(&schunk, 1, 3, &mut slice_dest),
            BLOSC2_ERROR_SUCCESS
        );
        let mut wide_slice_dest = vec![0xff; 12];
        assert_eq!(
            blosc2_schunk_get_slice_buffer_size_c(&schunk, 1, 3, &mut wide_slice_dest, 12),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            &wide_slice_dest[..8],
            [1u32.to_le_bytes(), 2u32.to_le_bytes()].concat().as_slice()
        );
        assert_eq!(&wide_slice_dest[8..], &[0, 0, 0, 0]);
        assert_eq!(
            blosc2_schunk_get_slice_buffer_size_c(&schunk, 1, 3, &mut wide_slice_dest, 7),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        assert_eq!(
            blosc2_schunk_get_slice_buffer_c(&schunk, -1, 3, &mut slice_dest),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_schunk_get_slice_nchunks_c(&schunk, -1, 3),
            (BLOSC2_ERROR_INVALID_PARAM, None)
        );
        assert_eq!(
            slice_dest,
            [1u32.to_le_bytes(), 2u32.to_le_bytes()].concat()
        );
        let mut short_dest = vec![0u8; 7];
        assert!(blosc2_schunk_get_slice_buffer(&schunk, 1, 3, &mut short_dest) < 0);
        assert_eq!(
            blosc2_schunk_get_slice_buffer(&schunk, 3, 2, &mut slice_dest),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_schunk_get_slice_buffer(&schunk, 0, 7, &mut slice_dest),
            BLOSC2_ERROR_INVALID_PARAM
        );
        schunk
            .set_slice_items(2, 4, &[20u32.to_le_bytes(), 21u32.to_le_bytes()].concat())
            .unwrap();
        assert_eq!(
            blosc2_schunk_set_slice_buffer(&mut schunk, 0, 1, &[100u32.to_le_bytes()].concat()),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            blosc2_schunk_set_slice_buffer_c(&mut schunk, -1, 1, &[100u32.to_le_bytes()].concat()),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_schunk_set_slice_buffer_size_c(
                &mut schunk,
                1,
                2,
                &[101u32.to_le_bytes().as_slice(), b"tail"].concat(),
                4
            ),
            BLOSC2_ERROR_SUCCESS
        );
        let expected: Vec<u8> = [100u32, 101, 20, 21, 4, 5]
            .into_iter()
            .flat_map(u32::to_le_bytes)
            .collect();
        assert_eq!(schunk.decompress_all().unwrap(), expected);
        assert_eq!(
            blosc2_schunk_set_slice_buffer(&mut schunk, 0, 2, &[1, 2]),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_schunk_set_slice_buffer(&mut schunk, 3, 2, &[]),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert!(schunk.set_slice_items(0, 2, &[1, 2]).is_err());
        assert!(schunk.get_slice_items(3, 2).is_err());
    }

    #[test]
    fn test_schunk_copy_with_params_recompresses_and_preserves_metalayers() {
        let src_cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            blocksize: 16,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };
        let mut schunk = Schunk::new(src_cparams.clone(), DParams::default());
        let first: Vec<u8> = (0..64u8).collect();
        let second: Vec<u8> = (64..128u8).collect();
        schunk.append_buffer(&first).unwrap();
        schunk.append_buffer(&second).unwrap();
        schunk.add_metalayer("fixed", b"meta").unwrap();
        schunk.add_vlmetalayer("variable", b"payload").unwrap();

        let zstd_first = compress::compress(
            &first,
            &CParams {
                compcode: BLOSC_ZSTD,
                clevel: 7,
                typesize: 1,
                blocksize: 16,
                splitmode: BLOSC_NEVER_SPLIT,
                filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
                ..Default::default()
            },
        )
        .unwrap();
        schunk.update_compressed_chunk(0, &zstd_first).unwrap();

        let raw_copied = schunk
            .copy_schunk_with_params(src_cparams.clone(), DParams::default())
            .unwrap();
        assert_eq!(raw_copied.get_cparams().compcode, BLOSC_LZ4);
        assert_eq!(raw_copied.get_dparams().nthreads, 1);
        assert_eq!(
            raw_copied.compressed_chunk(0).unwrap(),
            zstd_first.as_slice()
        );
        assert_eq!(
            raw_copied.decompress_all().unwrap(),
            schunk.decompress_all().unwrap()
        );

        let dst_cparams = CParams {
            compcode: BLOSC_BLOSCLZ,
            clevel: 9,
            typesize: 1,
            blocksize: 32,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };
        let copied = schunk
            .copy_schunk_with_params(dst_cparams, DParams::default())
            .unwrap();

        assert_eq!(
            copied.decompress_all().unwrap(),
            schunk.decompress_all().unwrap()
        );
        assert_eq!(copied.metalayer("fixed"), Some(&b"meta"[..]));
        assert_eq!(copied.vlmetalayer("variable"), Some(&b"payload"[..]));
        let copied_header = ChunkHeader::read(copied.compressed_chunk(0).unwrap()).unwrap();
        assert_eq!(copied_header.compcode(), BLOSC_BLOSCLZ);
        assert_eq!(copied_header.blocksize, 32);

        let mut changed = copied.clone();
        changed.update_chunk(0, &[7u8; 64]).unwrap();
        assert_eq!(schunk.decompress_chunk(0).unwrap(), first);
    }

    #[test]
    fn test_schunk_c_name_frame_io_aliases() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams.clone(), DParams::default());
        schunk.append_buffer(b"chunk-zero").unwrap();
        schunk.append_buffer(b"chunk-one").unwrap();
        let (new_rc, new_schunk) = blosc2_schunk_new_c(None, None);
        assert_eq!(new_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(new_schunk.unwrap().nchunks(), 0);
        assert_eq!(blosc2_schunk_free_c(None), BLOSC2_ERROR_SUCCESS);

        let (frame_len, frame, needs_free) = blosc2_schunk_to_buffer(&schunk);
        let frame = frame.unwrap();
        assert_eq!(frame_len, frame.len() as i64);
        assert!(needs_free);
        let restored = blosc2_schunk_from_buffer(&frame, frame_len, true).unwrap();
        let (from_buffer_rc, from_buffer_c) = blosc2_schunk_from_buffer_c(&frame, frame_len, true);
        assert_eq!(from_buffer_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(
            from_buffer_c.unwrap().decompress_all().unwrap(),
            b"chunk-zerochunk-one"
        );
        assert_eq!(
            blosc2_schunk_from_buffer_c(&frame, -1, true).0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(restored.decompress_all().unwrap(), b"chunk-zerochunk-one");
        assert_eq!(
            blosc2_schunk_from_buffer(&frame, frame_len, false)
                .unwrap()
                .decompress_all()
                .unwrap(),
            b"chunk-zerochunk-one"
        );
        let mut frame_with_trailing = frame.clone();
        frame_with_trailing.extend_from_slice(b"trailing");
        assert!(blosc2_schunk_from_buffer(&frame_with_trailing, frame_len, true).is_ok());
        assert!(blosc2_schunk_from_buffer(
            &frame_with_trailing,
            frame_with_trailing.len() as i64,
            true
        )
        .is_err());
        assert!(
            blosc2_schunk_from_buffer_c(
                &frame_with_trailing,
                frame_with_trailing.len() as i64,
                true
            )
            .0 < 0
        );

        let copied = blosc2_schunk_copy(&restored, None, None).unwrap();
        assert_eq!(
            copied.decompress_all().unwrap(),
            restored.decompress_all().unwrap()
        );
        let (copy_rc, copy_c) = blosc2_schunk_copy_c(&restored, None, None);
        assert_eq!(copy_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(
            copy_c.unwrap().decompress_all().unwrap(),
            restored.decompress_all().unwrap()
        );

        let (offset_rc, offsets) = blosc2_frame_get_offsets(&restored);
        assert_eq!(offset_rc, BLOSC2_ERROR_SUCCESS);
        assert_eq!(offsets.unwrap().len(), restored.nchunks() as usize);

        let (nchunks, chunks) = blosc2_get_slice_nchunks(&restored, 1, 11);
        assert_eq!(nchunks, chunks.as_ref().unwrap().len() as i32);
        assert_eq!(chunks.unwrap(), vec![0, 1]);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("frame.b2frame");
        let frame_len = blosc2_schunk_to_file(&restored, path.to_str().unwrap());
        assert_eq!(frame_len, frame.len() as i64);
        assert_eq!(
            blosc2_schunk_to_file(
                &restored,
                dir.path().join("missing/frame.b2frame").to_str().unwrap()
            ),
            i64::from(BLOSC2_ERROR_FILE_OPEN)
        );
        let opened = blosc2_schunk_open(path.to_str().unwrap()).unwrap();
        assert_eq!(
            opened.decompress_all().unwrap(),
            restored.decompress_all().unwrap()
        );
        assert_eq!(
            blosc2_schunk_open_c(path.to_str().unwrap()).0,
            BLOSC2_ERROR_SUCCESS
        );

        let offset = blosc2_schunk_append_file(&restored, &path);
        assert_eq!(offset, frame_len);
        assert_eq!(
            blosc2_schunk_append_file(&restored, dir.path().join("missing/frame.b2frame")),
            i64::from(BLOSC2_ERROR_FILE_OPEN)
        );
        assert!(blosc2_schunk_open_offset(&path, -1).is_err());
        let opened_negative_offset = blosc2_schunk_open_offset_c(&path, -1);
        assert_ne!(opened_negative_offset.0, BLOSC2_ERROR_SUCCESS);
        assert!(opened_negative_offset.1.is_none());
        let opened_offset = blosc2_schunk_open_offset(&path, offset).unwrap();
        assert_eq!(
            blosc2_schunk_open_offset_c(&path, offset).0,
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            opened_offset.decompress_all().unwrap(),
            restored.decompress_all().unwrap()
        );
        let lazy = blosc2_schunk_open_lazy(&path).unwrap();
        assert_eq!(blosc2_schunk_open_lazy_c(&path).0, BLOSC2_ERROR_SUCCESS);
        let (lazy_cbytes, lazy_chunk) = blosc2_schunk_get_lazychunk(&lazy, 0);
        assert!(lazy_cbytes > 0);
        assert!(lazy_chunk.unwrap().len() >= BLOSC_EXTENDED_HEADER_LENGTH);
        assert!(blosc2_schunk_open_lazy_offset(&path, -1).is_err());
        let opened_negative_lazy_offset = blosc2_schunk_open_lazy_offset_c(&path, -1);
        assert_ne!(opened_negative_lazy_offset.0, BLOSC2_ERROR_SUCCESS);
        assert!(opened_negative_lazy_offset.1.is_none());
        let lazy_offset = blosc2_schunk_open_lazy_offset(&path, offset).unwrap();
        assert_eq!(
            blosc2_schunk_open_lazy_offset_c(&path, offset).0,
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            lazy_offset.decompress_chunk(0).unwrap(),
            b"chunk-zero".to_vec()
        );
        let lazy_missing = blosc2_schunk_open_lazy(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            blosc2_schunk_get_lazychunk(&lazy_missing, 0).0,
            BLOSC2_ERROR_FILE_OPEN
        );
        assert_eq!(
            blosc2_schunk_get_lazychunk_c(&lazy_missing, 0).0,
            BLOSC2_ERROR_FILE_OPEN
        );
    }

    #[test]
    fn test_lazychunk_accessors_return_c_shaped_lazy_chunks() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            blocksize: 256,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let data: Vec<u8> = (0..4096).map(|idx| (idx % 4) as u8).collect();
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.append_buffer(&data).unwrap();
        let eager_chunk = schunk.compressed_chunk(0).unwrap().to_vec();
        let (eager_cbytes, eager_lazychunk) = blosc2_schunk_get_lazychunk(&schunk, 0);
        assert_eq!(eager_cbytes as usize, eager_chunk.len());
        assert_eq!(eager_lazychunk.unwrap(), eager_chunk);
        let (eager_cbytes_c, eager_lazychunk_c, eager_needs_free) =
            blosc2_schunk_get_lazychunk_c(&schunk, 0);
        assert_eq!(eager_cbytes_c as usize, eager_chunk.len());
        assert_eq!(eager_lazychunk_c.unwrap(), eager_chunk);
        assert!(!eager_needs_free);
        assert_eq!(
            blosc2_schunk_get_lazychunk(&schunk, -1).0,
            BLOSC2_ERROR_INVALID_PARAM
        );

        let dir = tempfile::tempdir().unwrap();
        let frame_path = dir.path().join("lazy-chunk.b2frame");
        schunk.to_file(frame_path.to_str().unwrap()).unwrap();
        let frame = std::fs::read(&frame_path).unwrap();
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        let lazy = Schunk::open_lazy(&frame_path).unwrap();
        let full_cbytes = lazy.chunk_refs()[0].cbytes;
        let (lazy_cbytes, lazy_chunk) = blosc2_schunk_get_lazychunk(&lazy, 0);
        let lazy_chunk = lazy_chunk.unwrap();
        assert_eq!(lazy_cbytes as usize, lazy_chunk.len());
        let (lazy_cbytes_c, lazy_chunk_c, lazy_needs_free) =
            blosc2_schunk_get_lazychunk_c(&lazy, 0);
        assert_eq!(lazy_cbytes_c as usize, lazy_chunk.len());
        assert_eq!(lazy_chunk_c.unwrap(), lazy_chunk);
        assert!(lazy_needs_free);
        assert!(lazy_chunk.len() < full_cbytes);
        assert_eq!(
            i32::from_le_bytes(lazy_chunk[12..16].try_into().unwrap()) as usize,
            full_cbytes
        );
        assert_ne!(lazy_chunk[BLOSC2_CHUNK_BLOSC2_FLAGS] & BLOSC2_LAZY_CHUNK, 0);

        let header = ChunkHeader::read(&lazy_chunk).unwrap();
        assert!(!header.memcpyed());
        let nblocks = header.nblocks();
        assert!(nblocks > 1);
        let bstarts_len = nblocks * 4;
        let trailer_offset = BLOSC_EXTENDED_HEADER_LENGTH + bstarts_len;
        assert_eq!(lazy_chunk.len(), trailer_offset + 12 + bstarts_len);
        assert_eq!(
            i32::from_le_bytes(
                lazy_chunk[trailer_offset..trailer_offset + 4]
                    .try_into()
                    .unwrap()
            ),
            0
        );
        assert_eq!(
            i64::from_le_bytes(
                lazy_chunk[trailer_offset + 4..trailer_offset + 12]
                    .try_into()
                    .unwrap()
            ),
            header_size as i64
        );

        let sframe_path = dir.path().join("lazy-chunk.sframe");
        schunk.to_sframe_dir(&sframe_path).unwrap();
        let lazy_sframe = Schunk::open_lazy_sframe(&sframe_path).unwrap();
        let (s_lazy_cbytes, s_lazy_chunk) = blosc2_schunk_get_lazychunk(&lazy_sframe, 0);
        let s_lazy_chunk = s_lazy_chunk.unwrap();
        assert_eq!(s_lazy_cbytes as usize, s_lazy_chunk.len());
        assert_ne!(
            s_lazy_chunk[BLOSC2_CHUNK_BLOSC2_FLAGS] & BLOSC2_LAZY_CHUNK,
            0
        );
        let s_header = ChunkHeader::read(&s_lazy_chunk).unwrap();
        let s_trailer_offset = BLOSC_EXTENDED_HEADER_LENGTH + s_header.nblocks() * 4;
        assert_eq!(
            i32::from_le_bytes(
                s_lazy_chunk[s_trailer_offset..s_trailer_offset + 4]
                    .try_into()
                    .unwrap()
            ),
            lazy_sframe.chunk_refs()[0].offset as i32
        );
        assert_eq!(
            i64::from_le_bytes(
                s_lazy_chunk[s_trailer_offset + 4..s_trailer_offset + 12]
                    .try_into()
                    .unwrap()
            ),
            lazy_sframe.chunk_refs()[0].offset as i64
        );

        let memcpy_cparams = CParams {
            compcode: BLOSC_BLOSCLZ,
            clevel: 0,
            typesize: 1,
            blocksize: 128,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let mut memcpy_schunk = Schunk::new(memcpy_cparams, DParams::default());
        memcpy_schunk.append_buffer(&data[..300]).unwrap();
        let memcpy_path = dir.path().join("lazy-memcpy.b2frame");
        memcpy_schunk
            .to_file(memcpy_path.to_str().unwrap())
            .unwrap();
        let memcpy_lazy = Schunk::open_lazy(&memcpy_path).unwrap();
        let (_, memcpy_lazy_chunk) = blosc2_schunk_get_lazychunk(&memcpy_lazy, 0);
        let memcpy_lazy_chunk = memcpy_lazy_chunk.unwrap();
        let memcpy_header = ChunkHeader::read(&memcpy_lazy_chunk).unwrap();
        assert!(memcpy_header.memcpyed());
        assert_ne!(
            memcpy_lazy_chunk[BLOSC2_CHUNK_BLOSC2_FLAGS] & BLOSC2_LAZY_CHUNK,
            0
        );
        let memcpy_nblocks = memcpy_header.nblocks();
        let memcpy_trailer_offset = BLOSC_EXTENDED_HEADER_LENGTH + memcpy_nblocks * 4;
        assert_eq!(
            memcpy_lazy_chunk.len(),
            memcpy_trailer_offset + 12 + memcpy_nblocks * 4
        );
        let memcpy_csizes = &memcpy_lazy_chunk
            [memcpy_lazy_chunk.len() - memcpy_nblocks * 4..memcpy_lazy_chunk.len()];
        assert_eq!(
            i32::from_le_bytes(memcpy_csizes[0..4].try_into().unwrap()),
            128
        );
        assert_eq!(
            i32::from_le_bytes(memcpy_csizes[8..12].try_into().unwrap()),
            44
        );

        let mut vl_schunk = Schunk::new(CParams::default(), DParams::default());
        let vl_blocks: [&[u8]; 3] = [b"red\0", b"green-green\0", b"blue-blue-blue\0"];
        vl_schunk.append_vlblocks(&vl_blocks).unwrap();
        let vl_path = dir.path().join("lazy-vl.b2frame");
        vl_schunk.to_file(vl_path.to_str().unwrap()).unwrap();
        let vl_lazy = Schunk::open_lazy(&vl_path).unwrap();
        let (_, vl_lazy_chunk) = blosc2_schunk_get_lazychunk(&vl_lazy, 0);
        let vl_lazy_chunk = vl_lazy_chunk.unwrap();
        let vl_header = ChunkHeader::read(&vl_lazy_chunk).unwrap();
        assert!(vl_header.vl_blocks());
        assert_eq!(vl_header.blocksize as usize, vl_blocks.len());
        assert_ne!(
            vl_lazy_chunk[BLOSC2_CHUNK_BLOSC2_FLAGS] & BLOSC2_LAZY_CHUNK,
            0
        );
        let vl_nblocks = vl_header.blocksize as usize;
        assert_eq!(
            vl_lazy_chunk.len(),
            BLOSC_EXTENDED_HEADER_LENGTH + vl_nblocks * 8 + 12
        );
    }

    #[test]
    fn test_schunk_copy_with_params_preserves_vlblocks() {
        let src_cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };
        let mut schunk = Schunk::new(src_cparams, DParams::default());
        schunk
            .append_vlblocks(&[b"alpha".as_slice(), b"bravo-bravo".as_slice()])
            .unwrap();
        schunk.add_metalayer("kind", b"vl").unwrap();
        schunk.add_vlmetalayer("owner", b"rust").unwrap();

        let copied = schunk
            .copy_schunk_with_params(
                CParams {
                    compcode: BLOSC_ZSTD,
                    clevel: 7,
                    typesize: 1,
                    splitmode: BLOSC_NEVER_SPLIT,
                    filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
                    ..Default::default()
                },
                DParams::default(),
            )
            .unwrap();

        assert_eq!(copied.metalayer("kind"), Some(&b"vl"[..]));
        assert_eq!(copied.vlmetalayer("owner"), Some(&b"rust"[..]));
        assert_eq!(copied.decompress_vlblock(0, 0).unwrap(), b"alpha");
        assert_eq!(copied.decompress_vlblock(0, 1).unwrap(), b"bravo-bravo");
        assert_eq!(
            copied.decompress_all().unwrap(),
            schunk.decompress_all().unwrap()
        );
        assert!(ChunkHeader::read(copied.compressed_chunk(0).unwrap())
            .unwrap()
            .vl_blocks());
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
        let mut schunk = Schunk::new(cparams.clone(), DParams::default());
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
        const CODEC_ID: u8 = 204;
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
        special_schunk.append_buffer(&[0u8; 128]).unwrap();
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
        assert_eq!(
            schunk.frame_get_offsets().unwrap(),
            offsets
                .iter()
                .map(|&offset| offset as i64)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            blosc2_schunk_get_slice_nchunks(&schunk, 2, 8),
            (2, Some(vec![0, 1]))
        );
        assert_eq!(
            blosc2_schunk_get_slice_nchunks(&schunk, 4, 4),
            (0, Some(vec![]))
        );
        assert_eq!(
            blosc2_schunk_get_slice_nchunks(&schunk, 3, 3),
            (1, Some(vec![0]))
        );
        assert!(blosc2_schunk_get_slice_nchunks(&schunk, 8, 2).0 < 0);
        let empty = Schunk::new(CParams::default(), DParams::default());
        assert_eq!(
            blosc2_schunk_get_slice_nchunks(&empty, 10, 20),
            (0, Some(vec![]))
        );
        assert_eq!(
            blosc2_schunk_get_slice_nchunks_c(&empty, 10, 20),
            (0, Some(vec![]))
        );
        assert_eq!(blosc2_get_slice_nchunks(&empty, 10, 20), (0, Some(vec![])));
        assert_eq!(
            blosc2_schunk_frame_get_offsets(&schunk),
            (BLOSC2_ERROR_FAILURE, None)
        );
        let opened_empty =
            Schunk::from_frame(&Schunk::new(CParams::default(), DParams::default()).to_frame())
                .unwrap();
        let (empty_code, empty_offsets) = blosc2_frame_get_offsets(&opened_empty);
        assert_ne!(empty_code, BLOSC2_ERROR_SUCCESS);
        assert!(empty_offsets.is_none());
        assert_eq!(
            blosc2_schunk_frame_len(&schunk),
            schunk.cbytes + schunk.nchunks() * 8
        );
        let mut typed = Schunk::new(
            CParams {
                typesize: 4,
                ..Default::default()
            },
            DParams::default(),
        );
        typed.append_buffer(&[1u8; 16]).unwrap();
        typed.append_buffer(&[2u8; 16]).unwrap();
        assert_eq!(blosc2_get_slice_nchunks(&typed, 4, 5), (1, Some(vec![1])));
        let (code, offsets) = blosc2_frame_get_offsets(&typed);
        assert_ne!(code, BLOSC2_ERROR_SUCCESS);
        assert!(offsets.is_none());

        assert_eq!(schunk.chunk_range_for_byte_slice(2, 6).unwrap(), 0..2);
        assert_eq!(schunk.get_slice_nchunks(2, 8).unwrap(), vec![0, 1]);
        assert_eq!(schunk.chunk_range_for_byte_slice(4, 6).unwrap(), 1..2);
        assert_eq!(schunk.get_slice_nchunks(4, 10).unwrap(), vec![1]);
        assert_eq!(schunk.chunk_range_for_byte_slice(12, 0).unwrap(), 3..3);
        assert_eq!(schunk.get_slice_nchunks(12, 12).unwrap(), Vec::<i64>::new());
        assert!(schunk.chunk_range_for_byte_slice(12, 1).is_err());
        assert!(schunk.get_slice_nchunks(0, 13).is_err());
        assert!(schunk.get_slice_nchunks(8, 2).is_err());

        schunk.reorder_chunks(&[2, 0, 1]).unwrap();
        assert_eq!(schunk.decompress_all().unwrap(), b"ccaaaabbbbbb");
        assert_eq!(schunk.chunksize, 0);

        assert_eq!(
            blosc2_schunk_reorder_offsets(&mut schunk, &[1, 2, 0]),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(schunk.decompress_all().unwrap(), b"aaaabbbbbbcc");
        assert_eq!(
            blosc2_schunk_reorder_offsets(&mut schunk, &[0, 0, 1]),
            BLOSC2_ERROR_DATA
        );
        assert_eq!(schunk.decompress_all().unwrap(), b"aaaabbbbbbcc");
        assert_eq!(
            blosc2_schunk_reorder_offsets(&mut schunk, &[0, 1]),
            BLOSC2_ERROR_DATA
        );
        assert!(schunk.reorder_chunks(&[0, 0, 1]).is_err());
        assert!(schunk.reorder_chunks(&[0, 1]).is_err());
        assert_eq!(schunk.decompress_all().unwrap(), b"aaaabbbbbbcc");
    }

    #[test]
    fn test_fill_special_zero_nan_uninit_with_leftover_chunk() {
        let mut zeros = Schunk::new(
            CParams {
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        );
        assert_eq!(zeros.fill_special(10, BLOSC2_SPECIAL_ZERO, 4).unwrap(), 3);
        let mut wrapped_zeros = Schunk::new(
            CParams {
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        );
        assert_eq!(
            blosc2_schunk_fill_special(&mut wrapped_zeros, 10, BLOSC2_SPECIAL_ZERO, 4),
            3
        );
        assert_eq!(wrapped_zeros.decompress_all().unwrap(), vec![0u8; 10]);
        assert_eq!(
            blosc2_schunk_fill_special(&mut wrapped_zeros, 1, BLOSC2_SPECIAL_ZERO, 1),
            i64::from(BLOSC2_ERROR_FRAME_SPECIAL)
        );
        assert_eq!(
            blosc2_schunk_fill_special(&mut wrapped_zeros, 0, BLOSC2_SPECIAL_ZERO, 0),
            0
        );
        assert_eq!(blosc2_schunk_fill_special(&mut wrapped_zeros, 0, 99, 0), 0);
        assert_eq!(wrapped_zeros.decompress_all().unwrap(), vec![0u8; 10]);
        let mut invalid_special = Schunk::new(
            CParams {
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        );
        assert_eq!(
            blosc2_schunk_fill_special(&mut invalid_special, 1, 99, 1),
            i64::from(BLOSC2_ERROR_SCHUNK_SPECIAL)
        );
        let mut invalid_chunksize = Schunk::new(
            CParams {
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        );
        assert_eq!(
            blosc2_schunk_fill_special(&mut invalid_chunksize, 1, BLOSC2_SPECIAL_ZERO, 0),
            i64::from(BLOSC2_ERROR_INVALID_PARAM)
        );
        let mut too_many = Schunk::new(
            CParams {
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        );
        assert_eq!(
            blosc2_schunk_fill_special(
                &mut too_many,
                i32::MAX as usize + 1,
                BLOSC2_SPECIAL_ZERO,
                1
            ),
            i64::from(BLOSC2_ERROR_SCHUNK_SPECIAL)
        );
        assert_eq!(zeros.nbytes, 10);
        assert_eq!(zeros.chunksize, 4);
        assert_eq!(zeros.decompress_all().unwrap(), vec![0u8; 10]);
        assert_eq!(
            Schunk::new(
                CParams {
                    typesize: 1,
                    ..Default::default()
                },
                DParams::default(),
            )
            .fill_special(1, BLOSC2_SPECIAL_ZERO, 0)
            .err(),
            Some("Invalid chunksize")
        );
        assert_eq!(
            Schunk::new(
                CParams {
                    typesize: 1,
                    ..Default::default()
                },
                DParams::default(),
            )
            .fill_special(i32::MAX as usize + 1, BLOSC2_SPECIAL_ZERO, 1)
            .err(),
            Some("Too many chunks")
        );
        assert_eq!(
            Schunk::new(
                CParams {
                    typesize: 1,
                    ..Default::default()
                },
                DParams::default(),
            )
            .fill_special(0, BLOSC2_SPECIAL_ZERO, 0)
            .unwrap(),
            0
        );

        let mut nans = Schunk::new(
            CParams {
                typesize: 4,
                ..Default::default()
            },
            DParams::default(),
        );
        assert_eq!(nans.fill_special(3, BLOSC2_SPECIAL_NAN, 8).unwrap(), 2);
        let data = nans.decompress_all().unwrap();
        assert_eq!(data.len(), 12);
        for item in data.chunks_exact(4) {
            assert!(f32::from_le_bytes(item.try_into().unwrap()).is_nan());
        }
        assert_eq!(
            Schunk::from_frame(&nans.to_frame())
                .unwrap()
                .decompress_all()
                .unwrap()
                .len(),
            12
        );

        let mut uninit = Schunk::new(
            CParams {
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        );
        uninit.fill_special(6, BLOSC2_SPECIAL_UNINIT, 4).unwrap();
        let chunk = uninit.compressed_chunk(0).unwrap();
        let mut dest = vec![0xAA; 4];
        assert_eq!(compress::decompress_into(chunk, &mut dest).unwrap(), 4);
        assert_eq!(dest, vec![0xAA; 4]);
        assert_eq!(uninit.decompress_all().unwrap(), vec![0u8; 6]);

        let mut invalid_nan = Schunk::new(
            CParams {
                typesize: 2,
                ..Default::default()
            },
            DParams::default(),
        );
        invalid_nan.fill_special(2, BLOSC2_SPECIAL_NAN, 4).unwrap();
        assert_eq!(
            invalid_nan.decompress_all(),
            Err("NaN special only valid for 4 or 8 byte types")
        );
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
        assert_eq!(frame[25] & FRAME_VL_BLOCKS, 0);
        assert_eq!(i32::from_be_bytes(frame[58..62].try_into().unwrap()), 0);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.chunksize, 0);
        let restored_zero_header =
            ChunkHeader::read(restored.compressed_chunk(1).unwrap()).unwrap();
        assert_eq!(restored_zero_header.special_type(), BLOSC2_NO_SPECIAL);
        assert!(restored_zero_header.memcpyed());
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
        let stored_zero = std::fs::read(path.join("00000001.chunk")).unwrap();
        let stored_zero_header = ChunkHeader::read(&stored_zero).unwrap();
        assert_eq!(stored_zero_header.special_type(), BLOSC2_NO_SPECIAL);
        assert!(stored_zero_header.memcpyed());
        let restored = Schunk::open_sframe(&path).unwrap();
        assert_eq!(restored.decompress_chunk(1).unwrap(), zeros);
    }

    #[test]
    fn test_indexed_frame_rejects_data_section_gaps() {
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

        let frame = schunk.to_frame();
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        let cbytes = i64::from_be_bytes(frame[39..47].try_into().unwrap()) as usize;
        let data_end = header_size + cbytes;
        assert!(frame::offsets_chunk_len(&frame, data_end, frame.len()).unwrap() > 0);

        let gap = b"gap";
        let mut gapped = Vec::with_capacity(frame.len() + gap.len());
        gapped.extend_from_slice(&frame[..data_end]);
        gapped.extend_from_slice(gap);
        gapped.extend_from_slice(&frame[data_end..]);
        let new_frame_size = gapped.len() as u64;
        let new_cbytes = (cbytes + gap.len()) as i64;
        gapped[16..24].copy_from_slice(&new_frame_size.to_be_bytes());
        gapped[39..47].copy_from_slice(&new_cbytes.to_be_bytes());

        assert_eq!(
            Schunk::from_frame(&gapped).err(),
            Some("Invalid frame: chunk offsets leave data gaps".to_string())
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gapped.b2frame");
        std::fs::write(&path, &gapped).unwrap();
        assert_eq!(
            Schunk::open_lazy(&path).err(),
            Some("Invalid frame: chunk offsets leave data gaps".to_string())
        );
    }

    #[test]
    fn test_frame_rejects_vlblocks_flag_mismatch() {
        let mut regular = Schunk::new(CParams::default(), DParams::default());
        regular.append_buffer(b"regular payload").unwrap();
        let mut regular_frame = regular.to_frame();
        regular_frame[25] |= FRAME_VL_BLOCKS;
        assert_eq!(
            Schunk::from_frame(&regular_frame).err(),
            Some("Invalid frame: VL-block flag mismatch".to_string())
        );

        let dir = tempfile::tempdir().unwrap();
        let regular_path = dir.path().join("regular-bad-vlflag.b2frame");
        std::fs::write(&regular_path, &regular_frame).unwrap();
        assert_eq!(
            Schunk::open_lazy(&regular_path).err(),
            Some("Invalid frame: VL-block flag mismatch".to_string())
        );

        let mut variable_regular = Schunk::new(CParams::default(), DParams::default());
        variable_regular.append_buffer(b"short").unwrap();
        variable_regular
            .append_buffer(b"longer regular payload")
            .unwrap();
        let mut variable_regular_frame = variable_regular.to_frame();
        assert_ne!(variable_regular_frame[25] & FRAME_VARIABLE_CHUNKS, 0);
        variable_regular_frame[25] |= FRAME_VL_BLOCKS;
        assert_eq!(
            Schunk::from_frame(&variable_regular_frame)
                .unwrap()
                .decompress_all()
                .unwrap(),
            variable_regular.decompress_all().unwrap()
        );
        let variable_regular_path = dir.path().join("variable-regular-c-vlflag.b2frame");
        std::fs::write(&variable_regular_path, &variable_regular_frame).unwrap();
        assert_eq!(
            Schunk::open_lazy(&variable_regular_path)
                .unwrap()
                .decompress_all()
                .unwrap(),
            variable_regular.decompress_all().unwrap()
        );

        let mut vlblocks = Schunk::new(
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        );
        vlblocks
            .append_vlblocks(&[b"red\0".as_slice(), b"green\0".as_slice()])
            .unwrap();
        let mut vl_frame = vlblocks.to_frame();
        vl_frame[25] &= !FRAME_VL_BLOCKS;
        assert_eq!(
            Schunk::from_frame(&vl_frame).err(),
            Some("Invalid frame: VL-block flag mismatch".to_string())
        );

        let vl_path = dir.path().join("vl-bad-vlflag.b2frame");
        std::fs::write(&vl_path, &vl_frame).unwrap();
        assert_eq!(
            Schunk::open_lazy(&vl_path).err(),
            Some("Invalid frame: VL-block flag mismatch".to_string())
        );
    }

    #[test]
    fn test_schunk_save_preserves_storage_kind() {
        let mut schunk = Schunk::new(
            CParams {
                typesize: 4,
                ..Default::default()
            },
            DParams::default(),
        );
        schunk.append_buffer(b"first chunk").unwrap();
        schunk.append_buffer(b"second chunk").unwrap();

        let dir = tempfile::tempdir().unwrap();
        let contiguous = dir.path().join("source.b2frame");
        schunk.to_file(contiguous.to_str().unwrap()).unwrap();
        let opened_contiguous = Schunk::open(contiguous.to_str().unwrap()).unwrap();
        let saved_contiguous = dir.path().join("saved.b2frame");
        opened_contiguous.save(&saved_contiguous).unwrap();
        assert!(saved_contiguous.is_file());
        assert_eq!(
            Schunk::open(saved_contiguous.to_str().unwrap())
                .unwrap()
                .decompress_all()
                .unwrap(),
            schunk.decompress_all().unwrap()
        );

        let sparse = dir.path().join("source.sframe");
        schunk.to_sframe_dir(&sparse).unwrap();
        let opened_sparse = Schunk::open_sframe(&sparse).unwrap();
        let saved_sparse = dir.path().join("saved.sframe");
        opened_sparse.save(&saved_sparse).unwrap();
        assert!(saved_sparse.is_dir());
        assert!(saved_sparse.join("chunks.b2frame").is_file());
        assert_eq!(
            Schunk::open_sframe(&saved_sparse)
                .unwrap()
                .decompress_all()
                .unwrap(),
            schunk.decompress_all().unwrap()
        );
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

        assert_eq!(schunk.chunksize, b"first chunk".len());

        let frame = schunk.to_frame();
        assert_eq!(frame[25] & 0x0F, BLOSC2_VERSION_FRAME_FORMAT_RC1);
        assert_eq!(frame[25] & FRAME_VARIABLE_CHUNKS, 0);
        assert_eq!(frame[25] & FRAME_VL_BLOCKS, 0);
        assert_eq!(
            i32::from_be_bytes(frame[58..62].try_into().unwrap()),
            b"first chunk".len() as i32
        );

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.chunksize, b"first chunk".len());
        assert_eq!(
            restored.decompress_all().unwrap(),
            b"first chunksecondchunkshort"
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("final-short.b2frame");
        schunk.to_file(path.to_str().unwrap()).unwrap();
        let lazy = Schunk::open_lazy(&path).unwrap();
        assert_eq!(lazy.chunksize, b"first chunk".len());
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
    fn test_empty_frame_preserves_declared_chunksize() {
        let mut schunk = Schunk::new(
            CParams {
                typesize: 4,
                ..Default::default()
            },
            DParams::default(),
        );
        schunk.chunksize = 16;

        let frame = schunk.to_frame();
        assert_eq!(frame[25] & 0x0F, BLOSC2_VERSION_FRAME_FORMAT_RC1);
        assert_eq!(frame[25] & FRAME_VARIABLE_CHUNKS, 0);
        assert_eq!(i32::from_be_bytes(frame[58..62].try_into().unwrap()), 16);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.nchunks(), 0);
        assert_eq!(restored.chunksize, 16);
        assert_eq!(restored.nbytes, 0);
        assert_eq!(restored.cbytes, 0);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty-declared-chunksize.b2frame");
        std::fs::write(&path, &frame).unwrap();
        let lazy = Schunk::open_lazy(&path).unwrap();
        assert_eq!(lazy.nchunks(), 0);
        assert_eq!(lazy.chunksize, 16);
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
    fn test_fixed_frame_writer_encodes_special_offsets() {
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
                frame::encoded_special_offset(BLOSC2_SPECIAL_ZERO),
                schunk.chunks[0].len() as u64,
            ]
        );

        let frame = schunk.to_frame();
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        let cbytes = i64::from_be_bytes(frame[39..47].try_into().unwrap()) as usize;
        let expected_cbytes = schunk.chunks[0].len() + schunk.chunks[2].len();
        assert_eq!(cbytes, expected_cbytes);

        let data_start = header_size;
        let data_end = data_start + cbytes;
        let mut pos = data_start;
        for chunk in [&schunk.chunks[0], &schunk.chunks[2]] {
            assert_eq!(&frame[pos..pos + chunk.len()], chunk);
            pos += chunk.len();
        }
        assert_eq!(pos, data_end);

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
                frame::encoded_special_offset(BLOSC2_SPECIAL_ZERO),
                schunk.chunks[0].len() as u64,
            ]
        );
        assert_eq!(
            Schunk::from_frame(&frame)
                .unwrap()
                .decompress_all()
                .unwrap(),
            expected
        );
        assert_eq!(
            Schunk::from_frame(&frame).unwrap().cbytes,
            schunk
                .chunks
                .iter()
                .map(|chunk| chunk.len() as i64)
                .sum::<i64>()
        );

        let mut streamed = Vec::new();
        frame::write_frame_to_writer(&schunk, &mut streamed).unwrap();
        assert_eq!(streamed, frame);

        let dir = tempfile::tempdir().unwrap();
        let frame_path = dir.path().join("fixed-special-contiguous.b2frame");
        std::fs::write(&frame_path, &frame).unwrap();
        let lazy = Schunk::open_lazy(&frame_path).unwrap();
        assert_eq!(lazy.cbytes, cbytes as i64);
        assert_eq!(
            lazy.chunk_refs()[1].offset,
            frame::encoded_special_offset(BLOSC2_SPECIAL_ZERO)
        );
        assert_eq!(lazy.chunk_refs()[1].special, Some(BLOSC2_SPECIAL_ZERO));
        assert_eq!(lazy.decompress_chunk(1).unwrap(), zeroes);
        assert_eq!(lazy.decompress_all().unwrap(), expected);

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

    fn frame_offsets_from_bytes(frame: &[u8], data_cbytes: usize) -> Vec<u64> {
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        let data_end = header_size + data_cbytes;
        let offsets_header = ChunkHeader::read(&frame[data_end..]).unwrap();
        let offsets_end = data_end + offsets_header.cbytes as usize;
        compress::decompress(&frame[data_end..offsets_end])
            .unwrap()
            .chunks_exact(8)
            .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
            .collect()
    }

    #[test]
    fn test_attached_contiguous_fill_repeatval_reuses_physical_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repeatval-attached.b2frame");
        Schunk::new(
            CParams {
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        )
        .to_file(path.to_str().unwrap())
        .unwrap();

        let mut opened = Schunk::open(path.to_str().unwrap()).unwrap();
        opened.fill_repeatval(10, &[7], 4).unwrap();

        let full_len = opened.chunks[0].len();
        let tail_len = opened.chunks[2].len();
        let frame = std::fs::read(&path).unwrap();
        let cbytes = i64::from_be_bytes(frame[39..47].try_into().unwrap()) as usize;
        assert_eq!(cbytes, full_len + tail_len);
        assert_eq!(
            frame_offsets_from_bytes(&frame, cbytes),
            vec![0, 0, full_len as u64]
        );
        assert_eq!(
            Schunk::open(path.to_str().unwrap())
                .unwrap()
                .decompress_all()
                .unwrap(),
            vec![7u8; 10]
        );
    }

    #[test]
    fn test_attached_sparse_fill_repeatval_reuses_chunk_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repeatval-attached-sparse.b2frame");
        Schunk::new(
            CParams {
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        )
        .to_sframe_dir(&path)
        .unwrap();

        let mut opened = Schunk::open_sframe(&path).unwrap();
        opened.fill_repeatval(10, &[7], 4).unwrap();

        assert!(path.join("00000000.chunk").is_file());
        assert!(path.join("00000001.chunk").is_file());
        assert!(!path.join("00000002.chunk").exists());

        let index = std::fs::read(path.join("chunks.b2frame")).unwrap();
        let cbytes = i64::from_be_bytes(index[39..47].try_into().unwrap()) as usize;
        assert_eq!(cbytes, opened.chunks[0].len() + opened.chunks[2].len());
        assert_eq!(frame_offsets_from_bytes(&index, 0), vec![0, 0, 1]);
        assert_eq!(
            Schunk::open_sframe(&path)
                .unwrap()
                .decompress_all()
                .unwrap(),
            vec![7u8; 10]
        );
        assert_eq!(
            Schunk::open_lazy_sframe(&path)
                .unwrap()
                .decompress_all()
                .unwrap(),
            vec![7u8; 10]
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
            lazy.compressed_chunk(0).unwrap(),
            eager.compressed_chunk(0).unwrap()
        );
        let file_url = format!("file:///{}", path.display());
        let eager_from_url = Schunk::open(&file_url).unwrap();
        let lazy_from_url = Schunk::open_lazy(&file_url).unwrap();
        assert_eq!(
            eager.decompress_all().unwrap(),
            schunk.decompress_all().unwrap()
        );
        assert_eq!(
            lazy.decompress_all().unwrap(),
            schunk.decompress_all().unwrap()
        );
        assert_eq!(
            eager_from_url.decompress_all().unwrap(),
            schunk.decompress_all().unwrap()
        );
        assert_eq!(
            lazy_from_url.decompress_all().unwrap(),
            schunk.decompress_all().unwrap()
        );
    }

    #[test]
    fn test_open_offset_reads_embedded_concatenated_frame() {
        let mut first = Schunk::new(CParams::default(), DParams::default());
        first.append_buffer(b"first frame").unwrap();
        let mut second = Schunk::new(CParams::default(), DParams::default());
        second.append_buffer(b"second frame").unwrap();

        let first_frame = first.to_frame();
        let second_offset = b"prefix".len() + first_frame.len();
        let mut file_data = b"prefix".to_vec();
        file_data.extend_from_slice(&first_frame);
        file_data.extend_from_slice(&second.to_frame());
        file_data.extend_from_slice(b"trailing bytes");

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("concatenated.b2frame");
        std::fs::write(&path, file_data).unwrap();

        let restored = Schunk::open_offset(&path, second_offset as u64).unwrap();
        let file_url = format!("file:///{}", path.display());
        let restored_from_url = Schunk::open_offset(&file_url, second_offset as u64).unwrap();
        assert_eq!(restored.decompress_all().unwrap(), b"second frame");
        assert_eq!(restored_from_url.decompress_all().unwrap(), b"second frame");
        let restored_from_reader = Schunk::open_from_reader_at(
            std::io::Cursor::new(std::fs::read(&path).unwrap()),
            second_offset as u64,
        )
        .unwrap();
        assert_eq!(
            restored_from_reader.decompress_all().unwrap(),
            b"second frame"
        );
        assert!(Schunk::open_offset(&path, u64::MAX).is_err());

        let sframe = dir.path().join("offset-sframe.b2frame");
        second.to_sframe_dir(&sframe).unwrap();
        let sparse_offset_zero = Schunk::open_offset(&sframe, 0).unwrap();
        assert_eq!(
            sparse_offset_zero.decompress_all().unwrap(),
            b"second frame"
        );
        let index_path = sframe.join("chunks.b2frame");
        let mut prefixed_index = b"prefix".to_vec();
        prefixed_index.extend_from_slice(&std::fs::read(&index_path).unwrap());
        std::fs::write(&index_path, prefixed_index).unwrap();
        let sparse_offset = Schunk::open_offset(&sframe, b"prefix".len() as u64).unwrap();
        assert_eq!(sparse_offset.decompress_all().unwrap(), b"second frame");
        let sparse_offset_c = blosc2_schunk_open_offset_c(&sframe, b"prefix".len() as i64);
        assert_eq!(sparse_offset_c.0, BLOSC2_ERROR_SUCCESS);
        assert_eq!(
            sparse_offset_c.1.unwrap().decompress_all().unwrap(),
            b"second frame"
        );
        let lazy_sparse_offset = Schunk::open_lazy_offset(&sframe, b"prefix".len() as u64).unwrap();
        assert_eq!(
            lazy_sparse_offset.decompress_chunk(0).unwrap(),
            b"second frame".to_vec()
        );
    }

    #[test]
    fn test_open_lazy_offset_reads_embedded_concatenated_frame() {
        let mut first = Schunk::new(CParams::default(), DParams::default());
        first.append_buffer(b"first frame").unwrap();
        let mut second = Schunk::new(CParams::default(), DParams::default());
        second.append_buffer(b"second frame").unwrap();

        let first_frame = first.to_frame();
        let second_offset = b"prefix".len() + first_frame.len();
        let mut file_data = b"prefix".to_vec();
        file_data.extend_from_slice(&first_frame);
        file_data.extend_from_slice(&second.to_frame());
        file_data.extend_from_slice(b"trailing bytes");

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("concatenated-lazy.b2frame");
        std::fs::write(&path, file_data).unwrap();

        let lazy = Schunk::open_lazy_offset(&path, second_offset as u64).unwrap();
        assert_eq!(lazy.decompress_all().unwrap(), b"second frame");
        assert!(lazy.chunk_refs()[0].offset >= second_offset as u64);

        let file_url = format!("file:///{}", path.display());
        let lazy_from_url = Schunk::open_lazy_offset(&file_url, second_offset as u64).unwrap();
        assert_eq!(lazy_from_url.decompress_all().unwrap(), b"second frame");
        assert!(Schunk::open_lazy_offset(&path, u64::MAX).is_err());
    }

    #[test]
    fn test_frame_len_matches_serialized_frame_size() {
        let empty = Schunk::new(CParams::default(), DParams::default());
        assert_eq!(empty.frame_len().unwrap(), empty.to_frame().len() as i64);
        assert_eq!(blosc2_schunk_frame_len(&empty), 0);

        let mut schunk = Schunk::new(CParams::default(), DParams::default());
        schunk.append_buffer(b"payload").unwrap();
        assert_eq!(schunk.frame_len().unwrap(), schunk.to_frame().len() as i64);
        assert_eq!(
            blosc2_schunk_frame_len(&schunk),
            schunk.cbytes + schunk.nchunks() * 8
        );
        assert_ne!(
            blosc2_schunk_frame_len(&schunk),
            schunk.to_frame().len() as i64
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("frame-len.b2frame");
        let written = schunk.to_file_len(path.to_str().unwrap()).unwrap();
        assert_eq!(written, schunk.frame_len().unwrap());
        assert_eq!(written as u64, std::fs::metadata(&path).unwrap().len());
        let opened = Schunk::open(path.to_str().unwrap()).unwrap();
        assert_eq!(
            blosc2_schunk_frame_len(&opened),
            opened.to_frame().len() as i64
        );
    }

    #[test]
    fn test_append_file_returns_openable_frame_offsets() {
        let mut first = Schunk::new(CParams::default(), DParams::default());
        first.append_buffer(b"first appended").unwrap();
        let mut second = Schunk::new(CParams::default(), DParams::default());
        second.append_buffer(b"second appended").unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("append.b2frame");
        let first_offset = first.append_file(&path).unwrap();
        let second_offset = second.append_file(&path).unwrap();

        assert_eq!(first_offset, 0);
        assert!(second_offset > first_offset);
        assert_eq!(
            Schunk::open_offset(&path, first_offset)
                .unwrap()
                .decompress_all()
                .unwrap(),
            b"first appended"
        );
        assert_eq!(
            Schunk::open_offset(&path, second_offset)
                .unwrap()
                .decompress_all()
                .unwrap(),
            b"second appended"
        );
    }

    fn frame_mutation_seed() -> Schunk {
        let mut schunk = Schunk::new(CParams::default(), DParams::default());
        schunk.append_buffer(b"alpha").unwrap();
        schunk.append_buffer(b"bravo").unwrap();
        schunk.append_buffer(b"charlie").unwrap();
        schunk.add_metalayer("fixed", b"oldmeta").unwrap();
        schunk.add_vlmetalayer("vl", b"old-vl").unwrap();
        schunk
    }

    fn mutate_opened_frame_schunk(schunk: &mut Schunk) {
        schunk.append_buffer(b"delta").unwrap();
        schunk.insert_buffer(1, b"insert").unwrap();
        schunk.update_chunk(2, b"BRAVO").unwrap();
        schunk.delete_chunk(0).unwrap();
        schunk.reorder_chunks(&[2, 0, 1, 3]).unwrap();
        schunk.update_metalayer("fixed", b"newmeta").unwrap();
        schunk.update_vlmetalayer("vl", b"new-vl").unwrap();
    }

    fn assert_opened_frame_mutations_persisted(restored: Schunk) {
        assert_eq!(
            restored.decompress_all().unwrap(),
            b"charlieinsertBRAVOdelta"
        );
        assert_eq!(restored.nchunks(), 4);
        assert_eq!(restored.metalayer("fixed"), Some(&b"newmeta"[..]));
        assert_eq!(restored.vlmetalayer("vl"), Some(&b"new-vl"[..]));
    }

    #[test]
    fn test_opened_contiguous_frame_mutations_persist() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("attached.b2frame");
        frame_mutation_seed()
            .to_file(path.to_str().unwrap())
            .unwrap();

        let mut opened = Schunk::open(path.to_str().unwrap()).unwrap();
        mutate_opened_frame_schunk(&mut opened);

        assert_opened_frame_mutations_persisted(Schunk::open(path.to_str().unwrap()).unwrap());
    }

    #[test]
    fn test_opened_sparse_frame_mutations_persist() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("attached-sparse.b2frame");
        frame_mutation_seed().to_sframe_dir(&path).unwrap();

        let mut opened = Schunk::open_sframe(&path).unwrap();
        mutate_opened_frame_schunk(&mut opened);

        assert_opened_frame_mutations_persisted(Schunk::open_sframe(&path).unwrap());
        assert!(path.join("chunks.b2frame").is_file());
        assert!(path.join("00000000.chunk").is_file());
        assert!(path.join("00000003.chunk").is_file());
        assert!(!path.join("00000004.chunk").exists());
    }

    #[test]
    fn test_opened_sparse_frame_mutations_rewrite_duplicate_wrapped_ids_and_stale_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("attached-wrapped-sparse.b2frame");
        let chunk = b"same-sparse-data".repeat(4);
        let left = b"left-sparse-data".repeat(4);
        let middle = b"mid-sparse-data!".repeat(4);
        let mut schunk = Schunk::new(CParams::default(), DParams::default());
        schunk.append_buffer(&chunk).unwrap();
        schunk.append_buffer(&chunk).unwrap();
        schunk.to_sframe_dir(&path).unwrap();

        let rewrite_sparse_offsets = |offsets: &[u64]| {
            let index_path = path.join("chunks.b2frame");
            let index = std::fs::read(&index_path).unwrap();
            let header_size = i32::from_be_bytes(index[11..15].try_into().unwrap()) as usize;
            let frame_size = u64::from_be_bytes(index[16..24].try_into().unwrap()) as usize;
            let offsets_header = ChunkHeader::read(&index[header_size..]).unwrap();
            let old_offsets_end = header_size + offsets_header.cbytes as usize;
            let mut offsets_payload = Vec::with_capacity(offsets.len() * 8);
            for &offset in offsets {
                offsets_payload.extend_from_slice(&offset.to_le_bytes());
            }
            let offsets_chunk = frame::build_offsets_chunk(&offsets_payload);
            let mut rewritten = index[..header_size].to_vec();
            rewritten.extend_from_slice(&offsets_chunk);
            rewritten.extend_from_slice(&index[old_offsets_end..frame_size]);
            let rewritten_len = rewritten.len() as u64;
            rewritten[16..24].copy_from_slice(&rewritten_len.to_be_bytes());
            std::fs::write(index_path, rewritten).unwrap();
        };

        rewrite_sparse_offsets(&[0x1_0000_0001, 0x1_0000_0001]);
        std::fs::write(
            path.join("00000002.chunk"),
            schunk.compressed_chunk(0).unwrap(),
        )
        .unwrap();

        let mut opened = Schunk::open_sframe(&path).unwrap();
        assert_eq!(
            blosc2_frame_get_offsets(&opened),
            (
                BLOSC2_ERROR_SUCCESS,
                Some(vec![0x1_0000_0001, 0x1_0000_0001])
            )
        );
        assert_eq!(
            opened.decompress_all().unwrap(),
            [chunk.as_slice(), chunk.as_slice()].concat()
        );

        opened.update_chunk(0, &left).unwrap();
        opened.insert_buffer(1, &middle).unwrap();
        opened.delete_chunk(2).unwrap();
        opened.reorder_chunks(&[1, 0]).unwrap();

        assert!(path.join("00000000.chunk").is_file());
        assert!(path.join("00000001.chunk").is_file());
        assert!(!path.join("00000002.chunk").exists());
        let index = std::fs::read(path.join("chunks.b2frame")).unwrap();
        assert_eq!(frame_offsets_from_bytes(&index, 0), vec![0, 1]);
        let expected = [middle.as_slice(), left.as_slice()].concat();
        assert_eq!(
            Schunk::open_sframe(&path)
                .unwrap()
                .decompress_all()
                .unwrap(),
            expected
        );
        assert_eq!(
            Schunk::open_lazy_sframe(&path)
                .unwrap()
                .decompress_all()
                .unwrap(),
            expected
        );
    }

    #[test]
    fn test_lazy_open_defers_later_chunk_payload_validation() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 4,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        let first: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
        let second: Vec<u8> = (4096..8192u32).flat_map(|i| i.to_le_bytes()).collect();
        schunk.append_buffer(&first).unwrap();
        schunk.append_buffer(&second).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lazy-corrupt.b2frame");
        schunk.to_file(path.to_str().unwrap()).unwrap();
        let refs = Schunk::open_lazy(&path).unwrap().chunk_refs().to_vec();

        let mut frame = std::fs::read(&path).unwrap();
        let corrupt_pos = refs[1].offset as usize + BLOSC_EXTENDED_HEADER_LENGTH;
        frame[corrupt_pos] ^= 0x5a;
        std::fs::write(&path, frame).unwrap();

        let lazy = Schunk::open_lazy(&path).unwrap();
        assert_eq!(lazy.decompress_chunk(0).unwrap(), first);
        assert_ne!(lazy.decompress_chunk(1).unwrap_or_default(), second);
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
        let mut schunk = Schunk::new(cparams.clone(), DParams::default());
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

        let mut replacement = Schunk::new(cparams, DParams::default());
        replacement.add_metalayer("kind", b"new").unwrap();
        replacement.append_buffer(b"replacement").unwrap();
        replacement.to_sframe_dir(&path).unwrap();
        assert!(path.join("chunks.b2frame").is_file());
        assert!(path.join("00000000.chunk").is_file());
        assert!(!path.join("00000001.chunk").exists());
        assert!(!path.join("00000002.chunk").exists());

        let overwritten = Schunk::open_sframe(&path).unwrap();
        assert_eq!(overwritten.nchunks(), 1);
        assert_eq!(overwritten.metalayer("kind"), Some(&b"new"[..]));
        assert_eq!(overwritten.decompress_all().unwrap(), b"replacement");
    }

    #[test]
    fn test_sparse_frame_accepts_c_style_duplicate_and_wrapped_chunk_ids() {
        let mut schunk = Schunk::new(
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            },
            DParams::default(),
        );
        let chunk = b"same sparse chunk payload".repeat(8);
        schunk.append_buffer(&chunk).unwrap();
        schunk.append_buffer(&chunk).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wrapped-sparse.b2frame");
        schunk.to_sframe_dir(&path).unwrap();

        let rewrite_sparse_offsets = |offsets: &[u64]| {
            let index_path = path.join("chunks.b2frame");
            let index = std::fs::read(&index_path).unwrap();
            let header_size = i32::from_be_bytes(index[11..15].try_into().unwrap()) as usize;
            let frame_size = u64::from_be_bytes(index[16..24].try_into().unwrap()) as usize;
            let offsets_header = ChunkHeader::read(&index[header_size..]).unwrap();
            let old_offsets_end = header_size + offsets_header.cbytes as usize;
            let mut offsets_payload = Vec::with_capacity(offsets.len() * 8);
            for &offset in offsets {
                offsets_payload.extend_from_slice(&offset.to_le_bytes());
            }
            let offsets_chunk = frame::build_offsets_chunk(&offsets_payload);
            let mut rewritten = index[..header_size].to_vec();
            rewritten.extend_from_slice(&offsets_chunk);
            rewritten.extend_from_slice(&index[old_offsets_end..frame_size]);
            let rewritten_len = rewritten.len() as u64;
            rewritten[16..24].copy_from_slice(&rewritten_len.to_be_bytes());
            std::fs::write(index_path, rewritten).unwrap();
        };

        rewrite_sparse_offsets(&[0, 0]);
        let duplicated = Schunk::open_sframe(&path).unwrap();
        assert_eq!(duplicated.decompress_chunk(0).unwrap(), chunk);
        assert_eq!(duplicated.decompress_chunk(1).unwrap(), chunk);
        assert_eq!(
            blosc2_frame_get_offsets(&duplicated),
            (BLOSC2_ERROR_SUCCESS, Some(vec![0, 0]))
        );
        let mut mutated = duplicated.clone();
        mutated.append_buffer(&chunk).unwrap();
        assert_eq!(mutated.nchunks(), 3);
        let (mutated_offsets_rc, mutated_offsets) = blosc2_frame_get_offsets(&mutated);
        assert_ne!(mutated_offsets_rc, BLOSC2_ERROR_SUCCESS);
        assert!(mutated_offsets.is_none());

        rewrite_sparse_offsets(&[0x1_0000_0001, 0x1_0000_0001]);
        let wrapped = Schunk::open_sframe(&path).unwrap();
        assert_eq!(wrapped.decompress_chunk(0).unwrap(), chunk);
        assert_eq!(
            blosc2_frame_get_offsets(&wrapped),
            (
                BLOSC2_ERROR_SUCCESS,
                Some(vec![0x1_0000_0001, 0x1_0000_0001])
            )
        );
        let lazy_wrapped = Schunk::open_lazy_sframe(&path).unwrap();
        assert_eq!(lazy_wrapped.chunk_refs()[0].offset, 0x1_0000_0001);
        assert!(blosc2_schunk_get_lazychunk(&lazy_wrapped, 0).0 > 0);
        assert_eq!(lazy_wrapped.decompress_chunk(1).unwrap(), chunk);
    }

    #[test]
    fn test_sparse_chunk_files_ignore_trailing_bytes() {
        let mut schunk = Schunk::new(CParams::default(), DParams::default());
        schunk.append_buffer(b"first payload").unwrap();
        schunk.append_buffer(b"second payload").unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trailing-sframe.b2frame");
        schunk.to_sframe_dir(&path).unwrap();
        let chunk_path = path.join("00000001.chunk");
        let mut chunk = std::fs::read(&chunk_path).unwrap();
        chunk.extend_from_slice(b"trailing chunk bytes");
        std::fs::write(&chunk_path, chunk).unwrap();

        let eager = Schunk::open_sframe(&path).unwrap();
        let lazy = Schunk::open_lazy_sframe(&path).unwrap();
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
        let mut schunk = Schunk::new(cparams.clone(), DParams::default());
        let blocks: [&[u8]; 3] = [b"red\0", b"green-green\0", b"blue-blue-blue-blue\0"];
        schunk.append_vlblocks(&blocks).unwrap();
        let mut vl_c = Schunk::new(cparams, DParams::default());
        assert_eq!(
            blosc2_schunk_append_vlblocks_c(&mut vl_c, &blocks, &[4, 12, 20], 3),
            1
        );
        assert_eq!(
            blosc2_schunk_append_vlblocks_c(&mut vl_c, &blocks, &[4, 12, 99], 3),
            i64::from(BLOSC2_ERROR_INVALID_PARAM)
        );
        let mut vl_block_dest = vec![0u8; 16];
        assert_eq!(
            blosc2_schunk_decompress_vlblock_c(&vl_c, 0, 1, &mut vl_block_dest, 16),
            b"green-green\0".len() as i32
        );
        assert_eq!(&vl_block_dest[..b"green-green\0".len()], b"green-green\0");
        assert_eq!(
            blosc2_schunk_decompress_vlblock_c(&vl_c, 0, -1, &mut vl_block_dest, 16),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_schunk_decompress_vlblock_c(&vl_c, 0, 1, &mut vl_block_dest, 4),
            BLOSC2_ERROR_WRITE_BUFFER
        );
        let (vl_rc, vl_block, vl_size) = blosc2_schunk_get_vlblock(&vl_c, 0, 1);
        assert_eq!(vl_rc, b"green-green\0".len() as i32);
        assert_eq!(vl_size, b"green-green\0".len() as i32);
        assert_eq!(vl_block.unwrap(), b"green-green\0");
        assert_eq!(
            blosc2_schunk_get_vlblock(&vl_c, 0, -1).0,
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(schunk.nchunks(), 1);
        assert_eq!(schunk.chunksize, 0);
        assert!(schunk.append_buffer(b"regular").is_err());
        assert_eq!(
            schunk.decompress_chunk(0).unwrap(),
            b"red\0green-green\0blue-blue-blue-blue\0"
        );
        assert_eq!(
            schunk.set_slice(0, b"RED"),
            Err("Cannot set byte slices on VL-block chunks")
        );
        assert_eq!(schunk.decompress_vlblock(0, 0).unwrap(), b"red\0");
        assert_eq!(schunk.decompress_vlblock(0, 1).unwrap(), b"green-green\0");
        assert_eq!(
            schunk.decompress_vlblock(0, 2).unwrap(),
            b"blue-blue-blue-blue\0"
        );
        assert!(schunk.decompress_vlblock(0, 3).is_err());
        schunk.dparams.postfilter = Some(record_postfilter_nchunk);
        LAST_POSTFILTER_NCHUNK.store(-99, AtomicOrdering::SeqCst);
        assert_eq!(schunk.decompress_vlblock(0, 1).unwrap(), b"green-green\0");
        assert_eq!(LAST_POSTFILTER_NCHUNK.load(AtomicOrdering::SeqCst), 0);
        schunk.dparams.postfilter = None;

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
        assert_eq!(restored.decompress_vlblock(0, 1).unwrap(), b"green-green\0");
        assert!(ChunkHeader::read(&restored.chunks[0]).unwrap().vl_blocks());

        let mut regular = Schunk::new(CParams::default(), DParams::default());
        regular.append_buffer(b"regular").unwrap();
        assert_eq!(
            regular.decompress_vlblock(0, 0),
            Err("Schunk does not contain VL-block chunks")
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vlblocks.b2frame");
        schunk.to_file(path.to_str().unwrap()).unwrap();
        let lazy = Schunk::open_lazy(&path).unwrap();
        assert_eq!(
            lazy.decompress_chunk(0).unwrap(),
            b"red\0green-green\0blue-blue-blue-blue\0"
        );
        assert_eq!(
            lazy.decompress_vlblock(0, 2).unwrap(),
            b"blue-blue-blue-blue\0"
        );
        assert!(lazy.decompress_vlblock(0, 3).is_err());

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
        assert_eq!(lazy.decompress_vlblock(0, 0).unwrap(), b"red\0");

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
    fn test_vlblock_insert_update_helpers() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk
            .append_vlblocks(&[b"alpha".as_slice(), b"bravo".as_slice()])
            .unwrap();
        assert_eq!(
            schunk
                .insert_vlblocks(0, &[b"zero".as_slice(), b"one".as_slice()])
                .unwrap(),
            2
        );
        assert_eq!(
            schunk
                .update_vlblocks(1, &[b"charlie".as_slice(), b"delta".as_slice()])
                .unwrap(),
            2
        );
        assert_eq!(schunk.decompress_vlblock(0, 0).unwrap(), b"zero");
        assert_eq!(schunk.decompress_vlblock(0, 1).unwrap(), b"one");
        assert_eq!(schunk.decompress_vlblock(1, 0).unwrap(), b"charlie");
        assert_eq!(schunk.decompress_vlblock(1, 1).unwrap(), b"delta");
        assert_eq!(
            schunk.insert_vlblocks(3, &[b"out".as_slice()]),
            Err("Chunk index out of range")
        );

        let mut regular = Schunk::new(CParams::default(), DParams::default());
        regular.append_buffer(b"regular").unwrap();
        assert_eq!(
            regular.insert_vlblocks(0, &[b"vl".as_slice()]),
            Err("Cannot mix regular and VL-block chunks")
        );
        assert_eq!(
            regular.update_vlblocks(0, &[b"vl".as_slice()]),
            Err("Schunk does not contain VL-block chunks")
        );
    }

    #[test]
    fn test_c_style_vlblock_insert_update_adapters() {
        let mut schunk = Schunk::new(
            CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 1,
                ..Default::default()
            },
            DParams::default(),
        );
        let first: [&[u8]; 1] = [b"first-full"];
        assert_eq!(
            blosc2_schunk_append_vlblocks_c(&mut schunk, &first, &[5], 1),
            1
        );
        let inserted: [&[u8]; 2] = [b"inserted-a", b"inserted-b"];
        assert_eq!(
            blosc2_schunk_insert_vlblocks_c(&mut schunk, 0, &inserted, &[10, 10], 2),
            2
        );
        let updated: [&[u8]; 1] = [b"updated"];
        assert_eq!(
            blosc2_schunk_update_vlblocks_c(&mut schunk, 1, &updated, &[7], 1),
            2
        );
        assert_eq!(schunk.decompress_vlblock(0, 0).unwrap(), b"inserted-a");
        assert_eq!(schunk.decompress_vlblock(0, 1).unwrap(), b"inserted-b");
        assert_eq!(schunk.decompress_vlblock(1, 0).unwrap(), b"updated");
        assert_eq!(
            blosc2_schunk_insert_vlblocks_c(&mut schunk, 3, &updated, &[7], 1),
            i64::from(BLOSC2_ERROR_INVALID_INDEX)
        );
        assert_eq!(
            blosc2_schunk_update_vlblocks_c(&mut schunk, 0, &updated, &[99], 1),
            i64::from(BLOSC2_ERROR_INVALID_PARAM)
        );
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

        assert_eq!(
            schunk.add_metalayer_index("author", b"pure-rust").unwrap(),
            0
        );
        assert_eq!(
            schunk
                .add_metalayer_index("revision", &[1, 2, 3, 4])
                .unwrap(),
            1
        );
        schunk.add_metalayer("", b"empty-name").unwrap();
        assert_eq!(blosc2_meta_add_c(&mut schunk, "short", b"short-tail", 5), 3);
        assert_eq!(schunk.metalayer("short"), Some(&b"short"[..]));
        assert_eq!(
            blosc2_meta_add_c(&mut schunk, "badlen", b"bad", 4),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert!(schunk.add_metalayer("author", b"duplicate").is_err());
        assert_eq!(
            schunk.update_metalayer_index("author", b"updated").unwrap(),
            0
        );
        assert_eq!(
            blosc2_meta_update_c(&mut schunk, "short", b"tiny-tail", 4),
            3
        );
        assert_eq!(schunk.metalayer("short"), Some(&b"tinyt"[..]));

        assert_eq!(schunk.metalayers.len(), 4);
        assert_eq!(schunk.metalayer("author"), Some(&b"updatedst"[..]));
        assert_eq!(schunk.metalayer_index("author"), Some(0));
        assert!(schunk.metalayer_exists("revision"));
        assert_eq!(
            schunk.metalayer_names(),
            vec!["author", "revision", "", "short"]
        );
        assert_eq!(schunk.remove_metalayer("revision"), Some(vec![1, 2, 3, 4]));
        schunk.add_metalayer("revision", &[5, 6]).unwrap();
        assert_eq!(schunk.metalayer_index("revision"), Some(3));
        assert!(!schunk.metalayer_exists("missing"));
        assert_eq!(
            schunk.metalayer_names(),
            vec!["author", "", "short", "revision"]
        );

        let frame = schunk.to_frame();
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        assert!(header_size > frame::FRAME_HEADER_MIN_LEN);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.decompress_all().unwrap(), b"payload");
        assert_eq!(restored.metalayer("author"), Some(&b"updatedst"[..]));
        assert_eq!(restored.metalayer(""), Some(&b"empty-name"[..]));
        assert_eq!(restored.metalayer("short"), Some(&b"tinyt"[..]));
        assert_eq!(restored.metalayer("revision"), Some(&[5, 6][..]));
        assert_eq!(restored.metalayer_index(""), Some(1));
        assert_eq!(
            restored.metalayer_names(),
            vec!["author", "", "short", "revision"]
        );
    }

    #[test]
    fn test_c_style_metalayer_add_defers_overlong_name_failure_until_frame_write() {
        let mut schunk = Schunk::new(CParams::default(), DParams::default());
        schunk.append_buffer(b"payload").unwrap();
        let long_name = "x".repeat(32);

        assert_eq!(blosc2_meta_add(&mut schunk, &long_name, b"x"), 0);
        assert_eq!(
            blosc2_meta_exists(&schunk, &long_name),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(schunk.metalayer(&long_name), Some(&b"x"[..]));
        assert!(blosc2_schunk_to_buffer(&schunk).0 < 0);

        let mut vl_schunk = Schunk::new(CParams::default(), DParams::default());
        vl_schunk.append_buffer(b"payload").unwrap();
        assert_eq!(
            blosc2_vlmeta_add(&mut vl_schunk, &long_name, b"vl", None),
            0
        );
        assert_eq!(
            blosc2_vlmeta_exists(&vl_schunk, &long_name),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(vl_schunk.vlmetalayer(&long_name), Some(&b"vl"[..]));
        assert!(blosc2_schunk_to_buffer(&vl_schunk).0 < 0);
    }

    #[test]
    fn test_schunk_metalayers_reject_invalid_inputs() {
        let mut schunk = Schunk::new(CParams::default(), DParams::default());

        schunk.add_metalayer("", b"data").unwrap();
        assert_eq!(schunk.metalayer(""), Some(&b"data"[..]));

        let large_name = "x".repeat(32);
        assert!(schunk.add_metalayer(&large_name, b"data").is_err());
        assert!(schunk.update_metalayer("missing", b"data").is_err());
        schunk.add_metalayer("fixed", b"abc").unwrap();
        schunk.update_metalayer("fixed", b"x").unwrap();
        assert_eq!(schunk.metalayer("fixed"), Some(&b"xbc"[..]));
        assert_eq!(schunk.meta_exists_c("fixed"), 1);
        assert_eq!(blosc2_meta_exists(&schunk, "fixed"), 1);
        assert_eq!(schunk.meta_update_c("fixed", b"yz"), 1);
        assert_eq!(schunk.metalayer("fixed"), Some(&b"yzc"[..]));
        assert_eq!(blosc2_meta_update(&mut schunk, "fixed", b"qw"), 1);
        assert_eq!(
            blosc2_meta_get(&schunk, "fixed"),
            (1, Some(b"qwc".to_vec()))
        );
        assert_eq!(schunk.meta_update_c("fixed", b"abcd"), 1);
        assert_eq!(schunk.metalayer("fixed"), Some(&b"qwc"[..]));
        assert_eq!(
            blosc2_meta_exists(&schunk, "missing"),
            BLOSC2_ERROR_NOT_FOUND
        );
        assert_eq!(
            blosc2_meta_get(&schunk, "missing"),
            (BLOSC2_ERROR_NOT_FOUND, None)
        );
        assert_eq!(
            schunk.meta_update_c(&large_name, b"data"),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(blosc2_meta_add(&mut schunk, "second", b"data"), 2);
        let (count, names) = blosc2_meta_get_names(&schunk);
        assert_eq!(count, names.len() as i32);
        assert_eq!(blosc2_meta_delete(&mut schunk, "fixed"), count - 1);
        assert_eq!(
            blosc2_meta_delete(&mut schunk, "fixed"),
            BLOSC2_ERROR_NOT_FOUND
        );
        assert_eq!(
            schunk.update_metalayer("fixed", b"abcd"),
            Err("Metalayer does not exist")
        );
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
        let mut too_many = vec![0u8; frame::FRAME_HEADER_MIN_LEN + 7];
        too_many[frame::FRAME_HEADER_MIN_LEN] = 0x93;
        too_many[frame::FRAME_HEADER_MIN_LEN + 1] = 0xcd;
        too_many[frame::FRAME_HEADER_MIN_LEN + 2..frame::FRAME_HEADER_MIN_LEN + 4]
            .copy_from_slice(&7u16.to_be_bytes());
        too_many[frame::FRAME_HEADER_MIN_LEN + 4] = 0xde;
        too_many[frame::FRAME_HEADER_MIN_LEN + 5..frame::FRAME_HEADER_MIN_LEN + 7]
            .copy_from_slice(&((BLOSC2_MAX_METALAYERS as u16) + 1).to_be_bytes());
        assert_eq!(
            frame::parse_metalayers(&too_many),
            Err("Invalid frame: too many metalayers".to_string())
        );

        let frame = schunk.to_frame();
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        let index_size = u16::from_be_bytes(
            frame[frame::FRAME_HEADER_MIN_LEN + 2..frame::FRAME_HEADER_MIN_LEN + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let value_count_pos = frame::FRAME_HEADER_MIN_LEN + index_size + 1;

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

        let mut mismatched_value_count = frame.clone();
        mismatched_value_count[value_count_pos..value_count_pos + 2]
            .copy_from_slice(&0u16.to_be_bytes());
        let restored = Schunk::from_frame(&mismatched_value_count).unwrap();
        assert_eq!(restored.metalayer("name"), Some(&b"value"[..]));

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
        let restored = Schunk::from_frame(&extra).unwrap();
        assert_eq!(restored.metalayer("name"), Some(&b"value"[..]));
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
        assert_eq!(
            schunk
                .add_vlmetalayer_index("vlmeta1", &long_content)
                .unwrap(),
            0
        );
        assert_eq!(blosc2_vlmeta_add(&mut schunk, "vlmeta2", b"small", None), 1);
        schunk.add_vlmetalayer("", b"empty-vl-name").unwrap();
        assert_eq!(schunk.vlmetalayer_index("vlmeta1"), Some(0));
        assert_eq!(schunk.vlmetalayer_index(""), Some(2));
        assert_eq!(blosc2_vlmeta_exists(&schunk, "vlmeta2"), 1);
        assert!(schunk.vlmetalayer_exists("vlmeta2"));
        assert!(!schunk.vlmetalayer_exists("missing"));
        assert_eq!(schunk.vlmetalayer_names(), vec!["vlmeta1", "vlmeta2", ""]);
        assert!(schunk.add_vlmetalayer("vlmeta2", b"duplicate").is_err());
        assert_eq!(
            schunk
                .update_vlmetalayer_index("vlmeta2", b"updated")
                .unwrap(),
            1
        );

        assert_eq!(schunk.vlmetalayer("vlmeta1"), Some(long_content.as_slice()));
        assert_eq!(
            blosc2_vlmeta_get(&schunk, "vlmeta2"),
            (1, Some(b"updated".to_vec()))
        );
        let (count, names) = blosc2_vlmeta_get_names(&schunk);
        assert_eq!(count, 3);
        assert_eq!(names, vec!["vlmeta1", "vlmeta2", ""]);
        assert_eq!(
            schunk.remove_vlmetalayer("vlmeta2"),
            Some(b"updated".to_vec())
        );
        assert_eq!(
            blosc2_vlmeta_exists(&schunk, "vlmeta2"),
            BLOSC2_ERROR_NOT_FOUND
        );
        assert_eq!(
            blosc2_vlmeta_get(&schunk, "vlmeta2"),
            (BLOSC2_ERROR_NOT_FOUND, None)
        );
        assert_eq!(
            blosc2_vlmeta_add(&mut schunk, "vlmeta2", b"restored", None),
            2
        );
        assert_eq!(
            blosc2_vlmeta_add_c(&mut schunk, "vlmeta3", b"short-tail", 5, None),
            3
        );
        assert_eq!(schunk.vlmetalayer("vlmeta3"), Some(&b"short"[..]));
        assert_eq!(
            blosc2_vlmeta_add_c(&mut schunk, "badlen", b"bad", 4, None),
            BLOSC2_ERROR_INVALID_PARAM
        );
        assert_eq!(
            blosc2_vlmeta_update(&mut schunk, "vlmeta2", b"restored2", None),
            2
        );
        assert_eq!(
            blosc2_vlmeta_update_c(&mut schunk, "vlmeta3", b"tiny-tail", 4, None),
            3
        );
        assert_eq!(schunk.vlmetalayer("vlmeta3"), Some(&b"tiny"[..]));
        assert_eq!(
            blosc2_vlmeta_update(
                &mut schunk,
                "vlmeta2",
                b"restored3",
                Some(CParams::default()),
            ),
            2
        );
        assert_eq!(
            blosc2_vlmeta_get(&schunk, "vlmeta2"),
            (2, Some(b"restored3".to_vec()))
        );
        assert_eq!(
            blosc2_vlmeta_get(&schunk, "vlmeta3"),
            (3, Some(b"tiny".to_vec()))
        );
        assert_eq!(blosc2_vlmeta_delete(&mut schunk, "vlmeta2"), 3);
        assert_eq!(
            blosc2_vlmeta_delete(&mut schunk, "vlmeta2"),
            BLOSC2_ERROR_NOT_FOUND
        );
        assert_eq!(
            blosc2_vlmeta_add(&mut schunk, "vlmeta2", b"restored", None),
            3
        );

        let frame = schunk.to_frame();
        assert_eq!(frame[68], 0xC3);
        let stored = first_vlmetalayer_cbuffer(&frame, schunk.cbytes as usize);
        let header = ChunkHeader::read(&stored).unwrap();
        assert_eq!(header.typesize, 8);
        assert_eq!(header.filters[BLOSC2_MAX_FILTERS - 1], BLOSC_SHUFFLE);
        assert_eq!(header.flags & BLOSC_DONT_SPLIT, 0);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.decompress_all().unwrap(), b"payload");
        assert_eq!(
            restored.vlmetalayer("vlmeta1"),
            Some(long_content.as_slice())
        );
        assert_eq!(restored.vlmetalayer_index(""), Some(1));
        assert!(restored.vlmetalayer_exists("vlmeta2"));
        assert_eq!(
            restored.vlmetalayer_names(),
            vec!["vlmeta1", "", "vlmeta3", "vlmeta2"]
        );
        assert_eq!(restored.vlmetalayer("vlmeta3"), Some(&b"tiny"[..]));
        assert_eq!(restored.vlmetalayer("vlmeta2"), Some(&b"restored"[..]));
        assert_eq!(restored.vlmetalayer(""), Some(&b"empty-vl-name"[..]));
    }

    #[test]
    fn test_schunk_vlmetalayer_writer_uses_c_frame_limit() {
        let mut schunk = Schunk::new(CParams::default(), DParams::default());
        schunk.append_buffer(b"payload").unwrap();
        for idx in 0..BLOSC2_MAX_METALAYERS {
            let name = format!("vl{idx}");
            let content = format!("content-{idx}");
            schunk.add_vlmetalayer(&name, content.as_bytes()).unwrap();
        }

        let restored = Schunk::from_frame(&schunk.to_frame()).unwrap();
        for idx in 0..BLOSC2_MAX_METALAYERS {
            let name = format!("vl{idx}");
            let content = format!("content-{idx}");
            assert_eq!(
                restored.vlmetalayer(&name),
                Some(content.as_bytes()),
                "{name}"
            );
        }

        assert_eq!(
            schunk
                .add_vlmetalayer_index("overflow", b"in-memory-overflow")
                .unwrap(),
            BLOSC2_MAX_METALAYERS
        );
        assert_eq!(
            schunk.vlmetalayer("overflow"),
            Some(&b"in-memory-overflow"[..])
        );
        assert_eq!(
            blosc2_vlmeta_add(&mut schunk, "overflow-c", b"in-memory-overflow-c", None),
            (BLOSC2_MAX_METALAYERS + 1) as i32
        );
        assert_eq!(
            schunk.vlmetalayer("overflow-c"),
            Some(&b"in-memory-overflow-c"[..])
        );
        assert_eq!(
            blosc2_schunk_to_buffer(&schunk).0,
            i64::from(BLOSC2_ERROR_INVALID_PARAM)
        );
    }

    #[test]
    fn test_schunk_vlmetalayers_preserve_custom_cparams() {
        let mut schunk = Schunk::new(CParams::default(), DParams::default());
        schunk.append_buffer(b"payload").unwrap();
        let vl_cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0; BLOSC2_MAX_FILTERS],
            ..Default::default()
        };
        let content = b"custom vlmetalayer compression params ".repeat(8);
        schunk
            .add_vlmetalayer_with_cparams("custom", &content, vl_cparams.clone())
            .unwrap();

        let frame = schunk.to_frame();
        let stored = first_vlmetalayer_cbuffer(&frame, schunk.cbytes as usize);
        let header = ChunkHeader::read(&stored).unwrap();
        assert_eq!(header.compcode(), BLOSC_LZ4);
        assert_eq!(header.filters, [0; BLOSC2_MAX_FILTERS]);
        assert_eq!(compress::decompress(&stored).unwrap(), content);

        let restored = Schunk::from_frame(&frame).unwrap();
        assert_eq!(restored.vlmetalayer("custom"), Some(content.as_slice()));
        let reserialized = restored.to_frame();
        assert_eq!(
            first_vlmetalayer_cbuffer(&reserialized, restored.cbytes as usize),
            stored
        );

        let copied = schunk
            .copy_schunk_with_params(schunk.cparams.clone(), DParams::default())
            .unwrap();
        assert_eq!(copied.vlmetalayer("custom"), Some(content.as_slice()));
        let copied_frame = copied.to_frame();
        let copied_stored = first_vlmetalayer_cbuffer(&copied_frame, copied.cbytes as usize);
        let copied_header = ChunkHeader::read(&copied_stored).unwrap();
        assert_eq!(copied_header.compcode(), BLOSC_BLOSCLZ);
        assert_eq!(copied_header.filters[BLOSC2_MAX_FILTERS - 1], BLOSC_SHUFFLE);
        assert_eq!(compress::decompress(&copied_stored).unwrap(), content);

        let updated = b"updated custom vlmetalayer compression params ".repeat(8);
        let mut changed = restored;
        changed
            .update_vlmetalayer_with_cparams("custom", &updated, vl_cparams)
            .unwrap();
        let changed_frame = changed.to_frame();
        let changed_stored = first_vlmetalayer_cbuffer(&changed_frame, changed.cbytes as usize);
        let changed_header = ChunkHeader::read(&changed_stored).unwrap();
        assert_eq!(changed_header.compcode(), BLOSC_LZ4);
        assert_eq!(changed_header.filters, [0; BLOSC2_MAX_FILTERS]);
        assert_eq!(compress::decompress(&changed_stored).unwrap(), updated);
    }

    #[test]
    fn test_schunk_vlmetalayers_reject_invalid_inputs() {
        let mut schunk = Schunk::new(CParams::default(), DParams::default());

        schunk.add_vlmetalayer("", b"data").unwrap();
        assert_eq!(schunk.vlmetalayer(""), Some(&b"data"[..]));
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
        let mut too_many = vec![0u8; 35];
        too_many[0] = 0x94;
        too_many[1] = 0x01;
        too_many[2] = 0x93;
        too_many[3] = 0xcd;
        too_many[4..6].copy_from_slice(&6u16.to_be_bytes());
        too_many[6] = 0xde;
        too_many[7..9].copy_from_slice(&((BLOSC2_MAX_VLMETALAYERS as u16) + 1).to_be_bytes());
        assert_eq!(
            frame::parse_vlmetalayers(&too_many, true).unwrap_err(),
            "Invalid frame: too many VL-metalayers"
        );

        let frame = schunk.to_frame();

        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        let data_end = header_size + schunk.cbytes as usize;
        let offsets_header = ChunkHeader::read(&frame[data_end..]).unwrap();
        let trailer_start = data_end + offsets_header.cbytes as usize;
        let index_size = u16::from_be_bytes(
            frame[trailer_start + 4..trailer_start + 6]
                .try_into()
                .unwrap(),
        ) as usize;
        let value_count_pos = trailer_start + 3 + index_size + 1;
        let content_marker_pos = trailer_start + 3 + index_size + 3;
        let content_len = u32::from_be_bytes(
            frame[content_marker_pos + 1..content_marker_pos + 5]
                .try_into()
                .unwrap(),
        ) as usize;
        let stored_content = &frame[content_marker_pos + 5..content_marker_pos + 5 + content_len];
        assert_ne!(stored_content, b"content");
        assert_eq!(compress::decompress(stored_content).unwrap(), b"content");

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
        bin8_content[content_marker_pos] = 0xC4;
        assert!(Schunk::from_frame(&bin8_content).is_err());

        let mut mismatched_value_count = frame.clone();
        mismatched_value_count[value_count_pos..value_count_pos + 2]
            .copy_from_slice(&0u16.to_be_bytes());
        let restored = Schunk::from_frame(&mismatched_value_count).unwrap();
        assert_eq!(restored.vlmetalayer("vlmeta"), Some(&b"content"[..]));

        let mut raw_content = frame.clone();
        raw_content[content_marker_pos + 5..content_marker_pos + 5 + content_len].fill(0xaa);
        assert!(Schunk::from_frame(&raw_content).is_err());

        let mut flag_mismatch = frame.clone();
        flag_mismatch[68] = 0xC2;
        let restored = Schunk::from_frame(&flag_mismatch).unwrap();
        assert_eq!(restored.vlmetalayer("vlmeta"), Some(&b"content"[..]));

        let footer_start = frame.len() - 23;
        let mut nonzero_fingerprint = frame.clone();
        nonzero_fingerprint[footer_start + 6] = 17;
        nonzero_fingerprint[footer_start + 7] = 0xA5;
        let restored = Schunk::from_frame(&nonzero_fingerprint).unwrap();
        assert_eq!(restored.vlmetalayer("vlmeta"), Some(&b"content"[..]));

        let mut extra = frame.clone();
        let footer_start = frame.len() - 23;
        extra.insert(footer_start, 0);
        let new_frame_size = extra.len() as u64;
        extra[16..24].copy_from_slice(&new_frame_size.to_be_bytes());
        let new_trailer_len = (frame.len() - trailer_start + 1) as u32;
        let new_footer_start = footer_start + 1;
        extra[new_footer_start + 1..new_footer_start + 5]
            .copy_from_slice(&new_trailer_len.to_be_bytes());
        let restored = Schunk::from_frame(&extra).unwrap();
        assert_eq!(restored.vlmetalayer("vlmeta"), Some(&b"content"[..]));
    }

    #[test]
    fn test_frame_without_vlmetalayers_accepts_absent_trailer() {
        let mut schunk = Schunk::new(CParams::default(), DParams::default());
        schunk.append_buffer(b"payload").unwrap();
        let frame = schunk.to_frame();
        let header_size = i32::from_be_bytes(frame[11..15].try_into().unwrap()) as usize;
        let data_end = header_size + schunk.cbytes as usize;
        let offsets_header = ChunkHeader::read(&frame[data_end..]).unwrap();
        let trailer_start = data_end + offsets_header.cbytes as usize;

        let mut no_trailer = frame[..trailer_start].to_vec();
        let new_frame_size = no_trailer.len() as u64;
        no_trailer[16..24].copy_from_slice(&new_frame_size.to_be_bytes());
        let restored = Schunk::from_frame(&no_trailer).unwrap();
        assert!(restored.vlmetalayers.is_empty());
        assert_eq!(restored.decompress_all().unwrap(), b"payload");
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
        assert_eq!(
            Schunk::from_frame(&mismatched_codec)
                .unwrap()
                .decompress_chunk(0)
                .unwrap(),
            data
        );

        let mut mismatched_filter = frame.clone();
        mismatched_filter[71 + BLOSC2_MAX_FILTERS - 1] = BLOSC_NOFILTER;
        assert_eq!(
            Schunk::from_frame(&mismatched_filter)
                .unwrap()
                .decompress_chunk(0)
                .unwrap(),
            data
        );

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
    fn test_frame_accepts_heterogeneous_raw_chunk_parameters() {
        let first_data: Vec<u8> = (0..512u32).flat_map(|i| i.to_le_bytes()).collect();
        let second_data: Vec<u8> = (512..1024u32).flat_map(|i| i.to_le_bytes()).collect();
        let first = compress::compress(
            &first_data,
            &CParams {
                compcode: BLOSC_LZ4,
                clevel: 5,
                typesize: 4,
                splitmode: BLOSC_FORWARD_COMPAT_SPLIT,
                filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
                ..Default::default()
            },
        )
        .unwrap();
        let second = compress::compress(
            &second_data,
            &CParams {
                compcode: BLOSC_ZSTD,
                clevel: 5,
                typesize: 4,
                splitmode: BLOSC_NEVER_SPLIT,
                filters: [0; BLOSC2_MAX_FILTERS],
                compcode_meta: 7,
                ..Default::default()
            },
        )
        .unwrap();

        let mut schunk = Schunk::new(
            CParams {
                typesize: 4,
                ..Default::default()
            },
            DParams::default(),
        );
        schunk.append_chunk(&first).unwrap();
        schunk.append_chunk(&second).unwrap();
        let restored = Schunk::from_frame(&schunk.to_frame()).unwrap();
        assert_eq!(restored.compressed_chunk(0).unwrap(), first.as_slice());
        assert_eq!(restored.compressed_chunk(1).unwrap(), second.as_slice());
        assert_eq!(restored.decompress_chunk(0).unwrap(), first_data);
        assert_eq!(restored.decompress_chunk(1).unwrap(), second_data);
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
    }
}
