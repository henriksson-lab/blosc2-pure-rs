//! Blosc2 N-dimensional array (b2nd) layer.
//!
//! A [`B2ndArray`] is a multidimensional array of fixed-size items backed by a
//! super-chunk ([`Schunk`]). It is described by three shapes:
//!
//! * `shape` — the logical extent of the array in items per dimension.
//! * `chunkshape` — the per-dimension extent of one compressed chunk. The
//!   array is tiled by chunks; each chunk maps to a [`Schunk`] entry.
//! * `blockshape` — the per-dimension extent of one block inside a chunk,
//!   which is also Blosc's compression unit.
//!
//! The shape/chunkshape/blockshape triple, the dtype string and the dtype
//! format are serialized into the `b2nd` fixed-size metalayer of the
//! super-chunk so that an array can be reconstructed from a frame on disk.

use crate::compress::{CParams, DParams};
use crate::schunk::Schunk;
use std::collections::BTreeMap;
use std::path::Path;

/// Name of the fixed-size metalayer that carries the b2nd shape descriptor.
pub const B2ND_METALAYER_NAME: &str = "b2nd";
/// Legacy Caterva metalayer name used by older b2nd frames.
const CATERVA_METALAYER_NAME: &str = "caterva";
/// Version of the b2nd metalayer format; must not exceed 127.
pub const B2ND_METALAYER_VERSION: u8 = 0;
/// Maximum number of dimensions supported by a b2nd array.
pub const B2ND_MAX_DIM: usize = 16;
/// `dtype_format` value indicating that the dtype string follows the
/// NumPy dtype convention.
pub const DTYPE_NUMPY_FORMAT: i8 = 0;

/// Shape descriptor for a b2nd array.
///
/// Holds the logical shape, the chunkshape, the blockshape and the dtype
/// string. This is exactly the information serialized into the `b2nd`
/// metalayer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct B2ndMeta {
    /// Shape of the original data in items per dimension.
    pub shape: Vec<i64>,
    /// Shape of each chunk in items per dimension.
    pub chunkshape: Vec<i32>,
    /// Shape of each block in items per dimension.
    pub blockshape: Vec<i32>,
    /// Data type as a string (NumPy dtype string when `dtype_format = 0`).
    pub dtype: String,
    /// Format of the data type string. `0` means NumPy.
    pub dtype_format: i8,
}

/// A multidimensional array of fixed-size items backed by a super-chunk.
#[derive(Clone)]
pub struct B2ndArray {
    /// Shape descriptor stored as the b2nd metalayer.
    pub meta: B2ndMeta,
    /// Underlying super-chunk holding the compressed chunks.
    pub schunk: Schunk,
}

impl B2ndMeta {
    /// Build a validated [`B2ndMeta`] from the array shape, chunkshape,
    /// blockshape and dtype string.
    pub fn new(
        shape: Vec<i64>,
        chunkshape: Vec<i32>,
        blockshape: Vec<i32>,
        dtype: impl Into<String>,
        dtype_format: i8,
    ) -> Result<Self, &'static str> {
        let meta = Self {
            shape,
            chunkshape,
            blockshape,
            dtype: dtype.into(),
            dtype_format,
        };
        meta.validate()?;
        Ok(meta)
    }

    /// Number of dimensions of the array.
    pub fn ndim(&self) -> usize {
        self.shape.len()
    }

    /// Total number of items in the original (un-padded) array.
    pub fn nitems(&self) -> Result<usize, &'static str> {
        product_i64(&self.shape)
    }

    /// Number of items in a single chunk.
    pub fn chunk_nitems(&self) -> Result<usize, &'static str> {
        product_i32(&self.chunkshape)
    }

    /// Check that ranks, sizes and dtype satisfy all b2nd invariants.
    pub fn validate(&self) -> Result<(), &'static str> {
        let ndim = self.shape.len();
        if ndim > B2ND_MAX_DIM {
            return Err("Invalid B2ND ndim");
        }
        if self.chunkshape.len() != ndim || self.blockshape.len() != ndim {
            return Err("B2ND shape ranks differ");
        }
        if self.dtype.len() > i32::MAX as usize {
            return Err("B2ND dtype too large");
        }
        if !(0..=127).contains(&self.dtype_format) {
            return Err("Invalid B2ND dtype format");
        }
        for dim in 0..ndim {
            if self.shape[dim] < 0 {
                return Err("Invalid B2ND shape");
            }
            if self.shape[dim] == 0 {
                if self.chunkshape[dim] < 0 || self.blockshape[dim] < 0 {
                    return Err("Invalid B2ND chunk or block shape");
                }
                if (self.chunkshape[dim] == 0) != (self.blockshape[dim] == 0) {
                    return Err("Invalid B2ND chunk or block shape");
                }
            } else if self.chunkshape[dim] <= 0 || self.blockshape[dim] <= 0 {
                return Err("Invalid B2ND chunk or block shape");
            }
        }
        self.nitems()?;
        self.chunk_nitems()?;
        Ok(())
    }

    /// Encode the metadata as a msgpack buffer suitable for the b2nd metalayer.
    pub fn serialize(&self) -> Result<Vec<u8>, &'static str> {
        self.validate()?;
        let ndim = self.ndim();

        let dtype = self.dtype.as_bytes();
        let mut out = Vec::with_capacity(3 + 3 * (3 + ndim * 9) + 6 + dtype.len());
        out.push(0x90 + 7);
        out.push(B2ND_METALAYER_VERSION);
        out.push(ndim as u8);

        write_array_header(&mut out, ndim)?;
        for &dim in &self.shape {
            out.push(0xd3);
            out.extend_from_slice(&dim.to_be_bytes());
        }

        write_array_header(&mut out, ndim)?;
        for &dim in &self.chunkshape {
            out.push(0xd2);
            out.extend_from_slice(&dim.to_be_bytes());
        }

        write_array_header(&mut out, ndim)?;
        for &dim in &self.blockshape {
            out.push(0xd2);
            out.extend_from_slice(&dim.to_be_bytes());
        }

        out.push(self.dtype_format as u8);
        out.push(0xdb);
        out.extend_from_slice(&(dtype.len() as i32).to_be_bytes());
        out.extend_from_slice(dtype);
        Ok(out)
    }

    /// Decode the msgpack buffer stored in the b2nd metalayer back into a
    /// validated [`B2ndMeta`].
    pub fn deserialize(data: &[u8]) -> Result<Self, &'static str> {
        Self::deserialize_inner(data, None)
    }

    fn deserialize_legacy_optional_dtype(
        data: &[u8],
        _typesize: usize,
    ) -> Result<Self, &'static str> {
        Self::deserialize_inner(data, Some("|u1".to_string()))
    }

    fn deserialize_caterva(data: &[u8], typesize: usize) -> Result<Self, &'static str> {
        Self::deserialize_legacy_optional_dtype(data, typesize)
    }

    fn deserialize_inner(data: &[u8], legacy_dtype: Option<String>) -> Result<Self, &'static str> {
        let mut pos = 0usize;
        let fields = read_array_header(data, &mut pos)?;
        if fields != 7 && !(fields == 5 && legacy_dtype.is_some()) {
            return Err("Invalid B2ND metadata");
        }
        let version = read_fixint(data, &mut pos)?;
        if version != B2ND_METALAYER_VERSION {
            return Err("Unsupported B2ND metalayer version");
        }
        let ndim = read_fixint(data, &mut pos)? as usize;
        if ndim > B2ND_MAX_DIM {
            return Err("Invalid B2ND ndim");
        }

        expect_array_header(data, &mut pos, ndim)?;
        let mut shape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            expect_byte(data, &mut pos, 0xd3)?;
            shape.push(read_i64(data, &mut pos)?);
        }

        expect_array_header(data, &mut pos, ndim)?;
        let mut chunkshape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            expect_byte(data, &mut pos, 0xd2)?;
            chunkshape.push(read_i32(data, &mut pos)?);
        }

        expect_array_header(data, &mut pos, ndim)?;
        let mut blockshape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            expect_byte(data, &mut pos, 0xd2)?;
            blockshape.push(read_i32(data, &mut pos)?);
        }

        let (dtype, dtype_format) = if legacy_dtype.is_some() && pos == data.len() {
            (legacy_dtype.unwrap(), DTYPE_NUMPY_FORMAT)
        } else if fields == 5 {
            if pos != data.len() {
                return Err("Invalid B2ND metadata length");
            }
            (legacy_dtype.unwrap(), DTYPE_NUMPY_FORMAT)
        } else {
            let dtype_format = read_fixint(data, &mut pos)? as i8;
            expect_byte(data, &mut pos, 0xdb)?;
            let dtype_len = read_i32(data, &mut pos)?;
            if dtype_len < 0 {
                return Err("Invalid B2ND dtype length");
            }
            let dtype_len = dtype_len as usize;
            let end = pos
                .checked_add(dtype_len)
                .ok_or("Invalid B2ND dtype length")?;
            if end != data.len() {
                return Err("Invalid B2ND metadata length");
            }
            let dtype = std::str::from_utf8(&data[pos..end])
                .map_err(|_| "B2ND dtype is not UTF-8")?
                .to_string();
            (dtype, dtype_format)
        };

        Self::new(shape, chunkshape, blockshape, dtype, dtype_format)
    }
}

impl B2ndArray {
    /// Build a b2nd array from a dense row-major C buffer.
    ///
    /// The buffer must contain `meta.nitems() * cparams.typesize` bytes laid out
    /// in C order. Data is split into chunks and blocks, compressed with
    /// `cparams`, and written to a new super-chunk that carries `meta` as the
    /// `b2nd` metalayer.
    pub fn from_cbuffer(
        meta: B2ndMeta,
        data: &[u8],
        mut cparams: CParams,
        dparams: DParams,
    ) -> Result<Self, &'static str> {
        meta.validate()?;
        let typesize = cparams.typesize as usize;
        let expected_len = meta
            .nitems()?
            .checked_mul(typesize)
            .ok_or("B2ND buffer too large")?;
        if data.len() < expected_len {
            return Err("B2ND buffer size does not match shape and typesize");
        }

        let chunk_nbytes = extchunk_nitems(&meta)?
            .checked_mul(typesize)
            .ok_or("B2ND chunk too large")?;
        if chunk_nbytes > i32::MAX as usize {
            return Err("B2ND chunk too large");
        }
        let block_nbytes = product_i32(&meta.blockshape)?
            .checked_mul(typesize)
            .ok_or("B2ND block too large")?;
        if block_nbytes > i32::MAX as usize {
            return Err("B2ND block too large");
        }
        cparams.blocksize = block_nbytes as i32;

        let mut schunk = Schunk::new(cparams, dparams);
        schunk.chunksize = chunk_nbytes;
        schunk.add_metalayer(B2ND_METALAYER_NAME, &meta.serialize()?)?;

        let chunk_grid = chunk_grid(&meta)?;
        let chunk_count = product_usize(&chunk_grid)?;
        if chunk_count > 0 {
            let layout = B2ndLayout::new(&meta, typesize)?;
            for linear_chunk in 0..chunk_count {
                let chunk_index = unravel_index(linear_chunk, &chunk_grid);
                let mut chunk = vec![0u8; chunk_nbytes];
                copy_dense_to_chunk(&meta, data, &layout, &chunk_index, &mut chunk)?;
                schunk.append_buffer(&chunk)?;
            }
        }

        Ok(Self { meta, schunk })
    }

    /// Reinterpret a super-chunk as a b2nd array by reading its `b2nd`
    /// metalayer. Chunk count and chunk byte sizes are checked by data accessors.
    pub fn from_schunk(schunk: Schunk) -> Result<Self, &'static str> {
        let meta = if let Some(content) = schunk.metalayer(B2ND_METALAYER_NAME) {
            B2ndMeta::deserialize_legacy_optional_dtype(content, schunk.cparams.typesize as usize)?
        } else if let Some(content) = schunk.metalayer(CATERVA_METALAYER_NAME) {
            B2ndMeta::deserialize_caterva(content, schunk.cparams.typesize as usize)?
        } else {
            return Err("Schunk does not contain a B2ND metalayer");
        };
        Ok(Self { meta, schunk })
    }

    /// Build a b2nd array from a serialized contiguous frame.
    pub fn from_frame(frame: &[u8]) -> Result<Self, String> {
        Self::from_schunk(Schunk::from_frame(frame)?).map_err(str::to_string)
    }

    /// Open a b2nd array from a contiguous frame file or sparse frame
    /// directory on disk.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::from_schunk(Schunk::open(path.as_ref().to_str().ok_or("Invalid path")?)?)
            .map_err(str::to_string)
    }

    /// Serialize the array as a contiguous in-memory frame.
    pub fn to_frame(&self) -> Vec<u8> {
        self.schunk.to_frame()
    }

    /// Write the array as a contiguous frame at `path`.
    pub fn save(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        std::fs::write(path, self.to_frame())
    }

    /// Decompress every chunk and assemble a dense row-major C buffer covering
    /// the full array shape.
    pub fn to_cbuffer(&self) -> Result<Vec<u8>, &'static str> {
        let typesize = self.schunk.cparams.typesize as usize;
        let out_len = self
            .meta
            .nitems()?
            .checked_mul(typesize)
            .ok_or("B2ND buffer too large")?;
        let mut out = vec![0u8; out_len];
        let chunk_grid = chunk_grid(&self.meta)?;
        let chunk_count = product_usize(&chunk_grid)?;
        if self.schunk.nchunks() as usize != chunk_count {
            return Err("B2ND chunk count does not match metadata");
        }
        if chunk_count == 0 {
            return Ok(out);
        }

        let layout = B2ndLayout::new(&self.meta, typesize)?;
        for linear_chunk in 0..chunk_count {
            let chunk = self.schunk.decompress_chunk(linear_chunk as i64)?;
            let expected_chunk_len = extchunk_nitems(&self.meta)?
                .checked_mul(typesize)
                .ok_or("B2ND chunk too large")?;
            if chunk.len() != expected_chunk_len {
                return Err("B2ND chunk size does not match metadata");
            }
            let chunk_index = unravel_index(linear_chunk, &chunk_grid);
            copy_chunk_to_dense(&self.meta, &chunk, &layout, &chunk_index, &mut out)?;
        }
        Ok(out)
    }

    /// Shape of the original data in items per dimension.
    pub fn shape(&self) -> &[i64] {
        &self.meta.shape
    }

    /// Shape of each chunk in items per dimension.
    pub fn chunkshape(&self) -> &[i32] {
        &self.meta.chunkshape
    }

    /// Shape of each block in items per dimension.
    pub fn blockshape(&self) -> &[i32] {
        &self.meta.blockshape
    }

    /// Return a dense row-major buffer for the half-open item slice
    /// `start..stop` in each dimension.
    pub fn get_slice(&self, start: &[i64], stop: &[i64]) -> Result<Vec<u8>, &'static str> {
        let slice = validate_slice_bounds(&self.meta, start, stop)?;
        self.get_slice_cbuffer(start, stop, &slice.extents_as_i64)
    }

    /// Return a dense row-major buffer with explicit buffer shape, filling the
    /// leading region with the half-open item slice and leaving padding zeroed.
    pub fn get_slice_cbuffer(
        &self,
        start: &[i64],
        stop: &[i64],
        buffershape: &[i64],
    ) -> Result<Vec<u8>, &'static str> {
        let typesize = self.schunk.cparams.typesize as usize;
        let slice = validate_slice_bounds(&self.meta, start, stop)?;
        if buffershape.len() != slice.extents.len() {
            return Err("B2ND buffer rank does not match array rank");
        }
        for (extent, &buffer_dim) in slice.extents_as_i64.iter().zip(buffershape) {
            if buffer_dim < *extent {
                return Err("B2ND buffer shape is smaller than slice shape");
            }
        }
        let out_len = product_i64(buffershape)?
            .checked_mul(typesize)
            .ok_or("B2ND slice too large")?;
        let mut out = vec![0u8; out_len];
        if slice.extents.iter().any(|&extent| extent == 0) {
            return Ok(out);
        }
        let dense = self.to_cbuffer()?;
        copy_dense_region(
            &dense,
            DenseRegion {
                shape: &self.meta.shape,
                start: &slice.starts,
            },
            &mut out,
            DenseRegion {
                shape: buffershape,
                start: &vec![0; slice.extents.len()],
            },
            &slice.extents,
            typesize,
        )?;
        Ok(out)
    }

    /// Overwrite the half-open item slice `start..stop` from a dense row-major
    /// source buffer whose shape is `stop - start`.
    pub fn set_slice(
        &mut self,
        start: &[i64],
        stop: &[i64],
        data: &[u8],
    ) -> Result<(), &'static str> {
        let slice = validate_slice_bounds(&self.meta, start, stop)?;
        self.set_slice_cbuffer(start, stop, &slice.extents_as_i64, data)
    }

    /// Overwrite the half-open item slice from the leading region of a dense
    /// row-major source buffer with explicit buffer shape.
    pub fn set_slice_cbuffer(
        &mut self,
        start: &[i64],
        stop: &[i64],
        buffershape: &[i64],
        data: &[u8],
    ) -> Result<(), &'static str> {
        let typesize = self.schunk.cparams.typesize as usize;
        let slice = validate_slice_bounds(&self.meta, start, stop)?;
        if buffershape.len() != slice.extents.len() {
            return Err("B2ND buffer rank does not match array rank");
        }
        for (extent, &buffer_dim) in slice.extents_as_i64.iter().zip(buffershape) {
            if buffer_dim < *extent {
                return Err("B2ND buffer shape is smaller than slice shape");
            }
        }
        if slice.extents.iter().any(|&extent| extent == 0) {
            return Ok(());
        }
        let required_len = dense_region_required_len(buffershape, &slice.extents, typesize)?;
        if data.len() < required_len {
            return Err("B2ND slice buffer size does not match slice shape and typesize");
        }

        self.update_slice_chunks_from_dense(&slice, buffershape, data, typesize)
    }

    /// Return a dense row-major buffer for an orthogonal selection.
    pub fn get_orthogonal_selection(
        &self,
        selection: &[Vec<i64>],
    ) -> Result<Vec<u8>, &'static str> {
        let (coords, extents, out_shape) = validate_orthogonal_selection(&self.meta, selection)?;
        self.get_orthogonal_selection_cbuffer_with_validated(&coords, &extents, &out_shape)
    }

    /// Return a dense row-major buffer for an orthogonal selection with an
    /// explicit output buffer shape.
    pub fn get_orthogonal_selection_cbuffer(
        &self,
        selection: &[Vec<i64>],
        buffershape: &[i64],
    ) -> Result<Vec<u8>, &'static str> {
        let (coords, extents, _) = validate_orthogonal_selection(&self.meta, selection)?;
        self.get_orthogonal_selection_cbuffer_with_validated(&coords, &extents, buffershape)
    }

    fn get_orthogonal_selection_cbuffer_with_validated(
        &self,
        coords: &[Vec<usize>],
        extents: &[usize],
        buffershape: &[i64],
    ) -> Result<Vec<u8>, &'static str> {
        let typesize = self.schunk.cparams.typesize as usize;
        validate_orthogonal_buffershape(buffershape, extents)?;
        let out_len = product_i64(buffershape)?
            .checked_mul(typesize)
            .ok_or("B2ND selection too large")?;
        let mut out = vec![0u8; out_len];
        if extents.iter().any(|&extent| extent == 0) {
            return Ok(out);
        }
        let dense = self.to_cbuffer()?;
        copy_orthogonal_selection(
            &dense,
            &self.meta.shape,
            coords,
            &mut out,
            buffershape,
            typesize,
        )?;
        Ok(out)
    }

    /// Overwrite an orthogonal selection from a dense row-major source buffer.
    pub fn set_orthogonal_selection(
        &mut self,
        selection: &[Vec<i64>],
        data: &[u8],
    ) -> Result<(), &'static str> {
        let (coords, extents, out_shape) = validate_orthogonal_selection(&self.meta, selection)?;
        self.set_orthogonal_selection_cbuffer_with_validated(&coords, &extents, &out_shape, data)
    }

    /// Overwrite an orthogonal selection from the leading region of a dense
    /// row-major source buffer with explicit buffer shape.
    pub fn set_orthogonal_selection_cbuffer(
        &mut self,
        selection: &[Vec<i64>],
        buffershape: &[i64],
        data: &[u8],
    ) -> Result<(), &'static str> {
        let (coords, extents, _) = validate_orthogonal_selection(&self.meta, selection)?;
        self.set_orthogonal_selection_cbuffer_with_validated(&coords, &extents, buffershape, data)
    }

    fn set_orthogonal_selection_cbuffer_with_validated(
        &mut self,
        coords: &[Vec<usize>],
        extents: &[usize],
        buffershape: &[i64],
        data: &[u8],
    ) -> Result<(), &'static str> {
        let typesize = self.schunk.cparams.typesize as usize;
        validate_orthogonal_buffershape(buffershape, extents)?;
        if extents.iter().any(|&extent| extent == 0) {
            if !data.is_empty() {
                return Err(
                    "B2ND selection buffer size does not match selection shape and typesize",
                );
            }
            return Ok(());
        }
        let required_len = dense_region_required_len(buffershape, extents, typesize)?;
        if data.len() != required_len {
            return Err("B2ND selection buffer size does not match selection shape and typesize");
        }
        self.update_orthogonal_chunks_from_dense(coords, extents, buffershape, data, typesize)
    }

    /// Resize the array, preserving the overlapping prefix region and zero-filling
    /// new cells.
    pub fn resize(&mut self, new_shape: Vec<i64>) -> Result<(), &'static str> {
        self.resize_at(new_shape, None)
    }

    /// Resize the array at `start`, following C-Blosc2 `b2nd_resize`
    /// semantics. `None` resizes at the array end in each dimension.
    pub fn resize_at(
        &mut self,
        new_shape: Vec<i64>,
        start: Option<&[i64]>,
    ) -> Result<(), &'static str> {
        let mut new_meta = self.meta.clone();
        new_meta.shape = new_shape;
        new_meta.validate()?;
        let resize = validate_resize_at(&self.meta, &new_meta.shape, start)?;

        if chunk_grid(&self.meta)? == chunk_grid(&new_meta)? {
            self.update_meta_preserving_chunks(new_meta)?;
            return Ok(());
        }

        self.resize_by_chunk_mutation(new_meta, &resize)
    }

    fn resize_by_chunk_mutation(
        &mut self,
        new_meta: B2ndMeta,
        resize: &B2ndResize,
    ) -> Result<(), &'static str> {
        let old_meta = self.meta.clone();
        if old_meta.chunkshape != new_meta.chunkshape
            || old_meta.blockshape != new_meta.blockshape
            || old_meta.dtype != new_meta.dtype
            || old_meta.dtype_format != new_meta.dtype_format
        {
            return Err("B2ND resize metadata is not chunk-compatible");
        }

        let old_grid = chunk_grid(&old_meta)?;
        let new_grid = chunk_grid(&new_meta)?;
        if self.schunk.nchunks() as usize != product_usize(&old_grid)? {
            return Err("B2ND chunk count does not match shape");
        }

        let shrunk_shape: Vec<i64> = old_meta
            .shape
            .iter()
            .zip(&new_meta.shape)
            .map(|(&old_dim, &new_dim)| old_dim.min(new_dim))
            .collect();

        let old_count = product_usize(&old_grid)?;
        for linear_chunk in (0..old_count).rev() {
            let chunk_index = unravel_index(linear_chunk, &old_grid);
            if chunk_origin_in_resize_region(
                &chunk_index,
                &old_meta.chunkshape,
                resize,
                &old_meta.shape,
                &shrunk_shape,
            )? {
                self.schunk.delete_chunk(linear_chunk as i64)?;
            }
        }

        let typesize = self.schunk.cparams.typesize as usize;
        let zero_chunk_nbytes = extchunk_nitems(&new_meta)?
            .checked_mul(typesize)
            .ok_or("B2ND chunk too large")?;
        let new_count = product_usize(&new_grid)?;
        for linear_chunk in 0..new_count {
            let chunk_index = unravel_index(linear_chunk, &new_grid);
            if chunk_origin_in_resize_region(
                &chunk_index,
                &new_meta.chunkshape,
                resize,
                &new_meta.shape,
                &shrunk_shape,
            )? {
                self.schunk
                    .insert_special_zero_chunk(linear_chunk as i64, zero_chunk_nbytes)?;
            }
        }

        if self.schunk.nchunks() as usize != new_count {
            return Err("B2ND resized chunk count does not match shape");
        }
        self.update_meta_preserving_chunks(new_meta)
    }

    fn update_meta_preserving_chunks(&mut self, meta: B2ndMeta) -> Result<(), &'static str> {
        let encoded = meta.serialize()?;
        if self.schunk.metalayer(B2ND_METALAYER_NAME).is_some() {
            self.schunk
                .update_metalayer(B2ND_METALAYER_NAME, &encoded)?;
        } else {
            self.schunk.add_metalayer(B2ND_METALAYER_NAME, &encoded)?;
        }
        self.meta = meta;
        Ok(())
    }

    /// Insert a dense row-major buffer along one axis.
    pub fn insert(
        &mut self,
        axis: usize,
        start: i64,
        buffershape: &[i64],
        data: &[u8],
    ) -> Result<(), &'static str> {
        let ndim = self.meta.ndim();
        if axis >= ndim || buffershape.len() != ndim {
            return Err("B2ND insert rank does not match array rank");
        }
        if start < 0 || start > self.meta.shape[axis] || buffershape[axis] < 0 {
            return Err("Invalid B2ND insert bounds");
        }
        for (dim, &buffer_dim) in buffershape.iter().enumerate() {
            if dim != axis && buffer_dim != self.meta.shape[dim] {
                return Err("B2ND insert buffer shape does not match array shape");
            }
        }

        let mut new_shape = self.meta.shape.clone();
        new_shape[axis] = new_shape[axis]
            .checked_add(buffershape[axis])
            .ok_or("B2ND shape too large")?;
        let mut resize_start = vec![0i64; ndim];
        resize_start[axis] = start;
        self.resize_at(new_shape, Some(&resize_start))?;

        let mut slice_start = vec![0i64; ndim];
        let mut slice_stop = self.meta.shape.clone();
        slice_start[axis] = start;
        slice_stop[axis] = start + buffershape[axis];
        self.set_slice_cbuffer(&slice_start, &slice_stop, buffershape, data)
    }

    /// Append a dense row-major buffer to the end of one axis.
    pub fn append(
        &mut self,
        axis: usize,
        buffershape: &[i64],
        data: &[u8],
    ) -> Result<(), &'static str> {
        if axis >= self.meta.ndim() {
            return Err("B2ND append axis out of range");
        }
        self.insert(axis, self.meta.shape[axis], buffershape, data)
    }

    /// Delete `len` items along one axis starting at `start`.
    pub fn delete(&mut self, axis: usize, start: i64, len: i64) -> Result<(), &'static str> {
        let ndim = self.meta.ndim();
        let end = start.checked_add(len).ok_or("Invalid B2ND delete bounds")?;
        if axis >= ndim || start < 0 || len < 0 || end > self.meta.shape[axis] {
            return Err("Invalid B2ND delete bounds");
        }
        let mut new_shape = self.meta.shape.clone();
        new_shape[axis] = new_shape[axis]
            .checked_sub(len)
            .ok_or("Invalid B2ND delete bounds")?;
        let mut resize_start = vec![0i64; ndim];
        resize_start[axis] = start;
        self.resize_at(new_shape, Some(&resize_start))
    }

    fn update_slice_chunks_from_dense(
        &mut self,
        slice: &B2ndSlice,
        buffershape: &[i64],
        data: &[u8],
        typesize: usize,
    ) -> Result<(), &'static str> {
        let chunk_grid = chunk_grid(&self.meta)?;
        let chunk_count = product_usize(&chunk_grid)?;
        if self.schunk.nchunks() as usize != chunk_count {
            return Err("B2ND chunk count does not match metadata");
        }
        if chunk_count == 0 {
            return Ok(());
        }

        let ndim = self.meta.ndim();
        let layout = B2ndLayout::new(&self.meta, typesize)?;
        let chunk_nbytes = extchunk_nitems(&self.meta)?
            .checked_mul(typesize)
            .ok_or("B2ND chunk too large")?;
        let src_strides = byte_strides_i64(buffershape, typesize)?;

        let mut first_chunk = vec![0usize; ndim];
        let mut last_chunk = vec![0usize; ndim];
        for dim in 0..ndim {
            let chunk = self.meta.chunkshape[dim] as usize;
            first_chunk[dim] = slice.starts[dim] / chunk;
            last_chunk[dim] = (slice.starts[dim] + slice.extents[dim] - 1) / chunk;
        }

        let mut chunk_index = first_chunk.clone();
        self.update_slice_chunks_from_dense_inner(
            0,
            &first_chunk,
            &last_chunk,
            &mut chunk_index,
            slice,
            data,
            &src_strides,
            &layout,
            chunk_nbytes,
            &chunk_grid,
            typesize,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn update_slice_chunks_from_dense_inner(
        &mut self,
        dim: usize,
        first_chunk: &[usize],
        last_chunk: &[usize],
        chunk_index: &mut [usize],
        slice: &B2ndSlice,
        data: &[u8],
        src_strides: &[usize],
        layout: &B2ndLayout,
        chunk_nbytes: usize,
        chunk_grid: &[usize],
        typesize: usize,
    ) -> Result<(), &'static str> {
        if dim < chunk_index.len() {
            for value in first_chunk[dim]..=last_chunk[dim] {
                chunk_index[dim] = value;
                self.update_slice_chunks_from_dense_inner(
                    dim + 1,
                    first_chunk,
                    last_chunk,
                    chunk_index,
                    slice,
                    data,
                    src_strides,
                    layout,
                    chunk_nbytes,
                    chunk_grid,
                    typesize,
                )?;
            }
            return Ok(());
        }

        let ndim = self.meta.ndim();
        let mut intersection_start = vec![0usize; ndim];
        let mut intersection_extents = vec![0usize; ndim];
        let mut chunk_local_start = vec![0usize; ndim];
        let mut src_start = vec![0usize; ndim];
        let mut covers_full_logical_chunk = true;
        for axis in 0..ndim {
            let chunk_start = chunk_index[axis]
                .checked_mul(self.meta.chunkshape[axis] as usize)
                .ok_or("B2ND chunk index overflow")?;
            let chunk_stop = chunk_start
                .checked_add(self.meta.chunkshape[axis] as usize)
                .ok_or("B2ND chunk index overflow")?;
            let slice_start = slice.starts[axis];
            let slice_stop = slice.starts[axis]
                .checked_add(slice.extents[axis])
                .ok_or("Invalid B2ND slice bounds")?;
            let start = chunk_start.max(slice_start);
            let stop = chunk_stop.min(slice_stop);
            let logical_chunk_stop = chunk_stop.min(self.meta.shape[axis] as usize);
            if start != chunk_start || stop != logical_chunk_stop {
                covers_full_logical_chunk = false;
            }
            intersection_start[axis] = start;
            intersection_extents[axis] = stop - start;
            chunk_local_start[axis] = start - chunk_start;
            src_start[axis] = start - slice_start;
        }

        let linear_chunk = ravel_index(chunk_index, chunk_grid)?;
        let mut chunk = if covers_full_logical_chunk {
            vec![0u8; chunk_nbytes]
        } else {
            self.schunk.decompress_chunk(linear_chunk as i64)?
        };
        if chunk.len() != chunk_nbytes {
            return Err("B2ND chunk size does not match metadata");
        }
        copy_region(
            0,
            &intersection_extents,
            |idx| {
                let src = dense_offset(&src_start, idx, src_strides)?;
                let mut local_idx = vec![0usize; ndim];
                for axis in 0..ndim {
                    local_idx[axis] = chunk_local_start[axis] + idx[axis];
                }
                let dst = b2nd_chunk_offset(
                    &local_idx,
                    &layout.extchunkshape,
                    &self.meta.blockshape,
                    &layout.blocks_in_chunk,
                    layout.block_nitems,
                    layout.typesize,
                )?;
                Ok((src, dst))
            },
            data,
            &mut chunk,
            typesize,
        )?;
        self.schunk.update_chunk(linear_chunk as i64, &chunk)?;
        Ok(())
    }

    fn update_orthogonal_chunks_from_dense(
        &mut self,
        coords: &[Vec<usize>],
        extents: &[usize],
        buffershape: &[i64],
        data: &[u8],
        typesize: usize,
    ) -> Result<(), &'static str> {
        let chunk_grid = chunk_grid(&self.meta)?;
        let chunk_count = product_usize(&chunk_grid)?;
        if self.schunk.nchunks() as usize != chunk_count {
            return Err("B2ND chunk count does not match metadata");
        }
        if chunk_count == 0 {
            return Ok(());
        }

        let layout = B2ndLayout::new(&self.meta, typesize)?;
        let chunk_nbytes = extchunk_nitems(&self.meta)?
            .checked_mul(typesize)
            .ok_or("B2ND chunk too large")?;
        let src_strides = byte_strides_i64(buffershape, typesize)?;
        let src_zero = vec![0usize; coords.len()];
        let mut touched_chunks: BTreeMap<usize, Vec<u8>> = BTreeMap::new();
        let mut idx = vec![0usize; coords.len()];

        self.update_orthogonal_chunks_from_dense_inner(
            0,
            extents,
            &mut idx,
            coords,
            data,
            &src_strides,
            &src_zero,
            &layout,
            chunk_nbytes,
            &chunk_grid,
            &mut touched_chunks,
            typesize,
        )?;

        for (linear_chunk, chunk) in touched_chunks {
            self.schunk.update_chunk(linear_chunk as i64, &chunk)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn update_orthogonal_chunks_from_dense_inner(
        &self,
        dim: usize,
        extents: &[usize],
        idx: &mut [usize],
        coords: &[Vec<usize>],
        data: &[u8],
        src_strides: &[usize],
        src_zero: &[usize],
        layout: &B2ndLayout,
        chunk_nbytes: usize,
        chunk_grid: &[usize],
        touched_chunks: &mut BTreeMap<usize, Vec<u8>>,
        typesize: usize,
    ) -> Result<(), &'static str> {
        if dim < extents.len() {
            for value in 0..extents[dim] {
                idx[dim] = value;
                self.update_orthogonal_chunks_from_dense_inner(
                    dim + 1,
                    extents,
                    idx,
                    coords,
                    data,
                    src_strides,
                    src_zero,
                    layout,
                    chunk_nbytes,
                    chunk_grid,
                    touched_chunks,
                    typesize,
                )?;
            }
            return Ok(());
        }

        let ndim = self.meta.ndim();
        let mut chunk_index = vec![0usize; ndim];
        let mut local_idx = vec![0usize; ndim];
        for axis in 0..ndim {
            let coord = coords[axis][idx[axis]];
            let chunk = self.meta.chunkshape[axis] as usize;
            chunk_index[axis] = coord / chunk;
            local_idx[axis] = coord % chunk;
        }
        let linear_chunk = ravel_index(&chunk_index, chunk_grid)?;
        if let std::collections::btree_map::Entry::Vacant(entry) =
            touched_chunks.entry(linear_chunk)
        {
            let chunk = self.schunk.decompress_chunk(linear_chunk as i64)?;
            if chunk.len() != chunk_nbytes {
                return Err("B2ND chunk size does not match metadata");
            }
            entry.insert(chunk);
        }

        let src = dense_offset(src_zero, idx, src_strides)?;
        let dst = b2nd_chunk_offset(
            &local_idx,
            &layout.extchunkshape,
            &self.meta.blockshape,
            &layout.blocks_in_chunk,
            layout.block_nitems,
            layout.typesize,
        )?;
        let src_end = src.checked_add(typesize).ok_or("B2ND copy overflow")?;
        let dst_end = dst.checked_add(typesize).ok_or("B2ND copy overflow")?;
        let src_item = data.get(src..src_end).ok_or("B2ND source too small")?;
        let chunk = touched_chunks
            .get_mut(&linear_chunk)
            .ok_or("B2ND chunk index out of range")?;
        let dst_item = chunk
            .get_mut(dst..dst_end)
            .ok_or("B2ND destination too small")?;
        dst_item.copy_from_slice(src_item);
        Ok(())
    }
}

/// Validated `start..stop` slice expressed in three forms used by callers.
struct B2ndSlice {
    starts: Vec<usize>,
    extents: Vec<usize>,
    extents_as_i64: Vec<i64>,
}

/// Validated per-dimension resize mapping.
struct B2ndResize {
    starts: Vec<usize>,
}

fn validate_resize_at(
    meta: &B2ndMeta,
    new_shape: &[i64],
    start: Option<&[i64]>,
) -> Result<B2ndResize, &'static str> {
    let ndim = meta.ndim();
    if new_shape.len() != ndim {
        return Err("B2ND resize rank does not match array rank");
    }

    let mut starts = Vec::with_capacity(ndim);
    for dim in 0..ndim {
        let old_dim = meta.shape[dim];
        let new_dim = new_shape[dim];
        let start_dim = match start {
            Some(start) => {
                if start.len() != ndim {
                    return Err("B2ND resize start rank does not match array rank");
                }
                if start[dim] < 0 || start[dim] > old_dim {
                    return Err("Invalid B2ND resize start");
                }
                start[dim]
            }
            None => {
                if new_dim > old_dim {
                    old_dim
                } else {
                    new_dim
                }
            }
        };

        if start.is_some() {
            if new_dim < old_dim && start_dim > new_dim {
                return Err("Invalid B2ND resize start");
            }
            let delta = new_dim
                .checked_sub(old_dim)
                .ok_or("Invalid B2ND resize shape")?;
            let touches_end = if delta > 0 {
                start_dim == old_dim
            } else if delta < 0 {
                start_dim == new_dim
            } else {
                true
            };
            if !touches_end {
                let chunk = meta.chunkshape[dim] as i64;
                if start_dim % chunk != 0 || delta % chunk != 0 {
                    return Err("B2ND resize start and delta must be chunk aligned");
                }
            }
        }

        starts.push(start_dim as usize);
    }

    Ok(B2ndResize { starts })
}

fn chunk_origin_in_resize_region(
    chunk_index: &[usize],
    chunkshape: &[i32],
    resize: &B2ndResize,
    shape: &[i64],
    shrunk_shape: &[i64],
) -> Result<bool, &'static str> {
    for dim in 0..chunk_index.len() {
        let origin = (chunk_index[dim] as i64)
            .checked_mul(chunkshape[dim] as i64)
            .ok_or("B2ND chunk index overflow")?;
        let start = resize.starts[dim] as i64;
        let delta = shape[dim]
            .checked_sub(shrunk_shape[dim])
            .ok_or("Invalid B2ND resize shape")?;
        let stop = start
            .checked_add(delta)
            .ok_or("Invalid B2ND resize shape")?;
        if start <= origin && origin < stop {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Validate that `start..stop` is an in-bounds slice and return it
/// in the convenience forms used by the dense copy helpers.
fn validate_slice_bounds(
    meta: &B2ndMeta,
    start: &[i64],
    stop: &[i64],
) -> Result<B2ndSlice, &'static str> {
    let ndim = meta.ndim();
    if start.len() != ndim || stop.len() != ndim {
        return Err("B2ND slice rank does not match array rank");
    }

    let mut starts = Vec::with_capacity(ndim);
    let mut extents = Vec::with_capacity(ndim);
    let mut extents_as_i64 = Vec::with_capacity(ndim);
    for dim in 0..ndim {
        if start[dim] < 0 || stop[dim] > meta.shape[dim] || start[dim] > stop[dim] {
            return Err("Invalid B2ND slice bounds");
        }
        let extent = stop[dim]
            .checked_sub(start[dim])
            .ok_or("Invalid B2ND slice bounds")?;
        starts.push(start[dim] as usize);
        extents.push(extent as usize);
        extents_as_i64.push(extent);
    }
    product_usize(&extents)?;
    Ok(B2ndSlice {
        starts,
        extents,
        extents_as_i64,
    })
}

fn validate_orthogonal_selection(
    meta: &B2ndMeta,
    selection: &[Vec<i64>],
) -> Result<(Vec<Vec<usize>>, Vec<usize>, Vec<i64>), &'static str> {
    let ndim = meta.ndim();
    if selection.len() != ndim {
        return Err("B2ND selection rank does not match array rank");
    }
    let mut coords = Vec::with_capacity(ndim);
    let mut extents = Vec::with_capacity(ndim);
    let mut shape = Vec::with_capacity(ndim);
    for (dim, dim_selection) in selection.iter().enumerate() {
        let mut dim_coords = Vec::with_capacity(dim_selection.len());
        for &coord in dim_selection {
            if coord < 0 || coord >= meta.shape[dim] {
                return Err("Invalid B2ND selection coordinate");
            }
            dim_coords.push(coord as usize);
        }
        extents.push(dim_coords.len());
        shape.push(dim_coords.len() as i64);
        coords.push(dim_coords);
    }
    product_usize(&extents)?;
    Ok((coords, extents, shape))
}

fn validate_orthogonal_buffershape(
    buffershape: &[i64],
    extents: &[usize],
) -> Result<(), &'static str> {
    if buffershape.len() != extents.len() {
        return Err("B2ND buffer rank does not match selection rank");
    }
    for (&buffer_dim, &extent) in buffershape.iter().zip(extents) {
        if buffer_dim < 0 || buffer_dim < extent as i64 {
            return Err("B2ND buffer shape is smaller than selection shape");
        }
    }
    if product_i64(buffershape)? > product_usize(extents)? {
        return Err("B2ND buffer shape is larger than selection shape");
    }
    Ok(())
}

fn copy_orthogonal_selection(
    src: &[u8],
    src_shape: &[i64],
    coords: &[Vec<usize>],
    dst: &mut [u8],
    dst_shape: &[i64],
    typesize: usize,
) -> Result<(), &'static str> {
    let extents: Vec<usize> = coords.iter().map(Vec::len).collect();
    let src_strides = byte_strides_i64(src_shape, typesize)?;
    let dst_strides = byte_strides_i64(dst_shape, typesize)?;
    let dst_zero = vec![0usize; coords.len()];
    copy_region(
        0,
        &extents,
        |idx| {
            let src_pos = coords.iter().zip(idx).zip(&src_strides).try_fold(
                0usize,
                |acc, ((dim_coords, &idx), &stride)| {
                    dim_coords[idx]
                        .checked_mul(stride)
                        .and_then(|offset| acc.checked_add(offset))
                        .ok_or("B2ND dense offset overflow")
                },
            )?;
            Ok((src_pos, dense_offset(&dst_zero, idx, &dst_strides)?))
        },
        src,
        dst,
        typesize,
    )
}

/// Consume one byte from `data` at `pos` and check it matches `expected`.
fn expect_byte(data: &[u8], pos: &mut usize, expected: u8) -> Result<(), &'static str> {
    if data.get(*pos).copied() != Some(expected) {
        return Err("Invalid B2ND metadata");
    }
    *pos += 1;
    Ok(())
}

/// Write a msgpack array header for ranks used by b2nd metadata.
fn write_array_header(out: &mut Vec<u8>, len: usize) -> Result<(), &'static str> {
    if len <= 15 {
        out.push(0x90 + len as u8);
    } else if len == 16 {
        // C-Blosc2 historically writes 0x90 + ndim even for ndim == 16.
        out.push(0xa0);
    } else if len <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        return Err("B2ND array too large");
    }
    Ok(())
}

/// Consume a msgpack array header and check it has the expected length.
fn expect_array_header(
    data: &[u8],
    pos: &mut usize,
    expected_len: usize,
) -> Result<(), &'static str> {
    let len = read_array_header(data, pos)?;
    if len != expected_len {
        return Err("Invalid B2ND metadata");
    }
    Ok(())
}

/// Consume a msgpack array header and return its length.
fn read_array_header(data: &[u8], pos: &mut usize) -> Result<usize, &'static str> {
    let byte = *data.get(*pos).ok_or("Truncated B2ND metadata")?;
    *pos += 1;
    let len = if (0x90..=0x9f).contains(&byte) {
        (byte - 0x90) as usize
    } else if byte == 0xa0 {
        16
    } else if byte == 0xdc {
        let end = pos.checked_add(2).ok_or("Invalid B2ND metadata")?;
        let bytes = data.get(*pos..end).ok_or("Truncated B2ND metadata")?;
        *pos = end;
        u16::from_be_bytes(bytes.try_into().unwrap()) as usize
    } else {
        return Err("Invalid B2ND metadata");
    };
    Ok(len)
}

/// Read a msgpack positive fixint (0x00-0x7f).
fn read_fixint(data: &[u8], pos: &mut usize) -> Result<u8, &'static str> {
    let byte = *data.get(*pos).ok_or("Truncated B2ND metadata")?;
    if byte > 0x7f {
        return Err("Invalid B2ND fixint");
    }
    *pos += 1;
    Ok(byte)
}

/// Read a big-endian `i64` and advance `pos` past it.
fn read_i64(data: &[u8], pos: &mut usize) -> Result<i64, &'static str> {
    let end = pos.checked_add(8).ok_or("Invalid B2ND metadata")?;
    let bytes = data.get(*pos..end).ok_or("Truncated B2ND metadata")?;
    *pos = end;
    Ok(i64::from_be_bytes(bytes.try_into().unwrap()))
}

/// Read a big-endian `i32` and advance `pos` past it.
fn read_i32(data: &[u8], pos: &mut usize) -> Result<i32, &'static str> {
    let end = pos.checked_add(4).ok_or("Invalid B2ND metadata")?;
    let bytes = data.get(*pos..end).ok_or("Truncated B2ND metadata")?;
    *pos = end;
    Ok(i32::from_be_bytes(bytes.try_into().unwrap()))
}

/// Product of non-negative `i64` values, or an error on overflow or negative
/// entries.
fn product_i64(values: &[i64]) -> Result<usize, &'static str> {
    values.iter().try_fold(1usize, |acc, &value| {
        if value < 0 {
            return Err("Invalid B2ND shape");
        }
        acc.checked_mul(value as usize)
            .ok_or("B2ND shape too large")
    })
}

/// Product of non-negative `i32` values, or an error on overflow or negative
/// entries.
fn product_i32(values: &[i32]) -> Result<usize, &'static str> {
    values.iter().try_fold(1usize, |acc, &value| {
        if value < 0 {
            return Err("Invalid B2ND shape");
        }
        acc.checked_mul(value as usize)
            .ok_or("B2ND shape too large")
    })
}

/// Product of `usize` values guarded against overflow.
fn product_usize(values: &[usize]) -> Result<usize, &'static str> {
    values.iter().try_fold(1usize, |acc, &value| {
        acc.checked_mul(value).ok_or("B2ND shape too large")
    })
}

/// Number of chunks needed to tile the shape along each dimension
/// (`ceil(shape[d] / chunkshape[d])`).
fn chunk_grid(meta: &B2ndMeta) -> Result<Vec<usize>, &'static str> {
    meta.shape
        .iter()
        .zip(&meta.chunkshape)
        .map(|(&shape, &chunk)| {
            if shape < 0 || chunk < 0 || (shape > 0 && chunk == 0) {
                return Err("Invalid B2ND shape");
            }
            Ok(if shape == 0 {
                0
            } else {
                (shape as usize).div_ceil(chunk as usize)
            })
        })
        .collect()
}

/// Padded chunk shape: each chunk dimension rounded up to a multiple of the
/// matching block dimension so that a chunk holds a whole number of blocks.
fn extchunkshape(meta: &B2ndMeta) -> Result<Vec<i32>, &'static str> {
    meta.chunkshape
        .iter()
        .zip(&meta.blockshape)
        .map(|(&chunk, &block)| {
            if chunk == 0 && block == 0 {
                return Ok(0);
            }
            if chunk <= 0 || block <= 0 {
                return Err("Invalid B2ND chunk or block shape");
            }
            Ok(if chunk % block == 0 {
                chunk
            } else {
                chunk + block - chunk % block
            })
        })
        .collect()
}

/// Number of items in a padded chunk.
fn extchunk_nitems(meta: &B2ndMeta) -> Result<usize, &'static str> {
    product_i32(&extchunkshape(meta)?)
}

/// Number of blocks per dimension inside one padded chunk.
fn blocks_in_chunk(extchunkshape: &[i32], blockshape: &[i32]) -> Result<Vec<usize>, &'static str> {
    extchunkshape
        .iter()
        .zip(blockshape)
        .map(|(&extchunk, &block)| {
            if extchunk == 0 && block == 0 {
                return Ok(0);
            }
            if extchunk <= 0 || block <= 0 || extchunk % block != 0 {
                return Err("Invalid B2ND block grid");
            }
            Ok((extchunk / block) as usize)
        })
        .collect()
}

/// Row-major byte strides for the given shape and item size.
fn byte_strides_i64(shape: &[i64], typesize: usize) -> Result<Vec<usize>, &'static str> {
    let mut strides = vec![0; shape.len()];
    let mut stride = typesize;
    for idx in (0..shape.len()).rev() {
        strides[idx] = stride;
        stride = stride
            .checked_mul(shape[idx] as usize)
            .ok_or("B2ND shape too large")?;
    }
    Ok(strides)
}

/// Byte offset of an item at multi-index `starts + idx` in a row-major buffer
/// with the given byte strides.
fn dense_offset(starts: &[usize], idx: &[usize], strides: &[usize]) -> Result<usize, &'static str> {
    starts
        .iter()
        .zip(idx)
        .zip(strides)
        .try_fold(0usize, |acc, ((&start, &idx), &stride)| {
            start
                .checked_add(idx)
                .and_then(|coord| coord.checked_mul(stride))
                .and_then(|offset| acc.checked_add(offset))
                .ok_or("B2ND dense offset overflow")
        })
}

fn dense_region_required_len(
    shape: &[i64],
    extents: &[usize],
    typesize: usize,
) -> Result<usize, &'static str> {
    if shape.len() != extents.len() {
        return Err("B2ND dense copy rank mismatch");
    }
    if extents.is_empty() {
        return Ok(typesize);
    }
    let strides = byte_strides_i64(shape, typesize)?;
    let last_idx: Vec<usize> = extents
        .iter()
        .map(|&extent| extent.checked_sub(1).ok_or("Invalid B2ND slice bounds"))
        .collect::<Result<_, _>>()?;
    dense_offset(&vec![0; extents.len()], &last_idx, &strides)?
        .checked_add(typesize)
        .ok_or("B2ND dense offset overflow")
}

/// Description of a rectangular region inside a dense row-major buffer.
struct DenseRegion<'a> {
    shape: &'a [i64],
    start: &'a [usize],
}

/// Copy an `extents`-shaped block of items from one dense row-major buffer
/// into another, given the source and destination regions.
fn copy_dense_region(
    src: &[u8],
    src_region: DenseRegion<'_>,
    dst: &mut [u8],
    dst_region: DenseRegion<'_>,
    extents: &[usize],
    typesize: usize,
) -> Result<(), &'static str> {
    if src_region.shape.len() != extents.len()
        || dst_region.shape.len() != extents.len()
        || src_region.start.len() != extents.len()
        || dst_region.start.len() != extents.len()
    {
        return Err("B2ND dense copy rank mismatch");
    }
    let src_strides = byte_strides_i64(src_region.shape, typesize)?;
    let dst_strides = byte_strides_i64(dst_region.shape, typesize)?;
    copy_region(
        0,
        extents,
        |idx| {
            Ok((
                dense_offset(src_region.start, idx, &src_strides)?,
                dense_offset(dst_region.start, idx, &dst_strides)?,
            ))
        },
        src,
        dst,
        typesize,
    )
}

/// Precomputed strides and block geometry shared by the chunk/dense copy
/// helpers.
struct B2ndLayout {
    data_strides: Vec<usize>,
    extchunkshape: Vec<i32>,
    blocks_in_chunk: Vec<usize>,
    block_nitems: usize,
    typesize: usize,
}

impl B2ndLayout {
    /// Build the layout cache for a given metadata and typesize.
    fn new(meta: &B2ndMeta, typesize: usize) -> Result<Self, &'static str> {
        let extchunkshape = extchunkshape(meta)?;
        let blocks_in_chunk = blocks_in_chunk(&extchunkshape, &meta.blockshape)?;
        Ok(Self {
            data_strides: byte_strides_i64(&meta.shape, typesize)?,
            extchunkshape,
            blocks_in_chunk,
            block_nitems: product_i32(&meta.blockshape)?,
            typesize,
        })
    }
}

/// Convert a linear C-order index into a multi-dimensional index.
fn unravel_index(mut index: usize, shape: &[usize]) -> Vec<usize> {
    let mut out = vec![0; shape.len()];
    for dim in (0..shape.len()).rev() {
        out[dim] = index % shape[dim];
        index /= shape[dim];
    }
    out
}

/// Convert a multi-dimensional index into a row-major linear index.
fn ravel_index(index: &[usize], shape: &[usize]) -> Result<usize, &'static str> {
    if index.len() != shape.len() {
        return Err("B2ND index rank mismatch");
    }
    index
        .iter()
        .zip(shape)
        .try_fold(0usize, |acc, (&coord, &extent)| {
            if coord >= extent {
                return Err("B2ND chunk index out of range");
            }
            acc.checked_mul(extent)
                .and_then(|value| value.checked_add(coord))
                .ok_or("B2ND chunk index overflow")
        })
}

/// Copy the items belonging to chunk `chunk_index` out of a dense row-major
/// source buffer into the chunk's block-interleaved layout.
fn copy_dense_to_chunk(
    meta: &B2ndMeta,
    data: &[u8],
    layout: &B2ndLayout,
    chunk_index: &[usize],
    chunk: &mut [u8],
) -> Result<(), &'static str> {
    let ndim = meta.ndim();
    let mut starts = vec![0usize; ndim];
    let mut extents = vec![0usize; ndim];
    for dim in 0..ndim {
        starts[dim] = chunk_index[dim]
            .checked_mul(meta.chunkshape[dim] as usize)
            .ok_or("B2ND chunk index overflow")?;
        let stop = (starts[dim] + meta.chunkshape[dim] as usize).min(meta.shape[dim] as usize);
        extents[dim] = stop - starts[dim];
    }
    copy_region(
        0,
        &extents,
        |idx| {
            let mut src = 0usize;
            let dst = b2nd_chunk_offset(
                idx,
                &layout.extchunkshape,
                &meta.blockshape,
                &layout.blocks_in_chunk,
                layout.block_nitems,
                layout.typesize,
            )?;
            for dim in 0..ndim {
                src += (starts[dim] + idx[dim]) * layout.data_strides[dim];
            }
            Ok((src, dst))
        },
        data,
        chunk,
        layout.typesize,
    )
}

/// Copy items from a single block-interleaved chunk back into the
/// corresponding region of a dense row-major destination buffer.
fn copy_chunk_to_dense(
    meta: &B2ndMeta,
    chunk: &[u8],
    layout: &B2ndLayout,
    chunk_index: &[usize],
    data: &mut [u8],
) -> Result<(), &'static str> {
    let ndim = meta.ndim();
    let mut starts = vec![0usize; ndim];
    let mut extents = vec![0usize; ndim];
    for dim in 0..ndim {
        starts[dim] = chunk_index[dim]
            .checked_mul(meta.chunkshape[dim] as usize)
            .ok_or("B2ND chunk index overflow")?;
        let stop = (starts[dim] + meta.chunkshape[dim] as usize).min(meta.shape[dim] as usize);
        extents[dim] = stop - starts[dim];
    }
    copy_region(
        0,
        &extents,
        |idx| {
            let src = b2nd_chunk_offset(
                idx,
                &layout.extchunkshape,
                &meta.blockshape,
                &layout.blocks_in_chunk,
                layout.block_nitems,
                layout.typesize,
            )?;
            let mut dst = 0usize;
            for dim in 0..ndim {
                dst += (starts[dim] + idx[dim]) * layout.data_strides[dim];
            }
            Ok((src, dst))
        },
        chunk,
        data,
        layout.typesize,
    )
}

/// Byte offset of item `idx` inside a padded chunk laid out as a grid of
/// row-major blocks (the C-Blosc2 b2nd in-chunk layout).
fn b2nd_chunk_offset(
    idx: &[usize],
    extchunkshape: &[i32],
    blockshape: &[i32],
    blocks_in_chunk: &[usize],
    block_nitems: usize,
    typesize: usize,
) -> Result<usize, &'static str> {
    let ndim = idx.len();
    let mut block_index = 0usize;
    let mut inblock_index = 0usize;
    for dim in 0..ndim {
        let block = blockshape[dim] as usize;
        let extchunk = extchunkshape[dim] as usize;
        if idx[dim] >= extchunk {
            return Err("B2ND chunk index out of range");
        }
        block_index = block_index
            .checked_mul(blocks_in_chunk[dim])
            .and_then(|value| value.checked_add(idx[dim] / block))
            .ok_or("B2ND chunk offset overflow")?;
        inblock_index = inblock_index
            .checked_mul(block)
            .and_then(|value| value.checked_add(idx[dim] % block))
            .ok_or("B2ND chunk offset overflow")?;
    }
    block_index
        .checked_mul(block_nitems)
        .and_then(|value| value.checked_add(inblock_index))
        .and_then(|value| value.checked_mul(typesize))
        .ok_or("B2ND chunk offset overflow")
}

/// Iterate over every multi-index in an `extents`-shaped region and copy one
/// item per index from `src` to `dst`, using `offsets` to map the index to the
/// source and destination byte positions.
fn copy_region(
    dim: usize,
    extents: &[usize],
    mut offsets: impl FnMut(&[usize]) -> Result<(usize, usize), &'static str>,
    src: &[u8],
    dst: &mut [u8],
    typesize: usize,
) -> Result<(), &'static str> {
    let mut idx = vec![0usize; extents.len()];
    copy_region_inner(dim, extents, &mut idx, &mut offsets, src, dst, typesize)
}

/// Recursive worker for [`copy_region`].
fn copy_region_inner(
    dim: usize,
    extents: &[usize],
    idx: &mut [usize],
    offsets: &mut impl FnMut(&[usize]) -> Result<(usize, usize), &'static str>,
    src: &[u8],
    dst: &mut [u8],
    typesize: usize,
) -> Result<(), &'static str> {
    if dim == extents.len() {
        let (src_pos, dst_pos) = offsets(idx)?;
        let src_end = src_pos.checked_add(typesize).ok_or("B2ND copy overflow")?;
        let dst_end = dst_pos.checked_add(typesize).ok_or("B2ND copy overflow")?;
        let src_item = src.get(src_pos..src_end).ok_or("B2ND source too small")?;
        let dst_item = dst
            .get_mut(dst_pos..dst_end)
            .ok_or("B2ND destination too small")?;
        dst_item.copy_from_slice(src_item);
        return Ok(());
    }
    for value in 0..extents[dim] {
        idx[dim] = value;
        copy_region_inner(dim + 1, extents, idx, offsets, src, dst, typesize)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::{
        BLOSC2_SPECIAL_ZERO, BLOSC2_VERSION_FORMAT_STABLE, BLOSC_BLOSCLZ,
        BLOSC_BLOSCLZ_VERSION_FORMAT, BLOSC_DOBITSHUFFLE, BLOSC_DOSHUFFLE,
        BLOSC_EXTENDED_HEADER_LENGTH, BLOSC_LZ4, BLOSC_NEVER_SPLIT, BLOSC_NOFILTER, BLOSC_SHUFFLE,
    };
    use crate::header::ChunkHeader;
    use crate::schunk::Schunk;

    #[test]
    fn test_b2nd_meta_matches_c_layout() {
        let meta = B2ndMeta::new(
            vec![10, 20],
            vec![4, 5],
            vec![2, 5],
            "<i4",
            DTYPE_NUMPY_FORMAT,
        )
        .unwrap();
        let encoded = meta.serialize().unwrap();
        assert_eq!(encoded[0], 0x97);
        assert_eq!(encoded[1], B2ND_METALAYER_VERSION);
        assert_eq!(encoded[2], 2);

        let decoded = B2ndMeta::deserialize(&encoded).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn test_b2nd_meta_allows_scalar_empty_dtype_and_16d() {
        let scalar =
            B2ndMeta::new(Vec::new(), Vec::new(), Vec::new(), "", DTYPE_NUMPY_FORMAT).unwrap();
        let encoded = scalar.serialize().unwrap();
        assert_eq!(encoded[3], 0x90);
        assert_eq!(B2ndMeta::deserialize(&encoded).unwrap(), scalar);

        let meta16 = B2ndMeta::new(vec![1; 16], vec![1; 16], vec![1; 16], "", 0).unwrap();
        let encoded = meta16.serialize().unwrap();
        assert_eq!(encoded[3], 0xa0);
        assert_eq!(B2ndMeta::deserialize(&encoded).unwrap(), meta16);
    }

    #[test]
    fn test_b2nd_array_frame_roundtrip() {
        let meta = B2ndMeta::new(vec![5, 7], vec![3, 4], vec![3, 2], "<u2", 0).unwrap();
        let mut data: Vec<u8> = (0..35u16).flat_map(u16::to_le_bytes).collect();
        let expected = data.clone();
        data.extend_from_slice(b"trailing bytes ignored");
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let array =
            B2ndArray::from_cbuffer(meta.clone(), &data, cparams, DParams::default()).unwrap();
        assert_eq!(
            array.schunk.metalayer(B2ND_METALAYER_NAME).unwrap(),
            meta.serialize().unwrap()
        );
        assert_eq!(array.to_cbuffer().unwrap(), expected);

        let frame = array.to_frame();
        let restored = B2ndArray::from_frame(&frame).unwrap();
        assert_eq!(restored.meta, meta);
        assert_eq!(restored.to_cbuffer().unwrap(), expected);
    }

    #[test]
    fn test_b2nd_scalar_empty_shape_and_caterva_fallback() {
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };

        let scalar_meta = B2ndMeta::new(Vec::new(), Vec::new(), Vec::new(), "<u2", 0).unwrap();
        let scalar =
            B2ndArray::from_cbuffer(scalar_meta.clone(), &[7, 0, 9], cparams, DParams::default())
                .unwrap();
        assert_eq!(scalar.to_cbuffer().unwrap(), vec![7, 0]);

        let mut legacy = scalar.schunk.clone();
        let content = legacy.remove_metalayer(B2ND_METALAYER_NAME).unwrap();
        legacy
            .add_metalayer(CATERVA_METALAYER_NAME, &content)
            .unwrap();
        let restored = B2ndArray::from_schunk(legacy).unwrap();
        assert_eq!(restored.meta, scalar_meta);

        let mut legacy_content = content.clone();
        legacy_content[0] = 0x90 + 5;
        legacy_content.truncate(6);
        assert!(B2ndMeta::deserialize(&legacy_content).is_err());

        let mut legacy_no_dtype = scalar.schunk.clone();
        legacy_no_dtype
            .remove_metalayer(B2ND_METALAYER_NAME)
            .unwrap();
        legacy_no_dtype
            .add_metalayer(CATERVA_METALAYER_NAME, &legacy_content)
            .unwrap();
        let restored_no_dtype = B2ndArray::from_schunk(legacy_no_dtype).unwrap();
        assert_eq!(
            restored_no_dtype.meta,
            B2ndMeta::new(Vec::new(), Vec::new(), Vec::new(), "|u1", 0).unwrap()
        );

        let mut legacy_b2nd_no_dtype = scalar.schunk.clone();
        legacy_b2nd_no_dtype
            .update_metalayer(B2ND_METALAYER_NAME, &legacy_content)
            .unwrap();
        let restored_b2nd_no_dtype = B2ndArray::from_schunk(legacy_b2nd_no_dtype).unwrap();
        assert_eq!(
            restored_b2nd_no_dtype.meta,
            B2ndMeta::new(Vec::new(), Vec::new(), Vec::new(), "|u1", 0).unwrap()
        );

        let empty_meta = B2ndMeta::new(vec![0, 3], vec![2, 2], vec![1, 1], "", 0).unwrap();
        let empty = B2ndArray::from_cbuffer(
            empty_meta.clone(),
            &[],
            CParams {
                typesize: 1,
                ..CParams::default()
            },
            DParams::default(),
        )
        .unwrap();
        assert_eq!(empty.schunk.nchunks(), 0);
        assert_eq!(empty.schunk.chunksize, 4);
        assert_eq!(empty.to_cbuffer().unwrap(), Vec::<u8>::new());
        assert_eq!(
            B2ndArray::from_frame(&empty.to_frame()).unwrap().meta,
            empty_meta
        );
        assert_eq!(Schunk::from_frame(&empty.to_frame()).unwrap().chunksize, 0);
    }

    #[test]
    fn test_b2nd_empty_dimension_allows_zero_chunk_and_block_shape() {
        let meta = B2ndMeta::new(vec![20, 0], vec![7, 0], vec![3, 0], "<u2", 0).unwrap();
        assert_eq!(meta.nitems().unwrap(), 0);
        assert_eq!(meta.chunk_nitems().unwrap(), 0);

        assert!(B2ndMeta::new(vec![20, 1], vec![7, 0], vec![3, 0], "<u2", 0).is_err());
        assert!(B2ndMeta::new(vec![20, 0], vec![7, 0], vec![3, 1], "<u2", 0).is_err());

        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let array =
            B2ndArray::from_cbuffer(meta.clone(), &[], cparams, DParams::default()).unwrap();
        assert_eq!(array.schunk.nchunks(), 0);
        assert_eq!(array.schunk.chunksize, 0);
        assert_eq!(array.to_cbuffer().unwrap(), Vec::<u8>::new());

        let restored = B2ndArray::from_frame(&array.to_frame()).unwrap();
        assert_eq!(restored.meta, meta);
        assert_eq!(restored.to_cbuffer().unwrap(), Vec::<u8>::new());
    }

    fn u16_bytes(values: &[u16]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }

    fn replace_raw_chunk(array: &mut B2ndArray, nchunk: usize, data: &[u8]) -> Vec<u8> {
        let alt_cparams = CParams {
            compcode: BLOSC_BLOSCLZ,
            clevel: 0,
            typesize: array.schunk.cparams.typesize,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_NOFILTER],
            ..Default::default()
        };
        let replacement = crate::compress::compress(data, &alt_cparams).unwrap();
        let old_len = array.schunk.chunks[nchunk].len() as i64;
        array.schunk.chunks[nchunk] = replacement.clone();
        array.schunk.cbytes += replacement.len() as i64 - old_len;
        replacement
    }

    #[test]
    fn test_b2nd_slice_set_and_resize_helpers() {
        let meta = B2ndMeta::new(vec![5, 7], vec![3, 4], vec![3, 2], "<u2", 0).unwrap();
        let values: Vec<u16> = (0..35u16).collect();
        let data = u16_bytes(&values);
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_cbuffer(meta.clone(), &data, cparams, DParams::default()).unwrap();
        array
            .schunk
            .add_metalayer(CATERVA_METALAYER_NAME, &meta.serialize().unwrap())
            .unwrap();
        array.schunk.add_metalayer("keep", b"fixed").unwrap();
        array.schunk.add_vlmetalayer("vkeep", b"variable").unwrap();
        assert_eq!(array.shape(), &[5, 7]);
        assert_eq!(array.chunkshape(), &[3, 4]);
        assert_eq!(array.blockshape(), &[3, 2]);

        let slice = array.get_slice(&[1, 2], &[4, 6]).unwrap();
        let mut expected_slice = Vec::new();
        for row in 1..4 {
            for col in 2..6 {
                expected_slice.push(values[row * 7 + col]);
            }
        }
        assert_eq!(slice, u16_bytes(&expected_slice));

        let replacement: Vec<u16> = (1000..1012).collect();
        array
            .set_slice(&[1, 2], &[4, 6], &u16_bytes(&replacement))
            .unwrap();
        assert!(array.schunk.metalayer(CATERVA_METALAYER_NAME).is_some());
        assert_eq!(array.schunk.metalayer("keep"), Some(&b"fixed"[..]));
        assert_eq!(array.schunk.vlmetalayer("vkeep"), Some(&b"variable"[..]));
        let mut expected = values.clone();
        for (idx, value) in replacement.iter().enumerate() {
            let row = 1 + idx / 4;
            let col = 2 + idx % 4;
            expected[row * 7 + col] = *value;
        }
        assert_eq!(array.to_cbuffer().unwrap(), u16_bytes(&expected));

        array.resize(vec![6, 4]).unwrap();
        assert!(array.schunk.metalayer(CATERVA_METALAYER_NAME).is_some());
        assert_eq!(array.schunk.metalayer("keep"), Some(&b"fixed"[..]));
        assert_eq!(array.schunk.vlmetalayer("vkeep"), Some(&b"variable"[..]));
        assert_eq!(array.shape(), &[6, 4]);
        let mut resized = vec![0u16; 6 * 4];
        for row in 0..5 {
            for col in 0..4 {
                resized[row * 4 + col] = expected[row * 7 + col];
            }
        }
        assert_eq!(array.to_cbuffer().unwrap(), u16_bytes(&resized));

        array.resize(vec![2, 3]).unwrap();
        let mut shrunk = Vec::new();
        for row in 0..2 {
            for col in 0..3 {
                shrunk.push(resized[row * 4 + col]);
            }
        }
        assert_eq!(array.to_cbuffer().unwrap(), u16_bytes(&shrunk));
        let before_empty_insert = array.to_cbuffer().unwrap();
        array.insert(0, 1, &[0, 3], &[]).unwrap();
        assert_eq!(array.shape(), &[2, 3]);
        assert_eq!(array.to_cbuffer().unwrap(), before_empty_insert);
        assert!(array.get_slice(&[0, 0], &[0, 1]).unwrap().is_empty());
        let padded = array.get_slice_cbuffer(&[0, 1], &[2, 3], &[2, 3]).unwrap();
        assert_eq!(
            padded,
            u16_bytes(&[shrunk[1], shrunk[2], 0, shrunk[4], shrunk[5], 0])
        );
        let before_empty_set = array.to_cbuffer().unwrap();
        array.set_slice(&[0, 0], &[0, 1], &[]).unwrap();
        assert_eq!(array.to_cbuffer().unwrap(), before_empty_set);
        assert!(array.set_slice(&[0, 0], &[1, 1], &[1]).is_err());
        assert!(array.delete(0, i64::MAX, 1).is_err());
    }

    #[test]
    fn test_b2nd_resize_exposes_retained_tail_padding() {
        let meta = B2ndMeta::new(vec![5], vec![3], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.chunksize = 3;
        schunk
            .add_metalayer(B2ND_METALAYER_NAME, &meta.serialize().unwrap())
            .unwrap();
        schunk.append_buffer(&[1, 1, 1]).unwrap();
        schunk.append_buffer(&[1, 1, 1]).unwrap();
        let mut array = B2ndArray::from_schunk(schunk).unwrap();

        assert_eq!(array.to_cbuffer().unwrap(), vec![1, 1, 1, 1, 1]);
        array.resize(vec![10]).unwrap();
        assert_eq!(
            array.to_cbuffer().unwrap(),
            vec![1, 1, 1, 1, 1, 1, 0, 0, 0, 0]
        );
    }

    #[test]
    fn test_b2nd_resize_same_chunk_grid_preserves_raw_chunks() {
        let meta = B2ndMeta::new(vec![5], vec![3], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_cbuffer(meta, &[1, 2, 3, 4, 5], cparams, DParams::default()).unwrap();
        let saved = replace_raw_chunk(&mut array, 1, &[4, 5, 9]);

        array.resize(vec![6]).unwrap();
        assert_eq!(array.schunk.compressed_chunk(1).unwrap(), saved.as_slice());
        assert_eq!(array.to_cbuffer().unwrap(), vec![1, 2, 3, 4, 5, 9]);
    }

    #[test]
    fn test_b2nd_resize_does_not_expose_block_only_padding() {
        let meta = B2ndMeta::new(vec![5], vec![5], vec![3], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.chunksize = 6;
        schunk
            .add_metalayer(B2ND_METALAYER_NAME, &meta.serialize().unwrap())
            .unwrap();
        schunk.append_buffer(&[1, 1, 1, 1, 1, 1]).unwrap();
        let mut array = B2ndArray::from_schunk(schunk).unwrap();

        array.resize(vec![6]).unwrap();
        assert_eq!(array.to_cbuffer().unwrap(), vec![1, 1, 1, 1, 1, 0]);
    }

    #[test]
    fn test_b2nd_set_slice_preserves_untouched_chunk_padding() {
        let meta = B2ndMeta::new(vec![5], vec![3], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.chunksize = 3;
        schunk
            .add_metalayer(B2ND_METALAYER_NAME, &meta.serialize().unwrap())
            .unwrap();
        schunk.append_buffer(&[1, 2, 3]).unwrap();
        schunk.append_buffer(&[4, 5, 9]).unwrap();
        let mut array = B2ndArray::from_schunk(schunk).unwrap();

        array.set_slice(&[0], &[1], &[7]).unwrap();
        array.resize(vec![6]).unwrap();
        assert_eq!(array.to_cbuffer().unwrap(), vec![7, 2, 3, 4, 5, 9]);
    }

    #[test]
    fn test_b2nd_set_slice_full_tail_chunk_clears_old_padding() {
        let meta = B2ndMeta::new(vec![5], vec![3], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut schunk = Schunk::new(cparams, DParams::default());
        schunk.chunksize = 3;
        schunk
            .add_metalayer(B2ND_METALAYER_NAME, &meta.serialize().unwrap())
            .unwrap();
        schunk.append_buffer(&[1, 2, 3]).unwrap();
        schunk.append_buffer(&[4, 5, 9]).unwrap();
        let mut array = B2ndArray::from_schunk(schunk).unwrap();

        array.set_slice(&[3], &[5], &[7, 8]).unwrap();
        array.resize(vec![6]).unwrap();
        assert_eq!(array.to_cbuffer().unwrap(), vec![1, 2, 3, 7, 8, 0]);
    }

    #[test]
    fn test_b2nd_set_slice_preserves_raw_untouched_chunks() {
        let meta = B2ndMeta::new(vec![6], vec![3], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_cbuffer(meta, &[1, 2, 3, 4, 5, 6], cparams, DParams::default())
                .unwrap();
        let saved = replace_raw_chunk(&mut array, 1, &[4, 5, 6]);

        array.set_slice(&[0], &[1], &[9]).unwrap();
        assert_eq!(array.schunk.compressed_chunk(1).unwrap(), saved.as_slice());
        assert_eq!(array.to_cbuffer().unwrap(), vec![9, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_b2nd_orthogonal_selection_cbuffer_and_bounds() {
        let meta = B2ndMeta::new(vec![3, 4], vec![2, 2], vec![1, 2], "<u2", 0).unwrap();
        let values: Vec<u16> = (0..12u16).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_cbuffer(meta, &u16_bytes(&values), cparams, DParams::default())
                .unwrap();
        array.schunk.add_metalayer("keep", b"fixed").unwrap();

        let selection = vec![vec![2, 0], vec![3, 1]];
        assert_eq!(
            array.get_orthogonal_selection(&selection).unwrap(),
            u16_bytes(&[11, 9, 3, 1])
        );
        assert_eq!(
            array
                .get_orthogonal_selection_cbuffer(&selection, &[2, 2])
                .unwrap(),
            u16_bytes(&[11, 9, 3, 1])
        );
        assert!(array
            .get_orthogonal_selection_cbuffer(&selection, &[2, 3])
            .is_err());

        array
            .set_orthogonal_selection_cbuffer(
                &selection,
                &[2, 2],
                &u16_bytes(&[100, 101, 102, 103]),
            )
            .unwrap();
        let mut expected = values.clone();
        expected[2 * 4 + 3] = 100;
        expected[2 * 4 + 1] = 101;
        expected[3] = 102;
        expected[1] = 103;
        assert_eq!(array.to_cbuffer().unwrap(), u16_bytes(&expected));
        assert_eq!(array.schunk.metalayer("keep"), Some(&b"fixed"[..]));

        assert!(array
            .get_orthogonal_selection(&[vec![-1], vec![0]])
            .is_err());
        assert!(array.get_orthogonal_selection(&[vec![3], vec![0]]).is_err());
        assert!(array
            .get_orthogonal_selection_cbuffer(&selection, &[1, 3])
            .is_err());
        assert!(array
            .set_orthogonal_selection_cbuffer(
                &selection,
                &[2, 3],
                &u16_bytes(&[100, 101, 0, 102, 103, 0]),
            )
            .is_err());
        assert!(array
            .set_orthogonal_selection_cbuffer(
                &selection,
                &[2, 2],
                &u16_bytes(&[100, 101, 102, 103, 104]),
            )
            .is_err());

        let before = array.to_cbuffer().unwrap();
        assert_eq!(
            array
                .get_orthogonal_selection_cbuffer(&[Vec::new(), vec![0]], &[0, 2])
                .unwrap(),
            Vec::<u8>::new()
        );
        array
            .set_orthogonal_selection(&[Vec::new(), vec![0]], &[])
            .unwrap();
        assert!(array
            .set_orthogonal_selection_cbuffer(&[Vec::new(), vec![0]], &[0, 1], &[1, 2])
            .is_err());
        assert_eq!(array.to_cbuffer().unwrap(), before);
    }

    #[test]
    fn test_b2nd_orthogonal_set_preserves_raw_untouched_chunks() {
        let meta = B2ndMeta::new(vec![4, 4], vec![2, 2], vec![2, 2], "|u1", 0).unwrap();
        let values: Vec<u8> = (0..16u8).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_cbuffer(meta, &values, cparams, DParams::default()).unwrap();
        let saved = replace_raw_chunk(&mut array, 3, &[10, 11, 14, 15]);

        array
            .set_orthogonal_selection(&[vec![0], vec![0]], &[99])
            .unwrap();
        assert_eq!(array.schunk.compressed_chunk(3).unwrap(), saved.as_slice());

        let mut expected = values;
        expected[0] = 99;
        assert_eq!(array.to_cbuffer().unwrap(), expected);
    }

    #[test]
    fn test_b2nd_resize_at_middle_insertion() {
        let meta = B2ndMeta::new(vec![4, 6], vec![2, 3], vec![1, 3], "<u2", 0).unwrap();
        let values: Vec<u16> = (0..24u16).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_cbuffer(meta, &u16_bytes(&values), cparams, DParams::default())
                .unwrap();

        array.resize_at(vec![6, 6], Some(&[2, 0])).unwrap();

        let mut expected = vec![0u16; 36];
        for row in 0..2 {
            for col in 0..6 {
                expected[row * 6 + col] = values[row * 6 + col];
            }
        }
        for row in 2..4 {
            for col in 0..6 {
                expected[(row + 2) * 6 + col] = values[row * 6 + col];
            }
        }
        assert_eq!(array.shape(), &[6, 6]);
        assert_eq!(array.to_cbuffer().unwrap(), u16_bytes(&expected));
    }

    #[test]
    fn test_b2nd_resize_at_middle_insertion_preserves_surviving_raw_chunks() {
        let meta = B2ndMeta::new(vec![4], vec![2], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_cbuffer(meta, &[1, 2, 3, 4], cparams, DParams::default()).unwrap();
        let saved_tail = replace_raw_chunk(&mut array, 1, &[3, 4]);

        array.resize_at(vec![6], Some(&[2])).unwrap();

        assert_eq!(array.shape(), &[6]);
        assert_eq!(array.to_cbuffer().unwrap(), vec![1, 2, 0, 0, 3, 4]);
        let inserted = ChunkHeader::read(array.schunk.compressed_chunk(1).unwrap()).unwrap();
        assert_eq!(inserted.version, BLOSC2_VERSION_FORMAT_STABLE);
        assert_eq!(inserted.versionlz, BLOSC_BLOSCLZ_VERSION_FORMAT);
        assert_eq!(inserted.flags, BLOSC_DOSHUFFLE | BLOSC_DOBITSHUFFLE);
        assert_eq!(inserted.special_type(), BLOSC2_SPECIAL_ZERO);
        assert_eq!(inserted.cbytes as usize, BLOSC_EXTENDED_HEADER_LENGTH);
        assert_eq!(inserted.nbytes, 2);
        assert_eq!(
            array.schunk.compressed_chunk(2).unwrap(),
            saved_tail.as_slice()
        );
    }

    #[test]
    fn test_b2nd_resize_at_middle_deletion_preserves_surviving_raw_chunks() {
        let meta = B2ndMeta::new(vec![6], vec![2], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_cbuffer(meta, &[1, 2, 3, 4, 5, 6], cparams, DParams::default())
                .unwrap();
        let saved_tail = replace_raw_chunk(&mut array, 2, &[5, 6]);

        array.resize_at(vec![4], Some(&[2])).unwrap();

        assert_eq!(array.shape(), &[4]);
        assert_eq!(array.to_cbuffer().unwrap(), vec![1, 2, 5, 6]);
        assert_eq!(
            array.schunk.compressed_chunk(1).unwrap(),
            saved_tail.as_slice()
        );
    }

    #[test]
    fn test_b2nd_resize_tail_append_preserves_partial_tail_chunk() {
        let meta = B2ndMeta::new(vec![5], vec![3], vec![1], "|u1", 0).unwrap();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_cbuffer(meta, &[1, 2, 3, 4, 5], cparams, DParams::default()).unwrap();
        let saved_tail = replace_raw_chunk(&mut array, 1, &[4, 5, 0]);

        array.resize(vec![7]).unwrap();

        assert_eq!(array.shape(), &[7]);
        assert_eq!(array.to_cbuffer().unwrap(), vec![1, 2, 3, 4, 5, 0, 0]);
        assert_eq!(
            array.schunk.compressed_chunk(1).unwrap(),
            saved_tail.as_slice()
        );
        assert_eq!(array.schunk.nchunks(), 3);
    }

    #[test]
    fn test_b2nd_resize_multi_axis_chunk_predicate() {
        let meta = B2ndMeta::new(vec![4, 4], vec![2, 2], vec![1, 1], "|u1", 0).unwrap();
        let values: Vec<u8> = (0..16u8).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 1,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_cbuffer(meta, &values, cparams, DParams::default()).unwrap();
        let saved_bottom_right = replace_raw_chunk(&mut array, 3, &[10, 11, 14, 15]);

        array.resize_at(vec![6, 6], Some(&[2, 2])).unwrap();

        let mut expected = vec![0u8; 36];
        for row in 0..2 {
            for col in 0..2 {
                expected[row * 6 + col] = values[row * 4 + col];
            }
            for col in 2..4 {
                expected[row * 6 + col + 2] = values[row * 4 + col];
            }
        }
        for row in 2..4 {
            for col in 0..2 {
                expected[(row + 2) * 6 + col] = values[row * 4 + col];
            }
            for col in 2..4 {
                expected[(row + 2) * 6 + col + 2] = values[row * 4 + col];
            }
        }

        assert_eq!(array.shape(), &[6, 6]);
        assert_eq!(array.to_cbuffer().unwrap(), expected);
        assert_eq!(
            array.schunk.compressed_chunk(8).unwrap(),
            saved_bottom_right.as_slice()
        );
    }

    #[test]
    fn test_b2nd_resize_at_middle_deletion_and_validation() {
        let meta = B2ndMeta::new(vec![6, 6], vec![2, 3], vec![1, 3], "<u2", 0).unwrap();
        let values: Vec<u16> = (0..36u16).collect();
        let cparams = CParams {
            compcode: BLOSC_LZ4,
            clevel: 5,
            typesize: 2,
            splitmode: BLOSC_NEVER_SPLIT,
            filters: [0, 0, 0, 0, 0, BLOSC_SHUFFLE],
            ..Default::default()
        };
        let mut array =
            B2ndArray::from_cbuffer(meta, &u16_bytes(&values), cparams, DParams::default())
                .unwrap();

        assert!(array.resize_at(vec![8, 6], Some(&[1, 0])).is_err());
        assert!(array.resize_at(vec![8, 6], Some(&[-2, 0])).is_err());
        assert!(array.resize_at(vec![8, 6], Some(&[2])).is_err());
        assert!(array.resize_at(vec![8], Some(&[2, 0])).is_err());

        array.resize_at(vec![4, 6], Some(&[2, 0])).unwrap();

        let mut expected = Vec::new();
        for row in 0..2 {
            for col in 0..6 {
                expected.push(values[row * 6 + col]);
            }
        }
        for row in 4..6 {
            for col in 0..6 {
                expected.push(values[row * 6 + col]);
            }
        }
        assert_eq!(array.shape(), &[4, 6]);
        assert_eq!(array.to_cbuffer().unwrap(), u16_bytes(&expected));
    }
}
