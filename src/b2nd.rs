use crate::compress::{CParams, DParams};
use crate::schunk::Schunk;
use std::path::Path;

pub const B2ND_METALAYER_NAME: &str = "b2nd";
pub const B2ND_METALAYER_VERSION: u8 = 0;
pub const B2ND_MAX_DIM: usize = 16;
pub const DTYPE_NUMPY_FORMAT: i8 = 0;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct B2ndMeta {
    pub shape: Vec<i64>,
    pub chunkshape: Vec<i32>,
    pub blockshape: Vec<i32>,
    pub dtype: String,
    pub dtype_format: i8,
}

#[derive(Clone)]
pub struct B2ndArray {
    pub meta: B2ndMeta,
    pub schunk: Schunk,
}

impl B2ndMeta {
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

    pub fn ndim(&self) -> usize {
        self.shape.len()
    }

    pub fn nitems(&self) -> Result<usize, &'static str> {
        product_i64(&self.shape)
    }

    pub fn chunk_nitems(&self) -> Result<usize, &'static str> {
        product_i32(&self.chunkshape)
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        let ndim = self.shape.len();
        if ndim == 0 || ndim > B2ND_MAX_DIM {
            return Err("Invalid B2ND ndim");
        }
        if self.chunkshape.len() != ndim || self.blockshape.len() != ndim {
            return Err("B2ND shape ranks differ");
        }
        if self.dtype.is_empty() {
            return Err("B2ND dtype cannot be empty");
        }
        if self.dtype.len() > i32::MAX as usize {
            return Err("B2ND dtype too large");
        }
        if !(0..=127).contains(&self.dtype_format) {
            return Err("Invalid B2ND dtype format");
        }
        for dim in 0..ndim {
            if self.shape[dim] <= 0 {
                return Err("Invalid B2ND shape");
            }
            if self.chunkshape[dim] <= 0 || self.blockshape[dim] <= 0 {
                return Err("Invalid B2ND chunk or block shape");
            }
            if self.blockshape[dim] > self.chunkshape[dim] {
                return Err("B2ND block shape cannot exceed chunk shape");
            }
        }
        self.nitems()?;
        self.chunk_nitems()?;
        Ok(())
    }

    pub fn serialize(&self) -> Result<Vec<u8>, &'static str> {
        self.validate()?;
        let ndim = self.ndim();
        if ndim > 15 {
            return Err("B2ND metadata currently requires fixarray dimensions");
        }

        let dtype = self.dtype.as_bytes();
        let mut out = Vec::with_capacity(3 + 3 * (1 + ndim * 9) + 6 + dtype.len());
        out.push(0x90 + 7);
        out.push(B2ND_METALAYER_VERSION);
        out.push(ndim as u8);

        out.push(0x90 + ndim as u8);
        for &dim in &self.shape {
            out.push(0xd3);
            out.extend_from_slice(&dim.to_be_bytes());
        }

        out.push(0x90 + ndim as u8);
        for &dim in &self.chunkshape {
            out.push(0xd2);
            out.extend_from_slice(&dim.to_be_bytes());
        }

        out.push(0x90 + ndim as u8);
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

    pub fn deserialize(data: &[u8]) -> Result<Self, &'static str> {
        let mut pos = 0usize;
        expect_byte(data, &mut pos, 0x90 + 7)?;
        let version = read_fixint(data, &mut pos)?;
        if version != B2ND_METALAYER_VERSION {
            return Err("Unsupported B2ND metalayer version");
        }
        let ndim = read_fixint(data, &mut pos)? as usize;
        if ndim == 0 || ndim > B2ND_MAX_DIM || ndim > 15 {
            return Err("Invalid B2ND ndim");
        }

        expect_byte(data, &mut pos, 0x90 + ndim as u8)?;
        let mut shape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            expect_byte(data, &mut pos, 0xd3)?;
            shape.push(read_i64(data, &mut pos)?);
        }

        expect_byte(data, &mut pos, 0x90 + ndim as u8)?;
        let mut chunkshape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            expect_byte(data, &mut pos, 0xd2)?;
            chunkshape.push(read_i32(data, &mut pos)?);
        }

        expect_byte(data, &mut pos, 0x90 + ndim as u8)?;
        let mut blockshape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            expect_byte(data, &mut pos, 0xd2)?;
            blockshape.push(read_i32(data, &mut pos)?);
        }

        let dtype_format = read_fixint(data, &mut pos)? as i8;
        expect_byte(data, &mut pos, 0xdb)?;
        let dtype_len = read_i32(data, &mut pos)?;
        if dtype_len <= 0 {
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

        Self::new(shape, chunkshape, blockshape, dtype, dtype_format)
    }
}

impl B2ndArray {
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
        if data.len() != expected_len {
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
        schunk.add_metalayer(B2ND_METALAYER_NAME, &meta.serialize()?)?;

        let chunk_grid = chunk_grid(&meta)?;
        let chunk_count = product_usize(&chunk_grid)?;
        let layout = B2ndLayout::new(&meta, typesize)?;
        for linear_chunk in 0..chunk_count {
            let chunk_index = unravel_index(linear_chunk, &chunk_grid);
            let mut chunk = vec![0u8; chunk_nbytes];
            copy_dense_to_chunk(&meta, data, &layout, &chunk_index, &mut chunk)?;
            schunk.append_buffer(&chunk)?;
        }

        Ok(Self { meta, schunk })
    }

    pub fn from_schunk(schunk: Schunk) -> Result<Self, &'static str> {
        let content = schunk
            .metalayer(B2ND_METALAYER_NAME)
            .ok_or("Schunk does not contain a B2ND metalayer")?;
        let meta = B2ndMeta::deserialize(content)?;
        let expected_chunks = product_usize(&chunk_grid(&meta)?)?;
        if schunk.nchunks() as usize != expected_chunks {
            return Err("B2ND chunk count does not match metadata");
        }
        Ok(Self { meta, schunk })
    }

    pub fn from_frame(frame: &[u8]) -> Result<Self, String> {
        Self::from_schunk(Schunk::from_frame(frame)?).map_err(str::to_string)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        Self::from_schunk(Schunk::open(path.as_ref().to_str().ok_or("Invalid path")?)?)
            .map_err(str::to_string)
    }

    pub fn to_frame(&self) -> Vec<u8> {
        self.schunk.to_frame()
    }

    pub fn save(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        std::fs::write(path, self.to_frame())
    }

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

    pub fn shape(&self) -> &[i64] {
        &self.meta.shape
    }

    pub fn chunkshape(&self) -> &[i32] {
        &self.meta.chunkshape
    }

    pub fn blockshape(&self) -> &[i32] {
        &self.meta.blockshape
    }

    /// Return a dense row-major buffer for the half-open item slice
    /// `start..stop` in each dimension.
    pub fn get_slice(&self, start: &[i64], stop: &[i64]) -> Result<Vec<u8>, &'static str> {
        let typesize = self.schunk.cparams.typesize as usize;
        let slice = validate_slice_bounds(&self.meta, start, stop)?;
        let dense = self.to_cbuffer()?;
        let out_len = product_usize(&slice.extents)?
            .checked_mul(typesize)
            .ok_or("B2ND slice too large")?;
        let mut out = vec![0u8; out_len];
        copy_dense_region(
            &dense,
            DenseRegion {
                shape: &self.meta.shape,
                start: &slice.starts,
            },
            &mut out,
            DenseRegion {
                shape: &slice.extents_as_i64,
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
        let typesize = self.schunk.cparams.typesize as usize;
        let slice = validate_slice_bounds(&self.meta, start, stop)?;
        let expected_len = product_usize(&slice.extents)?
            .checked_mul(typesize)
            .ok_or("B2ND slice too large")?;
        if data.len() != expected_len {
            return Err("B2ND slice buffer size does not match slice shape and typesize");
        }

        let mut dense = self.to_cbuffer()?;
        copy_dense_region(
            data,
            DenseRegion {
                shape: &slice.extents_as_i64,
                start: &vec![0; slice.extents.len()],
            },
            &mut dense,
            DenseRegion {
                shape: &self.meta.shape,
                start: &slice.starts,
            },
            &slice.extents,
            typesize,
        )?;
        self.rebuild_from_dense(self.meta.clone(), &dense)
    }

    /// Resize the array, preserving the overlapping prefix region and zero-filling
    /// new cells.
    pub fn resize(&mut self, new_shape: Vec<i64>) -> Result<(), &'static str> {
        let mut new_meta = self.meta.clone();
        new_meta.shape = new_shape;
        new_meta.validate()?;

        let typesize = self.schunk.cparams.typesize as usize;
        let old_dense = self.to_cbuffer()?;
        let new_len = new_meta
            .nitems()?
            .checked_mul(typesize)
            .ok_or("B2ND buffer too large")?;
        let mut new_dense = vec![0u8; new_len];
        let overlap: Vec<usize> = self
            .meta
            .shape
            .iter()
            .zip(&new_meta.shape)
            .map(|(&old_dim, &new_dim)| old_dim.min(new_dim) as usize)
            .collect();
        let zero_start = vec![0; overlap.len()];
        copy_dense_region(
            &old_dense,
            DenseRegion {
                shape: &self.meta.shape,
                start: &zero_start,
            },
            &mut new_dense,
            DenseRegion {
                shape: &new_meta.shape,
                start: &zero_start,
            },
            &overlap,
            typesize,
        )?;
        self.rebuild_from_dense(new_meta, &new_dense)
    }

    fn rebuild_from_dense(&mut self, meta: B2ndMeta, data: &[u8]) -> Result<(), &'static str> {
        let rebuilt = Self::from_cbuffer(
            meta,
            data,
            self.schunk.cparams.clone(),
            self.schunk.dparams.clone(),
        )?;
        *self = rebuilt;
        Ok(())
    }
}

struct B2ndSlice {
    starts: Vec<usize>,
    extents: Vec<usize>,
    extents_as_i64: Vec<i64>,
}

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
        if start[dim] < 0 || stop[dim] > meta.shape[dim] || start[dim] >= stop[dim] {
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

fn expect_byte(data: &[u8], pos: &mut usize, expected: u8) -> Result<(), &'static str> {
    if data.get(*pos).copied() != Some(expected) {
        return Err("Invalid B2ND metadata");
    }
    *pos += 1;
    Ok(())
}

fn read_fixint(data: &[u8], pos: &mut usize) -> Result<u8, &'static str> {
    let byte = *data.get(*pos).ok_or("Truncated B2ND metadata")?;
    if byte > 0x7f {
        return Err("Invalid B2ND fixint");
    }
    *pos += 1;
    Ok(byte)
}

fn read_i64(data: &[u8], pos: &mut usize) -> Result<i64, &'static str> {
    let end = pos.checked_add(8).ok_or("Invalid B2ND metadata")?;
    let bytes = data.get(*pos..end).ok_or("Truncated B2ND metadata")?;
    *pos = end;
    Ok(i64::from_be_bytes(bytes.try_into().unwrap()))
}

fn read_i32(data: &[u8], pos: &mut usize) -> Result<i32, &'static str> {
    let end = pos.checked_add(4).ok_or("Invalid B2ND metadata")?;
    let bytes = data.get(*pos..end).ok_or("Truncated B2ND metadata")?;
    *pos = end;
    Ok(i32::from_be_bytes(bytes.try_into().unwrap()))
}

fn product_i64(values: &[i64]) -> Result<usize, &'static str> {
    values.iter().try_fold(1usize, |acc, &value| {
        if value <= 0 {
            return Err("Invalid B2ND shape");
        }
        acc.checked_mul(value as usize)
            .ok_or("B2ND shape too large")
    })
}

fn product_i32(values: &[i32]) -> Result<usize, &'static str> {
    values.iter().try_fold(1usize, |acc, &value| {
        if value <= 0 {
            return Err("Invalid B2ND shape");
        }
        acc.checked_mul(value as usize)
            .ok_or("B2ND shape too large")
    })
}

fn product_usize(values: &[usize]) -> Result<usize, &'static str> {
    values.iter().try_fold(1usize, |acc, &value| {
        acc.checked_mul(value).ok_or("B2ND shape too large")
    })
}

fn chunk_grid(meta: &B2ndMeta) -> Result<Vec<usize>, &'static str> {
    meta.shape
        .iter()
        .zip(&meta.chunkshape)
        .map(|(&shape, &chunk)| {
            if shape <= 0 || chunk <= 0 {
                return Err("Invalid B2ND shape");
            }
            Ok((shape as usize).div_ceil(chunk as usize))
        })
        .collect()
}

fn extchunkshape(meta: &B2ndMeta) -> Result<Vec<i32>, &'static str> {
    meta.chunkshape
        .iter()
        .zip(&meta.blockshape)
        .map(|(&chunk, &block)| {
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

fn extchunk_nitems(meta: &B2ndMeta) -> Result<usize, &'static str> {
    product_i32(&extchunkshape(meta)?)
}

fn blocks_in_chunk(extchunkshape: &[i32], blockshape: &[i32]) -> Result<Vec<usize>, &'static str> {
    extchunkshape
        .iter()
        .zip(blockshape)
        .map(|(&extchunk, &block)| {
            if extchunk <= 0 || block <= 0 || extchunk % block != 0 {
                return Err("Invalid B2ND block grid");
            }
            Ok((extchunk / block) as usize)
        })
        .collect()
}

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

struct DenseRegion<'a> {
    shape: &'a [i64],
    start: &'a [usize],
}

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

struct B2ndLayout {
    data_strides: Vec<usize>,
    extchunkshape: Vec<i32>,
    blocks_in_chunk: Vec<usize>,
    block_nitems: usize,
    typesize: usize,
}

impl B2ndLayout {
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

fn unravel_index(mut index: usize, shape: &[usize]) -> Vec<usize> {
    let mut out = vec![0; shape.len()];
    for dim in (0..shape.len()).rev() {
        out[dim] = index % shape[dim];
        index /= shape[dim];
    }
    out
}

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
    use crate::constants::{BLOSC_LZ4, BLOSC_NEVER_SPLIT, BLOSC_SHUFFLE};

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
    fn test_b2nd_array_frame_roundtrip() {
        let meta = B2ndMeta::new(vec![5, 7], vec![3, 4], vec![3, 2], "<u2", 0).unwrap();
        let data: Vec<u8> = (0..35u16).flat_map(u16::to_le_bytes).collect();
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
        assert_eq!(array.to_cbuffer().unwrap(), data);

        let frame = array.to_frame();
        let restored = B2ndArray::from_frame(&frame).unwrap();
        assert_eq!(restored.meta, meta);
        assert_eq!(restored.to_cbuffer().unwrap(), data);
    }

    fn u16_bytes(values: &[u16]) -> Vec<u8> {
        values
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
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
        let mut array = B2ndArray::from_cbuffer(meta, &data, cparams, DParams::default()).unwrap();
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
        let mut expected = values.clone();
        for (idx, value) in replacement.iter().enumerate() {
            let row = 1 + idx / 4;
            let col = 2 + idx % 4;
            expected[row * 7 + col] = *value;
        }
        assert_eq!(array.to_cbuffer().unwrap(), u16_bytes(&expected));

        array.resize(vec![6, 4]).unwrap();
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
        assert!(array.get_slice(&[0, 0], &[0, 1]).is_err());
        assert!(array.set_slice(&[0, 0], &[1, 1], &[1, 2, 3]).is_err());
    }
}
