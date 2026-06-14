use blosc2_pure_rs::{
    b2nd_create_ctx_with_storage, b2nd_empty_ctx_c, B2ndArray, B2ndMeta, B2ndStorage, CParams,
    DParams, BLOSC2_ERROR_SUCCESS, BLOSC2_MAX_FILTERS, BLOSC_ALWAYS_SPLIT, BLOSC_BLOSCLZ,
    BLOSC_CODEC_NDLZ, BLOSC_FILTER_NDCELL, DTYPE_NUMPY_FORMAT,
};
use std::ffi::OsString;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);
const MAX_TEMP_DIR_ATTEMPTS: usize = 128;

fn f64_bytes(values: impl IntoIterator<Item = f64>) -> Vec<u8> {
    values
        .into_iter()
        .flat_map(f64::to_le_bytes)
        .collect::<Vec<_>>()
}

fn f32_bytes(values: impl IntoIterator<Item = f32>) -> Vec<u8> {
    values
        .into_iter()
        .flat_map(f32::to_le_bytes)
        .collect::<Vec<_>>()
}

fn i64_bytes(values: impl IntoIterator<Item = i64>) -> Vec<u8> {
    values
        .into_iter()
        .flat_map(i64::to_le_bytes)
        .collect::<Vec<_>>()
}

fn f64_values(bytes: &[u8]) -> Vec<f64> {
    bytes
        .chunks_exact(8)
        .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
        .collect()
}

fn next_rand(random_state: &mut u64) -> u32 {
    *random_state = random_state
        .wrapping_mul(1_103_515_245)
        .wrapping_add(12_345);
    ((*random_state / 65_536) % 32_768) as u32
}

fn fill_random_image(random_state: &mut u64, image: &mut [u8]) {
    for pixel in image.chunks_exact_mut(2) {
        let value = (next_rand(random_state) % 65_536) as u16;
        pixel.copy_from_slice(&value.to_le_bytes());
    }
}

fn msgpack_str(content: &str) -> Vec<u8> {
    let len = u8::try_from(content.len()).expect("example metadata string fits in fixstr8");
    let mut msgpack = Vec::with_capacity(content.len() + 2);
    msgpack.push(0xd9);
    msgpack.push(len);
    msgpack.extend_from_slice(content.as_bytes());
    msgpack
}

fn f64_array() -> Result<B2ndArray, &'static str> {
    let meta = B2ndMeta::with_default_dtype(vec![10, 10], vec![4, 4], vec![2, 2], 8)?;
    let data = f64_bytes((0..100).map(|i| i as f64));
    B2ndArray::from_dense_buffer(meta, &data, CParams::default(), DParams::default())
}

fn zeroed_f64_array() -> Result<B2ndArray, &'static str> {
    let meta = B2ndMeta::with_default_dtype(vec![10, 10], vec![4, 4], vec![2, 2], 8)?;
    let data = vec![0u8; 10 * 10 * 8];
    B2ndArray::from_dense_buffer(meta, &data, CParams::default(), DParams::default())
}

fn frame_generator_case(
    temp_dir: &TempDir,
    name: &str,
    shape: Vec<i64>,
    chunkshape: Vec<i32>,
    blockshape: Vec<i32>,
    typesize: i32,
    data: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let meta = B2ndMeta::with_default_dtype(
        shape.clone(),
        chunkshape,
        blockshape,
        usize::try_from(typesize)?,
    )?;
    let cparams = CParams {
        typesize,
        ..CParams::default()
    };
    let array = B2ndArray::from_dense_buffer(meta, &data, cparams, DParams::default())?;
    let path = temp_dir.path().join(name);
    array.save(&path)?;
    let opened = B2ndArray::open(&path)?;
    assert_eq!(opened.shape(), shape.as_slice());
    assert_eq!(opened.to_dense_buffer()?, data);
    println!("frame generator: {name}");
    println!("metadata:\n{}", opened.format_meta());
    Ok(())
}

fn frame_generator_examples() -> Result<TempDir, Box<dyn std::error::Error>> {
    let temp_dir = unique_temp_dir("blosc2-rs-b2nd-frame-generator")?;
    let mut random_state = 1;

    let rand_shape = vec![32, 18, 32];
    let rand_nelem = product_i64(&rand_shape)?;
    let rand_data = f32_bytes((0..rand_nelem).map(|_| (next_rand(&mut random_state) % 220) as f32));
    frame_generator_case(
        &temp_dir,
        "rand.b2nd",
        rand_shape,
        vec![17, 16, 24],
        vec![8, 9, 8],
        4,
        rand_data,
    )?;

    let all_eq_shape = vec![100, 50, 100];
    let all_eq_nelem = product_i64(&all_eq_shape)?;
    let mut all_eq_data = vec![0u8; all_eq_nelem * 8];
    all_eq_data[..all_eq_nelem].fill(22);
    frame_generator_case(
        &temp_dir,
        "all_eq.b2nd",
        all_eq_shape,
        vec![40, 20, 60],
        vec![20, 10, 30],
        8,
        all_eq_data,
    )?;

    let cyclic_shape = vec![100, 50, 100];
    let cyclic_nelem = product_i64(&cyclic_shape)?;
    let mut cyclic_data = vec![0u8; cyclic_nelem * 8];
    for (index, byte) in cyclic_data[..cyclic_nelem].iter_mut().enumerate() {
        *byte = index as i8 as u8;
    }
    frame_generator_case(
        &temp_dir,
        "cyclic.b2nd",
        cyclic_shape,
        vec![40, 20, 60],
        vec![20, 10, 30],
        8,
        cyclic_data,
    )?;

    let same_cells_shape = vec![128, 111];
    let same_cells_nelem = product_i64(&same_cells_shape)?;
    let mut same_cells = vec![0.0f64; same_cells_nelem];
    for i in 0..(same_cells_nelem / 4) {
        same_cells[i * 4] = 11_111_111.0;
        same_cells[i * 4 + 1] = 99_999_999.0;
    }
    frame_generator_case(
        &temp_dir,
        "same_cells.b2nd",
        same_cells_shape,
        vec![32, 11],
        vec![16, 7],
        8,
        f64_bytes(same_cells),
    )?;

    let some_matches_shape = vec![128, 111];
    let some_matches_nelem = product_i64(&some_matches_shape)?;
    let some_matches = (0..some_matches_nelem)
        .map(|i| {
            if i < some_matches_nelem / 2 {
                i as f64
            } else {
                1.0
            }
        })
        .collect::<Vec<_>>();
    frame_generator_case(
        &temp_dir,
        "some_matches.b2nd",
        some_matches_shape,
        vec![48, 32],
        vec![14, 18],
        8,
        f64_bytes(some_matches),
    )?;

    let many_matches_shape = vec![80, 120, 111];
    let many_matches_nelem = product_i64(&many_matches_shape)?;
    let mut many_matches_data = vec![0u8; many_matches_nelem * 8];
    for i in (0..many_matches_nelem).step_by(2) {
        many_matches_data[i] = i as i8 as u8;
        many_matches_data[i + 1] = 2;
    }
    frame_generator_case(
        &temp_dir,
        "many_matches.b2nd",
        many_matches_shape,
        vec![40, 30, 50],
        vec![11, 14, 24],
        8,
        many_matches_data,
    )?;

    let float_cyclic_shape = vec![40, 60, 20];
    let float_cyclic_nelem = product_i64(&float_cyclic_shape)?;
    let mut float_cyclic = vec![0.0f32; float_cyclic_nelem];
    for i in (0..float_cyclic_nelem).step_by(2) {
        let j = i as f32;
        float_cyclic[i] = j + j / 10.0 + j / 100.0;
        float_cyclic[i + 1] = 2.0 + j / 10.0 + j / 1000.0;
    }
    frame_generator_case(
        &temp_dir,
        "example_float_cyclic.b2nd",
        float_cyclic_shape,
        vec![20, 30, 16],
        vec![11, 14, 7],
        4,
        f32_bytes(float_cyclic),
    )?;

    let double_same_cells_shape = vec![40, 60];
    let double_same_cells_nelem = product_i64(&double_same_cells_shape)?;
    let mut double_same_cells = vec![0.0f64; double_same_cells_nelem];
    for i in (0..double_same_cells_nelem).step_by(4) {
        double_same_cells[i] = 1.5;
        double_same_cells[i + 1] = 14.7;
        double_same_cells[i + 2] = 23.6;
        double_same_cells[i + 3] = 3.2;
    }
    frame_generator_case(
        &temp_dir,
        "example_double_same_cells.b2nd",
        double_same_cells_shape,
        vec![20, 30],
        vec![16, 16],
        8,
        f64_bytes(double_same_cells),
    )?;

    let big_float_shape = vec![200, 310, 214];
    let big_float_nelem = product_i64(&big_float_shape)?;
    let mut big_float = vec![0.0f32; big_float_nelem];
    for i in (0..big_float_nelem).step_by(4) {
        let j = i as f32;
        big_float[i] = 2.73;
        big_float[i + 1] = 2.0 + j / 10.0 + j / 1000.0;
        big_float[i + 2] = 7.0 + j / 10.0 - j / 100.0;
        big_float[i + 3] = 11.0 + j / 100.0 - j / 1000.0;
    }
    frame_generator_case(
        &temp_dir,
        "example_big_float_frame.b2nd",
        big_float_shape,
        vec![110, 120, 76],
        vec![57, 52, 35],
        4,
        f32_bytes(big_float),
    )?;

    let day_month_shape = vec![400, 3];
    let day_month_nelem = product_i64(&day_month_shape)?;
    let mut day_month = vec![0.0f32; day_month_nelem];
    for i in (0..day_month_nelem).step_by(3) {
        day_month[i] = (next_rand(&mut random_state) % 31) as f32;
        day_month[i + 1] = (next_rand(&mut random_state) % 12) as f32;
        day_month[i + 2] = (next_rand(&mut random_state) % 10_000) as f32 / 10_000.0 * 60.0 - 20.0;
    }
    frame_generator_case(
        &temp_dir,
        "example_day_month_temp.b2nd",
        day_month_shape,
        vec![110, 3],
        vec![57, 3],
        4,
        f32_bytes(day_month),
    )?;

    let item_prices_shape = vec![12, 25, 250];
    let mut item_prices = Vec::with_capacity(product_i64(&item_prices_shape)?);
    for month in 1..=item_prices_shape[0] {
        for store in 1..=item_prices_shape[1] {
            for item in 1..=item_prices_shape[2] {
                let mut item_seed = item as u64;
                let random_price = (next_rand(&mut item_seed) % 1000) as f32 / 1000.0 * 250.0 + 1.0;
                item_prices.push(store as f32 + (3 - (month % 3)) as f32 * random_price);
            }
        }
    }
    frame_generator_case(
        &temp_dir,
        "example_item_prices.b2nd",
        item_prices_shape,
        vec![8, 10, 50],
        vec![4, 5, 10],
        4,
        f32_bytes(item_prices),
    )?;

    Ok(temp_dir)
}

fn print_meta_and_open(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let opened = B2ndArray::open(&path)?;
    println!("metadata:\n{}", opened.format_meta());
    assert_eq!(opened.shape(), &[200, 310, 214]);
    assert_eq!(opened.chunkshape(), &[110, 120, 76]);
    assert_eq!(opened.blockshape(), &[57, 52, 35]);
    Ok(())
}

fn serialization_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let array = f64_array()?;
    let frame = array.to_contiguous_frame();
    let restored = B2ndArray::from_contiguous_frame(&frame)?;
    assert_eq!(
        f64_values(&restored.to_dense_buffer()?),
        (0..100).map(|i| i as f64).collect::<Vec<_>>()
    );
    println!("serialization: {} byte frame roundtripped", frame.len());
    Ok(())
}

fn plugin_codec_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let shape = vec![745, 400];
    let chunkshape = vec![150, 100];
    let blockshape = vec![21, 30];
    let nitems = product_i64(&shape)?;
    let source = (0..nitems).map(|i| i as i64).collect::<Vec<_>>();
    let data = i64_bytes(source.iter().copied());
    let cparams = CParams {
        nthreads: 1,
        compcode: BLOSC_CODEC_NDLZ,
        splitmode: BLOSC_ALWAYS_SPLIT,
        compcode_meta: 4,
        clevel: 5,
        typesize: 8,
        ..CParams::default()
    };
    let meta = B2ndMeta::with_default_dtype(shape, chunkshape, blockshape, 8)?;
    let array = B2ndArray::from_dense_buffer(meta, &data, cparams, DParams::default())?;
    assert_eq!(array.to_dense_buffer()?, data);
    println!(
        "plugin codec: NDLZ roundtripped {} i64 values",
        source.len()
    );
    Ok(())
}

fn plugin_filter_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let shape = vec![345, 200, 50];
    let chunkshape = vec![150, 100, 50];
    let blockshape = vec![21, 30, 27];
    let nitems = product_i64(&shape)?;
    let source = (0..nitems).map(|i| i as i64).collect::<Vec<_>>();
    let data = i64_bytes(source.iter().copied());
    let mut filters = [0u8; BLOSC2_MAX_FILTERS];
    filters[4] = BLOSC_FILTER_NDCELL;
    let mut filters_meta = [0u8; BLOSC2_MAX_FILTERS];
    filters_meta[4] = 4;
    let cparams = CParams {
        nthreads: 1,
        filters,
        filters_meta,
        typesize: 8,
        ..CParams::default()
    };
    let meta = B2ndMeta::with_default_dtype(shape, chunkshape, blockshape, 8)?;
    let array = B2ndArray::from_dense_buffer(meta, &data, cparams, DParams::default())?;
    assert_eq!(array.to_dense_buffer()?, data);
    println!(
        "plugin filter: NDCELL roundtripped {} i64 values",
        source.len()
    );
    Ok(())
}

fn plain_buffer_slicing() -> Result<(), Box<dyn std::error::Error>> {
    let dparams = DParams {
        nthreads: 2,
        ..DParams::default()
    };
    let meta = B2ndMeta::with_default_dtype(vec![10, 10], vec![4, 4], vec![2, 2], 8)?;
    let data = vec![0u8; 10 * 10 * 8];
    let array = B2ndArray::from_dense_buffer(meta, &data, CParams::default(), dparams.clone())?;
    let slice_meta = B2ndMeta::with_default_dtype(vec![10, 10], vec![1, 1], vec![1, 1], 8)?;
    let slice = array.slice_with_meta(&[2, 5], &[3, 6], slice_meta, CParams::default(), dparams)?;
    let slice_view = slice.squeeze_view()?;
    let buffer = slice_view.to_dense_buffer()?;
    assert_eq!(f64_values(&buffer), vec![0.0]);
    println!(
        "plain-buffer slicing: squeezed shape={:?}",
        slice_view.shape()
    );
    Ok(())
}

fn append_and_set_slice() -> Result<(), Box<dyn std::error::Error>> {
    const WIDTH: i64 = 4 * 512;
    const HEIGHT: i64 = 4 * 272;
    const N_IMAGES: i64 = 10;
    let buffershape = [1, HEIGHT, WIDTH];
    let buffersize = (WIDTH * HEIGHT * u16::BITS as i64 / 8) as usize;
    let mut image = vec![0u8; buffersize];
    let mut random_state = 1;

    let cparams = CParams {
        compcode: BLOSC_BLOSCLZ,
        clevel: 5,
        typesize: 2,
        nthreads: 4,
        ..CParams::default()
    };
    let temp_dir = unique_temp_dir("blosc2-rs-b2nd-stack-images")?;

    let set_slice_meta = B2ndMeta::new(
        vec![N_IMAGES, HEIGHT, WIDTH],
        vec![1, HEIGHT as i32, WIDTH as i32],
        vec![1, HEIGHT as i32, WIDTH as i32],
        "|u2",
        DTYPE_NUMPY_FORMAT,
    )?;
    let set_slice_path = temp_dir.path().join("example_stack_images_set_slice.b2nd");
    let set_slice_ctx = b2nd_create_ctx_with_storage(
        set_slice_meta,
        cparams.clone(),
        DParams::default(),
        Vec::new(),
        B2ndStorage::contiguous_urlpath(&set_slice_path),
    )?;
    let (rc, set_slice_stack) = b2nd_empty_ctx_c(&set_slice_ctx);
    assert_eq!(rc, BLOSC2_ERROR_SUCCESS);
    let mut set_slice_stack = set_slice_stack.expect("b2nd_empty_ctx_c returned success");
    for image_index in 0..N_IMAGES {
        fill_random_image(&mut random_state, &mut image);
        set_slice_stack.set_slice_from_dense_buffer(
            &[image_index, 0, 0],
            &[image_index + 1, HEIGHT, WIDTH],
            &buffershape,
            &image,
        )?;
    }
    set_slice_stack
        .schunk
        .add_vlmetalayer("method", &msgpack_str("Using b2nd_set_slice_cbuffer()"))?;
    assert_eq!(set_slice_stack.shape(), &[N_IMAGES, HEIGHT, WIDTH]);
    assert!(set_slice_path.exists());

    let append_meta = B2ndMeta::new(
        vec![0, HEIGHT, WIDTH],
        vec![1, HEIGHT as i32, WIDTH as i32],
        vec![1, HEIGHT as i32, WIDTH as i32],
        "|u2",
        DTYPE_NUMPY_FORMAT,
    )?;
    let append_path = temp_dir.path().join("example_stack_images_append.b2nd");
    let append_ctx = b2nd_create_ctx_with_storage(
        append_meta,
        cparams,
        DParams::default(),
        Vec::new(),
        B2ndStorage::contiguous_urlpath(&append_path),
    )?;
    let (rc, append_stack) = b2nd_empty_ctx_c(&append_ctx);
    assert_eq!(rc, BLOSC2_ERROR_SUCCESS);
    let mut append_stack = append_stack.expect("b2nd_empty_ctx_c returned success");
    for _ in 0..N_IMAGES {
        fill_random_image(&mut random_state, &mut image);
        append_stack.append_dense_buffer(0, &image)?;
    }
    append_stack
        .schunk
        .add_vlmetalayer("method", &msgpack_str("Using b2nd_append()"))?;
    assert_eq!(append_stack.shape(), &[N_IMAGES, HEIGHT, WIDTH]);
    assert!(append_path.exists());

    println!(
        "stack images: set_slice filled a fixed stack; append_dense_buffer grew an empty stack"
    );
    Ok(())
}

fn empty_shape_slicing() -> Result<(), Box<dyn std::error::Error>> {
    let array = zeroed_f64_array()?;
    let slice_meta = B2ndMeta {
        shape: vec![10, 10],
        chunkshape: vec![0, 1],
        blockshape: vec![0, 1],
        dtype: "|S8".to_string(),
        dtype_format: DTYPE_NUMPY_FORMAT,
    };
    // The C example supplies this metadata via a destination context with a
    // urlpath; the Rust helper mirrors the resulting empty slice directly.
    let empty = array.slice_with_meta(
        &[2, 5],
        &[2, 6],
        slice_meta,
        CParams::default(),
        DParams::default(),
    )?;
    assert_eq!(empty.shape(), &[0, 1]);
    assert_eq!(empty.chunkshape(), &[0, 1]);
    assert_eq!(empty.blockshape(), &[0, 1]);
    assert!(empty.to_dense_buffer()?.is_empty());
    println!("empty-shape slicing: shape={:?}", empty.shape());
    Ok(())
}

fn orthogonal_indexing() -> Result<(), Box<dyn std::error::Error>> {
    let mut array = f64_array()?;
    let c_selection = [vec![3, 1, 2], vec![2, 5], vec![3, 3, 3, 9, 3, 1, 0]];
    let selection = &c_selection[..array.shape().len()];
    let buffershape = selection
        .iter()
        .map(|axis| axis.len() as i64)
        .collect::<Vec<_>>();
    let nitems = product_i64(&buffershape)?;
    let buffer = vec![0u8; nitems * 8];
    array.set_orthogonal_selection_from_dense_buffer(selection, &buffershape, &buffer)?;
    let values = f64_values(&array.orthogonal_selection_to_dense_buffer(selection, &buffershape)?);
    assert_eq!(values, vec![0.0; nitems]);
    println!("Results: ");
    for (index, value) in values.iter().enumerate() {
        if index % selection[1].len() == 0 {
            println!();
        }
        print!(" {value:.6} ");
    }
    println!();
    Ok(())
}

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn unique_temp_dir(prefix: &str) -> io::Result<TempDir> {
    let mut last_exists = None;
    for _ in 0..MAX_TEMP_DIR_ATTEMPTS {
        let path = unique_temp_dir_path(prefix);
        match std::fs::create_dir(&path) {
            Ok(()) => return Ok(TempDir { path }),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                last_exists = Some(err);
            }
            Err(err) => return Err(err),
        }
    }

    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "failed to create a unique temporary directory after {MAX_TEMP_DIR_ATTEMPTS} attempts: {}",
            last_exists
                .map(|err| err.to_string())
                .unwrap_or_else(|| "temporary directory already exists".to_string())
        ),
    ))
}

fn unique_temp_dir_path(prefix: &str) -> PathBuf {
    let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut name = OsString::from(prefix);
    name.push(format!("-{}-{counter}", std::process::id()));
    std::env::temp_dir().join(name)
}

fn product_i64(shape: &[i64]) -> Result<usize, &'static str> {
    shape.iter().try_fold(1usize, |acc, &dim| {
        let dim = usize::try_from(dim).map_err(|_| "negative shape dimension")?;
        acc.checked_mul(dim).ok_or("shape product overflow")
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let frame_dir = frame_generator_examples()?;
    print_meta_and_open(&frame_dir.path().join("example_big_float_frame.b2nd"))?;
    serialization_roundtrip()?;
    plugin_codec_roundtrip()?;
    plugin_filter_roundtrip()?;
    plain_buffer_slicing()?;
    append_and_set_slice()?;
    empty_shape_slicing()?;
    orthogonal_indexing()?;
    Ok(())
}
