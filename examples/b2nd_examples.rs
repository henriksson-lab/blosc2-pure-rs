use blosc2_pure_rs::{B2ndArray, B2ndMeta, CParams, DParams, DTYPE_NUMPY_FORMAT};
use std::path::PathBuf;

fn f64_bytes(values: impl IntoIterator<Item = f64>) -> Vec<u8> {
    values
        .into_iter()
        .flat_map(f64::to_le_bytes)
        .collect::<Vec<_>>()
}

fn u16_bytes(values: impl IntoIterator<Item = u16>) -> Vec<u8> {
    values
        .into_iter()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>()
}

fn f64_values(bytes: &[u8]) -> Vec<f64> {
    bytes
        .chunks_exact(8)
        .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
        .collect()
}

fn u16_values(bytes: &[u8]) -> Vec<u16> {
    bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes(chunk.try_into().unwrap()))
        .collect()
}

fn f64_array() -> Result<B2ndArray, &'static str> {
    let meta = B2ndMeta::new(
        vec![10, 10],
        vec![4, 4],
        vec![2, 2],
        "<f8",
        DTYPE_NUMPY_FORMAT,
    )?;
    let data = f64_bytes((0..100).map(|i| i as f64));
    B2ndArray::from_cbuffer(meta, &data, CParams::default(), DParams::default())
}

fn print_meta_and_open() -> Result<(), Box<dyn std::error::Error>> {
    let array = f64_array()?;
    let path = unique_temp_path("blosc2-rs-b2nd-example.b2frame");
    array.save(&path)?;

    let opened = B2ndArray::open(&path)?;
    println!("metadata:\n{}", opened.format_meta());
    assert_eq!(opened.shape(), &[10, 10]);
    assert_eq!(opened.to_cbuffer()?, array.to_cbuffer()?);
    std::fs::remove_dir_all(path)?;
    Ok(())
}

fn serialization_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let array = f64_array()?;
    let frame = array.to_frame();
    let restored = B2ndArray::from_frame(&frame)?;
    assert_eq!(
        f64_values(&restored.to_cbuffer()?),
        (0..100).map(|i| i as f64).collect::<Vec<_>>()
    );
    println!("serialization: {} byte frame roundtripped", frame.len());
    Ok(())
}

fn plain_buffer_slicing() -> Result<(), Box<dyn std::error::Error>> {
    let array = f64_array()?;
    let point = array.get_slice(&[2, 5], &[3, 6])?;
    assert_eq!(f64_values(&point), vec![25.0]);

    let padded = array.get_slice_cbuffer(&[1, 2], &[3, 5], &[2, 4])?;
    assert_eq!(
        f64_values(&padded),
        vec![12.0, 13.0, 14.0, 0.0, 22.0, 23.0, 24.0, 0.0]
    );
    println!("plain-buffer slicing: point={:?}", f64_values(&point));
    Ok(())
}

fn append_and_set_slice() -> Result<(), Box<dyn std::error::Error>> {
    let cparams = CParams {
        typesize: 2,
        ..CParams::default()
    };
    let meta = B2ndMeta::new(
        vec![0, 3, 4],
        vec![1, 3, 4],
        vec![1, 3, 4],
        "|u2",
        DTYPE_NUMPY_FORMAT,
    )?;
    let mut stack = B2ndArray::empty(meta, cparams.clone(), DParams::default())?;
    for image in 0..3u16 {
        let pixels = u16_bytes((0..12).map(|pixel| image * 100 + pixel));
        stack.append(0, &[1, 3, 4], &pixels)?;
    }
    assert_eq!(stack.shape(), &[3, 3, 4]);

    let replacement = u16_bytes((0..12).map(|pixel| 900 + pixel));
    stack.set_slice_cbuffer(&[1, 0, 0], &[2, 3, 4], &[1, 3, 4], &replacement)?;
    let middle = stack.get_slice(&[1, 0, 0], &[2, 3, 4])?;
    assert_eq!(u16_values(&middle), (900..912).collect::<Vec<_>>());

    let set_meta = B2ndMeta::new(
        vec![3, 3, 4],
        vec![1, 3, 4],
        vec![1, 3, 4],
        "|u2",
        DTYPE_NUMPY_FORMAT,
    )?;
    let mut set_slice_stack = B2ndArray::empty(set_meta, cparams, DParams::default())?;
    set_slice_stack.set_slice_cbuffer(&[1, 0, 0], &[2, 3, 4], &[1, 3, 4], &replacement)?;
    assert_eq!(
        u16_values(&set_slice_stack.get_slice(&[1, 0, 0], &[2, 3, 4])?),
        (900..912).collect::<Vec<_>>()
    );
    println!("stack images: append and set_slice paths updated one image each");
    Ok(())
}

fn empty_shape_slicing() -> Result<(), Box<dyn std::error::Error>> {
    let array = f64_array()?;
    let empty = array.get_slice_array(&[2, 5], &[2, 6])?;
    assert_eq!(empty.shape(), &[0, 1]);
    assert!(empty.to_cbuffer()?.is_empty());
    println!("empty-shape slicing: shape={:?}", empty.shape());
    Ok(())
}

fn orthogonal_indexing() -> Result<(), Box<dyn std::error::Error>> {
    let mut array = f64_array()?;
    let selection = [vec![3, 1, 2], vec![2, 5]];
    array.set_orthogonal_selection(&selection, &f64_bytes([0.0; 6]))?;
    assert_eq!(
        f64_values(&array.get_orthogonal_selection(&selection)?),
        vec![0.0; 6]
    );

    let corners = array.get_orthogonal_selection_cbuffer(&[vec![0, 2], vec![1, 3]], &[2, 2])?;
    assert_eq!(f64_values(&corners), vec![1.0, 3.0, 21.0, 23.0]);
    println!(
        "orthogonal indexing: selected {} values",
        selection[0].len() * selection[1].len()
    );
    Ok(())
}

fn unique_temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{name}-{}", std::process::id()))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    print_meta_and_open()?;
    serialization_roundtrip()?;
    plain_buffer_slicing()?;
    append_and_set_slice()?;
    empty_shape_slicing()?;
    orthogonal_indexing()?;
    Ok(())
}
