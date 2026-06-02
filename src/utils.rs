//! Small C-Blosc2-compatible utility helpers.

use crate::constants::*;
use std::borrow::Cow;
use std::path::Path;

/// Normalize local Blosc2 URL paths.
///
/// C-Blosc2 accepts `file:///relative/path` as a local-file URL spelling and
/// strips that prefix before filesystem access.
pub fn normalize_urlpath(urlpath: &str) -> &str {
    urlpath.strip_prefix("file:///").unwrap_or(urlpath)
}

pub(crate) fn normalized_path(path: &Path) -> Cow<'_, Path> {
    match path.to_str() {
        Some(urlpath) => Cow::Owned(Path::new(normalize_urlpath(urlpath)).to_path_buf()),
        None => Cow::Borrowed(path),
    }
}

/// Initialize global Blosc2 state.
///
/// The pure-Rust implementation initializes lazily, so this is a no-op kept
/// for API parity with C-Blosc2.
pub fn blosc2_init() {}

/// Destroy global Blosc2 state.
///
/// The pure-Rust implementation has no mandatory global teardown, so this is a
/// no-op kept for API parity with C-Blosc2.
pub fn blosc2_destroy() {}

/// Free cached resources.
///
/// Drops process-wide caches that can be rebuilt lazily on the next use.
pub fn blosc2_free_resources() {
    crate::compress::free_cached_resources();
}

// Blosc1 compatibility aliases matching C-Blosc2's `BLOSC1_COMPAT` macro names.
pub use self::blosc2_destroy as blosc_destroy;
pub use self::blosc2_free_resources as blosc_free_resources;
pub use self::blosc2_init as blosc_init;

/// Return the C-Blosc2 error string for an error code.
pub fn blosc2_error_string(error_code: i32) -> &'static str {
    match error_code {
        BLOSC2_ERROR_FAILURE => "Generic failure",
        BLOSC2_ERROR_STREAM => "Bad stream",
        BLOSC2_ERROR_DATA => "Invalid data",
        BLOSC2_ERROR_MEMORY_ALLOC => "Memory alloc/realloc failure",
        BLOSC2_ERROR_READ_BUFFER => "Not enough space to read",
        BLOSC2_ERROR_WRITE_BUFFER => "Not enough space to write",
        BLOSC2_ERROR_CODEC_SUPPORT => "Codec not supported",
        BLOSC2_ERROR_CODEC_PARAM => "Invalid parameter supplied to codec",
        BLOSC2_ERROR_CODEC_DICT => "Codec dictionary error",
        BLOSC2_ERROR_VERSION_SUPPORT => "Version not supported",
        BLOSC2_ERROR_INVALID_HEADER => "Invalid value in header",
        BLOSC2_ERROR_INVALID_PARAM => "Invalid parameter supplied to function",
        BLOSC2_ERROR_FILE_READ => "File read failure",
        BLOSC2_ERROR_FILE_WRITE => "File write failure",
        BLOSC2_ERROR_FILE_OPEN => "File open failure",
        BLOSC2_ERROR_NOT_FOUND => "Not found",
        BLOSC2_ERROR_RUN_LENGTH => "Bad run length encoding",
        BLOSC2_ERROR_FILTER_PIPELINE => "Filter pipeline error",
        BLOSC2_ERROR_CHUNK_INSERT => "Chunk insert failure",
        BLOSC2_ERROR_CHUNK_APPEND => "Chunk append failure",
        BLOSC2_ERROR_CHUNK_UPDATE => "Chunk update failure",
        BLOSC2_ERROR_2GB_LIMIT => "Sizes larger than 2gb not supported",
        BLOSC2_ERROR_SCHUNK_COPY => "Super-chunk copy failure",
        BLOSC2_ERROR_FRAME_TYPE => "Wrong type for frame",
        BLOSC2_ERROR_FILE_TRUNCATE => "File truncate failure",
        BLOSC2_ERROR_THREAD_CREATE => "Thread or thread context creation failure",
        BLOSC2_ERROR_POSTFILTER => "Postfilter failure",
        BLOSC2_ERROR_FRAME_SPECIAL => "Special frame failure",
        BLOSC2_ERROR_SCHUNK_SPECIAL => "Special super-chunk failure",
        BLOSC2_ERROR_PLUGIN_IO => "IO plugin error",
        BLOSC2_ERROR_FILE_REMOVE => "Remove file failure",
        BLOSC2_ERROR_NULL_POINTER => "Pointer is null",
        BLOSC2_ERROR_INVALID_INDEX => "Invalid index",
        BLOSC2_ERROR_METALAYER_NOT_FOUND => "Metalayer has not been found",
        BLOSC2_ERROR_MAX_BUFSIZE_EXCEEDED => "Maximum buffersize exceeded",
        BLOSC2_ERROR_TUNER => "Tuner failure",
        _ => "Unknown error",
    }
}

/// Convert a row-major linear index to multidimensional coordinates.
pub fn blosc2_unidim_to_multidim(shape: &[i64], i: i64) -> Vec<i64> {
    blosc2_unidim_to_multidim_checked(shape, i).unwrap_or_default()
}

/// Checked conversion from a row-major linear index to multidimensional
/// coordinates.
pub fn blosc2_unidim_to_multidim_checked(shape: &[i64], i: i64) -> Result<Vec<i64>, &'static str> {
    if i < 0 {
        return Err("Invalid linear index");
    }
    let ndim = shape.len();
    let mut index = vec![0; ndim];
    if ndim == 0 {
        return if i == 0 {
            Ok(index)
        } else {
            Err("Invalid linear index")
        };
    }
    if shape.iter().any(|&dim| dim <= 0) {
        return Err("Invalid shape");
    }
    let mut strides = vec![1i64; ndim];
    for dim in (0..ndim - 1).rev() {
        strides[dim] = shape[dim + 1]
            .checked_mul(strides[dim + 1])
            .ok_or("Shape too large")?;
    }
    let total = shape[0].checked_mul(strides[0]).ok_or("Shape too large")?;
    if i >= total {
        return Err("Invalid linear index");
    }
    index[0] = i / strides[0];
    for dim in 1..ndim {
        index[dim] = (i % strides[dim - 1]) / strides[dim];
    }
    Ok(index)
}

/// Convert multidimensional coordinates to a row-major linear index using
/// caller-provided strides.
pub fn blosc2_multidim_to_unidim(index: &[i64], strides: &[i64]) -> i64 {
    blosc2_multidim_to_unidim_checked(index, strides).unwrap_or(-1)
}

/// Checked conversion from multidimensional coordinates to a row-major linear
/// index using caller-provided strides.
pub fn blosc2_multidim_to_unidim_checked(
    index: &[i64],
    strides: &[i64],
) -> Result<i64, &'static str> {
    if index.len() != strides.len() {
        return Err("Index rank does not match strides rank");
    }
    index
        .iter()
        .zip(strides)
        .try_fold(0i64, |acc, (&idx, &stride)| {
            if idx < 0 || stride < 0 {
                return Err("Invalid index or stride");
            }
            idx.checked_mul(stride)
                .and_then(|value| acc.checked_add(value))
                .ok_or("Index overflow")
        })
}

/// Remove a directory and its direct file entries, returning a C-style status code.
pub fn blosc2_remove_dir(path: impl AsRef<std::path::Path>) -> i32 {
    let normalized = normalized_path(path.as_ref());
    let path = normalized.as_ref();
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return BLOSC2_ERROR_FAILURE;
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return BLOSC2_ERROR_NOT_FOUND;
        }
        Err(_) => return BLOSC2_ERROR_FAILURE,
    }
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(_) => return BLOSC2_ERROR_FAILURE,
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => return BLOSC2_ERROR_FAILURE,
        };
        let entry_path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => return BLOSC2_ERROR_FAILURE,
        };
        if (file_type.is_dir() && !file_type.is_symlink())
            || std::fs::remove_file(&entry_path).is_err()
        {
            return BLOSC2_ERROR_FAILURE;
        }
    }
    match std::fs::remove_dir(path) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(_) => BLOSC2_ERROR_FAILURE,
    }
}

/// Remove a path, accepting files and directories.
pub fn blosc2_remove_urlpath(path: impl AsRef<std::path::Path>) -> i32 {
    let normalized = normalized_path(path.as_ref());
    let path = normalized.as_ref();
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return BLOSC2_ERROR_SUCCESS;
        }
        Err(_) => return BLOSC2_ERROR_FAILURE,
    };
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        return blosc2_remove_dir(path);
    }
    match std::fs::remove_file(path) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(_) => BLOSC2_ERROR_FILE_REMOVE,
    }
}

/// Rename a path, returning a C-style status code.
pub fn blosc2_rename_urlpath(
    old_urlpath: impl AsRef<std::path::Path>,
    new_urlpath: impl AsRef<std::path::Path>,
) -> i32 {
    let old_urlpath = normalized_path(old_urlpath.as_ref());
    let new_urlpath = normalized_path(new_urlpath.as_ref());
    match std::fs::rename(old_urlpath.as_ref(), new_urlpath.as_ref()) {
        Ok(()) => BLOSC2_ERROR_SUCCESS,
        Err(_) => BLOSC2_ERROR_FAILURE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_strings_and_lifecycle_noops() {
        blosc2_init();
        blosc2_free_resources();
        blosc2_destroy();
        assert_eq!(blosc2_error_string(BLOSC2_ERROR_SUCCESS), "Unknown error");
        assert_eq!(
            blosc2_error_string(BLOSC2_ERROR_CODEC_PARAM),
            "Invalid parameter supplied to codec"
        );
        assert_eq!(blosc2_error_string(BLOSC2_ERROR_TUNER), "Tuner failure");
        assert_eq!(blosc2_error_string(12345), "Unknown error");
    }

    #[test]
    fn test_multidim_index_helpers_match_c_layout() {
        let shape = [3, 4, 5];
        let strides = [20, 5, 1];
        assert_eq!(blosc2_unidim_to_multidim(&shape, 47), vec![2, 1, 2]);
        assert_eq!(
            blosc2_unidim_to_multidim_checked(&shape, 47).unwrap(),
            vec![2, 1, 2]
        );
        assert_eq!(blosc2_multidim_to_unidim(&[2, 1, 2], &strides), 47);
        assert_eq!(
            blosc2_multidim_to_unidim_checked(&[2, 1, 2], &strides).unwrap(),
            47
        );
        assert_eq!(blosc2_unidim_to_multidim(&[], 0), Vec::<i64>::new());
        assert!(blosc2_unidim_to_multidim_checked(&[10, 0], 0).is_err());
        assert!(blosc2_unidim_to_multidim_checked(&[i64::MAX, 2], 0).is_err());
        assert!(blosc2_unidim_to_multidim_checked(&[3, 4], 12).is_err());
        assert!(blosc2_multidim_to_unidim_checked(&[1, 2], &[4]).is_err());
        assert!(blosc2_multidim_to_unidim_checked(&[-1], &[1]).is_err());
        assert!(blosc2_multidim_to_unidim_checked(&[i64::MAX], &[2]).is_err());
        assert_eq!(normalize_urlpath("file:///frame.b2frame"), "frame.b2frame");
        assert_eq!(
            normalize_urlpath("file:////tmp/frame.b2frame"),
            "/tmp/frame.b2frame"
        );
        assert_eq!(normalize_urlpath("plain.b2frame"), "plain.b2frame");
    }

    #[test]
    fn test_filesystem_helpers_return_c_style_codes() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            blosc2_remove_urlpath(dir.path().join("missing")),
            BLOSC2_ERROR_SUCCESS
        );
        assert_eq!(
            blosc2_remove_dir(dir.path().join("missing-dir")),
            BLOSC2_ERROR_NOT_FOUND
        );
        let file = dir.path().join("file.bin");
        std::fs::write(&file, b"payload").unwrap();
        let renamed = dir.path().join("renamed.bin");
        assert_eq!(blosc2_rename_urlpath(&file, &renamed), BLOSC2_ERROR_SUCCESS);
        assert_eq!(blosc2_remove_urlpath(&renamed), BLOSC2_ERROR_SUCCESS);

        let url_file = dir.path().join("url-file");
        std::fs::write(&url_file, b"x").unwrap();
        let prefixed = format!("file:///{}", url_file.display());
        assert_eq!(blosc2_remove_urlpath(prefixed), BLOSC2_ERROR_SUCCESS);
        assert!(!url_file.exists());

        let old_url_file = dir.path().join("url-old");
        let new_url_file = dir.path().join("url-new");
        std::fs::write(&old_url_file, b"x").unwrap();
        let old_prefixed = format!("file:///{}", old_url_file.display());
        let new_prefixed = format!("file:///{}", new_url_file.display());
        assert_eq!(
            blosc2_rename_urlpath(old_prefixed, new_prefixed),
            BLOSC2_ERROR_SUCCESS
        );
        assert!(!old_url_file.exists());
        assert!(new_url_file.exists());

        let nested = dir.path().join("nested");
        std::fs::create_dir(&nested).unwrap();
        std::fs::write(nested.join("child"), b"payload").unwrap();
        let nested_prefixed = format!("file:///{}", nested.display());
        assert_eq!(blosc2_remove_dir(nested_prefixed), BLOSC2_ERROR_SUCCESS);
        assert!(!nested.exists());

        let nested_parent = dir.path().join("nested-parent");
        let nested_child = nested_parent.join("child-dir");
        std::fs::create_dir(&nested_parent).unwrap();
        std::fs::create_dir(&nested_child).unwrap();
        assert_eq!(blosc2_remove_dir(&nested_parent), BLOSC2_ERROR_FAILURE);
        assert!(nested_child.is_dir());
        assert_eq!(blosc2_remove_urlpath(&nested_parent), BLOSC2_ERROR_FAILURE);
        std::fs::remove_dir(&nested_child).unwrap();
        assert_eq!(blosc2_remove_urlpath(&nested_parent), BLOSC2_ERROR_SUCCESS);
    }

    #[cfg(unix)]
    #[test]
    fn test_remove_urlpath_unlinks_directory_symlink_without_touching_target() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        std::fs::create_dir(&real).unwrap();
        std::fs::write(real.join("child"), b"payload").unwrap();
        let link = dir.path().join("link");
        symlink(&real, &link).unwrap();

        assert_eq!(blosc2_remove_urlpath(&link), BLOSC2_ERROR_SUCCESS);
        assert!(!link.exists());
        assert_eq!(std::fs::read(real.join("child")).unwrap(), b"payload");
    }

    #[cfg(unix)]
    #[test]
    fn test_remove_urlpath_reports_access_errors() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let restricted = dir.path().join("restricted");
        std::fs::create_dir(&restricted).unwrap();
        let child = restricted.join("child");
        std::fs::write(&child, b"payload").unwrap();

        let original_permissions = std::fs::metadata(&restricted).unwrap().permissions();
        std::fs::set_permissions(&restricted, std::fs::Permissions::from_mode(0o0)).unwrap();
        let rc = blosc2_remove_urlpath(&child);
        std::fs::set_permissions(&restricted, original_permissions).unwrap();

        assert_eq!(rc, BLOSC2_ERROR_FAILURE);
        assert_eq!(std::fs::read(&child).unwrap(), b"payload");
    }
}
